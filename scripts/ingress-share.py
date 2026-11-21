#!/usr/bin/env python3
"""Compare same-priority single and batch SMS on the local Compose stack."""
import argparse
from bisect import bisect_right
from collections import Counter
import concurrent.futures
import datetime
import json
import math
from pathlib import Path
import subprocess
import urllib.request
import time
import uuid

from recovery import batch_source

ROOT = Path(__file__).resolve().parents[1]
# Number of campaign items, standalone requests, offered standalone requests per second.
PROFILES = {"bulk": (1600, 0, 0), "single": (0, 800, 140),
            "sparse": (1600, 24, 2), "saturated": (1600, 1600, 140),
            "campaign10k": (10000, 8, 2)}
LANES = ("single", "batch")
MIN_BACKLOG = 72  # Total worker concurrency in the three-replica test.


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def sql(query):
    return command("docker", "compose", "exec", "-T", "postgres", "psql", "-U", "notify", "-d", "notify",
                   "-XAtq", "-v", "ON_ERROR_STOP=1", "-c", query)


def wait_for(condition, seconds=180):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.15)
    raise TimeoutError("workload did not reach its expected state")


def http(url, body=None):
    payload = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json", "Idempotency-Key": str(uuid.uuid4())}
    with urllib.request.urlopen(urllib.request.Request(url, data=payload, headers=headers), timeout=15) as response:
        return json.load(response)


def item(text):
    return {"channel": "sms", "priority": "low", "recipient": "+12025550123", "content": text}


def worker_info(share):
    command("scripts/require-local-provider.sh")
    workers = command("docker", "compose", "ps", "--status", "running", "-q", "worker").splitlines()
    assert len(workers) == 3, "expected three active worker replicas"
    images = set()
    for worker in workers:
        for setting in (f"BATCH_SHARE_PERCENT={share}", "WORKER_CONCURRENCY=24"):
            template = '{{range .Config.Env}}{{if eq . "' + setting + '"}}yes{{end}}{{end}}'
            assert command("docker", "inspect", "--format", template, worker) == "yes", setting
        images.add(command("docker", "inspect", "--format", "{{.Image}}", worker))
    assert len(images) == 1
    return images.pop()


def post_batch(api, size, content):
    result = http(api + "/v1/batches", {"notifications": [item(content)] * size})
    assert len(result["items"]) == size and all(row.get("id") for row in result["items"])
    return result["items"]


def post_singles(api, size, rate, content):
    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as pool:
        pending = []
        for index in range(size):
            time.sleep(max(0, start + index / rate - time.monotonic()))
            pending.append(pool.submit(http, api + "/v1/notifications", item(content)))
        assert all(future.result().get("id") for future in pending)


def load_rows(marker, control_id):
    # marker is a local UUID. Include actual outbox route so a mislabeled API
    # or a one-item batch accidentally joining the single lane fails the test.
    control = f"'{uuid.UUID(control_id)}'" if control_id else "NULL::uuid"
    return json.loads(sql(f"""
        SELECT json_agg(json_build_object(
            'lane', CASE WHEN n.id={control} THEN 'control' ELSE split_part(n.content, ' ', 2) END,
            'ingress', n.ingress, 'priority', n.priority, 'route', o.aggregatetype,
            'created', extract(epoch from n.created_at), 'claim', extract(epoch from n.first_attempt_at),
            'accepted', extract(epoch from n.updated_at), 'status', n.status, 'attempt', n.attempt))
        FROM notifications n JOIN outbox o ON o.aggregateid=n.id
        WHERE n.content IN ('{marker} single','{marker} batch')
    """))


def percentile(values, p):
    ordered = sorted(values)
    return round(ordered[math.ceil(len(ordered) * p) - 1], 2)


def summarize_lane(rows):
    if not rows:
        return None
    queue = [1000 * (r["claim"] - r["created"]) for r in rows]
    wall = [1000 * (r["accepted"] - r["created"]) for r in rows]
    drain = max(r["accepted"] for r in rows) - min(r["created"] for r in rows)
    arrival = max(r["created"] for r in rows) - min(r["created"] for r in rows)
    return {
        "arrivalSpanSeconds": round(arrival, 3),
        "observedSingleIngressPerSecond": round((len(rows) - 1) / arrival, 2) if rows[0]["lane"] == "single" and arrival > 0 else None,
        "count": len(rows), "queueP50Ms": percentile(queue, 0.50), "queueP95Ms": percentile(queue, 0.95),
        "acceptedP95Ms": percentile(wall, 0.95), "drainSeconds": round(drain, 3),
        "acceptedPerSecond": round(len(rows) / drain, 2),
    }


def contention(rows):
    # Aggregate complete one-second bins from the first creation. Include a bin
    # only if the unclaimed population of EACH lane stays above the threshold
    # at every timestamp within the bin, including both boundaries.
    origin = min(row["created"] for row in rows)
    end = max(row["claim"] for row in rows)
    duration = int(end - origin)
    bins = [[] for _ in range(duration)]
    for row in rows:
        for field, delta in (("created", 1), ("claim", -1)):
            second = int(row[field] - origin)
            if second < duration:
                bins[second].append((row[field], delta, row["lane"]))
    queued = Counter()
    eligible = []
    for second, events in enumerate(bins):
        minimum = {lane: queued[lane] for lane in LANES}
        claims = Counter()
        for _, delta, lane in sorted(events):
            queued[lane] += delta
            minimum[lane] = min(minimum[lane], queued[lane])
            if delta < 0:
                claims[lane] += 1
        if min(minimum.values()) >= MIN_BACKLOG:
            eligible.append({"second": second, "single": claims["single"], "batch": claims["batch"],
                             "minSingleQueued": minimum["single"], "minBatchQueued": minimum["batch"]})
    if not eligible:
        return None
    batch = sum(b["batch"] for b in eligible)
    single = sum(b["single"] for b in eligible)
    return {
        "seconds": len(eligible), "batchPercent": round(100 * batch / (batch + single), 2),
        "batchPerSecond": round(batch / len(eligible), 2), "singlePerSecond": round(single / len(eligible), 2),
        "bins": eligible,
    }


def campaign_probes(rows):
    campaign_accepted = sorted(r["accepted"] for r in rows if r["lane"] == "batch")
    probes = sorted((r for r in rows if r["lane"] != "batch"), key=lambda row: row["created"])
    return [{
        "lane": row["lane"], "admittedAfterFirstProbeMs": round(1000 * (row["created"] - probes[0]["created"]), 2),
        "queueMs": round(1000 * (row["claim"] - row["created"]), 2),
        "acceptedMs": round(1000 * (row["accepted"] - row["created"]), 2),
        "campaignOutstandingAtCreate": len(campaign_accepted) - bisect_right(campaign_accepted, row["created"]),
        "campaignOutstandingAtAcceptance": len(campaign_accepted) - bisect_right(campaign_accepted, row["accepted"]),
    } for row in probes]


def run(name, api, provider):
    n_bulk, n_single, rate = PROFILES[name]
    marker = str(uuid.uuid4())
    before = http(provider + "/stats")
    remaining = n_bulk
    while remaining:
        count = min(1000, remaining)
        post_batch(api, count, marker + " batch")
        remaining -= count
    control_id = None
    if n_single:
        start_after = 500 if name == "campaign10k" else 100
        if n_bulk:
            wait_for(lambda: http(provider + "/stats")["attempts"] - before["attempts"] >= start_after)
        if name == "campaign10k":
            control_id = post_batch(api, 1, marker + " single")[0]["id"]
        post_singles(api, n_single, rate, marker + " single")
    expected = n_bulk + n_single + int(control_id is not None)
    wait_for(lambda: http(provider + "/stats")["attempts"] - before["attempts"] >= expected)
    wait_for(lambda: int(sql(f"""SELECT count(*) FROM notifications WHERE status='accepted' AND
                        content IN ('{marker} single','{marker} batch')""")) == expected)
    after = http(provider + "/stats")
    delta = {k: after[k] - before[k] for k in ("attempts", "accepted", "failures", "responseLost", "deduplicated")}
    assert delta["attempts"] == delta["accepted"] == expected
    assert delta["failures"] == delta["responseLost"] == delta["deduplicated"] == 0
    rows = load_rows(marker, control_id)
    counts = Counter(r["lane"] for r in rows)
    assert len(rows) == expected and counts["batch"] == n_bulk and counts["single"] == n_single
    ingress = {"single": "single", "batch": "batch", "control": "batch"}
    routes = {"single": "notifications.low.single", "batch": batch_source("low")[0]}
    assert all(r["status"] == "accepted" and r["attempt"] == 1 and r["priority"] == "low"
               and ingress[r["lane"]] == r["ingress"] and r["route"] == routes[r["ingress"]] for r in rows)
    result = {lane: summarize_lane([row for row in rows if row["lane"] == lane]) for lane in LANES}
    result.update({"marker": marker, "offeredSinglePerSecond": rate, "providerDelta": delta,
                   "outboxTopics": dict(Counter(r["route"] for r in rows)),
                   "rollingSmsPeakSinceProviderStart": after["channels"]["sms"]["peakPerSecond"],
                   "contended": contention([r for r in rows if r["lane"] in LANES])})
    if control_id:
        result["probes"] = campaign_probes(rows)
    return result


def verify_contention(data, share):
    interval = data["contended"]
    assert interval and interval["seconds"] >= 5, "need five fully backlogged seconds"
    assert interval["batchPerSecond"] > 0, "batch made no progress"
    # A moderate configured share leaves margin for variation across Kafka
    # readers, including the first and last seconds of the contended interval.
    if share <= 20:
        assert interval["singlePerSecond"] > interval["batchPerSecond"], "standalone lost its priority under contention"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--share", type=int, required=True, choices=range(1, 50), metavar="1..49")
    parser.add_argument("--profiles", nargs="+", choices=PROFILES, default=["bulk", "single", "sparse", "saturated"])
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    api = "http://" + command("docker", "compose", "port", "api", "8080")
    provider = "http://" + command("docker", "compose", "port", "provider", "8081")
    image = worker_info(args.share)
    assert sql("SELECT count(*) FROM notifications WHERE status IN ('pending','sending','retry_wait')") == "0"
    # Ensure both low consumers have seen an event after a config restart.
    warm = [http(api + "/v1/notifications", item("Warm single"))["id"],
            http(api + "/v1/batches", {"notifications": [item("Warm batch")]})["items"][0]["id"]]
    wait_for(lambda: all(http(api + "/v1/notifications/" + key)["status"] == "accepted" for key in warm))
    time.sleep(1.1)
    result = {"utcStart": datetime.datetime.now(datetime.timezone.utc).isoformat(), "image": image,
              "share": args.share, "workerReplicas": 3, "concurrencyEach": 24,
              "minimumUnclaimedEachLane": MIN_BACKLOG, "profiles": {}}
    for name in args.profiles:
        print(f"Starting {name} at {args.share}% batch", flush=True)
        data = run(name, api, provider)
        result["profiles"][name] = data
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result, indent=2) + "\n")
        brief = {k: v for k, v in data.items() if k != "contended"}
        brief["contended"] = None if not data["contended"] else {
            k: v for k, v in data["contended"].items() if k != "bins"}
        print(json.dumps(brief), flush=True)
        if name == "saturated":
            verify_contention(data, args.share)


if __name__ == "__main__":
    main()
