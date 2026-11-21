#!/usr/bin/env python3
"""Kill one worker under competing single and batch traffic and audit every ID."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import uuid

from recovery import Experiment, ROOT, batch_source, require, wait_until

COUNT = 400
BATCH_TOPIC, BATCH_PARTITIONS = batch_source()
PARTITIONS = {BATCH_TOPIC: BATCH_PARTITIONS, "notifications.normal.single": 3}
GROUPS = [topic.replace("notifications.", "delivery.", 1) for topic in PARTITIONS]
GROUP_ARGS = [arg for group in GROUPS for arg in ("--group", group)]


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def sql(exp, query):
    return exp.compose("exec", "-T", "postgres", "psql", "-XAtq", "-v", "ON_ERROR_STOP=1",
                       "-U", "notify", "-d", "notify", "-c", query, capture=True).stdout.strip()


def assignments(exp):
    args = ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "kafka:9092", "--describe", "--members", "--verbose",
            *GROUP_ARGS]
    output = exp.compose(*args, capture=True).stdout
    owners = {topic: {} for topic in PARTITIONS}
    for row in output.splitlines():
        fields = row.split()
        if len(fields) < 6 or fields[0] not in GROUPS:
            continue
        topic = fields[0].replace("delivery.", "notifications.", 1)
        match = re.fullmatch(re.escape(topic) + r":([0-9,]+)", fields[6]) if len(fields) >= 7 else None
        if match:
            for partition in match[1].split(","):
                owners[topic][partition.strip()] = fields[2].lstrip("/")
    return owners


def membership(exp, ips):
    def stable():
        owners = assignments(exp)
        for topic, n in PARTITIONS.items():
            mapping = owners[topic]
            if set(mapping) != {str(i) for i in range(n)}:
                return None
            assigned = set(mapping.values())
            if not assigned <= ips or len(assigned) != min(len(ips), n):
                return None
        return owners
    return wait_until(stable, "partition ownership " + str(sorted(ips)), seconds=180)


def worker_ips(exp):
    ids = exp.compose("ps", "-q", "--status", "running", "worker", capture=True).stdout.splitlines()
    return {docker("inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", c): c for c in ids}


def state_counts(exp):
    return json.loads(sql(exp, """SELECT coalesce(json_object_agg(k,n),'{}'::json)
        FROM (SELECT ingress||'/'||status k,count(*) n FROM notifications GROUP BY 1) s"""))


def seed(exp):
    request_id = str(uuid.uuid4())
    notice = exp.item("one same-priority notification")
    def batch():
        response = exp.request("POST", exp.api + "/v1/batches", {"notifications": [notice] * COUNT},
                               202, {"Idempotency-Key": request_id})
        require(len(response["items"]) == COUNT, "partial batch response")
        return {entry["id"]: "batch" for entry in response["items"]}
    with ThreadPoolExecutor(max_workers=12) as pool:
        batch_result = pool.submit(batch)
        singles = list(pool.map(lambda _: exp.create(notice["content"]), range(COUNT)))
    cohort = {**batch_result.result(), **dict.fromkeys(singles, "single")}
    require(len(cohort) == 2 * COUNT, "admission IDs missing or repeated")
    return cohort


def posts(exp, ids):
    def lookup(item):
        return exp.request("GET", exp.provider + "/stats/" + item)["attempts"]
    with ThreadPoolExecutor(max_workers=12) as pool:
        return dict(zip(ids, pool.map(lookup, ids)))


def ledger_rows(exp, killed_at):
    query = """SELECT coalesce(json_agg(row_to_json(v) ORDER BY id),'[]') FROM (
        SELECT n.id, n.ingress, n.status, n.message_id,
          string_agg(e.status,'>' ORDER BY e.seq) AS transitions,
          (array_agg(e.status ORDER BY e.seq DESC) FILTER(WHERE e.changed_at <= TIMESTAMPTZ '%s'))[1] AS at_kill,
          round(extract(epoch FROM (n.updated_at-n.first_attempt_at))::numeric,3) AS seconds_after_claim
        FROM notifications n JOIN status_events e ON e.notification_id=n.id GROUP BY n.id) v""" % killed_at
    return json.loads(sql(exp, query))


def offset_snapshot(exp):
    output = exp.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                         "--bootstrap-server", "kafka:9092", "--describe", "--offsets",
                         *GROUP_ARGS, capture=True).stdout
    snapshot = {}
    for row in output.splitlines():
        fields = row.split()
        if len(fields) >= 6 and fields[1] in PARTITIONS and all(value.isdigit() for value in fields[2:6]):
            partition, committed, end, lag = map(int, fields[2:6])
            snapshot.setdefault(fields[1], {})[partition] = {"committed": committed, "end": end, "lag": lag}
    return snapshot


def verify_broker_drain(exp):
    def drained():
        snapshot = offset_snapshot(exp)
        for topic in PARTITIONS:
            partitions = snapshot.get(topic, {})
            if set(partitions) != set(range(PARTITIONS[topic])) or sum(p["end"] for p in partitions.values()) != COUNT:
                return None
            if any(p["lag"] != 0 for p in partitions.values()):
                return None
        return snapshot
    observed = wait_until(drained, "both Kafka groups commit all workload offsets", seconds=150)
    before = exp.stats()["attempts"]
    time.sleep(7)  # Exceeds this fixture's 1s response delay and the worker's 5s HTTP timeout.
    after = exp.stats()["attempts"]
    require(before == after, "additional provider POST after broker drain")
    return {"offsets": observed, "guardSeconds": 7, "postsAtDrain": before, "postsAfterGuard": after}


def run(exp, out_dir):
    exp.compose("up", "-d", "--build", "--scale", "worker=3", "--wait")
    workers = worker_ips(exp)
    require(len(workers) == 3, "expected three live workers")
    owners = membership(exp, set(workers))
    victim_ip = owners[BATCH_TOPIC]["0"]
    victim = workers[victim_ip]
    image = docker("inspect", "--format", "{{.Image}}", victim)
    print("Original assignments established. Admitting both cohorts.", flush=True)
    cohort = seed(exp)
    # A successful conditional cancellation before a send claim must survive
    # replay after the owning worker dies. The cancelled record stays in Kafka.
    cancelled = None
    if BATCH_PARTITIONS == 1:
        pending = sql(exp, "SELECT id FROM notifications WHERE ingress='batch' AND status='pending' ORDER BY id DESC").splitlines()
        for item in pending:
            try:
                exp.request("POST", exp.api + "/v1/notifications/" + item + "/cancel", expected=204)
                cancelled = item
                break
            except urllib.error.HTTPError as error:
                if error.code != 409:
                    raise
        require(cancelled, "no pending batch notification could be cancelled")
    def active_lanes():
        counts = state_counts(exp)
        return counts if all(counts.get(lane + "/sending", 0) for lane in ("batch", "single")) else None
    before = wait_until(active_lanes, "both lanes sending")
    require(min(before.get(lane + "/pending", 0) for lane in ("batch", "single")) >= 50, before)
    sending = sql(exp, "SELECT id FROM notifications WHERE status='sending'").splitlines()
    pre_posts = posts(exp, sending)
    docker("kill", "--signal", "KILL", victim)
    killed_at = docker("inspect", "--format", "{{.State.FinishedAt}}", victim)
    require(re.fullmatch(r"[\dT:Z.\-]+", killed_at) and docker("inspect", "--format", "{{.State.ExitCode}}", victim) == "137",
            "expected worker to exit on SIGKILL")
    remaining = set(workers) - {victim_ip}
    require(set(worker_ips(exp)) == remaining, "unexpected live workers after kill")
    print(f"One worker killed at {killed_at}; {before}. Waiting for Kafka reassignment.", flush=True)
    replacement = membership(exp, remaining)
    reassigned_at = datetime.now(timezone.utc).isoformat()
    print(f"Kafka reassigned both topics. Waiting for stale-send recovery. States: {state_counts(exp)}", flush=True)
    broker_drain = verify_broker_drain(exp)
    def drained():
        return not any(k.split("/")[1] in ("scheduled", "pending", "sending", "retry_wait")
                       for k in state_counts(exp))
    wait_until(drained, "cohort reaches terminal status", seconds=240)
    rows = ledger_rows(exp, killed_at)
    after_posts = posts(exp, list(cohort))
    require({r["id"]: r["ingress"] for r in rows} == cohort, "DB and admission receipts differ")
    provider = exp.stats()
    for r in rows:
        r["provider_posts"] = after_posts[r["id"]]
        r["provider_posts_before_kill_sample"] = pre_posts.get(r["id"])
    with (out_dir / "ids.csv").open("w", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=rows[0], lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    unknown = [r for r in rows if r["status"] == "unknown"]
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "image": image, "providerLatency": exp.env["PROVIDER_LATENCY"], "workerConcurrency": 24,
        "batchSharePercent": 20, "providerIdempotent": False, "singleAdmitted": COUNT, "batchAdmitted": COUNT,
        "batchTopic": BATCH_TOPIC, "cancelledId": cancelled, "killedWorkerIP": victim_ip, "killedAt": killed_at, "reassignmentObservedAt": reassigned_at,
        "assignmentBefore": owners, "assignmentAfter": replacement, "preKillSnapshot": before,
        "brokerDrain": broker_drain,
        "atKill": dict(Counter(r["ingress"]+"/"+str(r["at_kill"]) for r in rows)),
        "terminal": state_counts(exp), "provider": provider,
        "perIdPostCounts": dict(Counter(after_posts.values())),
        "statusByPostCount": dict(Counter(r["status"] + "/" + str(r["provider_posts"]) for r in rows)),
        "zeroPostIds": [i for i, n in after_posts.items() if n == 0],
        "multiplePostIds": [i for i, n in after_posts.items() if n > 1],
        "unknownIds": [r["id"] for r in unknown],
        "unknownObservedAtProviderBeforeKill": sum((r["provider_posts_before_kill_sample"] or 0) > 0 for r in unknown),
    }
    (out_dir / "result.json").write_text(json.dumps(report, indent=2, sort_keys=True)+"\n")
    require(len(rows) == COUNT*2 and provider["attempts"] == sum(after_posts.values()), "provider reconciliation failed")
    require(provider["accepted"] == provider["attempts"] and provider["failures"] == 0, "unexpected fixture rejection")
    require(provider["channels"]["sms"]["peakPerSecond"] <= 100, "provider exceeded rolling SMS quota")
    for r in rows:
        if r["id"] == cancelled:
            require(r["transitions"] == "pending>cancelled" and r["provider_posts"] == 0, "cancelled ID reached provider")
        else:
            require(r["status"] in ("accepted", "unknown") and r["transitions"] == "pending>sending>"+r["status"],
                    "unexpected terminal state history")
    require(min(report["atKill"].get(lane + "/pending", 0) for lane in ("batch","single")) >= 50, "lost backlog before kill")
    require(unknown and all(r["at_kill"] == "sending" and not r["message_id"] for r in unknown), "unexpected unknown state history")
    require(not report["multiplePostIds"], "duplicate POST detected; see result.json")
    require(all(r["provider_posts"] == 1 and r["message_id"] for r in rows if r["status"] == "accepted"),
            "accepted ID without exactly one provider POST and receipt")
    print("ID audit complete:", report["terminal"], report["perIdPostCounts"], flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=ROOT / ".local" / "worker-rebalance")
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    exp = Experiment()
    exp.env.update(APP_IMAGE="notification-system:" + exp.project, WORKER_CONCURRENCY="24",
                   PROVIDER_LATENCY="1s", BATCH_SHARE_PERCENT="20")
    try:
        run(exp, args.out_dir)
    finally:
        exp.compose("down", "--volumes", "--remove-orphans")
        subprocess.run(["docker", "image", "rm", exp.env["APP_IMAGE"]], check=False)
