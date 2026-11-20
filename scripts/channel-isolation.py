#!/usr/bin/env python3
"""Compare sparse push latency on independent channel quotas during an SMS backlog."""
import argparse
from bisect import bisect_left
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
import io
import json
import math
from pathlib import Path
import subprocess
import time
import uuid

from recovery import Experiment, ROOT, require, wait_until

SMS_COUNT = 4000
PAIRS = 24
BATCH_TOPIC = "notifications.normal"
SINGLE_TOPIC = BATCH_TOPIC + ".single"
TOPICS = [BATCH_TOPIC, SINGLE_TOPIC]


def kafka(exp, tool, *args):
    return exp.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-" + tool + ".sh",
                       "--bootstrap-server", "kafka:9092", *args, capture=True).stdout


def ends(exp):
    result = {}
    for line in kafka(exp, "get-offsets", "--topic", r"notifications\.normal(\.single)?").splitlines():
        topic, partition, end = line.split(":")
        result[(topic, int(partition))] = int(end)
    require(len(result) == 6, "expected two three-partition topics")
    return result


def coordinates(exp, prior):
    current = ends(exp)

    def read(key):
        topic, partition = key
        start, end = prior[key], current[key]
        if end <= start:
            return {}
        data = kafka(exp, "console-consumer", "--topic", topic, "--partition", str(partition),
                     "--offset", str(start), "--max-messages", str(end-start), "--timeout-ms", "20000",
                     "--property", "print.offset=true")
        result = {}
        for line in data.splitlines():
            offset, encoded = line.split("\t", 1)
            event = json.loads(encoded)
            if isinstance(event, str):
                event = json.loads(event)
            result[event["id"]] = (topic, partition, int(offset.split(":")[1]))
        require(len(result) == end-start, "unexpected repeated or absent source event")
        return result

    with ThreadPoolExecutor(max_workers=2) as pool:
        return {item: coord for records in pool.map(read, current) for item, coord in records.items()}


def final_offsets(exp):
    def read():
        result = {}
        log = kafka(exp, "consumer-groups", "--describe", "--offsets",
                    "--group", "delivery.normal", "--group", "delivery.normal.single")
        for line in log.splitlines():
            fields = line.split()
            if len(fields) >= 6 and fields[1] in TOPICS and all(v.isdigit() for v in fields[2:6]):
                result[f"{fields[1]}/{fields[2]}"] = {"committed": int(fields[3]), "end": int(fields[4])}
        if len(result) == 6 and all(p["committed"] == p["end"] for p in result.values()):
            return result
        return None
    return wait_until(read, "six measured source partitions committed to log end")


def create(exp, content, channel, lane, count=1):
    payload = exp.item(content)
    payload.update(channel=channel, recipient="device-token-synthetic" if channel == "push" else payload["recipient"])
    headers = {"Idempotency-Key": str(uuid.uuid4())}
    if lane == "single":
        receipt = exp.request("POST", exp.api+"/v1/notifications", payload, 202, headers)
        return [receipt["id"]]
    receipt = exp.request("POST", exp.api+"/v1/batches", {"notifications": [payload]*count}, 202, headers)
    ids = [entry["id"] for entry in receipt["items"]]
    require(len(ids) == count, "partial admission")
    return ids


def sql_rows(exp, token):
    require(token.isalnum(), "unexpected cohort token")
    query = f"""COPY (SELECT id,channel,ingress,status, extract(epoch from created_at),
        extract(epoch from first_attempt_at), extract(epoch from updated_at)
        FROM notifications WHERE content='{token}') TO STDOUT WITH CSV"""
    text = exp.compose("exec", "-T", "postgres", "psql", "-Xq", "-v", "ON_ERROR_STOP=1", "-U", "notify",
                       "-d", "notify", "-c", query, capture=True).stdout
    return {row[0]: row[1:] for row in csv.reader(io.StringIO(text))}


def per_id_posts(exp, ids):
    def lookup(item):
        return exp.request("GET", exp.provider+"/stats/"+item)["attempts"]
    with ThreadPoolExecutor(max_workers=12) as pool:
        return dict(zip(ids, pool.map(lookup, ids)))


def percentiles(values):
    ordered = sorted(values)
    return {"n": len(ordered), **{f"p{p}Ms": round(ordered[math.ceil(len(ordered)*p/100)-1], 3)
                                  for p in (50, 95, 99)}}


def backlog_by_probe(probes, campaign, rows, coords):
    partitions = defaultdict(list)
    for item in campaign:
        topic, partition, offset = coords[item]
        claimed = float(rows[item][4])
        partitions[topic, partition].append((offset, claimed))
    for events in partitions.values():
        events.sort()
    for probe in probes:
        topic, partition, offset = coords[probe["id"]]
        events = partitions[topic, partition]
        ahead = bisect_left(events, (offset,))
        created = float(rows[probe["id"]][3])
        probe["precedingCampaignSMS"] = ahead
        probe["precedingSMSUnclaimedAtPushCreation"] = sum(claimed > created for _, claimed in events[:ahead])


def measure(exp, token, loaded):
    prior = ends(exp)
    exp.request("DELETE", exp.provider+"/stats", expected=204)
    probes, campaign, cancelled, samples = [], [], [], []
    with ThreadPoolExecutor(max_workers=6) as pool:
        if loaded:
            submissions = [pool.submit(create, exp, token, "sms", "batch", 1000) for _ in range(SMS_COUNT//1000)]
            campaign = [item for request in submissions for item in request.result()]
            wait_until(lambda: exp.stats()["channels"].get("sms", {}).get("attempts", 0) >= 50, "SMS backlog active")
        start = time.monotonic()
        for index in range(PAIRS):
            due = start + index * (1.0 if loaded else 0.4)
            time.sleep(max(0, due-time.monotonic()))
            stats = exp.stats()["channels"].get("sms", {}).get("attempts", 0)
            samples.append({"s": round(time.monotonic()-start, 4), "smsPosts": stats})
            if loaded:
                require(SMS_COUNT-stats > 300, "campaign drained before last push")
            # Submission order alternates while the two lane-matched probes run concurrently.
            lanes = ("single", "batch") if index % 2 else ("batch", "single")
            requests = {lane: pool.submit(create, exp, token, "push", lane) for lane in lanes}
            for lane, request in requests.items():
                probes.append({"lane": lane, "pair": index+1, "id": request.result()[0]})
            if loaded and index in (1, 8, 15, 22):
                for channel in ("sms", "push"):
                    item = create(exp, token, channel, "batch")[0]
                    exp.request("POST", exp.api+"/v1/notifications/"+item+"/cancel", expected=204)
                    cancelled.append(item)
    ids = [p["id"] for p in probes] + campaign + cancelled
    require(len(ids) == len(set(ids)), "duplicate API IDs")
    wait_until(lambda: exp.stats()["attempts"] >= len(ids)-len(cancelled), "provider drain", seconds=150)
    wait_until(lambda: all(r[2] in ("accepted", "cancelled") for r in sql_rows(exp, token).values()), "DB drain")
    rows = sql_rows(exp, token)
    require(set(rows) == set(ids), "API and PostgreSQL IDs differ")
    actual = per_id_posts(exp, ids)
    skipped = set(cancelled)
    require(all(rows[i][2] == ("cancelled" if i in skipped else "accepted") and actual[i] == (0 if i in skipped else 1)
                for i in ids), "per-ID provider receipt or terminal status mismatch")
    coords = coordinates(exp, prior)
    require(set(coords) == set(ids), "topic records do not match API admissions")
    backlog_by_probe(probes, campaign, rows, coords)
    grouped = defaultdict(list)
    for probe in probes:
        item = probe["id"]
        channel, ingress, state, created, claimed, finished = rows[item]
        topic, partition, offset = coords[item]
        require(ingress == probe["lane"] and topic == (SINGLE_TOPIC if ingress == "single" else BATCH_TOPIC),
                "probe routed to an unexpected source topic")
        queue_ms = (float(claimed)-float(created))*1000
        provider_ms = (float(finished)-float(claimed))*1000
        total_ms = queue_ms + provider_ms
        grouped[ingress].append(total_ms)
        probe.update(partition=partition, offset=offset,
                     createToClaimMs=round(queue_ms, 3), claimToAcceptMs=round(provider_ms, 3), latencyMs=round(total_ms, 3))
    fixture = exp.stats()
    require(fixture["accepted"] == fixture["attempts"] == len(ids)-len(cancelled), "provider total differs")
    require(all(c["peakPerSecond"] <= 100 for c in fixture["channels"].values()), "channel quota exceeded")
    report = {"push": {k: percentiles(v) for k,v in grouped.items()}, "probes": probes, "cancelledIDs": cancelled,
              "receiptAudit": {"checked": len(ids), "statusAndPOSTs": dict(Counter(rows[i][2]+"/"+str(actual[i]) for i in ids))},
              "provider": fixture}
    if loaded:
        span = samples[-1]["s"]-samples[0]["s"]
        report["smsPostsPerSecondDuringProbes"] = round((samples[-1]["smsPosts"]-samples[0]["smsPosts"])/span, 3)
        report["smsSamples"] = samples
    return report


def trial(exp, directory, name, image):
    exp.env["APP_IMAGE"] = image
    exp.compose("up", "-d", "--no-deps", "--force-recreate", "worker", capture=True)
    digest = subprocess.check_output(["docker", "inspect", "--format", "{{.Image}}",
                                      exp.compose("ps", "-q", "worker", capture=True).stdout.strip()], text=True).strip()
    for lane in ("batch", "single"):
        item = create(exp, "warm", "push", lane)[0]
        exp.expect(item, "accepted")
    time.sleep(1.1)
    output = {"trial": name, "image": digest, "workerConcurrency": 24, "providerLatencyMs": 20, "campaignSMS": SMS_COUNT}
    for condition in ("idle", "loaded"):
        output[condition] = measure(exp, uuid.uuid4().hex, condition == "loaded")
        print(name, condition, output[condition]["push"], flush=True)
    output["sourceOffsetsAfterDrain"] = final_offsets(exp)
    directory.mkdir(parents=True, exist_ok=True)
    (directory/(name+".json")).write_text(json.dumps(output, indent=2)+"\n")


def configure(exp, image):
    exp.env.update(APP_IMAGE=image, WORKER_CONCURRENCY="24", PROVIDER_LATENCY="20ms", BATCH_SHARE_PERCENT="20")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out-dir", type=Path, default=ROOT/".local"/"channel-isolation")
    p.add_argument("--baseline-image", required=True)
    p.add_argument("--candidate-image", required=True)
    args = p.parse_args()
    exp = Experiment()
    configure(exp, args.baseline_image)
    try:
        exp.compose("up", "-d", "--no-build", "--wait")
        for label in ("a1", "b1", "b2", "a2"):
            trial(exp, args.out_dir, label, args.baseline_image if label.startswith("a") else args.candidate_image)
    finally:
        exp.compose("down", "-v", "--remove-orphans")
