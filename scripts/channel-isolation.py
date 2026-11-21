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
import re
import subprocess
import time
from urllib.error import HTTPError
import uuid

from recovery import Experiment, ROOT, require, wait_until

SMS_COUNT = 4000
PAIRS = 24
BATCH_TOPIC = "notifications.normal"
SINGLE_TOPIC = BATCH_TOPIC + ".single"
LEGACY_TOPICS = {BATCH_TOPIC: 3, SINGLE_TOPIC: 3}


def source_topics(channel_readers):
    topics = dict(LEGACY_TOPICS)
    if channel_readers:
        for channel in ("sms", "email", "push"):
            topics[f"{BATCH_TOPIC}.batch.{channel}"] = 1
    return topics


def source_topic(channel, lane, channel_readers):
    if lane == "single":
        return SINGLE_TOPIC
    return f"{BATCH_TOPIC}.batch.{channel}" if channel_readers else BATCH_TOPIC


def kafka(exp, tool, *args):
    return exp.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-" + tool + ".sh",
                       "--bootstrap-server", "kafka:9092", *args, capture=True).stdout


def ends(exp, topics):
    result = {}
    expression = "^(?:" + "|".join(re.escape(topic) for topic in topics) + ")$"
    for line in kafka(exp, "get-offsets", "--topic", expression).splitlines():
        topic, partition, end = line.split(":")
        result[(topic, int(partition))] = int(end)
    expected = {(topic, p) for topic, count in topics.items() for p in range(count)}
    require(result.keys() == expected, f"unexpected source partitions: {result.keys()}")
    return result


def coordinates(exp, prior, topics):
    current = ends(exp, topics)

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


def final_offsets(exp, topics):
    def read():
        result = {}
        groups = {topic: "delivery." + topic.removeprefix("notifications.") for topic in topics}
        args = [s for group in groups.values() for s in ("--group", group)]
        log = kafka(exp, "consumer-groups", "--describe", "--offsets",
                    *args)
        for line in log.splitlines():
            fields = line.split()
            if len(fields) < 6 or groups.get(fields[1]) != fields[0] or not fields[2].isdigit():
                continue
            if fields[4].isdigit() and (fields[3] == "-" or fields[3].isdigit()):
                result[f"{fields[1]}/{fields[2]}"] = {
                    "committed": None if fields[3] == "-" else int(fields[3]), "end": int(fields[4])}
        expected = {f"{topic}/{p}" for topic, count in topics.items() for p in range(count)}
        # An assigned partition that never held any records may have no commit.
        if result.keys() == expected and all((p["committed"] or 0) == p["end"] for p in result.values()):
            return result
        return None
    return wait_until(read, "measured source partitions committed to log end")


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


def measure(exp, token, loaded, channel_readers=False, allow_cancel_race=False):
    topics = source_topics(channel_readers)
    prior = ends(exp, topics)
    exp.request("DELETE", exp.provider+"/stats", expected=204)
    probes, campaign, cancelled, samples = [], [], [], []
    late_cancels = []
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
                    try:
                        exp.request("POST", exp.api+"/v1/notifications/"+item+"/cancel", expected=204)
                    except HTTPError as err:
                        if not allow_cancel_race or err.code != 409:
                            raise
                        late_cancels.append(item)
                    else:
                        cancelled.append(item)
    ids = [p["id"] for p in probes] + campaign + cancelled + late_cancels
    require(not loaded or len(cancelled) > 0, "no successful cancellation exercised")
    require(len(ids) == len(set(ids)), "duplicate API IDs")
    wait_until(lambda: exp.stats()["attempts"] >= len(ids)-len(cancelled), "provider drain", seconds=150)
    wait_until(lambda: all(r[2] in ("accepted", "cancelled") for r in sql_rows(exp, token).values()), "DB drain")
    rows = sql_rows(exp, token)
    require(set(rows) == set(ids), "API and PostgreSQL IDs differ")
    actual = per_id_posts(exp, ids)
    skipped = set(cancelled)
    require(all(rows[i][2] == ("cancelled" if i in skipped else "accepted") and actual[i] == (0 if i in skipped else 1)
                for i in ids), "per-ID provider receipt or terminal status mismatch")
    coords = coordinates(exp, prior, topics)
    require(set(coords) == set(ids), "topic records do not match API admissions")
    for item in ids:
        channel, ingress = rows[item][:2]
        require(coords[item][0] == source_topic(channel, ingress, channel_readers),
                f"incorrect source topic for {item}")
    backlog_by_probe(probes, campaign, rows, coords)
    grouped = defaultdict(list)
    for probe in probes:
        item = probe["id"]
        channel, ingress, state, created, claimed, finished = rows[item]
        topic, partition, offset = coords[item]
        require(ingress == probe["lane"], "probe persisted in an unexpected ingress lane")
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
              "lateCancelIDs": late_cancels,
              "receiptAudit": {"checked": len(ids), "statusAndPOSTs": dict(Counter(rows[i][2]+"/"+str(actual[i]) for i in ids))},
              "provider": fixture}
    if loaded:
        span = samples[-1]["s"]-samples[0]["s"]
        report["smsPostsPerSecondDuringProbes"] = round((samples[-1]["smsPosts"]-samples[0]["smsPosts"])/span, 3)
        report["smsSamples"] = samples
    return report


def trial(exp, directory, name, image, channel_readers=False, allow_cancel_race=False):
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
        output[condition] = measure(exp, uuid.uuid4().hex, condition == "loaded", channel_readers, allow_cancel_race)
        print(name, condition, output[condition]["push"], flush=True)
    output["sourceOffsetsAfterDrain"] = final_offsets(exp, source_topics(channel_readers))
    directory.mkdir(parents=True, exist_ok=True)
    (directory/(name+".json")).write_text(json.dumps(output, indent=2)+"\n")


def configure(exp, image):
    exp.env.update(APP_IMAGE=image, WORKER_CONCURRENCY="24", PROVIDER_LATENCY="20ms", BATCH_SHARE_PERCENT="20")


def compare_readers(directory, labels, baseline_image, candidate_image):
    for label in labels:
        exp = Experiment()
        candidate = label.startswith("b")
        image = candidate_image if candidate else baseline_image
        configure(exp, image)
        exp.env["BATCH_ROUTING"] = "channel" if candidate else "shared"
        require(not (directory/(label + ".json")).exists(), f"existing result for {label}")
        try:
            exp.compose("up", "-d", "--no-build", "--wait")
            trial(exp, directory, label, image, channel_readers=candidate, allow_cancel_race=True)
        finally:
            exp.compose("down", "-v", "--remove-orphans")
    if all((directory / (cell + ".json")).is_file() for cell in ("a1", "b1", "b2", "a2")):
        comparisons = []
        for pair in (1, 2):
            a, b = [json.loads((directory/f"{arm}{pair}.json").read_text())["loaded"] for arm in ("a", "b")]
            sms_change = b["smsPostsPerSecondDuringProbes"] / a["smsPostsPerSecondDuringProbes"] - 1
            push_p95 = b["push"]["batch"]["p95Ms"]
            comparisons.append({"pair": pair, "batchPushP95Ms": push_p95, "smsThroughputChangePercent": 100*sms_change,
                                "pushThresholdPassed": push_p95 <= 2000, "smsThresholdPassed": sms_change >= -0.05})
        passed = all(pair["pushThresholdPassed"] and pair["smsThresholdPassed"] for pair in comparisons)
        result = {"pairs": comparisons, "performanceGatePassed": passed}
        (directory/"gate.json").write_text(json.dumps(result, indent=2)+"\n")
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out-dir", type=Path, default=ROOT/".local"/"channel-isolation")
    p.add_argument("--baseline-image", required=True)
    p.add_argument("--candidate-image", required=True)
    p.add_argument("--compare-readers", action="store_true",
                   help="test pre-fetch channel routing in fresh isolated stacks for each trial")
    p.add_argument("--cells", nargs="+", choices=("a1", "b1", "b2", "a2"), default=("a1", "b1", "b2", "a2"),
                   help="run these cells in order for the pre-fetch comparison")
    args = p.parse_args()
    if args.compare_readers:
        compare_readers(args.out_dir, args.cells, args.baseline_image, args.candidate_image)
        raise SystemExit
    exp = Experiment()
    configure(exp, args.baseline_image)
    try:
        exp.compose("up", "-d", "--no-build", "--wait")
        for label in ("a1", "b1", "b2", "a2"):
            trial(exp, args.out_dir, label, args.baseline_image if label.startswith("a") else args.candidate_image)
    finally:
        exp.compose("down", "-v", "--remove-orphans")
