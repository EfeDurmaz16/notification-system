#!/usr/bin/env python3
"""Send SIGTERM after provider acceptance with a real backlog and reconcile every ID."""
import argparse
from collections import Counter
import json
from pathlib import Path
import subprocess
import time
import uuid

from recovery import Experiment, batch_source, require, wait_until


TOPIC, PARTITIONS = batch_source()
COUNT = 6


def kafka(exp, command, *args):
    return exp.compose("exec", "-T", "kafka", f"/opt/kafka/bin/kafka-{command}.sh",
                       "--bootstrap-server", "kafka:9092", *args, capture=True).stdout


def backlog_published(exp):
    lines = kafka(exp, "get-offsets", "--topic", TOPIC, "--time", "latest").splitlines()
    offsets = {int(p): int(end) for topic, p, end in (line.split(":") for line in lines) if topic == TOPIC}
    return set(offsets) == set(range(PARTITIONS)) and sum(offsets.values()) == COUNT


def drained_offsets(exp):
    output = kafka(exp, "consumer-groups", "--describe", "--offsets", "--group", TOPIC.replace("notifications.", "delivery.", 1))
    offsets = {}
    for row in output.splitlines():
        f = row.split()
        if len(f) >= 6 and f[1] == TOPIC and all(v.isdigit() for v in f[2:6]):
            partition, committed, end, lag = map(int, f[2:6])
            offsets[partition] = {"committed": committed, "end": end, "lag": lag}
        elif len(f) >= 6 and f[1] == TOPIC and f[3:6] == ["-", "0", "-"]:
            offsets[int(f[2])] = {"committed": None, "end": 0, "lag": 0}
    if len(offsets) == PARTITIONS and sum(p["end"] for p in offsets.values()) == COUNT and all(
            p["lag"] == 0 for p in offsets.values()):
        return offsets


def notification(exp, item):
    return exp.request("GET", exp.api + "/v1/notifications/" + item)


def posts(exp, item):
    return exp.request("GET", exp.provider + "/stats/" + item)["attempts"]


def inspect(container, template):
    return subprocess.check_output(["docker", "inspect", "--format", template, container], text=True).strip()


def run(exp, build):
    exp.compose("up", "--build" if build else "--no-build", "-d", "--wait", "--scale", "worker=0")
    receipt = exp.request("POST", exp.api + "/v1/batches",
                          {"notifications": [exp.item("graceful worker shutdown")] * COUNT},
                          202, {"Idempotency-Key": str(uuid.uuid4())})
    ids = [item["id"] for item in receipt["items"]]
    require(len(set(ids)) == COUNT, "batch admission did not return six unique IDs")
    wait_until(lambda: backlog_published(exp), "all six records published while the worker is offline")
    exp.compose("up", "--no-build", "--no-deps", "-d", "--scale", "worker=1", "worker")
    original = exp.compose("ps", "-q", "worker", capture=True).stdout.strip()
    image = inspect(original, "{{.Image}}")

    wait_until(lambda: exp.stats()["accepted"] == 1, "provider accepts the first SMS")
    in_flight = [item for item in ids if posts(exp, item) == 1]
    require(len(in_flight) == 1, "expected one in-flight POST")
    first = in_flight[0]
    before = {item: notification(exp, item)["status"] for item in ids}
    require(before[first] == "sending" and Counter(before.values()) == {"sending": 1, "pending": COUNT-1},
            "missed the interval between provider acceptance and acknowledgement")

    started = time.monotonic()
    exp.compose("stop", "worker")
    elapsed = round(time.monotonic() - started, 3)
    exit_code = int(inspect(original, "{{.State.ExitCode}}"))
    stopped = {item: notification(exp, item) for item in ids}
    at_exit = {item: posts(exp, item) for item in ids}

    exp.compose("up", "--no-build", "--no-deps", "-d", "--force-recreate", "worker")
    wait_until(lambda: all(notification(exp, item)["status"] in ("accepted", "unknown", "failed")
                           for item in ids), "cohort reaches terminal state", seconds=160)
    offsets = wait_until(lambda: drained_offsets(exp), "source consumer group commits all six records")
    ledger = []
    for item in ids:
        final = notification(exp, item)
        ledger.append({"id": item, "beforeStop": before[item], "atExit": stopped[item]["status"],
                       "postsAtExit": at_exit[item], "final": final["status"], "posts": posts(exp, item),
                       "receiptAtExit": stopped[item].get("messageId"), "finalReceipt": final.get("messageId")})
    return {"imageId": image, "inFlightId": first, "workerExitCode": exit_code,
            "providerLatency": exp.env["PROVIDER_LATENCY"], "workerConcurrency": 1,
            "sourceTopic": TOPIC, "composeStopSeconds": elapsed, "provider": exp.stats(), "finalOffsets": offsets, "ids": ledger}


def verify(report):
    require(report["workerExitCode"] == 0, "worker did not exit cleanly")
    for row in report["ids"]:
        if row["id"] == report["inFlightId"]:
            require(row["receiptAtExit"] and row["atExit"] == "accepted" and row["postsAtExit"] == 1,
                    "provider accepted the POST but the exiting worker did not persist its acknowledgement")
            require(row["finalReceipt"] == row["receiptAtExit"], "provider receipt changed after replacement")
        else:
            require(row["atExit"] == "pending" and row["postsAtExit"] == 0, "stopping worker took another job")
        require(row["final"] == "accepted" and row["finalReceipt"] and row["posts"] == 1, "ID audit failed")
    require(report["provider"]["attempts"] == report["provider"]["accepted"] == COUNT, "aggregate audit failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", help="test an existing image instead of building the current checkout")
    parser.add_argument("--report", type=Path, default=Path(".local/results/worker-drain.json"))
    args = parser.parse_args()
    exp = Experiment()
    exp.env.update(APP_IMAGE=args.image or "notification-system:" + exp.project,
                   WORKER_CONCURRENCY="1", PROVIDER_LATENCY="3s")
    try:
        result = run(exp, build=not args.image)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(result, indent=2) + "\n")
        verify(result)
        print("SIGTERM drained one in-flight receipt. The replacement delivered all five pending IDs.")
    finally:
        exp.compose("down", "--volumes", "--remove-orphans")
        if not args.image:
            subprocess.run(["docker", "image", "rm", exp.env["APP_IMAGE"]], check=False)
