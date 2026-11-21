#!/usr/bin/env python3
"""Recover an observed provider receipt after a PostgreSQL outage and worker replacement."""
import argparse
import json
from pathlib import Path
import subprocess

from recovery import Experiment, require, wait_until


RECEIPTS = "notifications:{receipts}"


def redis(exp, *args):
    return exp.compose("exec", "-T", "redis", "redis-cli", "--raw", *args, capture=True).stdout.strip()


def experiment(exp, build):
    exp.compose("up", "--build" if build else "--no-build", "-d", "--wait", "--wait-timeout", "300")
    original = exp.compose("ps", "-q", "worker", capture=True).stdout.strip()
    image = subprocess.check_output(["docker", "inspect", "--format", "{{.Image}}", original], text=True).strip()
    item = exp.create("receipt recovery during a database outage")
    url = exp.api + "/v1/notifications/" + item
    wait_until(lambda: exp.stats()["accepted"] == 1, "synthetic provider accepts the SMS")
    require(exp.get_state(item) == "sending", "missed the delayed response")
    exp.compose("stop", "-t", "1", "postgres")
    wait_until(lambda: redis(exp, "SCARD", RECEIPTS) == "1", "receipt is buffered while PostgreSQL is down")
    receipt = json.loads(redis(exp, "SMEMBERS", RECEIPTS))
    require(receipt["ID"] == item and receipt["Attempt"] == 1 and receipt["Status"] == 202 and receipt["MessageID"],
            "buffered receipt does not identify the provider acceptance")
    exp.compose("stop", "worker")
    # With PostgreSQL and the original worker offline, verify the AOF alone
    # preserves the exact receipt across a Redis restart.
    exp.compose("restart", "redis")
    exp.compose("up", "-d", "--no-build", "--no-deps", "--wait", "redis")
    wait_until(lambda: redis(exp, "SCARD", RECEIPTS) == "1", "receipt survives Redis restart")
    require(json.loads(redis(exp, "SMEMBERS", RECEIPTS)) == receipt, "receipt changed in Redis")
    exp.compose("start", "postgres")
    wait_until(lambda: exp.get_state(item) == "sending", "database restarts with the original send claim")
    exp.compose("up", "--no-build", "--no-deps", "--force-recreate", "-d", "worker")
    replacement = exp.compose("ps", "-q", "worker", capture=True).stdout.strip()
    require(replacement and replacement != original, "the worker was not replaced")
    exp.expect(item, "accepted")
    wait_until(lambda: redis(exp, "SCARD", RECEIPTS) == "0", "confirmed receipt is removed")
    final = exp.request("GET", url)
    posts = exp.request("GET", exp.provider + "/stats/" + item)["attempts"]
    require(final["messageId"] == receipt["MessageID"] and final["attempt"] == 1,
            "the original attempt and provider message ID were not restored")
    require(posts == 1 and exp.stats()["attempts"] == 1, "notification was posted more than once")
    require(redis(exp, "SCARD", RECEIPTS + ":conflicts") == "0", "unexpected receipt conflict")
    return {"imageId": image, "notificationId": item, "providerMessageId": receipt["MessageID"],
            "providerDelay": exp.env["PROVIDER_LATENCY"], "providerIdempotent": False,
            "originalWorker": original, "replacementWorker": replacement, "receiptSurvivedRedisRestart": True,
            "databaseBeforeReplacement": "sending", "finalStatus": final["status"], "attempt": final["attempt"],
            "providerPOSTsForID": posts, "pendingReceiptsAfterRecovery": 0, "receiptConflicts": 0}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", help="use an existing application image instead of building the checkout")
    parser.add_argument("--report", type=Path, default=Path(".local/results/receipt-recovery.json"))
    args = parser.parse_args()
    exp = Experiment()
    exp.env.update(APP_IMAGE=args.image or "notification-system:" + exp.project,
                   PROVIDER_LATENCY="4s", PROVIDER_ACK_MODE="assessment", WORKER_CONCURRENCY="1")
    try:
        report = experiment(exp, build=not args.image)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(f"Recovered {report['notificationId']} with the original receipt and one provider POST.")
    finally:
        exp.compose("down", "--volumes", "--remove-orphans")
        if not args.image:
            subprocess.run(["docker", "image", "rm", exp.env["APP_IMAGE"]], check=False)
