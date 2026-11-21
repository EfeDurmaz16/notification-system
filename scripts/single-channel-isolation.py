#!/usr/bin/env python3
"""Measure same-priority email behind standalone SMS on a disposable local stack."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import importlib
import json
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

from recovery import Experiment, ROOT, require, wait_until

audit = importlib.import_module("channel-isolation")
EMAILS = 30
SMS = 1500
COOLDOWN = 12


def send(exp, channel, token):
    payload = exp.item(token)
    payload["channel"] = channel
    if channel == "email":
        payload["recipient"] = "probe@example.invalid"
    receipt = exp.request("POST", exp.api + "/v1/notifications", payload, expected=202,
                          headers={"Idempotency-Key": str(uuid.uuid4())})
    return receipt["id"]


def measure(exp, mode):
    token = uuid.uuid4().hex
    exp.request("DELETE", exp.provider + "/stats", expected=204)
    count = {"idle": 0, "backlog": SMS, "cooldown": 180}[mode]
    if mode == "cooldown":
        # Inject only the shared cooldown key, reproducing a tripped SMS breaker.
        key = "notifications:quota:{sms}:pause"
        require(exp.compose("exec", "-T", "redis", "redis-cli", "SET", key, "1",
                            "EX", str(COOLDOWN), "NX", capture=True).stdout.strip() == "OK",
                "SMS cooldown key already exists")
    with ThreadPoolExecutor(max_workers=24) as pool:
        sms = list(pool.map(lambda _: send(exp, "sms", token), range(count)))
        sms_posts = exp.stats()["channels"].get("sms", {}).get("attempts", 0)
        require(not count or count - sms_posts >= count // 2, "SMS backlog drained during ingress")
        emails = list(pool.map(lambda _: send(exp, "email", token), range(EMAILS)))
    if mode == "cooldown":
        remaining = int(exp.compose("exec", "-T", "redis", "redis-cli", "PTTL", key,
                                    capture=True).stdout.strip())
        require(remaining >= 5000, "cooldown expired during ingress")
    ids = sms + emails
    require(len(ids) == len(set(ids)), "duplicate admission ID")
    wait_until(lambda: exp.stats()["accepted"] >= len(ids), "provider drain", seconds=120)

    def accepted_rows():
        rows = audit.sql_rows(exp, token)
        return rows if set(rows) == set(ids) and all(r[2] == "accepted" for r in rows.values()) else None

    rows = wait_until(accepted_rows, "persisted accepted receipt for every ID")
    actual = audit.per_id_posts(exp, ids)
    stats = exp.stats()
    require(all(actual[i] == 1 for i in ids) and stats["attempts"] == len(ids),
            "per-ID POST count or provider total differs")
    peaks = {c: d["peakPerSecond"] for c, d in stats["channels"].items()}
    require(all(0 < p <= 100 for p in peaks.values()), "channel exceeded rolling one-second limit")
    p95 = audit.percentiles([(float(rows[i][5]) - float(rows[i][3])) * 1000 for i in emails])
    result = {"case": mode, "acceptedIdsChecked": len(ids), "providerPosts": stats["attempts"],
              "smsOutstandingAtFirstEmail": count - sms_posts, "emailCreateToAcceptance": p95,
              "peakProviderPostsInRollingSecond": peaks}
    if sms:
        elapsed = max(float(rows[i][5]) for i in sms) - min(float(rows[i][3]) for i in sms)
        result["smsCompletionRatePerSecond"] = round(count / elapsed, 2)
    if mode == "cooldown":
        result["cooldownMsRemainingAfterEmailSubmissions"] = remaining
        first_sms = min(float(rows[i][4]) for i in sms)
        result["emailsAcceptedBeforeFirstSmsClaim"] = sum(float(rows[i][5]) < first_sms for i in emails)
    print(json.dumps(result), flush=True)
    return result


def run(source, output, revision):
    # All subprocesses see this revision's Compose manifest and deploy configuration.
    exp = Experiment(root=source)
    exp.env.update(APP_IMAGE="notification-system:" + exp.project, WORKER_CONCURRENCY="24",
                   BATCH_SHARE_PERCENT="20", PROVIDER_LATENCY="20ms", BATCH_ROUTING="")
    try:
        exp.compose("up", "--build", "-d", "--wait", "--scale", "worker=1")
        for channel in ("sms", "email"):
            exp.expect(send(exp, channel, "warmup"), "accepted")
        mode = exp.compose("exec", "-T", "worker", "printenv", "BATCH_ROUTING",
                           capture=True).stdout.strip()
        cases = [measure(exp, mode) for mode in ("idle", "backlog", "cooldown")]
        report = {"source": revision, "routing": mode, "imageId": subprocess.check_output(
            ["docker", "image", "inspect", "-f", "{{.Id}}", exp.env["APP_IMAGE"]], text=True).strip(),
            "workerReplicas": 1, "workerConcurrency": 24, "providerLatencyMs": 20, "cases": cases}
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n")
        if mode == "isolated":
            require(cases[-1]["emailsAcceptedBeforeFirstSmsClaim"] == EMAILS,
                    "a healthy channel waited for the SMS cooldown")
    finally:
        exp.compose("down", "--volumes", "--remove-orphans")
        subprocess.run(["docker", "image", "rm", exp.env["APP_IMAGE"]], check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", default="HEAD", help="git commit to build and measure")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    sha = subprocess.check_output(["git", "rev-parse", "--verify", args.revision + "^{commit}"],
                                  cwd=ROOT, text=True).strip()
    with tempfile.TemporaryDirectory() as tmp:
        archive = Path(tmp) / "source.tar"
        subprocess.run(["git", "archive", "-o", str(archive), sha], cwd=ROOT, check=True)
        subprocess.run(["tar", "-xf", str(archive), "-C", tmp], check=True)
        archive.unlink()
        run(Path(tmp), args.out, sha)


if __name__ == "__main__":
    main()
