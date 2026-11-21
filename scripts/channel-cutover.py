#!/usr/bin/env python3
"""Exercise forward and reverse standalone channel routing changes on an isolated Compose stack."""
import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import uuid

from recovery import Experiment, ROOT, require, wait_until

BASE = "4ca75cc9c4dca3265da3888553d880be324e7059"
SHARED = "notifications.normal"
RECIPIENTS = {"sms": "+15550100123", "email": "fixture@example.invalid", "push": "fixture-device"}


def main(report_path):
    exp = Experiment()
    baseline = "notification-system:" + exp.project + "-baseline"
    candidate = "notification-system:" + exp.project + "-candidate"
    exp.env.update(WORKER_CONCURRENCY="4", BATCH_SHARE_PERCENT="20", PROVIDER_LATENCY="20ms")
    jobs = {}
    proof = {"baselineRevision": BASE, "steps": [], "jobs": jobs}

    def select(image, mode):
        exp.env.update(APP_IMAGE=image, BATCH_ROUTING=mode)

    def replace(service, image, mode):
        select(image, mode)
        # The previous worker predates the readiness endpoint. Its ability to
        # consume after rollback is checked with a new API notification below.
        wait = () if service == "worker" and image == baseline else ("--wait",)
        exp.compose("up", "--no-build", "--no-deps", "-d", *wait, "--force-recreate", service)

    def stage(label):
        proof["steps"].append(label)
        print(label, flush=True)

    def sql(query):
        return exp.compose("exec", "-T", "postgres", "psql", "-XAtq", "-v", "ON_ERROR_STOP=1",
                           "-U", "notify", "-d", "notify", "-c", query, capture=True).stdout.strip()

    def create(label, channel, routes, *, cancel=False, scheduled=False, standalone=False):
        item = {**exp.item(label), "channel": channel, "recipient": RECIPIENTS[channel]}
        if scheduled:
            item["scheduledAt"] = (datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat()
        headers = {"Idempotency-Key": str(uuid.uuid4())}
        if standalone:
            created = exp.request("POST", exp.api + "/v1/notifications", item, 202, headers)["id"]
        else:
            receipt = exp.request("POST", exp.api + "/v1/batches", {"notifications": [item]}, 202, headers)
            created = receipt["items"][0]["id"]
        jobs[label] = {"id": created, "expectedTopics": routes,
                       "expectedStatus": "cancelled" if cancel else "accepted"}
        if cancel:
            exp.request("POST", f"{exp.api}/v1/notifications/{created}/cancel", expected=204)
        if scheduled:
            require(exp.get_state(created) == "scheduled", "old producer did not persist a scheduled job")
        return created

    def all_final():
        return all(exp.get_state(j["id"]) == j["expectedStatus"] for j in jobs.values())

    def committed():
        # The connector can be offline while Kafka lag is already zero. First
        # reconcile the expected number of routed outbox events on each topic.
        expected = Counter(t for job in jobs.values() for t in job["expectedTopics"])
        groups = [part for topic in expected for part in ("--group", topic.replace("notifications.", "delivery.", 1))]
        output = exp.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                             "--bootstrap-server", "kafka:9092", "--describe", "--offsets", *groups,
                             capture=True).stdout
        found = {}
        totals = Counter()
        for line in output.splitlines():
            f = line.split()
            if len(f) < 6 or f[1] not in expected or not f[4].isdigit():
                continue
            offset, end = f[3], int(f[4])
            if not (offset.isdigit() and int(offset) == end or end == 0 and offset == "-"):
                return None
            found[f"{f[1]}/{f[2]}"] = {"end": end, "committed": None if offset == "-" else int(offset)}
            totals[f[1]] += end
        return found if totals == expected else None

    def batch_topic(channel):
        return SHARED + ".batch." + channel

    def single_topic(channel):
        return SHARED + ".single." + channel

    try:
        # Archive supplies the actual previous binary and its original migrations.
        with subprocess.Popen(["git", "archive", BASE], cwd=ROOT, stdout=subprocess.PIPE) as archive:
            subprocess.run(["docker", "build", "-q", "-t", baseline, "-"], stdin=archive.stdout, check=True)
            require(archive.wait() == 0, "could not archive the baseline")
        subprocess.run(["docker", "build", "-q", "-t", candidate, "."], cwd=ROOT, check=True)
        proof["images"] = {name: subprocess.check_output(
            ["docker", "image", "inspect", "-f", "{{.Id}}", image], text=True).strip()
            for name, image in (("legacy", baseline), ("candidate", candidate))}

        select(baseline, "channel")
        exp.compose("up", "--no-build", "-d", "--wait", "--scale", "worker=0")
        for channel in RECIPIENTS:
            create("legacy-" + channel, channel, [batch_topic(channel)])
            create("legacy-single-" + channel, channel, [SHARED + ".single"], standalone=True)
        create("cancel-before-cutover", "sms", [SHARED + ".single"], cancel=True, standalone=True)
        require(exp.stats()["attempts"] == 0, "expected legacy backlog before workers started")
        replace("worker", candidate, "read-isolated")
        wait_until(all_final, "dual readers drain backlog written by legacy API")
        stage("Dual readers drained legacy standalone and batch work; the old API remained live.")

        # One definite pre-send failure. Stop the worker while the retry is
        # persisted. No test-only writes to business tables or clocks.
        exp.env["PROVIDER_URL"] = "http://provider:1/send"
        replace("worker", candidate, "read-isolated")
        retry = create("retry-across-cutover", "email", [SHARED + ".single", single_topic("email")], standalone=True)
        exp.expect(retry, "retry_wait")
        exp.compose("stop", "worker")
        require(exp.get_state(retry) == "retry_wait", "retry advanced before shutdown")
        del exp.env["PROVIDER_URL"]
        create("scheduled-by-legacy", "push", [single_topic("push")], scheduled=True, standalone=True)
        exp.compose("stop", "connect")
        create("legacy-during-cdc-outage", "push", [SHARED + ".single"], standalone=True)
        replace("api", candidate, "isolated")
        for channel in RECIPIENTS:
            create("new-batch-" + channel, channel, [batch_topic(channel)])
            create("new-single-" + channel, channel, [single_topic(channel)], standalone=True)
        checkpoint = exp.stats()["attempts"]
        replace("worker", candidate, "isolated")
        wait_until(lambda: exp.get_state(retry) == "pending" and
                   exp.get_state(jobs["scheduled-by-legacy"]["id"]) == "pending",
                   "new scheduler promotes pre-cutover retry and scheduled job")
        require(exp.stats()["attempts"] == checkpoint, "work escaped paused CDC")
        exp.compose("start", "connect")
        wait_until(all_final, "dual-format backlog and original scheduled/retry jobs drain")
        stage("Channel writers and workers drained both formats after CDC resumed.")

        exp.compose("stop", "connect")
        for channel in RECIPIENTS:
            create("rollback-queued-" + channel, channel, [single_topic(channel)], standalone=True)
        replace("worker", candidate, "read-isolated")
        replace("api", baseline, "channel")
        create("rollback-shared", "email", [SHARED + ".single"], standalone=True)
        exp.compose("start", "connect")
        wait_until(all_final, "dual readers drain split records emitted before writer rollback")
        proof["committedBeforeReaderDowngrade"] = wait_until(committed, "published and committed topic totals")
        replace("worker", baseline, "channel")
        create("legacy-after-rollback", "sms", [SHARED + ".single"], standalone=True)
        wait_until(all_final, "restored baseline handles fresh shared record")
        stage("Legacy readers resumed after publication and offset reconciliation.")

        records = json.loads(sql("""SELECT json_object_agg(id,topics) FROM (
            SELECT aggregateid id, json_agg(aggregatetype ORDER BY (payload->>'attempt')::int) topics
            FROM outbox GROUP BY aggregateid) s"""))
        for name, j in jobs.items():
            n = exp.request("GET", exp.api + "/v1/notifications/" + j["id"])
            posts = exp.request("GET", exp.provider + "/stats/" + j["id"])["attempts"]
            j.update(status=n["status"], attempt=n["attempt"], posts=posts, topics=records.get(j["id"], []))
            require(j["topics"] == j["expectedTopics"], f"{name} unexpected routing: {j['topics']}")
            require(j["status"] == j["expectedStatus"] and posts == int(j["status"] == "accepted"),
                    f"{name} unexpected final status/POST count")
            if j["status"] == "accepted":
                require(bool(n.get("messageId")), f"{name} missing provider receipt")
        require(jobs["retry-across-cutover"]["attempt"] == 2, "expected exactly one safe retry")
        proof["provider"] = exp.stats()
        require(proof["provider"]["attempts"] == len(jobs)-1, "unexpected provider POST count")
        proof["result"] = "pass"
    finally:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(proof, indent=2) + "\n")
        exp.compose("down", "-v", "--remove-orphans", check=False)
        subprocess.run(["docker", "image", "rm", baseline, candidate], check=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path(".local/results/channel-cutover.json"))
    main(parser.parse_args().out)
