#!/usr/bin/env python3
"""Compare fixed, scheduled and backlog-triggered workers on an isolated SMS wave."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
import io
import json
import math
from pathlib import Path
import subprocess
import threading
import time
from urllib.request import urlopen
import uuid

from recovery import Experiment, ROOT, require, wait_until

RATES = (20, 60, 90, 150, 90, 60, 20)
SCRIPTED = (1, 2, 2, 3, 3, 2, 1)
BATCH = 10
HANDLERS = 24


def percentile(values, p):
    ordered = sorted(values)
    return round(ordered[math.ceil(len(ordered) * p) - 1], 3) if ordered else None


def scale(exp, desired):
    exp.compose("up", "-d", "--no-deps", "--scale", f"worker={desired}", "worker", capture=True)


def worker_count(exp):
    output = exp.compose("ps", "--status", "running", "-q", "worker", capture=True)
    return len(output.stdout.splitlines())


def worker_cpu(exp):
    ids = exp.compose("ps", "-q", "--status", "running", "worker", capture=True).stdout.splitlines()

    def sample(container):
        before = time.time()
        output = subprocess.check_output(["docker", "exec", container, "sh", "-ec",
                "stat -c %i /sys/fs/cgroup && cat /sys/fs/cgroup/cpu.stat && cat /proc/self/cgroup"],
                text=True).splitlines()
        after = time.time()
        require(output[-1] == "0::/", "expected private cgroup-v2 namespace")
        counters = dict(line.split() for line in output[1:-1])
        return container, {"unixSeconds": (before + after) / 2, "inode": output[0],
                           "usageUsec": int(counters["usage_usec"])}

    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            samples = dict(pool.map(sample, ids))
        require(len(samples) == len({v["inode"] for v in samples.values()}) == 3, "expected three cgroups")
        return samples
    except (subprocess.CalledProcessError, AssertionError, KeyError) as error:
        return {"unavailable": str(error)}


def cpu_cost(before, after):
    if "unavailable" in before or "unavailable" in after:
        return {"unavailable": before.get("unavailable") or after["unavailable"]}
    require(before.keys() == after.keys(), "the fixed CPU cohort changed")
    used, intervals = 0, []
    for key, row in after.items():
        require(before[key]["inode"] == row["inode"], "the worker cgroup changed")
        used += (row["usageUsec"] - before[key]["usageUsec"]) / 1e6
        intervals.append(row["unixSeconds"] - before[key]["unixSeconds"])
    elapsed = sum(intervals) / len(intervals)
    return {"before": before, "after": after, "workerCpuSeconds": round(used, 3),
            "meanSamplingIntervalSeconds": round(elapsed, 3), "averageCores": round(used / elapsed, 4)}


def metrics(exp):
    with urlopen(exp.api + "/metrics", timeout=10) as response:
        gauges = dict(line.split() for line in response.read().decode().splitlines() if not line.startswith("#"))
    return int(gauges["notification_queue_depth"]), float(gauges["notification_oldest_pending_seconds"])


def kafka_sample(exp):
    result = exp.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                         "--bootstrap-server", "kafka:9092", "--group", "delivery.normal",
                         "--describe", "--offsets", capture=True, check=False)
    partitions = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) >= 8 and fields[1] == "notifications.normal":
            partitions[fields[2]] = {"lag": int(fields[5]) if fields[5].isdigit() else None,
                                     "owner": fields[7].strip("/")}
    return {"partitions": partitions, "diagnostic": result.stderr.strip()}


def send(exp, token, phase, index, due):
    started = time.monotonic()
    payload = {"notifications": [exp.item(token)] * BATCH}
    receipt = exp.request("POST", exp.api + "/v1/batches", payload, 202,
                          {"Idempotency-Key": f"{token}-{phase}-{index}"})
    ids = [item["id"] for item in receipt["items"]]
    require(len(ids) == BATCH, "incomplete batch")
    return phase, ids, (started - due) * 1000, (time.monotonic() - started) * 1000


def offer(exp, token, start, seconds):
    submissions = []
    with ThreadPoolExecutor(max_workers=24) as pool:
        for phase, rate in enumerate(RATES):
            for i in range(rate * seconds // BATCH):
                due = start + phase * seconds + i * BATCH / rate
                time.sleep(max(0, due - time.monotonic()))
                submissions.append(pool.submit(send, exp, token, phase, i, due))
        return [future.result() for future in submissions]


def monitor(exp, policy, start, seconds, done, timeline, events):
    target = 3 if policy == "fixed3" else 1
    quiet_since = None
    previous = -60
    scale_task = None
    with ThreadPoolExecutor(max_workers=1) as actuator:
        while not done.is_set():
            now = time.monotonic() - start
            if scale_task is not None and scale_task.done():
                scale_task.result()
                scale_task = None
            depth, age = metrics(exp)
            actual = worker_count(exp)
            timeline.append({"s": round(now, 3), "replicas": actual, "pending": depth,
                             "oldestPendingSeconds": round(age, 3), "providerPosts": exp.stats()["attempts"]})
            quiet = depth <= 20 and age < 0.6
            quiet_since = (now if quiet_since is None else quiet_since) if quiet else None
            desired, reason = target, ""
            if policy == "scripted":
                desired = SCRIPTED[min(int(now // seconds), len(RATES) - 1)]
                reason = "phase schedule"
            elif policy == "feedback" and now - previous >= 12:
                if depth >= 100 and age >= 1.0:
                    desired, reason = min(3, target + 1), "pending >= 100 and oldest >= 1s"
                elif quiet_since is not None and now - quiet_since >= 12:
                    desired, reason = max(1, target - 1), "pending <= 20 and oldest < 0.6s for 12s"
            if desired != target and scale_task is None:
                events.append({"s": round(now, 3), "from": target, "to": desired,
                               "pending": depth, "oldestPendingSeconds": round(age, 3), "reason": reason})
                print(f"{policy} {now:.1f}s: {target} -> {desired}, depth {depth}, oldest {age:.2f}s", flush=True)
                target, previous, quiet_since = desired, now, None
                scale_task = actuator.submit(scale, exp, desired)
            done.wait(max(0.05, start + now + 2 - time.monotonic()))
        if scale_task is not None:
            scale_task.result()


def watch_kafka(exp, start, done, snapshots):
    while not done.is_set():
        snapshot = kafka_sample(exp)
        snapshot["s"] = round(time.monotonic() - start, 3)
        snapshots.append(snapshot)
        done.wait(12)


def fetch_rows(exp, token):
    # The token is generated locally, restricted to alphanumerics, and never supplied by a client.
    require(token.isalnum(), "invalid cohort token")
    sql = f"""COPY (SELECT id, status, extract(epoch from (updated_at-created_at))::float8,
             extract(epoch from updated_at)::float8 FROM notifications WHERE content='{token}') TO STDOUT WITH CSV"""
    output = exp.compose("exec", "-T", "postgres", "psql", "-Xq", "-v", "ON_ERROR_STOP=1",
                         "-U", "notify", "-d", "notify", "-c", sql, capture=True)
    return list(csv.reader(io.StringIO(output.stdout)))


def receipt_counts(exp, ids):
    def lookup(item):
        return exp.request("GET", exp.provider + "/stats/" + item)["attempts"]

    with ThreadPoolExecutor(max_workers=12) as pool:
        return dict(zip(ids, pool.map(lookup, ids)))


def replica_seconds(timeline, end_s, initial):
    count, cursor, area = initial, 0, 0
    for sample in timeline:
        if sample["s"] >= end_s:
            break
        area += count * (sample["s"] - cursor)
        cursor, count = sample["s"], sample["replicas"]
    return round(area + count * (end_s - cursor), 2)


def summarize(exp, policy, token, batches, timeline, events, snapshots, seconds, wall_start):
    ids = {item: phase for phase, batch, _, _ in batches for item in batch}
    expected = seconds * sum(RATES)
    require(len(ids) == expected, "duplicate or missing admission IDs")
    rows = fetch_rows(exp, token)
    require({row[0] for row in rows} == ids.keys(), "admitted and persisted IDs differ")
    posts = receipt_counts(exp, list(ids))
    status = Counter()
    outcomes = Counter()
    phases = [[] for _ in RATES]
    exceptions = []
    last = wall_start
    for item, state, elapsed, finished in rows:
        status[state] += 1
        outcomes[f"{state}/{posts[item]}"] += 1
        last = max(last, float(finished))
        if state == "accepted":
            phases[ids[item]].append(float(elapsed))
        if state != "accepted" or posts[item] != 1:
            exceptions.append({"id": item, "phase": ids[item] + 1, "state": state, "providerPOSTs": posts[item]})
    phase_summary = []
    for i, rate in enumerate(RATES):
        samples = [s for s in timeline if i * seconds <= s["s"] < (i + 1) * seconds]
        phase_summary.append({"phase": i + 1, "offeredPerSecond": rate, "accepted": len(phases[i]),
                              "p95Seconds": percentile(phases[i], .95),
                              "maxPending": max((s["pending"] for s in samples), default=None)})
    all_times = [v for phase in phases for v in phase]
    duration = seconds * len(RATES)
    cutoff = max(last - wall_start, duration)
    post_stats = exp.stats()
    checked = sum(posts.values())
    require(post_stats["attempts"] == checked, "aggregate and per-ID POST counts differ")

    def drained():
        sample = kafka_sample(exp)
        partitions = sample["partitions"]
        if set(partitions) == {"0", "1", "2"} and all(p["lag"] == 0 for p in partitions.values()):
            return sample
        return None

    actual_lag = wait_until(drained, "zero committed lag on every batch partition", seconds=60)
    capacity = replica_seconds(timeline, duration, 3 if policy == "fixed3" else 1)
    return {"policy": policy, "cohort": token, "phaseSeconds": seconds, "offered": expected,
            "statuses": dict(status), "receiptAudit": {"checked": len(posts), "statesAndPOSTs": dict(outcomes),
            "exceptions": exceptions}, "provider": post_stats, "p95Seconds": percentile(all_times, .95),
            "p99Seconds": percentile(all_times, .99), "terminalSecondsFromStart": round(last - wall_start, 3),
            "acceptedPerSecondThroughWaveAndDrain": round(status["accepted"] / cutoff, 3),
            "peakSampledPending": max(s["pending"] for s in timeline),
            "peakSampledOldestPendingSeconds": max(s["oldestPendingSeconds"] for s in timeline),
            "replicaSecondsDuringWave": capacity,
            "handlerSecondsDuringWave": round(capacity * HANDLERS, 2),
            "createResponseP95Ms": percentile([b[3] for b in batches], .95),
            "generatorDelayP95Ms": percentile([b[2] for b in batches], .95),
            "generatorDelayMaxMs": round(max(b[2] for b in batches), 3),
            "phases": phase_summary, "scaleActions": events, "timeline": timeline,
            "kafkaSnapshots": snapshots, "finalKafka": actual_lag}


def trial(exp, policy, seconds):
    initial = 3 if policy == "fixed3" else 1
    scale(exp, initial)
    token = uuid.uuid4().hex
    warm_id = exp.create("wave warm-up")
    exp.expect(warm_id, "accepted")
    # Exercise the measured batch lane too; ensure warm records have left the limiter.
    _, warm, _, _ = send(exp, "warm" + token, 0, 0, time.monotonic())
    for item in warm:
        exp.expect(item, "accepted")
    # The pre-provisioned control must own all three partitions before arrivals begin.
    initial_assignment = None
    if policy == "fixed3":
        def assigned():
            snapshot = kafka_sample(exp)
            owners = {p["owner"] for p in snapshot["partitions"].values()} - {"-"}
            return snapshot if len(owners) == initial else None

        initial_assignment = wait_until(assigned, "all three batch owners active", seconds=120)
    time.sleep(1.2)
    exp.request("DELETE", exp.provider + "/stats", expected=204)
    cpu_before = worker_cpu(exp) if policy == "fixed3" else None
    start, wall_start = time.monotonic(), time.time()
    done = threading.Event()
    timeline, events, snapshots = [], [], []
    print(f"Starting {policy}: {seconds}s/phase, {sum(RATES) * seconds} SMS", flush=True)
    with ThreadPoolExecutor(max_workers=2) as observers:
        control = observers.submit(monitor, exp, policy, start, seconds, done, timeline, events)
        broker = observers.submit(watch_kafka, exp, start, done, snapshots)
        try:
            batches = offer(exp, token, start, seconds)
            elapsed = time.monotonic() - start
            time.sleep(max(0, seconds * len(RATES) - elapsed))
            if cpu_before is not None:
                cpu_after = worker_cpu(exp)
            # An interrupted in-flight call can remain sending for two minutes.
            def settled():
                if control.done():
                    control.result()
                if worker_count(exp) != initial or metrics(exp)[0]:
                    return False
                query = """SELECT count(*) FROM notifications
                    WHERE status IN ('pending','sending','scheduled','retry_wait')"""
                output = exp.compose("exec", "-T", "postgres", "psql", "-XAtq", "-U", "notify",
                                     "-d", "notify", "-c", query, capture=True)
                return int(output.stdout) == 0

            wait_until(settled, "terminal states and stable final replica count", seconds=240)
        finally:
            done.set()
            control.result()
            broker.result()
    result = summarize(exp, policy, token, batches, timeline, events, snapshots, seconds, wall_start)
    if initial_assignment is not None:
        result["initialKafka"] = initial_assignment
        result["workerCPU"] = cpu_cost(cpu_before, cpu_after)
    return result


def verify(result):
    require(result["provider"]["channels"]["sms"]["peakPerSecond"] <= 100, "provider rate exceeded")
    require(set(result["statuses"]) <= {"accepted", "unknown"}, "unexpected terminal state")
    require(sum(result["statuses"].values()) == result["offered"], "missing terminal state")
    require(set(result["receiptAudit"]["statesAndPOSTs"]) <= {"accepted/1", "unknown/0", "unknown/1"},
            "inconsistent or duplicate provider POST, inspect receiptAudit.exceptions")
    # This success-only fixture counts every POST that reached it as one acceptance.
    require(result["provider"]["accepted"] == result["provider"]["attempts"], "provider count mismatch")
    partitions = result["finalKafka"]["partitions"]
    require(set(partitions) == {"0", "1", "2"} and all(p["lag"] == 0 for p in partitions.values()),
            "Kafka offsets did not drain")
    if result["policy"] in ("scripted", "feedback"):
        counts = set(sample["replicas"] for sample in result["timeline"])
        require(counts == {1, 2, 3} and result["scaleActions"][-1]["to"] == 1,
                "worker scale-up and return to one not exercised")


def run_scenarios(exp, directory, seconds, policies=("fixed", "scripted", "feedback")):
    directory.mkdir(parents=True, exist_ok=True)
    for policy in policies:
        result = trial(exp, policy, seconds)
        (directory / (policy + ".json")).write_text(json.dumps(result, indent=2) + "\n")
        verify(result)
        print(policy, result["statuses"], "p95", result["p95Seconds"],
              "replica-seconds", result["replicaSecondsDuringWave"], flush=True)


def isolated_run(directory, seconds, policies):
    exp = Experiment()
    exp.env.update(WORKER_CONCURRENCY=str(HANDLERS), BATCH_SHARE_PERCENT="20", PROVIDER_LATENCY="20ms",
                   APP_IMAGE="notification-system:" + exp.project)
    try:
        exp.compose("up", "--build", "-d", "--scale", "worker=1", "--wait")
        run_scenarios(exp, directory, seconds, policies)
    finally:
        exp.compose("down", "-v", "--remove-orphans")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=ROOT / ".local" / "traffic-wave")
    parser.add_argument("--phase-seconds", type=int, default=25)
    parser.add_argument("--fixed-three-only", action="store_true",
                        help="run only the warm, fixed three-replica control on a fresh stack")
    args = parser.parse_args()
    if args.phase_seconds < 15:
        parser.error("phase-seconds must be at least 15 for the feedback observation window")
    if not args.fixed_three_only:
        isolated_run(args.out_dir, args.phase_seconds, ("fixed", "scripted", "feedback"))
    isolated_run(args.out_dir, args.phase_seconds, ("fixed3",))
