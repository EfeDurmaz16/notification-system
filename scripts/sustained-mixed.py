#!/usr/bin/env python3
"""Measure three channels and their rolling rates on a disposable local stack."""
import argparse
import csv
import io
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time
from urllib.request import urlopen

from check_live_outcomes import check_window
from recovery import Experiment, ROOT, require


CHANNELS = ("sms", "email", "push")
OUTCOMES = ("accepted", "failed", "unknown")
METRIC = "notification_outcomes_per_second"
RATE = 120


def sample(exp):
    with urlopen(exp.api + "/metrics", timeout=15) as response:
        lines = response.read().decode().splitlines()
    data = {}
    for line in lines:
        if line.startswith("notification_queue_depth "):
            data["pending"] = int(line.split()[-1])
        if not line.startswith(METRIC + "{"):
            continue
        series, value = line.rsplit(" ", 1)
        labels = dict(re.findall(r'(\w+)="([^"]+)"', series))
        if labels.get("window") == "5m":
            data[f'{labels["channel"]}_{labels["status"]}'] = float(value)
    expected = {f"{c}_{s}" for c in CHANNELS for s in OUTCOMES}
    require(data.keys() == expected | {"pending"}, "missing five-minute channel or outcome series")
    return data


def summarize_channels(exp):
    # This stack has a fresh volume, and the load generator is its only writer.
    sql = """COPY (
        WITH outcomes AS (
            SELECT channel, status,
                CASE WHEN starts_with(content, '[mock:reject] ') THEN 'failed'
                     WHEN starts_with(content, '[mock:unknown] ') THEN 'unknown'
                     ELSE 'accepted' END AS expected,
                extract(epoch from updated_at-created_at) * 1000 AS elapsed_ms
            FROM notifications)
        SELECT channel, count(*) AS total,
            count(*) FILTER (WHERE status='accepted') AS accepted,
            count(*) FILTER (WHERE status='failed') AS failed,
            count(*) FILTER (WHERE status='unknown') AS unknown,
            count(*) FILTER (WHERE status<>expected) AS mismatches,
            round((percentile_disc(.95) WITHIN GROUP (ORDER BY elapsed_ms)
                FILTER (WHERE status='accepted'))::numeric, 3) AS accepted_p95_ms,
            round((percentile_disc(.99) WITHIN GROUP (ORDER BY elapsed_ms)
                FILTER (WHERE status='accepted'))::numeric, 3) AS accepted_p99_ms
        FROM outcomes GROUP BY channel ORDER BY channel) TO STDOUT WITH CSV HEADER"""
    proc = exp.compose("exec", "-T", "postgres", "psql", "-Xq", "-v", "ON_ERROR_STOP=1",
                       "-U", "notify", "-d", "notify", "-c", sql, capture=True)
    return list(csv.DictReader(io.StringIO(proc.stdout)))


def csv_file(path, rows):
    with path.open("w", newline="") as out:
        writer = csv.DictWriter(out, rows[0].keys(), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def run(directory, minutes):
    directory.mkdir(parents=True, exist_ok=True)
    outputs = ("load.json", "timeline.csv", "rates.csv", "channels.csv", "environment.json",
               "window-check.json")
    require(not any((directory / name).exists() for name in outputs), "use a fresh output directory")
    exp = Experiment()
    image = "notification-system:" + exp.project
    exp.env.update(APP_IMAGE=image, WORKER_CONCURRENCY="24", BATCH_SHARE_PERCENT="20",
                   PROVIDER_LATENCY="20ms")
    seconds = minutes * 60
    try:
        exp.compose("up", "--build", "-d", "--scale", "worker=3", "--wait")
        before = sample(exp)
        require(all(value == 0 for value in before.values()), "stack must begin with zero counts")
        meta = {"image": subprocess.check_output(["docker", "image", "inspect", image, "-f", "{{.Id}}"],
                                                  text=True).strip(),
                "replicas": 3, "workerConcurrency": 24, "batchSharePercent": 20,
                "providerLatency": "20ms", "providerIdempotency": False,
                "offeredPerSecondPerChannel": RATE // 3, "plannedSeconds": seconds}
        (directory / "environment.json").write_text(json.dumps(meta, indent=2) + "\n")
        with tempfile.TemporaryDirectory() as temp:
            binary, raw = Path(temp) / "loadtest", Path(temp) / "load.json"
            subprocess.run(["go", "build", "-trimpath", "-o", str(binary), "./cmd/loadtest"],
                           cwd=ROOT, check=True)
            cmd = [str(binary), "-scenario", "mixed-faults", "-api", exp.api, "-provider", exp.provider,
                   "-total", str(seconds * RATE), "-rate", str(RATE), "-batch", "30",
                   "-audit-receipts", "-deadline", f"{minutes+10}m", "-out", str(raw)]
            print(f"Starting {minutes}-minute sustained load: {seconds*RATE} notifications.", flush=True)
            start = time.monotonic()
            samples = []
            with subprocess.Popen(cmd, stdout=subprocess.DEVNULL) as load:
                while load.poll() is None:
                    elapsed = round(time.monotonic() - start, 3)
                    point = {"elapsed_s": elapsed, **sample(exp)}
                    samples.append(point)
                    print(f'{elapsed:.1f}s pending={point["pending"]} '
                          f'sms accepted/s={point["sms_accepted"]:.3f}', flush=True)
                    audit = directory / "window-check.json"
                    if elapsed >= 330 and not audit.exists():
                        audit.write_text(json.dumps(check_window(exp, sample), indent=2) + "\n")
                    try:
                        load.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        pass
            data = json.loads(raw.read_text())
            csv_file(directory / "timeline.csv", data.pop("timeline"))
            (directory / "load.json").write_text(json.dumps(data, indent=2) + "\n")
            csv_file(directory / "rates.csv", samples)
            require(load.returncode == 0, "load generator failed; inspect load.json")
        channels = summarize_channels(exp)
        csv_file(directory / "channels.csv", channels)
        require({row["channel"] for row in channels} == set(CHANNELS), "missing persisted channel")
        require(all(int(row["total"]) == seconds * RATE // 3 and int(row["mismatches"]) == 0
                    for row in channels), "persisted channel totals or individual outcomes differ")
        require(any(300 <= row["elapsed_s"] < seconds and
                    all(row[f"{ch}_{state}"] > 0 for ch in CHANNELS for state in OUTCOMES)
                    for row in samples), "all nine nonzero rates must appear within a mature window")
    finally:
        exp.compose("down", "-v", "--remove-orphans")
        subprocess.run(["docker", "image", "rm", image], cwd=ROOT, check=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--minutes", type=int, default=15)
    args = parser.parse_args()
    if args.minutes < 6:
        parser.error("use at least six minutes to sample a mature five-minute window")
    run(args.out, args.minutes)
