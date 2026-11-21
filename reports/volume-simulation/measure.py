"""Compare two local /metrics endpoints against the seeded snapshot."""

import argparse
import datetime
import json
import math
import re
import statistics
import time
import urllib.request


def read_metrics(port):
    start = time.perf_counter_ns()
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=30) as response:
        body = response.read().decode()
    return body, round((time.perf_counter_ns() - start) / 1_000_000, 3)


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("baseline_port", type=int)
parser.add_argument("current_port", type=int)
args = parser.parse_args()
ports = {"baseline": args.baseline_port, "current": args.current_port}
counts = {
    "sms": {"accepted": 2450, "failed": 25, "unknown": 25},
    "email": {"accepted": 2425, "failed": 50, "unknown": 25},
    "push": {"accepted": 2375, "failed": 75, "unknown": 50},
}
pattern = re.compile(
    r'^notification_outcomes_per_second\{channel="([^"]+)",status="([^"]+)",window="5m"\} (\S+)$',
    re.MULTILINE,
)
started = datetime.datetime.now(datetime.timezone.utc).isoformat()
durations = {label: [] for label in ports}
for warmup in (True, False):
    order = ["baseline", "current"] if warmup else ["baseline", "current", "current", "baseline"] * 4
    for label in order:
        body, duration = read_metrics(ports[label])
        if label == "current":
            values = {(c, s): float(v) for c, s, v in pattern.findall(body)}
            expected = {(c, s): n / 300 for c, states in counts.items() for s, n in states.items()}
            if values.keys() != expected.keys() or not all(
                math.isclose(values[key], rate) for key, rate in expected.items()
            ):
                raise RuntimeError(f"Window no longer matches recent cohort: {values}")
        if not warmup:
            durations[label].append(duration)
print(json.dumps({
    "startedUTC": started,
    "finishedUTC": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "verifiedRecentCounts": counts,
    "requestMs": durations,
    "medianRequestMs": {label: statistics.median(samples) for label, samples in durations.items()},
}, indent=2))
