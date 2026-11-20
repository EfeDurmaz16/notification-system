#!/usr/bin/env python3
"""Render reports produced by cmd/loadtest. Percentile calculations happen in Go."""
from pathlib import Path
import json
import sys

directory = Path(sys.argv[1])
print("# Local notification experiments\n")
print("All rows below are measurements from the files in this directory. "
      "The provider is local, with a synthetic delay. Rates exclude setup time.\n")
print("| Run | Offered | API accepted | Terminal | API p95 ms | Persisted terminal p95 ms | "
      "Observed terminal p95 ms | Terminal/s | Drain s | Max SMS attempts in 1 s |")
print("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
reports = []
for path in sorted(directory.glob("*.json")):
    data = json.loads(path.read_text())
    if "config" not in data or "checks" not in data:
        continue
    reports.append((path, data))
    persisted = data["createStartToPersistedTerminal"]
    persisted_p95 = f"{persisted['p95Ms']:.1f}" if persisted["n"] else "unavailable"
    peak = (data["provider"].get("channels") or {}).get("sms", {}).get("peakPerSecond", "unavailable")
    print(f"| {path.stem} | {data['config']['offeredNotifications']} | {data['acceptedNotifications']} "
          f"| {data['observedTerminal']} | {data['createResponseLatency']['p95Ms']:.1f} | {persisted_p95} "
          f"| {data['createStartToObservedTerminal']['p95Ms']:.1f} | {data['observedTerminalPerElapsedSecond']:.1f} "
          f"| {data['secondsFromLastCreateResponseToFinalObservation']:.2f} | {peak} |")
print("\n## Outcomes\n")
for path, data in reports:
    counts = ", ".join(f"{k}={v}" for k, v in sorted(data["finalObservedStates"].items()))
    checks = [k for k, ok in data["checks"].items() if not ok]
    errors = data.get("errors", [])
    audit = data.get("receiptAudit")
    audit_note = (f" {audit['checked']} individual IDs audited after the timed run: "
                  f"{audit['zeroPosts']} missing, {audit['multiplePosts']} repeated, "
                  f"{audit['readErrors']} audit read errors.") if audit else ""
    print(f"- **{path.stem}**: {counts or 'no states observed'}. "
          f"Provider accepted {data['provider']['accepted']}, rejected {data['provider']['failures']}, "
          f"lost responses {data['provider']['responseLost']}. "
          f"Outstanding at deadline {data['notObservedTerminal']}. "
          f"Failed checks: {', '.join(checks) or 'none'}."
          + audit_note + (f" Errors: {'; '.join(errors)}." if errors else ""))
print("\n## Method\n")
print("- The 1,000-item burst is one request, so its API percentiles describe that single call. "
      "The 10,000-item burst is ten simultaneous batches of 1,000. Replica counts are recorded in each JSON file.")
print("- Load cases submit batches at a fixed arrival rate; stress uses three equal-population stages "
      "at 1x, 2x, and 4x base rate. See scheduling delay in the JSON to identify generator lag.")
print("- Terminal latency starts before the create request. Persisted latency ends at the "
      "database status timestamp when available. Observed latency includes polling delay. "
      "Observed terminal throughput includes the ramp and the final poll.")
print("- Peak SMS rate is calculated from every provider handler arrival, using a sliding "
      "one-second window. Separate retries count again. In the failures case the provider "
      "accepts the synthetic unknown notifications, then deliberately loses their responses.")
print("- Expired deadlines and quota violations remain visible in the raw reports. "
      "Queueing and timing from a local run do not establish production capacity.")
print("\n## Queueing and priority\n")
print("The largest observed backlog is sampled once per second and equals API acknowledgments minus client-observed "
      "terminal states, including any work awaiting its next status poll. The latency columns use persisted terminal times.\n")
print("| Experiment | Highest sampled backlog | High p95 ms | Normal p95 ms | Low p95 ms |")
print("| --- | ---: | ---: | ---: | ---: |")
for path, data in reports:
    backlog = max((s["acceptedMinusObservedTerminal"] for s in data["timeline"]), default=0)
    values = data.get("createStartToPersistedTerminalByPriority", {})
    cells = [f"{values[p]['p95Ms']:.1f}" if p in values else "unavailable"
             for p in ["high", "normal", "low"]]
    print(f"| {path.stem} | {backlog} | {' | '.join(cells)} |")

for path, data in reports:
    if data["config"]["scenario"] != "mixed":
        continue
    print("\n## Independent channel quotas\n")
    print("Three-channel burst across independent budgets, with each priority represented on each channel.\n")
    print("| Channel | Requests received | Peak requests in a sliding second |")
    print("| --- | ---: | ---: |")
    for channel, stats in sorted(data["provider"]["channels"].items()):
        print(f"| {channel} | {stats['attempts']} | {stats['peakPerSecond']} |")
