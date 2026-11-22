"""Print a comparison of completed cohorts and keep a CSV beside the raw evidence."""

import csv
import json
import sys
from pathlib import Path


def row(path):
    result = json.loads(path.read_text())
    receipts = result["receiptAudit"]
    passed = (
        result["functionalOK"]
        and result["withinQuota"]
        and result["deltaCheck"]
        and result["scaledDown"] is not False
    )
    return {
        "cohort": result["name"],
        "workers": " -> ".join(map(str, result["actualScale"])),
        "seconds": round(result["elapsedSeconds"], 2),
        "accepted_per_second": round(result["observedTerminalPerElapsedSecond"], 1),
        "p95_ms": result["createStartToPersistedTerminal"]["p95Ms"],
        "checked_ids": receipts["checked"],
        "missing_posts": receipts["zeroPosts"],
        "duplicate_posts": receipts["multiplePosts"],
        "unknown": result["finalObservedStates"].get("unknown", 0),
        "passed": passed,
    }


if __name__ == "__main__":
    lab = Path(__file__).resolve().parent
    rows = [row(path) for path in sorted((lab / "runs").glob("*/result.json"))]
    if not rows:
        sys.exit("No completed cohorts found.")
    with (lab / "summary.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print((lab / "summary.csv").read_text())
    if len(rows) != 11 or not all(item["passed"] for item in rows):
        sys.exit("The replay did not pass all eleven cohorts. See runs/ for evidence.")
