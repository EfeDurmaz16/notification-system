#!/usr/bin/env python3
"""Verify the runnable psql report with session-local rows on the migrated schema."""
import argparse
import csv
import io
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
REPORT = (ROOT / "scripts/outcome-report.sql").read_text()
FROM = "2026-11-22T23:30:00Z"
TO = "2026-11-23T01:15:00Z"
FIXTURE = """
SET TIME ZONE 'America/New_York';
CREATE TEMP TABLE notifications AS TABLE public.notifications WITH NO DATA;
INSERT INTO notifications (channel, status, attempt, created_at, updated_at) VALUES
    ('sms','accepted',1,'2026-11-22 21:00Z','2026-11-22 23:29:59Z'),
    ('sms','accepted',1,'2026-11-22 21:00Z','2026-11-22 23:30Z'),
    ('email','retry_wait',2,'2026-11-22 23:59Z','2026-11-22 23:59:30Z'),
    ('push','unknown',1,'2026-11-22 12:00Z','2026-11-23 00:59Z'),
    ('sms','failed',1,'2026-11-23 01:00Z','2026-11-23 01:12Z'),
    ('email','accepted',1,'2026-11-23 01:00Z','2026-11-23 01:15Z'),
    ('push','cancelled',1,'2026-11-22 23:30Z','2026-11-22 23:50Z'),
    ('email','sending',1,'2026-11-22 23:30Z','2026-11-22 23:50Z');
UPDATE notifications SET status='accepted', attempt=3, updated_at='2026-11-23 00:00Z'
WHERE status='retry_wait';
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true", help="use psql and its PG* environment")
    args = parser.parse_args()
    cmd = ["psql"] if args.local else ["docker", "compose", "exec", "-T", "postgres", "psql",
                                       "-U", "notify", "-d", "notify"]

    def run(params, valid=True):
        flags = [arg for key, val in params.items() for arg in ("-v", f"{key}={val}")]
        result = subprocess.run(cmd + ["-Xq", "--csv", "-v", "ON_ERROR_STOP=1"] + flags,
                                input=FIXTURE + REPORT, text=True, cwd=ROOT, capture_output=True)
        if valid != (result.returncode == 0):
            raise AssertionError(f"params={params} exit={result.returncode}: {result.stderr}")
        return list(csv.DictReader(io.StringIO(result.stdout)))

    totals = {"sms": (1, 1, 0), "email": (1, 0, 0), "push": (0, 0, 1)}
    for grain in ("hour", "day", "week"):
        rows = run({"from": FROM, "to": TO, "bucket": grain})
        buckets = [row for row in rows if row["row_kind"] == "bucket"]
        assert len(buckets) == (9 if grain == "hour" else 6), rows
        assert buckets[0]["from_utc"] == "2026-11-22 23:30:00+00"
        assert buckets[-1]["to_utc"] == "2026-11-23 01:15:00+00"
        for row in rows[-3:]:
            assert row["row_kind"] == "total"
            assert tuple(int(row[key]) for key in ("accepted", "failed", "unknown")) == totals[row["channel"]]
        email = [row for row in buckets if row["channel"] == "email"]
        assert email[0]["accepted"] == "0"
        assert email[1]["from_utc"] == "2026-11-23 00:00:00+00" and email[1]["accepted"] == "1"
        assert all(row["failed"] == "0" and row["unknown"] == "0" for row in email)

    # Giving only an end time uses the prior five hours and the default hour grain.
    empty = run({"to": "2026-10-10T05:00:00Z"})
    assert len(empty) == 18 and all(r[k] == "0" for r in empty for k in ("accepted", "failed", "unknown"))
    assert empty[0]["from_utc"] == "2026-10-10 00:00:00+00"
    for values in ({"bucket": "year"}, {"to": FROM, "from": FROM}, {"from": TO, "to": FROM},
                   {"from": "not-a-time"}, {"to": "infinity"}, {"from": "2001-01-01", "to": "2026-01-01"}):
        run(values, valid=False)
    print("PASS UTC hour, day, Monday week, clipped boundaries, retry, totals, zeros and invalid ranges")


if __name__ == "__main__":
    main()
