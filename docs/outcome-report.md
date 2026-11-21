# Historical notification outcomes

From the repository root, run the read-only report on the Compose database. This example shows the five hours ending at 15:00 UTC as five hourly buckets, followed by one total per channel.

```sh
docker compose exec -T postgres psql -U notify -d notify -Xq --csv \
  -v from=2026-11-21T10:00:00Z -v to=2026-11-21T15:00:00Z -v bucket=hour \
  < scripts/outcome-report.sql
```

Omit the three variables to report the trailing five hours at the database clock. For daily or weekly reports, pass `-v bucket=day` or `-v bucket=week` with the desired range. Use ISO timestamps with a UTC offset. Times without an offset are interpreted as UTC. Weeks begin Monday at 00:00 UTC. The report accepts up to 10,000 calendar buckets per query.

Each `bucket` row covers `[from_utc, to_utc)` for one channel. The first and last buckets stop at the requested boundaries, so a rolling five-hour range can intersect six calendar hours. Empty channel/bucket combinations return zero. Each final `total` row counts the same channel over the entire selected period.

The three columns count notifications reaching confirmed provider acceptance (`accepted`), definitive failure (`failed`) or an unresolved result (`unknown`). The report uses the time the service recorded the terminal outcome. For example, an email created on Sunday at 23:59 UTC and accepted on Monday at 00:01 UTC counts toward Monday and the new week. Retries of that notification still produce one outcome. In-progress and cancelled notifications are excluded. Results cover the records retained in the database. If a buffered provider receipt later resolves an expired send lease, the notification moves from its earlier unknown bucket to the accepted bucket at the time the service saved the receipt.
