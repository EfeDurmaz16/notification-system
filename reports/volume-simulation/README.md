# Five-minute outcome rates with one day of retained notifications

All nine channel and status rates matched the expected counts from 7,500 recent notifications with 2,160,000 older notifications already in the database. A complete HTTP `/metrics` scrape took a median 444 ms on the feature build and 411 ms on the previous build. The old rows span 24 hours at 25 notifications/s. The fixture populated the tables directly, so these timings measure reading retained data. Sustained delivery is a separate end-to-end experiment.

| Channel | Recent accepted | Recent failed | Recent unknown | Accepted/s | Failed/s | Unknown/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SMS | 2,450 | 25 | 25 | 8.1667 | 0.0833 | 0.0833 |
| Email | 2,425 | 50 | 25 | 8.0833 | 0.1667 | 0.0833 |
| Push | 2,375 | 75 | 50 | 7.9167 | 0.2500 | 0.1667 |

The HTTP checker compared the emitted values with the exact counts divided by 300 on every feature scrape. Older rows ended at least ten minutes before measurement. The recent cohort had recorded completion times between two and three minutes before its insert. This margin gives the repeated requests the same nine expected rates. After the latest completion aged out naturally, [one more scrape](aged-out.json) found nine zero rates and all 2,167,500 lifetime outcomes still present.

The synthetic database held 2,167,500 notifications, 6,502,500 status transitions, 2,168 batch headers, and zero outbox rows. The database occupied 2,048,924,695 bytes. Batch receipts were empty and recipients, content and latency were synthetic. Production storage, dispatch, retries, disk loss and process uptime require separate measurements.

## Cost of the complete endpoint

Two API builds shared an isolated PostgreSQL 16.14 cluster on a local arm64 Mac. The baseline was `e3e3202`; the feature build included the new channel/status rates. There were no active writers during sampling. After a warm-up request to each build, we alternated eight measurements per build in repeated A, B, B, A order. These are local warm-cache measurements. Other work ran on the host.

| Measurement | Previous API | API with channel rates |
| --- | ---: | ---: |
| Median complete HTTP scrape | 410.91 ms | 443.82 ms |
| Smallest to largest complete scrape | 404.63 to 415.59 ms | 431.31 to 451.57 ms |
| State aggregation, one query plan sample | 99.41 ms | 131.16 ms |

Both state queries scanned the notification table with two parallel workers. The five-minute filter shares the scan that counts all current states. The existing all-time acceptance histogram took another 327.29 ms in its plan sample. Repeated scrapes therefore remain sensitive to retained notification count and scrape frequency. [HTTP samples](result.json) and [query plans and table counts](plans.txt).

## Reproduce

Use a fresh local PostgreSQL cluster with free loopback ports. The following commands require PostgreSQL client and server tools, Go, Git, Python 3 and curl. They assume the repository root as the working directory and a shell with job control.

```sh
scratch=$(mktemp -d)
initdb -D "$scratch/data" -U volume --locale=C -E UTF8 --auth=trust
pg_ctl -D "$scratch/data" -l "$scratch/postgres.log" \
  -o "-h 127.0.0.1 -k $scratch -p 46374 -c timezone=UTC -c max_connections=30 -c shared_buffers=192MB -c max_parallel_workers=2" -w start
export PGHOST=127.0.0.1 PGPORT=46374 PGUSER=volume
createdb notification_volume_sim
export PGDATABASE=notification_volume_sim
export DATABASE_URL=postgres://volume@127.0.0.1:46374/notification_volume_sim
mkdir "$scratch/base"
git archive e3e3202 | tar -xf - -C "$scratch/base"
(cd "$scratch/base" && go build -trimpath -o "$scratch/base-api" ./cmd/notify)
go build -trimpath -o "$scratch/current-api" ./cmd/notify
HTTP_ADDR=127.0.0.1:46375 "$scratch/base-api" api > "$scratch/base.log" 2>&1 &
base_pid=$!
HTTP_ADDR=127.0.0.1:46376 "$scratch/current-api" api > "$scratch/current.log" 2>&1 &
current_pid=$!
curl --retry 20 --retry-delay 1 --retry-connrefused -fsS http://127.0.0.1:46376/healthz
psql -X -v recent=0 -f reports/volume-simulation/seed.sql
psql -X -c 'VACUUM (ANALYZE) notifications' -c 'VACUUM (ANALYZE) status_events'
psql -X -v recent=1 -f reports/volume-simulation/seed.sql
python3 -B reports/volume-simulation/measure.py 46375 46376
kill "$base_pid" "$current_pid"
wait "$base_pid" "$current_pid"
pg_ctl -D "$scratch/data" -m fast -w stop
```

The data directory is disposable. Remove the temporary directory after shutdown to reclaim its space.
