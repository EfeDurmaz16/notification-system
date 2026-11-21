# Persistence and recovery

PostgreSQL is authoritative for accepted requests, cancellation, and delivery state. The API and worker apply embedded, numbered migrations at startup under a transaction-scoped advisory lock. A batch insert stores the request digest, stable result for each index, valid notifications, initial status history, and ready events in one transaction. Replaying an idempotency key returns the original creation response, whereas batch queries report current status and timestamps.

## Claim and retry

A worker obtains a channel permit, then conditionally changes one notification from pending to sending for the Kafka event's generation. Only a successful UPDATE allows the provider POST. Cancellation uses the same row and closes permanently once the first send starts. A later generation cannot be claimed by an earlier event. Provider payload fields and notification identity remain immutable across attempts.

A due-time scan uses bounded chunks and row locks with SKIP LOCKED. Promoting a retry increments its generation and inserts its ready event atomically. Scheduled jobs start at generation one. After a worker crash, an abandoned send becomes unknown. Under an explicitly enabled provider idempotency contract, the scheduler can instead enqueue another bounded attempt within the original guarantee window. Choose the stale timeout longer than the outbound HTTP deadline and its final state write.

## CDC and retention

Only outbox INSERTs carry ready events. The rows contain an ID, attempt and trace context; recipient and content stay in PostgreSQL. Republished events are safe for dispatch because the send claim is conditional. Each consumer group should independently handle replay for its own effects.

The delivery contract keeps notification lookups, batch responses and idempotency keys available indefinitely. These tables grow with admissions. Set customer lookup and replay windows before expiring them. Monitor table sizes and replication-slot WAL lag independently.

An operator can prune old audit transitions and already-published outbox events with `scripts/retention.sql`. Choose a cutoff that satisfies your audit requirements and archive the data first if it must remain available. The default mode previews a bounded page. Supply `-v apply=true` to process a page from the apply cursor and delete eligible rows. Use a cutoff strictly in the past.

```sh
psql "$DATABASE_URL" -X -v before='2026-01-01T00:00:00Z' -f scripts/retention.sql
```

Each call considers at most 1,000 audit events for notifications already `accepted`, `failed` or `cancelled` before the cutoff. Unresolved `unknown` notifications retain their full history. Historical outcome reports and live status use the notification rows, while attempt-by-attempt forensic and stage-latency queries lose the pruned history.

For outbox cleanup, pass `-v published_ids="$EVENT_IDS"` with up to 1,000 comma-separated **outbox event UUIDs** from Kafka records observed on this deployment's ready topics. Debezium puts the outbox UUID in the `id` header. The Kafka record key is the notification UUID. Never build this list from PostgreSQL. With an empty list, zero outbox rows qualify. This allows CDC outages to last longer than the retention window without aging out unpublished work. Register the bundled connector configuration before applying cleanup so deletes emit no tombstones. [Debezium event headers and delete handling](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)

### Scheduled audit cleanup

Use the optional Compose file to preview one chunk every minute. Set `RETAIN_DAYS` to the approved audit horizon. The example retains thirty days. In a running Compose deployment:

```sh
RETAIN_DAYS=30 docker compose -f docker-compose.yml -f deploy/retention.compose.yml up -d retention
RETAIN_DAYS=30 docker compose -f docker-compose.yml -f deploy/retention.compose.yml logs --tail 20 retention
```

Use the same two files and add `RETENTION_APPLY=true` to the first command after reviewing the counts and archiving any needed transitions. To stop the job:

```sh
RETAIN_DAYS=30 docker compose -f docker-compose.yml -f deploy/retention.compose.yml stop retention
```

The job examines up to 1,000 old transitions per minute using the indexed cursor `(changed_at, seq)`. The cutoff stays fixed during each sweep. After the last page, the next pass starts at the oldest retained event, so histories that have since resolved become eligible. Preview and apply keep separate progress rows. A failed transaction advances neither deletions nor progress. Logs show the number examined and the number eligible. It retains unresolved histories, current notifications, creation responses and idempotency keys. A status-history request after pruning returns the transitions still retained. The scheduled job supplies no Kafka publication evidence, so outbox cleanup uses the separate verified-ID command above. A sustained rate over 1,000 eligible transitions per minute can outgrow this schedule.

To check disk use, run `psql "$DATABASE_URL" -X -f scripts/storage-status.sql`. Record table bytes and retained WAL across successive samples. Outbox growth together with increasing unconfirmed WAL is a reason to check the connector and slot before running cleanup. Estimate remaining time from free disk bytes divided by recent growth in bytes per hour. Provision capacity for the full lookup and idempotency horizon.

## Metrics over long histories

State counts and acceptance latency buckets are maintained in the same transaction as the notification change. Thirty-two stripes distribute counter updates across rows. A scrape aggregates the bounded totals table, uses a partial index for the last five minutes of outcomes and reads the oldest pending timestamp from the status index. The initial migration performs one full backfill and index build under the migration lock. For an upgrade from before migrations 003 and 004, inspect table sizes first. Rehearse the upgrade on a restored copy of the same size and reserve a maintenance window for the measured runtime. Stop admissions and drain workers before applying the schema. PostgreSQL holds the table locks until the startup transaction commits. If the deployment requires a no-downtime upgrade, it needs a separately staged online backfill and concurrent index build before rollout. Fresh installs apply the schema to empty tables.

## Reproduce the storage tests

Run the following against a disposable PostgreSQL database whose test user can create schemas. The suite creates and removes an isolated schema per test.

```sh
TEST_DATABASE_URL='postgres://notify:notify@127.0.0.1:54329/notify?sslmode=disable' go test -race -count=1 -v ./internal/store
```

The suite covers concurrent idempotent creation, partial validation, atomic rollback, cancellation versus send claims, stable creation replay after state changes, scheduling, stale event generations, crash recovery, query filters, and latency aggregation.

## Stage latency after a load run

Read the `runId` field from the harness JSON and pass its value to
the read-only query after traffic has drained:

```sh
psql "$DATABASE_URL" -X -q -v run_id="$RUN_ID" -f scripts/latency-breakdown.sql
```

The query selects batch keys matching the exact run identity followed by a numeric
offset. Check the batch and notification counts before interpreting percentiles.
Each duration uses PostgreSQL trigger timestamps from the first generation of the
same notification. Scheduled or retried deliveries can lack one or more stage
samples. The table reports sample counts explicitly.

- Pending to sending covers the remainder of the create transaction after its
  trigger, CDC propagation, Kafka backlog, scheduling, limiter waits, and the send
  claim. The query cannot isolate these costs individually.
- Sending to accepted includes the remaining send-claim commit, worker and HTTP
  time, the local provider, and the update up to the accepted status trigger.
- Pending to accepted is the per-notification sum. Percentiles are calculated
  separately per stage; adding stage percentiles does not recover the total
  percentile.
- All times use one database clock. These boundaries differ from client request
  start, API response receipt, external provider receipt, and queue polling.
  They measure simulated provider acceptance, not final handset delivery.

For priority behavior, run `scripts/priority-dispatch.sql` with the same
`run_id`. Its half-open windows start at the cohort's first durable send claim.
Counts use first-attempt claims, with pending-but-unclaimed rows at each boundary
to show whether a class had outstanding database work. Those rows can still be
in CDC, in Kafka, or waiting for a local dispatch slot. A positive outstanding
count alone does not prove every worker's local queue was populated.
