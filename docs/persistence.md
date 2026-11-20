# Persistence and recovery

PostgreSQL is authoritative for accepted requests, cancellation, and delivery state. The API and worker apply embedded, numbered migrations at startup under a transaction-scoped advisory lock. A batch insert stores the request digest, stable result for each index, valid notifications, initial status history, and ready events in one transaction. Replaying an idempotency key returns the original creation response, whereas batch queries report current status and timestamps.

## Claim and retry

A worker obtains a channel permit, then conditionally changes one notification from pending to sending for the Kafka event's generation. Only a successful UPDATE allows the provider POST. Cancellation uses the same row and closes permanently once the first send starts. A later generation cannot be claimed by an earlier event. Provider payload fields and notification identity remain immutable across attempts.

A due-time scan uses bounded chunks and row locks with SKIP LOCKED. Promoting a retry increments its generation and inserts its ready event atomically. Scheduled jobs start at generation one. After a worker crash, an abandoned send becomes unknown. Under an explicitly enabled provider idempotency contract, the scheduler can instead enqueue another bounded attempt within the original guarantee window. Choose the stale timeout longer than the outbound HTTP deadline and its final state write.

## CDC and retention

Only outbox INSERTs carry ready events. The rows contain an ID, attempt and trace context; recipient and content stay in PostgreSQL. Republished events are safe for dispatch because the send claim is conditional. Each consumer group should independently handle replay for its own effects.

This assessment retains idempotency responses, terminal rows, status audit events, and outbox rows. There is no expiry job; storage grows with traffic. For a longer-running deployment, define customer replay and audit windows, monitor outbox growth and replication-slot WAL lag, and design cleanup against verified connector progress. Expiring an idempotency key too early permits a repeated create to become a new operation. Do not delete outbox rows simply because they are old while the connector is behind.

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
