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
