# Inspecting a delayed or ambiguous notification

Start with the API notification ID, batch ID, channel and status. Use the configuration of the affected worker when interpreting a timeout: provider idempotency and its retention window are external guarantees.

| State | Inspect | Expected next action |
|---|---|---|
| pending | Age in the HTTP metrics, connector status, Kafka consumer lag, Redis availability | An eligible worker acquires a channel permit and the DB send claim. The client may still win cancellation before the first send. |
| scheduled / retry_wait | The due time, DB scheduler and remaining attempts | A new ready event appears when the scheduler promotes the job. |
| sending | Worker logs for the notification ID and its trace; provider request history | A response resolves the attempt, or stale-send recovery applies the configured contract. |
| failed | Worker log field `reason`, HTTP status and attempt number | Inspect the explicit rejection or exhausted pre-send failures. |
| unknown | Original notification ID, timestamps, trace and provider evidence | Investigate before another submission. The captured Webhook.site header is `X-Notification-ID`. A missing search match is inconclusive. |
| accepted | Persisted provider message ID | The simulated provider accepted the request. There are no recipient delivery receipts in the assessment protocol. |

The following commands read the running Compose stack. Replace `delivery.normal` or the connector name if configured differently.

```sh
curl -fsS http://127.0.0.1:18083/connectors/notification-outbox/status
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group delivery.normal
docker compose exec postgres psql -U notify -d notify -c \
  "SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn) AS retained_bytes FROM pg_replication_slots"
docker compose logs --since 5m worker
```

Redis unavailability prevents new permits. Durable work can keep entering PostgreSQL during a Kafka outage. Once the connector resumes, the outbox replays through Kafka. Repeated ready records reuse the same notification and generation. Malformed records are quarantined with source coordinates in `notifications.invalid`.

Keep an eye on the age of pending work and available disk, especially for extended outages. Admission can eventually fail when PostgreSQL runs out of storage. Compose uses one broker and one database; its interruption tests exercise process recovery on persistent volumes. The suite records the tested conditions and times in [reports](../reports/).
