# Inspecting a delayed or ambiguous notification

Start with the API notification ID, batch ID, channel and status. Use the configuration of the affected worker when interpreting a timeout: provider idempotency and its retention window are external guarantees.

| State | Inspect | Expected next action |
|---|---|---|
| pending | Age in the HTTP metrics, connector status, Kafka consumer lag, Redis availability | An eligible worker acquires a channel permit and the DB send claim. The client may still win cancellation before the first send. |
| scheduled / retry_wait | The due time, DB scheduler and remaining attempts | A new ready event appears when the scheduler promotes the job. |
| sending | Worker logs for the notification ID and its trace; provider request history | A response resolves the attempt, or stale-send recovery applies the configured contract. |
| failed | Worker log field `reason`, HTTP status and attempt number | Inspect the explicit rejection or exhausted pre-send failures. |
| unknown | Original notification ID, timestamps, trace and provider evidence | [Check the Webhook.site request history](#checking-an-unknown-notification-in-webhooksite) if the worker used that provider. |
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

## Checking an unknown notification in Webhook.site

1. Find the affected notification in the API response and copy its `items[].id`. The worker sends that value in the `X-Notification-ID` header.

   ```sh
   curl -fsS "${API_URL:-http://127.0.0.1:8080}/v1/notifications?status=unknown&limit=20"
   ```

2. Open the Webhook.site inbox for the URL that the affected worker used at the time of the attempt. Paste the filter below into the request search and replace `<notification-id>` with the copied UUID, keeping the quotes. A default Compose run sends to the local simulator and has no Webhook.site history. [Webhook.site search syntax](https://docs.webhook.site/api/requests.html#search-query-examples)

   ```text
   method:POST AND headers.x-notification-id:"<notification-id>"
   ```

3. If a request matches, check its header and record the capture UUID and timestamp with the investigation. This confirms HTTP arrival. Keep the notification `unknown` until an authoritative provider receipt confirms acceptance. Multiple matches are evidence of multiple HTTP requests.
4. If nothing matches, verify the inbox and the time range. History may have expired or been deleted. Keep the outcome `unknown`. An empty result does not justify a resend. [Provider evidence and retry policy](decision-history.md#what-can-a-replacement-worker-learn-after-a-lost-acknowledgement)

## Stopping a worker

During a normal worker stop, Compose sends SIGTERM and allows 50 seconds before forcing termination. Kafka intake, quota waits and unclaimed work stop. A worker that already acquired the database send claim can receive and save the provider response within the remaining request budget. A partially processed Kafka poll can replay on another worker. The conditional database claim prevents that replay from submitting the same attempt twice.

The service allows up to 30 seconds for the send path (including its five-second provider timeout), five seconds to persist an outcome, two seconds to release a quota reservation and five seconds to flush telemetry. A provider timeout or a forced process exit can still leave an ambiguous outcome. Run `make drain` to exercise normal termination and `make rebalance` to exercise a forced exit.

## Configuration

All worker replicas in a deployment must use the same database, Kafka cluster, consumer group prefix, Redis instance and provider contract. Changing the group prefix can replay earlier records. Existing DB rows still guard send claims.

| Environment | Meaning |
|---|---|
| `DATABASE_URL` | Required PostgreSQL DSN for both modes. Startup applies migrations under a DB advisory lock. |
| `HTTP_ADDR` | API bind address, default :8080 inside the container. `API_PORT` selects the published host port in Compose. |
| `KAFKA_BROKERS`, `KAFKA_GROUP` | Comma-separated Kafka bootstrap servers and delivery consumer group prefix. |
| `REDIS_ADDR` | Shared rate limiter in host:port form. |
| `WORKER_CONCURRENCY` | Total active worker slots in one replica. Compose starts with 24. |
| `BATCH_ROUTING` | Compose defaults to `channel` and the binary alone defaults to `shared`. Use `dual-read` during migration. See the [routing guide](channel-batch-routing.md). |
| `BATCH_SHARE_PERCENT` | Batch dispatch share from 1 to 49 within a priority when both lanes have work ready on a replica. The initial default is 20. Use the same setting on all workers. |
| `PROVIDER_URL` | Worker submission URL. Compose defaults to the local synthetic HTTP service. |
| `PROVIDER_IDEMPOTENT`, `PROVIDER_KEY_WINDOW` | Default false, window default 24h. Enable only with a confirmed provider contract and set its actual key-retention interval. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional trace collector. The Compose tracing profile supplies Jaeger. |

## Upgrading an existing deployment

Use the [batch routing upgrade and rollback steps](channel-batch-routing.md) to add the nine single-partition channel topics and drain existing outbox work. Each priority retains its three-partition standalone topic and its original three-partition shared batch topic. Compose provisions 15 delivery topics plus the invalid-record topic.

For upgrades from releases predating the standalone ingress lane, provision `notifications.high.single`, `notifications.normal.single` and `notifications.low.single` with three partitions and the same retention and reader permissions as the original shared batch topics. Upgrade the workers before the API so every new topic has consumers when standalone traffic starts. Existing topics and group offsets continue to serve batch work. Rows created before the migration use the batch lane because their original endpoint was not recorded.

For a rollback across the original standalone-ingress change, switch the API back first. Keep the new workers until the single topics have drained and all stored single jobs, including scheduled work and retries, are terminal. Independent consumers needing every notification should subscribe to standalone, shared batch and channel batch topics throughout a rolling upgrade.

The percentage counts dispatch turns within each priority on a worker. If a Kafka poll shows that another record is waiting behind the current poll, the scheduler holds that lane's next turn for up to two seconds while its reader commits and refills. Other priorities can use free workers during this interval. A lane that has waited 30 seconds locally can take the next available turn for its priority ahead of its usual share. Work already waiting for a Redis permit or being sent completes under the same channel quota.

Kafka lag, partition assignment and outstanding sends affect the share of send claims observed across the deployment. For low priority, inspect the `delivery.low.single` standalone group, the `delivery.low.batch.sms`, `delivery.low.batch.email` and `delivery.low.batch.push` channel groups, and the `delivery.low` group for legacy work.
