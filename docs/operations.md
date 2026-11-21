# Inspecting a delayed or ambiguous notification

Start with the API notification ID, batch ID, channel and status. Use the configuration of the affected worker when interpreting a timeout: provider idempotency and its retention window are external guarantees.

| State | Inspect | Expected next action |
|---|---|---|
| pending | Age in the HTTP metrics, connector status, Kafka consumer lag, Redis availability | An eligible worker acquires a channel permit and the DB send claim. The client may still win cancellation before the first send. |
| scheduled / retry_wait | API field `nextAttemptAt`, DB scheduler and remaining attempts | A new ready event appears when the scheduler promotes the job. |
| sending | Worker logs for the notification ID and its trace; provider request history | A response resolves the attempt, or stale-send recovery applies the configured contract. |
| failed | GET the notification for `lastReason`, `providerHttpStatus` (if a response arrived) and `attempt` | Inspect the explicit rejection or exhausted pre-send failures. |
| unknown | API field `lastReason`, original notification ID, timestamps, trace and provider evidence | [Check the Webhook.site request history](#checking-an-unknown-notification-in-webhooksite) if the worker used that provider. |
| accepted | Persisted provider message ID | The simulated provider accepted the request. There are no recipient delivery receipts in the assessment protocol. |

The notification detail, list and WebSocket snapshots include the latest recorded reason and provider HTTP status when available. A `202` with `lastReason: malformed_ack` means the provider response could not confirm acceptance. A `send_lease_expired` means a worker stopped recording progress after claiming the attempt. Scheduled and retrying notifications expose `nextAttemptAt`. Rows created before delivery evidence was introduced can have no recorded reason.

Filter the API and worker logs by the `X-Correlation-ID` returned on the original create response. For a batch, narrow the worker results by `notification_id`. A later status lookup or replay can have its own request correlation ID. Provider result logs include `provider_message_id` when a receipt exists. Transient database failures allow up to three attempts to save the same receipt within five seconds. Saving an identical result for the same generation leaves its timestamp and status history intact. A conflicting result or a different generation stops the write. These attempts issue no further provider POST. If `persistence_confirmed` is false, check the database and receipt before acting. The database write may have committed before its response was lost.

If PostgreSQL returns an error when saving a confirmed acceptance, the worker tries to buffer its receipt in Redis. A non-transient error can take this path after the first write. The buffer stores the notification ID, attempt, HTTP status and provider message ID in the shared Redis set `notifications:{receipts}`. Compose persists this set with AOF and fsync on each write. Every worker checks it every five seconds. Recovery writes the receipt to PostgreSQL and removes the Redis entry only after confirmation. It can repair `sending` or an expired lease for the same attempt, including `unknown` with reason `send_lease_expired`. A superseded attempt or conflicting outcome moves to `notifications:{receipts}:conflicts` for operator inspection. Inspect backlog size with `docker compose exec redis redis-cli SCARD 'notifications:{receipts}'` and use the same command with the `:conflicts` suffix for conflicts. Receipt recovery never calls the provider. A crash before buffering, or simultaneous loss of both stores, can still leave the outcome unknown.

The following commands read the running Compose stack. Under the default `isolated` routing, normal-priority standalone SMS uses group `delivery.normal.single.sms`. For other traffic, substitute the actual priority, request type and channel (e.g. `delivery.high.batch.push`). Adjust the prefix and connector name for your deployment.

```sh
curl -fsS http://127.0.0.1:18083/connectors/notification-outbox/status
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group delivery.normal.single.sms
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

Each worker image probes `127.0.0.1:8082/readyz` inside its own container. Run `docker compose ps worker` for the health result or `docker compose exec --index 1 worker wget -S -O /dev/null http://127.0.0.1:8082/readyz` to probe a specific replica. Readiness checks PostgreSQL, Redis and Kafka under one two-second deadline. While the listener is serving, dependency failure returns HTTP 503. At shutdown the probe server closes alongside the worker, so a probe can instead see a closed connection. A reachable broker may still have consumer lag. The separate `/livez` endpoint checks only the HTTP process. The worker probe sends no provider requests. Outside Compose, `WORKER_HEALTH_ADDR` sets the probe bind address and `HEALTH_URL` selects the image probe URL.

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
| `LOG_LEVEL` | JSON log threshold for API and worker. Default `INFO` includes HTTP access status and confirmed provider acceptances. |
| `RETRY_MAX_ATTEMPTS`, `RETRY_BASE_BACKOFF` | Six total attempts by default, with equal jitter on a two-second starting delay. Applies only to eligible retries. |
| `PROVIDER_429_NOT_ACCEPTED` | Defaults to true for the bundled `http://provider:8081/send` and false for every other URL. Enable for an external provider only with a guarantee that the throttled POST has no side effect. |
| `PROVIDER_ACK_MODE` | Defaults to `assessment`, the full HTTP 202 receipt. Use `message_id` when the provider defines a successful HTTP response with a JSON message ID as proof of acceptance. [Response examples](webhook-site.md#other-provider-contracts) |
| `PROVIDER_403_404_ENDPOINT` | Defaults to true for `webhook.site` and its subdomains. Set true for a provider that uses 403 or 404 for shared endpoint access problems. Set false when those codes reject an individual recipient or request. |
| `WORKER_CONCURRENCY` | Total active worker slots in one replica. Compose starts with 24. |
| `BATCH_ROUTING` | Compose defaults to `isolated` and the binary alone defaults to `shared`. Use `read-isolated` to drain both formats when upgrading from `channel`. See the [routing guide](channel-batch-routing.md). |
| `BATCH_SHARE_PERCENT` | Batch dispatch share from 1 to 49 within a priority when both lanes have work ready on a replica. The initial default is 20. Use the same setting on all workers. |
| `PROVIDER_URL` | Worker submission URL. Compose defaults to the local synthetic HTTP service. |
| `PROVIDER_IDEMPOTENT`, `PROVIDER_KEY_WINDOW` | Default false, window default 24h. Enable only with a confirmed provider contract and set its actual key-retention interval. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional trace collector. The Compose tracing profile supplies Jaeger. |

## Upgrading an existing deployment

Use the [delivery topic upgrade and rollback steps](channel-batch-routing.md) to provision all lanes and drain existing outbox work. Compose provisions nine standalone channel topics with three partitions each, nine batch channel topics with one partition each, six legacy topics and the invalid-record topic.

For upgrades from releases predating the standalone ingress lane, provision `notifications.high.single`, `notifications.normal.single` and `notifications.low.single` with three partitions and the same retention and reader permissions as the original shared batch topics. Upgrade the workers before the API so every new topic has consumers when standalone traffic starts. Existing topics and group offsets continue to serve batch work. Rows created before the migration use the batch lane because their original endpoint was not recorded.

For a rollback across the original standalone-ingress change, switch the API back first. Keep the new workers until the single topics have drained and all stored single jobs, including scheduled work and retries, are terminal. Independent consumers needing every notification should subscribe to legacy and channel-specific topics for both ingress lanes throughout a rolling upgrade.

The percentage counts dispatch turns within each priority on a worker. If a Kafka poll shows that another record is waiting behind the current poll, the scheduler holds that lane's next turn for up to two seconds while its reader commits and refills. Other priorities can use free workers during this interval. A lane that has waited 30 seconds locally can take the next available turn for its priority ahead of its usual share. Work already waiting for a Redis permit or being sent completes under the same channel quota.

Kafka lag, partition assignment and outstanding sends affect the share of send claims observed across the deployment. For low priority, inspect the `delivery.low.single.<channel>` and `delivery.low.batch.<channel>` groups for SMS, email and push, plus the legacy `delivery.low.single` and `delivery.low` groups.

Workers log `Kafka broker connection failed` on a broker dial or connection initialization failure, even when the Kafka client retries internally. All readers on a worker share one warning per 30 seconds. The warning includes the numeric broker ID and error type. Check broker health, connectivity and credentials if it repeats.

Scheduler database errors and Redis client diagnostics repeat at most once per 30 seconds during a continuous outage on one worker. The scheduler logs its recovery with the elapsed time and number of failed ticks. Storage failures include an error category and, when PostgreSQL supplied one, its SQLSTATE code. API access logs record successful health checks at `DEBUG` and failed checks at `WARN`.

### Provider outage cooldown

After one replica sees three consecutive endpoint failures on the same channel, Redis pauses new quota grants for that channel across all replicas for 30 seconds. HTTP 5xx, 429, 401, connection failures and missing, malformed or unexpected acknowledgements count toward the threshold. A valid acceptance or a recipient/content rejection clears that replica's streak. Ambiguous HTTP responses such as redirects, 408, 409 and 410 leave the streak unchanged, as does a local deadline before dispatch. For example, 503, 503, 408, 503 triggers a pause on the final response. Inspect worker logs for `provider admissions paused` and the reported channel and HTTP status. For 401, verify the credentials. When `PROVIDER_403_404_ENDPOINT` is enabled, 403 and 404 count toward the pause. Check endpoint expiry, access and the configured response. The current non-idempotent attempt stays `unknown` with reason `provider_endpoint_access`. With this setting disabled, those codes record a rejection and clear the streak.

A queued notification keeps its current state until it can obtain a permit. Work already admitted can still reach the provider, and its outcome follows the normal retry rules. Expiry resumes paced admission. A replica that tripped its threshold discards that streak and ignores responses observed during its cooldown. It must then see three fresh consecutive failures to request another pause. The threshold is local to each replica, so sparse failures spread over different replicas can take longer to trigger protection. A pause never resends an `unknown` notification.
