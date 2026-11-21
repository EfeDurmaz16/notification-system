# Delivery decisions

## Product guarantees

| Choice | Consequence for the caller |
|---|---|
| Commit the batch and outbox before replying | On a successful create, each accepted item can immediately be queried. Kafka can be temporarily offline. |
| Require a create idempotency key | Retries of the same intent return the same IDs. A batch keeps the original index of each item error. A repeated key with a different valid request conflicts. |
| Keep recipients and rendered messages immutable | A safe provider retry can use the same key and identical parameters. Two independent campaigns may contain identical content. |
| Allow cancellation until the first send claim | A 204 cancellation wins a conditional state transition. No POST can follow for that notification. A consumer reading a cancelled event skips the send. |
| Independent notifications | An ambiguous campaign SMS cannot hold up a later security alert to the same recipient. Kafka order within a partition supplies no handset delivery guarantee. |
| Preserve an ambiguous HTTP outcome | Webhook-style integrations park it as unknown. A provider with a documented idempotency window may retry with the original notification ID. |
| Budget per channel across all replicas | Retries count. An unavailable coordinator pauses new sends. Backlog stays durable while the API can still commit. |
| Favor standalone work within each priority | A standalone request can bypass a campaign of the same priority. A locally ready batch receives a configurable share of dispatches during contention. Each lane can use idle capacity. |

## Implementation choices for this assessment

- PostgreSQL owns current state, immutable create receipts, the outbox, scheduling deadlines and status history. Queries use the same authoritative database. Separate CQRS read infrastructure would add a replication delay to status and cancellation reads.
- Kafka and Debezium support durable, independently replayable consumer groups. Redis coordinates shared channel permits. Adding Redis Pub/Sub or MQTT to the delivery path would introduce another publication boundary.
- Each priority has a three-partition standalone topic and three single-partition batch topics, one per channel. The original three-partition batch topic remains readable during upgrades. One Go executable runs in API or worker mode. A worker instance also polls for due retries and scheduled items every 250 ms. Row locks allow multiple schedulers to coexist.
- The worker makes up to `RETRY_MAX_ATTEMPTS` attempts including the first (default 6, range 1 to 100). Exponential backoff uses `RETRY_BASE_BACKOFF` (default 2s, positive and at most one minute), equal jitter and a one-minute backoff cap. For six immediate failures, the waits before the final attempt total 31 to 62 seconds. A TCP dial has a two-second deadline inside the five-second HTTP attempt deadline. Two minutes after an uncompleted send claim, the recovery worker applies the same attempt budget and provider policy.
- Set `PROVIDER_IDEMPOTENT=true` only for a provider that guarantees deduplication for the configured `PROVIDER_KEY_WINDOW`. The notification ID and payload stay stable across retries. The bundled simulator rejects a 429 before acceptance. For every other endpoint, set `PROVIDER_429_NOT_ACCEPTED=true` only when its contract provides that guarantee. An explicit false disables this retry even for the simulator. For an eligible retry after 429 or 503, a valid Retry-After date or delay sets the earliest retry. A delay beyond the supported 24-hour window, or past a provider key deadline, ends retries rather than resending early. A generic gateway 503 or 504 can follow acceptance and remains unresolved under the default webhook contract.
- For the demo, content limits are 160 Unicode code points for SMS, 10,000 for email and 256 for push. This validates the exercise's character limits; carrier billing and SMS segmentation need provider-specific rules. Templates accept literal variables and render once at admission.
- One trusted caller namespace, loopback ports, retained status history, and a single node for each infrastructure dependency keep the exercise reproducible. Multi-tenant auth, long-term retention, standby failover and cross-region quotas need deployment-specific decisions.
- Status updates use WebSocket snapshots and the same durable query used by HTTP. The optional tracing profile collects OTLP spans in Jaeger; the metrics endpoint emits Prometheus text.

Further reading: [Kafka concepts](https://kafka.apache.org/intro/), [PostgreSQL logical decoding](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html), [safe retries](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/), [Uber Go style](https://github.com/uber-go/guide), [Effective Go](https://go.dev/doc/effective_go).

## Default batch share

Use a 20% batch weight within each priority. Oracle's ERP example gives interactive transactions 80% of CPU and allows batch jobs to consume at least 20% plus unused capacity. In SMS, Twilio illustrates a 100-message/s sender with a 70/30 split between important and regular traffic and redistributes idle capacity. These examples allocate different resources and classify traffic differently. [Oracle Resource Manager example](https://docs.oracle.com/html/E25494_01/dbrm007.htm) · [Twilio Traffic Shaping](https://www.twilio.com/docs/messaging/features/traffic-shaping)

At the 20% setting our two local saturation runs gave batches 25 to 30 send claims/s and standalone traffic 58 to 60/s. A 10% setting shortened standalone p95 by about four seconds and reduced campaign progress. A 40% setting raised campaign progress to about 42/s and pushed standalone p95 above 21 seconds. For a deployment with a campaign deadline, divide the number of messages by the available seconds, then choose the smallest tested share that can sustain that batch rate while meeting the standalone latency target. [Measured tradeoffs](../reports/ingress-share/README.md)

## State transitions

```mermaid
stateDiagram-v2
    [*] --> scheduled
    [*] --> pending
    scheduled --> pending: due
    scheduled --> cancelled: cancel wins
    pending --> cancelled: cancel before first send
    pending --> sending: claim generation
    sending --> accepted: provider acknowledgment
    sending --> failed: final rejection or exhausted pre-send failures
    sending --> unknown: unresolved outcome
    sending --> retry_wait: retry is safe
    retry_wait --> pending: due with new generation
```
