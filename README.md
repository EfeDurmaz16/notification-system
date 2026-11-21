# Event-driven notification service

A Go service for accepting SMS, email and push notifications and submitting them to an HTTP provider. Clients can create up to 1,000 items in a request, follow progress, and cancel a notification until its first send starts. PostgreSQL records the accepted work. Kafka distributes ready events. A Redis limiter coordinates delivery attempts across worker replicas.

## Run locally

Install Docker with Compose v2 or later. From the repository root:

```sh
docker compose up --build -d
curl http://127.0.0.1:8080/healthz
docker compose ps -a
```

If port 8080 is occupied, prefix Compose commands with `API_PORT=18080` and use `http://127.0.0.1:18080` for the API. For make targets also set `API_URL=http://127.0.0.1:18080`.

A fresh Compose installation uses per-channel batch readers. Each priority has a separate batch topic for SMS, email and push, feeding the same worker pool. Existing installations can [roll out or roll back the routing change](docs/channel-batch-routing.md) while preserving committed work.

The first run downloads the service images, applies versioned SQL migrations, creates Kafka topics, and registers the PostgreSQL CDC connector. Wait for the API health check and the connector-init job to finish before measuring delivery. The default provider is a local simulator. The development ports bind to loopback. Go 1.26.4 is used for host-side builds and tests.

```sh
curl -sS http://127.0.0.1:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-001' \
  -d '{"channel":"sms","recipient":"+12025550123","content":"Your order is ready","priority":"high"}'
```

A successful create returns HTTP 202 with an ID and batch ID. Poll `GET /v1/notifications/{id}`, or connect a WebSocket to `/v1/notifications/{id}/events`. The provider's subsequent 202 records `accepted`: the provider accepted the submission. Recipient receipts require an additional provider contract.

See [OpenAPI](openapi.yaml) for the full HTTP contract and [API examples](docs/api-examples.md) for batches, scheduling, templates and reconnects. [Webhook.site setup and verification](docs/webhook-site.md) uses a disposable endpoint and one synthetic message. Run load experiments only with the local simulator.

## Delivery path

```mermaid
flowchart LR
    C[Client] --> A[Go API]
    A --> P[(PostgreSQL)]
    P -->|Outbox inserts via WAL| D[Debezium]
    D --> K[Standalone and channel batch topics per priority]
    K --> W[Delivery workers]
    W -->|Reserve channel slot| R[(Redis)]
    W -->|Conditional send claim| P
    W --> H[HTTP provider]
    W -->|Record outcome| P
    S[Due job scheduler] -->|Promote and emit in one transaction| P
```

1. Create validates each item, expands templates and fingerprints the request. A transaction stores the batch replay response, immutable accepted payloads and ready outbox events. Invalid items get indexed errors. A repeated key and matching payload returns the original IDs. A conflicting valid payload gets HTTP 409.
2. Debezium routes ready events to a standalone topic per priority or a batch topic per priority and channel. Workers also consume existing shared batch topics during upgrades. The create endpoint sets the lane, including for one-item batches. PostgreSQL preserves it for scheduled deliveries and retries. Consumers load the notification by ID and attempt generation.
3. The shared worker pool chooses a priority with weights 6:3:1 for high, normal and low. Within each priority, standalone requests get more turns. Batch requests continue to receive turns during sustained standalone traffic. Either lane can take all available turns when the other is empty. Both share the channel's Redis quota.
4. A worker changes the matching pending generation to sending in PostgreSQL before making a POST. A successful cancellation prevents that transition. Even if Kafka replays a ready event, the DB predicate fences workers from consuming the same send right.
5. The worker stores the outcome before the consumer commits the fetched offsets. A future retry or scheduled send is promoted by a DB scheduler. The promotion and its outbox insert share a transaction.

The [design decisions and state diagram](docs/decisions.md) explain the options, defaults and recovery boundaries.

See the [standalone versus batch measurements](reports/ingress-share/README.md) to compare dispatch shares under sustained, sparse and single-lane traffic.

## Delivery contract

| Outcome | Worker action |
|---|---|
| Confirmed dial failure before a request was sent | Schedule bounded exponential backoff with jitter. Four attempts including the first. |
| Valid provider 202 with message ID, accepted status and timestamp | Store provider acceptance and message ID. |
| Explicit permanent rejection, such as an invalid recipient (400) | Mark failed. |
| Lost response, malformed acknowledgment, or ambiguous error | Record unknown for a webhook-style provider. Investigate by notification ID before deciding whether to send a new notification. |
| Provider explicitly guarantees idempotency | Reuse the notification ID as the provider key. A bounded retry must fit inside the configured key-retention window. |
| Worker receives SIGTERM during scale-down | Stop taking new work. Finish provider attempts already claimed in PostgreSQL within their original deadline, persist the outcome, and let another worker pick up uncommitted records. |
| Worker disappears while sending | Recover stale sends after two minutes. A webhook-style provider leaves the outcome unknown; an idempotent provider may retry within its remaining attempt and time budget. |

The client create key identifies API intent. The provider key identifies a single notification across attempts. The rate limiter tracks each attempt separately. Cancellation succeeds only before the first send right is acquired. The first-stage TDD transcripts are in [reports/tdd](reports/tdd).

## Verification and packaging

The [design decision history](docs/decision-history.md) records the options, candidate contributions, measurements and reasons for changing course. The [assessment checklist](reports/assessment-requirements.md) links each requirement to implementation or observed evidence. The [channel-reader comparison](reports/channel-readers/README.md) explains the batch latency and throughput trade-off behind the release configuration.

In a [15-minute run across all three channels](reports/sustained-mixed/README.md), the API admitted 36,000 notifications per channel. Each channel recorded 35,640 provider acknowledgements, 180 deliberate rejections and 180 lost responses. An independent check found exactly one provider POST for each of the 108,000 IDs. The [restart and recovery tests](reports/final-recovery/README.md) exercise cases where a retry is required.

| Command | Scope |
|---|---|
| `make verify-all` | Full local suite including benchmarks, load matrix and isolated fault tests. [Verification guide](docs/verification.md) |
| `make check` | Formatting, vet, unit tests, race detector, binary build |
| `make ci` | Local checks, Compose, database integration tests and end-to-end smoke |
| `make burst`, `make load`, `make stress`, `make failures` | Independent local traffic and failure experiments |
| `make performance` | Repeated isolated Go benchmarks and allocation counts |
| `make package` | Source archive, native executables and SHA-256 checksums for the current commit |
| `docker compose up -d --scale worker=3` | Exercise the same channel budget with three workers |
| `docker compose down` | Stop the stack and retain data volumes |

[Verification results and review guide](reports/README.md) include measurements of this implementation, the environment and the exact scope of each run. Run the complete replica and workload comparison with `scripts/bench.sh` against an isolated, idle stack. It temporarily scales workers and resets the local simulator counters. See [failure investigation](docs/operations.md) for manual inspection. The CI workflow runs on GitHub. The release workflow can build a versioned container image and publish packages on a version tag.

## Observability and operating assumptions

The [historical outcome report](docs/outcome-report.md) groups recorded SMS, email and push outcomes by UTC hour, day or week.

- `/metrics` exposes each channel's average accepted, failed and unknown notifications per second over the last five minutes. It also reports logical pending depth, oldest pending age, state counts, terminal outcome counters and an acceptance latency histogram. `/healthz` checks API database readiness. Worker and connector health are separate from admission readiness.
- JSON logs link requests and worker outcomes to identifiers. [Distributed tracing](docs/tracing.md) shows the API, worker and provider boundary with W3C propagation and optional OTLP export.
- The notification row is authoritative. WebSocket subscribers receive current snapshots, may skip intermediate states, and can refetch the resource after reconnecting.
- The simulator and single-node PostgreSQL, Kafka and Redis volumes model process restarts on a development host. Production HA, authenticated tenants, retention and provider receipts require deployment-specific configuration. The API uses one trusted client namespace. Keep the default bindings on loopback.
- Retain outbox rows, notification history, and replay keys for the assessment. Capacity planning must account for these tables and WAL growth during outages. Terminal failed and unknown items remain queryable by status. Malformed stream envelopes are quarantined separately.

## When would we change the design?

| If this changes | What we would do and why |
| --- | --- |
| A real provider replaces Webhook.site | Verify which responses prove rejection, what a 429 and `Retry-After` mean, and how long the provider remembers an idempotency key. If it can return an authoritative receipt using our notification ID, use it to resolve `unknown` after a lost response. [Provider evidence](docs/decision-history.md#what-can-a-replacement-worker-learn-after-a-lost-acknowledgement) |
| The service must select a campaign audience | Today the caller supplies up to 1,000 notifications per request. The 10,000-SMS burst used ten requests. Add a campaign ID, audience and consent rules, and a durable record of which recipients have been scheduled so an interrupted campaign can resume. [Batch contract and evidence](reports/assessment-requirements.md) |
| Independent customers share the API | Authenticate callers, scope status reads and cancellation to the owner, and enforce uniqueness on `(tenant_id, idempotency_key)`. Two customers can then use the same key for unrelated requests. Consider physical sharding if measured load warrants it. |
| A slow provider occupies the shared worker slots | The separate batch readers addressed waiting in Kafka. If other channels start waiting *after fetch*, measure slot occupancy and per-channel latency, then test a channel concurrency limit before splitting worker pools. [Matched reader trials](reports/channel-readers/README.md) |
| Delivery remains the only consumer and independent replay has little value | Compare a PostgreSQL work queue with the current outbox, Debezium and Kafka path. SQL can serve batches, priority claims and analytics. Kafka gives each additional service its own offsets and replay schedule, at the cost of operating the broker, connector and retained WAL. [Persistence and recovery](docs/persistence.md) |
| The service must survive host loss or keep data long term | Agree on acceptable data loss, recovery time and replay windows. Test failover and restore, then verify retention against connector progress and the promised idempotency window before pruning records. [Operating assumptions](docs/operations.md) |
