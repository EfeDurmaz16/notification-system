# Event-driven notification service

Submit SMS, email or push notifications, individually or in batches of up to 1,000. The API records work in PostgreSQL before replying. Workers deliver it asynchronously and share a rolling limit of 100 provider attempts per second per channel.

## Run one notification

From a fresh checkout, start Docker Compose v2 and send a synthetic SMS to the bundled simulator. The first start downloads the images and registers the CDC connector.

```sh
docker compose up --build -d --wait
api=http://127.0.0.1:8080
curl -fsS "$api/v1/notifications" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: first-demo' \
  -d '{"channel":"sms","recipient":"+12025550123","content":"Your order is ready","priority":"high"}'
```

Copy the returned `id` into the command below. The create response (`202`) confirms that the API saved the notification. Once the provider acknowledges it, the query returns `"status":"accepted"` and its `messageId`. A recipient delivery receipt would require a separate provider signal.

```sh
curl -fsS "$api/v1/notifications/<id>"
```

To exercise the **external provider in the assessment**, run `scripts/webhook-smoke.sh`. It starts an isolated Compose project, configures a disposable Webhook.site endpoint, sends one synthetic email and checks both the persisted receipt and captured POST. [Manual configuration and example acknowledgement](docs/webhook-site.md)

## Review the implementation

| Question | Where to look |
| --- | --- |
| Which requirements and bonuses are covered? | [Assessment checklist](reports/assessment-requirements.md), [API examples](docs/api-examples.md), [OpenAPI](openapi.yaml) |
| Why this queue and delivery policy? | [Decisions and state diagram](docs/decisions.md), [measured alternatives and contributions](docs/decision-history.md) |
| Does it keep processing batch work during interactive traffic? | [Standalone versus batch](reports/ingress-share/README.md), [SMS and push reader comparison](reports/channel-readers/README.md) |
| What happened when a worker died? | [Graceful exit](reports/worker-drain/README.md), [hard stop with a per-ID audit](reports/channel-cutover/rebalance/README.md) |
| Which version ran the 108,000-ID test? | [v1.1.0 report and build provenance](https://github.com/EfeDurmaz16/notification-system/releases/download/v1.1.0/notification-system-verification-report.md), [raw evidence archive](https://github.com/EfeDurmaz16/notification-system/releases/tag/v1.1.0) |

In that 15-minute **v1.1.0** run (`fa3042f`, independent SMS, email and push batch readers), the local simulator recorded one POST per ID. Each channel received 36,000 notifications: 35,640 acknowledgements, 180 deliberate rejections and 180 deliberately lost responses. The candidate chose the delivery guarantees and scheduling trade-offs. A coding assistant implemented those choices and ran the experiments. [Decision history](docs/decision-history.md)

## Delivery path

```mermaid
flowchart LR
    C[Client] --> A[Go API]
    A -->|Notification and outbox in one commit| P[(PostgreSQL)]
    P -->|Outbox inserts via WAL| D[Debezium]
    D --> K[Kafka topics by priority and request type]
    K --> W[Delivery workers]
    W -->|Reserve channel quota| R[(Redis)]
    W -->|Claim pending attempt| P
    W --> H[HTTP provider]
    W -->|Record outcome| P
    S[Worker scheduler] -->|Publish due work through outbox| P
```

- **Admission.** A required `Idempotency-Key` identifies each create request. Repeating the same key and body returns the original IDs. Batch admission returns indexed errors for invalid items. Both the 1,000-item maximum and the 5 MiB request-body limit apply.
- **Dispatch.** PostgreSQL keeps the payload. Kafka distributes ready IDs. A shared worker pool selects high, normal and low priority in a 6:3:1 ratio when all are ready. Within a priority, batches receive a default 20% share while standalone traffic is waiting. Independent batch readers keep an email or push batch from sitting behind an SMS campaign on the same Kafka partition.
- **Cancellation.** A notification can be cancelled while waiting for quota. Cancellation and the first send compete for the same PostgreSQL row. Once the worker changes it to `sending`, cancellation closes. A replay cannot claim the same attempt twice.
- **Retries.** A confirmed pre-send failure can retry with exponential backoff and jitter. A timeout after the POST or an invalid receipt leaves a webhook-style provider outcome `unknown`. A gateway may return 503 or 504 after the provider accepted the request. Repeating it safely requires a provider guarantee. See the [retry settings and recovery rules](docs/decisions.md#implementation-choices-for-this-assessment). Repeated endpoint failures [pause new attempts on the affected channel](docs/operations.md#provider-outage-cooldown).

## API and verification

| Operation | Endpoint |
| --- | --- |
| Create one / create a batch | `POST /v1/notifications` / `POST /v1/batches` |
| Read one / read a batch | `GET /v1/notifications/{id}` / `GET /v1/batches/{id}` |
| List, filter and paginate | `GET /v1/notifications` |
| Cancel pending / watch snapshots | `POST /v1/notifications/{id}/cancel` / `WS /v1/notifications/{id}/events` |
| API database readiness / operational metrics | `GET /healthz` / `GET /metrics` |

| Command | What it exercises |
| --- | --- |
| `make test` | Unit tests. Service-dependent tests skip when their dependencies are absent. |
| `make ci` | Lint, vulnerability scan, unit and race tests, builds, live OpenAPI checks and Docker-backed integration |
| `make verify-all` | Local CI, benchmarks, load matrix, worker shutdown and dependency interruptions |
| `make performance`, `make load`, `make stress`, `make burst` | Isolated Go benchmarks or individual traffic profiles |
| `make sustained-mixed` | 15-minute SMS, email and push workload, with a per-ID audit |
| `make package` | Source archive, native executables and checksums for the current commit |

For a port conflict, set `API_PORT=18080` before starting Compose and use `http://127.0.0.1:18080`. `docker compose down` stops the stack and retains its data. The [verification guide](docs/verification.md) lists host prerequisites, timings and result files. The [operations guide](docs/operations.md) covers queues, investigation by ID and ambiguous outcomes. [Tracing](docs/tracing.md) and the [historical outcome report](docs/outcome-report.md) cover cross-service diagnosis and UTC reporting.

## Operating limits and further improvements

| Current boundary | Next change and evidence needed |
| --- | --- |
| Webhook.site simulates HTTP acceptance. Production providers differ on rate limits, idempotency windows and receipts. | Test a lost acknowledgement and a documented throttle response against the actual provider. Identify a stable reference for lookup before enabling automatic reconciliation. |
| Compose runs a single PostgreSQL, Kafka and Redis node. Replay keys are retained. Cleanup of history and published outbox events is opt-in. | Choose [retention windows and a cleanup schedule](docs/persistence.md), then validate failover and disk use under sustained writes. A seeded 2.16-million-row database took 444 ms per median metrics scrape in the [original volume experiment](reports/volume-simulation/README.md). |
| The caller supplies recipients in a single trusted namespace. | For multi-tenant traffic, authenticate and scope keys and reads by tenant. For audience selection, add campaign progress and consent checks. |
| Readers separate batch channels. Send slots are shared. | Measure slot occupancy under a slow provider before introducing channel-specific limits. [Routing measurements and rollback](docs/channel-batch-routing.md) |
