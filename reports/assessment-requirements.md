# Assessment requirements and evidence

This checklist covers the notification service, the six optional features and the six requested deliverables. A provider acknowledgement confirms submission to the simulated provider. Delivery to a handset or inbox would require a separate provider receipt.

## Required behavior

| Requirement | Implementation and evidence |
| --- | --- |
| Create with recipient, channel, content and priority | The single and batch routes validate these fields and return stable IDs. [API contract checks](api-contract.md) |
| Create up to 1,000 notifications per request | Per-item validation returns indexed errors. Ten concurrent requests of 1,000 produced the 10,000-SMS test. [Burst results](bench/tuned/summary.md) |
| Query by notification or batch ID | Both endpoints read committed state. [Live API contract](api-contract.md) |
| Cancel a pending notification | Cancellation and the first send claim compete for the same row. A successful cancellation produces zero provider POSTs. [Cancellation test](tdd/README.md) |
| Filter by status, channel and date range with pagination | List queries support creation-time bounds and both cursor and limit/offset pagination. [Query implementation](../internal/store/queries.go) and [HTTP tests](../internal/api/api_test.go) |
| Asynchronous queue workers | The API commits the notification and its outbox event together. Workers claim ready items and persist outcomes before committing offsets. [Delivery path](../README.md#delivery-path) |
| Maximum 100 messages per second per channel | Replicas share a separate rolling quota for each channel. Retries consume permits. The fixture measured a peak of 50 actual POSTs in a sliding second on each channel in the sustained run. [Per-channel results](sustained-mixed/README.md) |
| High, normal and low priorities | A weighted scheduler serves all three. Measurements cover competition and progress under load. [Priority experiment](high-p95/README.md) |
| Validate required fields and character limits | Channel-specific recipient checks and limits apply after template expansion. [Validation rules](../internal/api/validate.go) |
| Prevent duplicate sends | Create keys replay the original IDs. A conditional send claim fences replayed queue events. Provider-side retries after ambiguity require an explicit idempotency guarantee. [Replay tests](api/checks.md) and [recovery results](final-recovery/README.md) |
| Design delivery and retry behavior | Confirmed pre-send failures back off with jitter within a retry budget. Rejections, missing acknowledgements and abandoned attempts receive distinct treatment. [State transitions](../docs/decisions.md#state-transitions) |
| Live queue depth, success/failure rates and latency | One scrape exposes pending depth, acceptance latency and the five-minute accepted, failed and unknown rates for each channel. [Live rate audit](sustained-mixed/window-check.json) |
| Structured logs and correlation IDs | JSON request logs contain the correlation ID and trace ID. The worker logs the create correlation ID alongside the notification and trace IDs. [API logging](../internal/api/api.go) and [worker logging](../internal/delivery/engine.go) |
| Health check | The health endpoint checks database readiness for API admission. Worker and connector health can be inspected separately. [Health contract](../openapi.yaml) |
| External provider integration | A disposable webhook.site endpoint captured one synthetic **email** POST with `to`, `channel` and `content`. The test checked the configured HTTP 202 accepted response and its provider message ID. The engine validates the acknowledgement timestamp. [Recorded smoke run](webhook-site.md) and [test input](../scripts/webhook-compose.py) |

## Capacity and visibility

- The [v1.1.0 release verification](https://github.com/EfeDurmaz16/notification-system/releases/tag/v1.1.0) reconciled 108,000 IDs against actual provider POST counts, including deliberate rejections and lost responses. Consumers can query or watch the resulting status. Operators can inspect rates, logs and traces. [Run and limits](sustained-mixed/README.md)
- A separate volume experiment seeded 2,160,000 historical notifications across one simulated day and added 7,500 recent outcomes. It verified window expiry and measured scrape cost. Sustained end-to-end delivery for a full day and production provider capacity remain unmeasured. [Volume experiment](volume-simulation/README.md)

## Optional features

| Feature | Evidence |
| --- | --- |
| Failure handling | [Dependency interruption and recovery](final-recovery/README.md), [invalid-event quarantine](quarantine/README.md) |
| Scheduled delivery | [Database-backed scheduling tests](../internal/store/store_test.go) |
| Templates with variable substitution | [Template and validation tests](../internal/api/api_test.go) |
| WebSocket updates | [End-to-end status subscription](../tests/integration/workflow_test.go). Clients receive current snapshots and can refetch after reconnect. |
| Distributed tracing | [Recorded API, worker and provider-client span chain](trace.md) |
| Automated CI/CD testing and linting | [Test and lint workflow](../.github/workflows/ci.yml) and [tagged release workflow](../.github/workflows/release.yml) |

## Deliverables

| Deliverable | Entry point |
| --- | --- |
| Git repository and reviewable commit history | [Public source and history](https://github.com/EfeDurmaz16/notification-system/commits/main/) |
| Setup, architecture and API examples | [README](../README.md) and [API examples](../docs/api-examples.md) |
| Single-command Compose setup | `docker compose up --build -d` from the repository root. [Compose file](../docker-compose.yml) |
| API specification | [OpenAPI](../openapi.yaml) |
| Versioned database migrations | [SQL migrations](../internal/store/migrations/) |
| Test suite runnable with one command | `make ci` runs the functional suite with its service dependencies. `make verify-all` also runs benchmarks, load and recovery experiments. [Verification guide](../docs/verification.md) |
