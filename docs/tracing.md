# Follow a notification trace

Use the local provider and enable the optional Jaeger service:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317 docker compose --profile tracing up --build -d
python3 scripts/check-trace.py
```

The check submits one synthetic SMS, polls Jaeger for the incoming trace ID and prints the trace URL after observing the API server span, worker consumer span and provider client span. The web UI listens at [localhost:16686](http://localhost:16686).

The API records W3C `traceparent` alongside the notification and the outbox event. The worker extracts the context of the persisted notification when Kafka dispatches it. Each provider call gets a separate client span in the original trace. Client span completion measures local request handling. A provider-side span requires instrumentation in the provider. Recipient, message text and webhook URLs are excluded from application span attributes.

Without an exporter endpoint, traces still propagate and no OTLP connection is attempted. Configure sampling through standard SDK variables, for example `OTEL_TRACES_SAMPLER=parentbased_traceidratio` and `OTEL_TRACES_SAMPLER_ARG=0.05` in each service environment when running benchmarks.

Run the portable propagation and real loopback gRPC export tests with:

```sh
go test -race -v ./internal/observability
```

## Find slow or unsuccessful work

The API exposes Prometheus text format on `/metrics`. Queue depth counts runnable pending notifications, including work waiting for a permit. Scheduled jobs and delayed retries have separate status gauges. Queue depth excludes Kafka records for work that has already been handled or cancelled. Use a single API scrape target for the following aggregate database metrics. Multiple API replicas would expose the same values.

Read all nine channel and outcome rates in one request:

```sh
curl -fsS http://127.0.0.1:8080/metrics | grep '^notification_outcomes_per_second{'
```

For each of `sms`, `email` and `push`, the response includes `accepted`, `failed` and `unknown`. Each gauge counts notifications whose terminal outcome was recorded in the trailing five minutes and divides by 300 seconds. The database samples the interval `(now - 5 minutes, now]` on each request. Empty combinations report zero, including immediately after startup on an empty database. An accepted SMS counts once even when the provider needed several attempts. `accepted` means confirmed provider acceptance. `unknown` means the service could not establish the outcome.

| Question | Query |
| --- | --- |
| How many runnable jobs are pending? | `notification_queue_depth` |
| How many SMS became unknown per second over the last five minutes? | `notification_outcomes_per_second{channel="sms",status="unknown",window="5m"}` |
| How long has the oldest runnable job been waiting since creation? | `notification_oldest_pending_seconds` |
| How many provider accepts per second over five minutes? | `rate(notification_outcomes_total{status="accepted"}[5m])` |
| How many new terminal outcomes of each kind? | `sum by (status) (increase(notification_outcomes_total[5m]))` |
| What is p95 from notification creation to provider acceptance? | `histogram_quantile(0.95, rate(notification_acceptance_latency_seconds_bucket[5m]))` |

The acceptance latency histogram includes time spent waiting for a scheduled notification. For a slow notification, compare the client span duration with the interval between the API and worker spans. The latter includes database commit, CDC publication, Kafka consumption, permit acquisition and any deliberate schedule delay.
