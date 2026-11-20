# Trace verification

On 2026-11-20 the local Docker Compose stack used PostgreSQL, Debezium, Kafka, Redis, the Go API and worker, a synthetic HTTP provider, and the Jaeger tracing profile. The API host port was 18080.

```console
$ API_PORT=18080 python3 scripts/check-trace.py
PASS http://127.0.0.1:16686/trace/df291e9d036b3ad6ceb723899e7ade90 (notification.api, notification.process, notification.provider)
```

The probe sent a new SMS request, obtained HTTP 202, read the trace from Jaeger's query API and checked both parent span IDs. The provider span is recorded by the worker's HTTP client. The trace ID above belongs to that local Jaeger instance and its in-memory retention period.

```console
$ go test -race -count=1 -v ./internal/observability
=== RUN   TestSetupExportsOTLPOverGRPC
--- PASS: TestSetupExportsOTLPOverGRPC (0.00s)
=== RUN   TestTraceContinuesAcrossDurableQueue
--- PASS: TestTraceContinuesAcrossDurableQueue (0.00s)
PASS
ok      github.com/EfeDurmaz16/notification-system/internal/observability  1.297s
```

The first test exports spans to a loopback gRPC receiver. The second reconstructs the trace from a persisted W3C carrier and checks the outgoing provider header.
