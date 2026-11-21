# Load, stress and performance check

The checked service revision was `ad607d365736`. The traffic harness used the real API, PostgreSQL, Debezium, Kafka, three workers with 24 slots each, Redis and a local HTTP provider with a 20 ms response delay. Each run used SMS only and rotated high, normal and low priority across its inputs. The standalone lane has its own [competition test](../ingress-share/README.md).

| Command | Workload |
|---|---|
| `make load` | 600 notifications at a constant 50/s, submitted in 60 batches of 10. |
| `make stress` | 1,200 notifications submitted in batches of 20. Each third of the items uses an offered rate of 75, 150 or 300/s. The harness measures the time required to drain after the last API response. |
| `make performance` | Five repetitions each of SMS validation, provider request construction, in-process delivery, simulator rolling-window accounting and the load generator's percentile calculation. Measures ns/op, bytes/op and allocations/op on the host. |

## Results

All three commands exited successfully. Each traffic test ran once on the local stack. The provider accepted all 1,800 notifications. A post-run audit found exactly one provider POST for each of the 1,800 IDs and zero receipt read errors.

| Scenario | Accepted | Create response p95 | Create start to persisted acceptance p95 | Terminal results/s | Time to drain after last create response | Max SMS POSTs in a rolling second |
|---|---:|---:|---:|---:|---:|---:|
| [Load](load.json) | 600 | 5.87 ms | 280 ms | 49.77 | 0.25 s | 59 |
| [Stress](stress.json) | 1,200 | 5.37 ms | 4.86 s | 82.63 | 5.25 s | 93 |

Under stress the last two stages offered more than the 100 SMS/s channel quota. The full trial lasted 14.52 seconds including drain. See the JSON timeline for the number of accepted inputs still awaiting terminal observation at each one-second sample.

On a Go 1.26.4 Apple M5 Pro host the in-process delivery microbenchmark measured 3.394 to 3.732 µs/op across five repetitions, with 80 allocations/op. This benchmark uses an in-memory store and HTTP transport. The real service latency is measured by the load and stress trials above. [All five microbenchmarks and sample values](bench.txt)

## Reproduce

Use a dedicated local Compose project. The traffic harness resets the simulator counters at the start of each scenario. Select unused loopback ports and stop other test traffic during the run.

```sh
export COMPOSE_PROJECT_NAME=notify-suite-check APP_IMAGE=notification-system:suite-check
export API_PORT=62494 PROVIDER_PORT=62495 POSTGRES_PORT=62496 REDIS_PORT=62497 KAFKA_PORT=62498 CONNECT_PORT=62499
export PROVIDER_URL=http://provider:8081/send PROVIDER_IDEMPOTENT=false PROVIDER_LATENCY=20ms
export WORKER_CONCURRENCY=24 BATCH_SHARE_PERCENT=20 OTEL_EXPORTER_OTLP_ENDPOINT=
make performance RESULTS=.local/suite-check
docker compose up --build -d --wait --scale worker=3
make load RESULTS=.local/suite-check LOAD_FLAGS=-audit-receipts
make stress RESULTS=.local/suite-check LOAD_FLAGS=-audit-receipts
docker compose down -v
```

The optional receipt audit runs after collection of latency and aggregate counters. It counts actual provider POSTs for each accepted ID. Create-start to persisted terminal latency ends at the timestamp of the status update. API response latency ends after the initial batch creation response. Observed throughput is the number of terminal results divided by elapsed time including final polling.

## Other entry points

- `make burst`, `make mixed` and `make failures` run a concurrent burst, three-channel traffic and controlled provider rejection/lost-acknowledgement scenarios. `make matrix` runs seven scenarios, including one- and three-worker bursts and a 10,000-item per-ID receipt audit.
- `make ci` runs formatting checks, vet, unit and race tests, builds, OpenAPI validation, database and stream integration tests, and the HTTP/WebSocket smoke and response contract checks. `make verify-all` additionally runs microbenchmarks, the traffic matrix and dependency/crash recovery scenarios.
- GitHub Actions runs functional and integration tests, microbenchmarks, a 300-item burst and a three-worker saturated single/batch test at 20% batch weight. The full `load` and `stress` scenarios run through the local targets above. [CI workflow](../../.github/workflows/ci.yml)
