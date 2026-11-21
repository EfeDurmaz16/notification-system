# Verification

Use a development machine with Go 1.26.4, Docker Engine and Compose v2, Git, Make, Bash and Python 3. The first run needs internet access for images, Go modules and the pinned OpenAPI validator. Keep the default API and provider URLs on localhost. The local suite uses a recording HTTP provider and synthetic recipients.

```sh
make verify-all RESULTS=.local/review
```

Run the complete suite on an otherwise idle stack. It changes worker replica counts, generates test notifications and resets the mock provider's counters. Existing development data persists in the main stack. Each fault experiment creates a disposable, independently named stack with dedicated volumes and ports. Both the crash and rebalance cases wait through the two-minute stale-send recovery interval. Runtime also depends on image downloads, compilation and local capacity. The suite exits at the first failing stage.

| Target | Scope and output under RESULTS |
|---|---|
| `make ci` | Formatting, vet, unit and race tests, builds, static and live OpenAPI validation, full Compose integration and WebSocket/HTTP smoke. The combined target saves the transcript as ci.txt. |
| `make performance` | Five samples per Go microbenchmark, one package at a time, including allocation counts, written to bench.txt. |
| `make matrix` | Seven experiments, each with a JSON report, also environment.txt and summary.md in matrix/. One- and three-replica bursts, sustained load, increasing load, failures, 10,000 notifications and a mixed-channel run. |
| `make recovery` | PostgreSQL-backed accepted work during worker, Kafka, Debezium, Redis and provider interruptions. Saves recovery.json. |
| `make crash` | Abrupt worker death during a provider attempt. Includes a real two-minute stale-send wait, saves crash.json. |
| `make rebalance` | Stop one of three workers during concurrent single and batch sends, verify ownership of the Kafka partitions, and reconcile every notification ID with the synthetic provider. Results go to worker-rebalance/. |
| `make drain` | Send SIGTERM to one worker while the provider is delaying its response. Audit queued and in-flight jobs before and after starting a replacement. Local and GitHub CI save worker-drain.json. |
| `make quarantine` | On a disposable broker, verify the quarantine marker for a malformed record, the committed source offset, and delivery of the following valid notification. Part of local and GitHub CI. Writes quarantine.json. |
| `make wave` | Compare fixed one- and three-worker controls, a replica schedule, and queue-driven scaling on the same rising and falling SMS traffic. Each 175-second wave is followed by a per-ID provider audit. Writes the reports under traffic-wave/. |

The [ingress share experiment](../reports/ingress-share/README.md) compares standalone and batch traffic at the same priority. It measures queue latency, provider acceptance, and the batch share of send claims during intervals when both lanes have waiting work.

Run `make channel-isolation` for the separate [channel admission experiment](../reports/channel-isolation/README.md). It builds two worker images from the pinned baseline, applies the experimental patch to one of them, and compares push latency and SMS throughput. Output goes to `$(RESULTS)/channel-isolation`. The default service build uses the original worker.

Run `make sustained-mixed` separately to exercise all three channels together for 15 minutes on an isolated stack. The workload offers 40 notifications per second per channel, including explicit provider rejections and lost acknowledgements. The [run report](../reports/sustained-mixed/README.md) records the resulting application states, provider observations and five-minute rates. Each invocation writes to its own UTC timestamped directory under `$(RESULTS)/sustained-mixed/`.

For a single burst, load, stress, mixed-channel or failure run, start Compose with `make up`, then use e.g. `make burst LOAD_FLAGS="-total 1000 -batch 1000"`. Run `make down` to stop the primary stack while retaining its volumes. Set `API_PORT=18080` consistently when 8080 is occupied. Set `POSTGRES_PORT`, `REDIS_PORT`, `KAFKA_PORT` and `PROVIDER_PORT` when their default published ports are occupied.

GitHub CI runs functional checks, isolated benchmarks, a bounded burst and the saturated ingress test at a 20% batch target on three workers. On a version tag, the release workflow requires a clean rerun of CI before publishing release assets and a container image. The complete matrix and fault experiments remain individually runnable on demand. [Distributed trace inspection](tracing.md) and the [one-request webhook.site smoke test](webhook-site.md) have their own setup and commands.
