# Verification

Use a development machine with Go 1.26.4, Docker Engine and Compose v2, Git, Make, Bash and Python 3. The first run needs internet access for images, Go modules and the pinned OpenAPI validator. Keep the default API and provider URLs on localhost. The local suite uses a recording HTTP provider and synthetic recipients.

```sh
make verify-all RESULTS=.local/review
```

Allow approximately 10 to 20 minutes with warm caches on a developer machine, plus image pull and compilation time on a cold host. Runtime varies with machine capacity. Run the complete suite on an otherwise idle stack. It deliberately changes worker replica counts, generates test notifications and resets the mock provider's counters. Existing development data persists in the main stack. Each fault experiment creates a disposable, independently named stack with dedicated volumes and ports. The suite exits at the first failing stage.

| Target | Scope and output under RESULTS |
|---|---|
| `make ci` | Formatting, vet, unit and race tests, builds, static and live OpenAPI validation, full Compose integration and WebSocket/HTTP smoke. The combined target saves the transcript as ci.txt. |
| `make performance` | Five samples per Go microbenchmark, one package at a time, including allocation counts, written to bench.txt. |
| `make matrix` | Seven experiments, each with a JSON report, also environment.txt and summary.md in matrix/. One- and three-replica bursts, sustained load, increasing load, failures, 10,000 notifications and a mixed-channel run. |
| `make recovery` | PostgreSQL-backed accepted work during worker, Kafka, Debezium, Redis and provider interruptions. Saves recovery.json. |
| `make crash` | Abrupt worker death during a provider attempt. Includes a real two-minute stale-send wait, saves crash.json. |

For a single burst, load, stress, mixed-channel or failure run, start Compose with `make up`, then use e.g. `make burst LOAD_FLAGS="-total 1000 -batch 1000"`. Run `make down` to stop the primary stack while retaining its volumes. Set `API_PORT=18080` consistently when 8080 is occupied. Set `POSTGRES_PORT`, `REDIS_PORT`, `KAFKA_PORT` and `PROVIDER_PORT` when their default published ports are occupied.

GitHub CI runs functional checks, isolated benchmarks and a bounded burst. On a version tag, the release workflow requires a clean rerun of CI before publishing release assets and a container image. The complete matrix and fault experiments remain individually runnable on demand. [Distributed trace inspection](tracing.md) and the [one-request webhook.site smoke test](webhook-site.md) have their own setup and commands.
