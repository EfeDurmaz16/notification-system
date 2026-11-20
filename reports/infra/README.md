# Compose integration

Verified on 20 November 2026 with OrbStack on macOS arm64, Docker 29.4.0, Compose 5.1.2, and Go 1.26.4. The first-party service image contains the API/worker and synthetic HTTP provider binaries. It runs as UID 65532. All host ports bind to loopback.

| Stage | Command or probe | Observed result |
| --- | --- | --- |
| Declarative validation | `docker compose config --quiet` and `shellcheck deploy/register.sh` | Both exited 0. |
| Start data plane | `docker compose up -d postgres redis kafka topics connect` | PostgreSQL, Redis, Kafka and Connect healthy. Topic initialization exited 0. |
| Start application and CDC | `API_PORT=18080 docker compose up --build -d --wait --wait-timeout 300` | Exit 0. API and provider healthy, worker running, connector-init exited 0. |
| API readiness | `curl -fsS http://127.0.0.1:18080/healthz` | `{"status":"ok"}` |
| CDC readiness | `curl -fsS http://127.0.0.1:18083/connectors/notification-outbox/status` | Connector and task both reported RUNNING, version 3.4.3.Final. |
| Optional OTLP export | Enable the tracing profile and set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317` | Jaeger indexed spans from notification-api and notification-worker. See [trace verification](../trace.md). |
| End-to-end delivery | Real POST to the API, PostgreSQL outbox, CDC, Kafka worker, Redis and HTTP provider | See the [Compose smoke transcript](../ci/compose-smoke.txt). |

The API normally publishes on 8080. An existing local service occupied that port, so this run used 18080. The first Kafka startup also exposed that a new named volume mounted under a custom /tmp path belonged to root. The final configuration mounts the volume at the Kafka image's worker-owned /var/lib/kafka/data and was verified with a fresh volume.

## Runtime image inventory

| Image | Pulled digest |
| --- | --- |
| postgres:17-alpine | `sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193` |
| apache/kafka:4.1.2 | `sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e` |
| redis:7.4-alpine | `sha256:7f7d30b7fdd09467bac58198a1f166dd69789d348d4fe859cccb20e0cf287fbe` |
| quay.io/debezium/connect:3.4 | `sha256:1376da3485e149e6dc76c855d212a65c24673e1925d8749f733183e5b7fdc22b` |
| curlimages/curl:8.17.0 | `sha256:935d9100e9ba842cdb060de42472c7ca90cfe9a7c96e4dacb55e79e560b3ff40` |
| jaegertracing/all-in-one:1.73.0 | `sha256:fe884668a2bf8d586b6fb7697d48a6a19a02f9e78a1eddf09c5b0f08c27d63b9` |

Three priority topics have three partitions each. The invalid-event quarantine has one partition. The single-node Kafka volume also retains the Connect config, status and offset topics. PostgreSQL persists notification state, outbox rows and the logical replication slot. Redis persists granted permits using AOF with appendfsync always. The demo requires one functioning host and its volumes. Recovering after losing the host or volumes requires a separate backup and replication plan.
