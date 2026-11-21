# Same-priority SMS under single and batch traffic

## Local results

Under sustained contention, the observed batch share rose with the configured weight in both sweeps. Standalone throughput exceeded batch throughput in all six runs. A worker uses the configured weight for arbitration within one priority; the achieved cluster share includes in-flight work and periods when one local lane has spare capacity. The displayed ranges are the minimum and maximum of two runs. They are not confidence intervals.

| Configured batch weight | Observed batch share | Batch claims/s | Standalone claims/s | Standalone queue p95 | Standalone acceptance p95 |
|---:|---:|---:|---:|---:|---:|
| 10% | 23.34% to 24.34% | 19.87 to 21.91 | 65.26 to 68.09 | 12.35 to 13.26 s | 12.37 to 13.28 s |
| 20% | 30.40% to 33.73% | 25.15 to 30.44 | 57.58 to 59.80 | 15.71 to 16.99 s | 15.73 to 17.01 s |
| 40% | 46.36% to 47.36% | 41.87 to 42.03 | 46.72 to 48.45 | 21.32 to 22.47 s | 21.34 to 22.49 s |

The client reached 139.97 to 140.03 standalone admissions/s during the mixed saturation runs, exceeding even the SMS channel's total quota. With occasional standalone arrivals during a 1,600-item campaign, the observed standalone acceptance p95 was below one second for every setting.

| Configured batch weight | Batch-only throughput | Single-only throughput | Sparse standalone acceptance p95 (24 samples) | Sparse campaign throughput |
|---:|---:|---:|---:|---:|
| 10% | 86.84/s | 87.80/s | 0.872 s | 87.22/s |
| 20% | 84.30/s | 85.66/s | 0.803 s | 79.65/s |
| 40% | 87.24/s | 88.98/s | 0.927 s | 79.80/s |

Raw JSON for the forward sweep: [10%](share-10.json), [20%](share-20.json), [40%](share-40.json). Saturated reverse sweep: [40%](repeat-40.json), [20%](repeat-20.json), [10%](repeat-10.json).

## Late standalone SMS during 10,000 campaign deliveries

At 20% the one-item batch control arrived with 9,492 campaign items outstanding and reached recorded provider acceptance in 108.312 s. The first standalone was created 7.19 ms after the control with the same recipient, priority and content. It took 100.46 ms, leaving 9,483 campaign items outstanding at acceptance. The eight standalone measurements ranged from 100.46 to 595.51 ms. The campaign finished in 114.379 s (87.43/s). [Per-probe timestamps, outstanding counts and provider audit](campaign-10000.json).

Across the 16 final local workloads, there were 41,281 initial provider POSTs and 41,281 accepted notifications. There were zero provider rejections, lost responses or deduplicated attempts. The maximum provider observation was 96 SMS attempts in a rolling second. All persisted notification priorities were `low`; the outbox routed each item to the expected low batch or low standalone topic.

## Environment and workload

All scenarios use the real REST endpoints, PostgreSQL, Debezium, Kafka, Redis and the local provider with a 20 ms response delay. Each worker runs 24 concurrent handlers. The three workers share a channel-wide limit of 100 provider attempts per rolling second. The fixture receives synthetic low-priority SMS only.

| Profile | Campaign items | Standalone items | Standalone arrivals |
|---|---:|---:|---|
| Bulk only | 1,600 | 0 | None |
| Single only | 0 | 800 | 140/s |
| Sparse | 1,600 | 24 | 2/s, starting after 100 campaign POSTs |
| Saturated | 1,600 | 1,600 | 140/s, starting after 100 campaign POSTs |
| 10k campaign at 20% | 10,000 | 8 | 2/s, immediately following a late one-item batch after 500 campaign POSTs |

The harness submits a 1,600-item campaign through two batch requests (1,000 and 600 items). The 10k comparison uses ten requests of 1,000 items. Its additional one-item batch and eight standalone probes carry identical payloads. It sends every standalone notification through its own HTTP request. Each run records observed standalone arrivals, provider POSTs, terminal outcomes, ready-event topics and the image digest. Queue latency starts at the PostgreSQL creation timestamp and ends at the first send claim. The later acceptance timestamp includes the provider call and recording its outcome. Neither measurement covers delivery from a carrier to a handset.

The feature implementation was committed as `b095dcc`. All final trials used the same worker image, as recorded in the raw JSON. Provider counters are isolated from other projects. The rolling SMS peak is the simulator's running maximum over the experiment session; the other provider counters are per-workload differences. Each JSON workload excludes its invocation's two warm-up notifications.

The saturated comparison counts send claims per lane in complete one-second bins where at least 72 notifications **in each lane** awaited their first claim throughout the bin. The harness reconstructs the populations at each creation and claim. The raw results include every eligible bin with its minimum queue populations. This is a cluster-wide observation of PostgreSQL state. Kafka partition assignment, consumer refill and in-flight work can affect how many tasks each worker has locally ready. A percentile uses nearest rank over the corresponding lane's complete cohort. Sparse p95 has 24 observations per setting.

A [separate GitHub Actions run](https://github.com/EfeDurmaz16/notification-system/actions/runs/44818238324) at 20% recorded 29.93% batch over 22 fully contended seconds. It measured 28.55 batch and 66.82 standalone claims/s, 12.717 s standalone acceptance p95, and 3,200 accepted notifications. Hosted-runner figures are excluded from the local trial ranges.

## Intermediate regression

The [first 10% pilot](before-10.json) assigned 52.09% of contended claims to batch, averaging 46.79 batch and 43.03 standalone claims/s over 29 seconds. A Kafka reader had to wait for in-flight jobs in its current poll to finish before it could replenish the local ready queue. Work donation during those gaps overwhelmed the configured weights. The worker now uses the partition high watermark to recognize known backlog and briefly preserves the lane's turn while its reader refills. A separate guard bounds how long an empty reader can hold its sibling. The saturated test writes its observations before checking for batch progress and, at a configured share up to 20%, higher standalone throughput than batch.

## Reproduce

Use a dedicated local Compose stack with no unrelated traffic. Select unused ports on a shared host. Configure one simulator and three workers. The harness discovers the published API and provider ports from Compose and verifies worker destinations, replica count, concurrency and share before sending traffic. It writes one JSON file per invocation.

```sh
export COMPOSE_PROJECT_NAME=notify-ingress-demo APP_IMAGE=notification-system:ingress-bench
export API_PORT=28080 PROVIDER_PORT=28090 KAFKA_PORT=29092 REDIS_PORT=26379 POSTGRES_PORT=55432 CONNECT_PORT=28083
export PROVIDER_URL=http://provider:8081/send PROVIDER_LATENCY=20ms WORKER_CONCURRENCY=24
export PROVIDER_IDEMPOTENT=false OTEL_EXPORTER_OTLP_ENDPOINT= BATCH_SHARE_PERCENT=10
docker compose up --build --scale worker=3 -d --wait
for share in 10 20 40; do
    export BATCH_SHARE_PERCENT=$share
    docker compose up -d --no-deps --scale worker=3 worker
    python3 scripts/ingress-share.py --share "$share" --out ".local/ingress-share/$share.json"
done
for share in 40 20 10; do
    export BATCH_SHARE_PERCENT=$share
    docker compose up -d --no-deps --scale worker=3 worker
    python3 scripts/ingress-share.py --share "$share" --profiles saturated --out ".local/ingress-share/repeat-$share.json"
done
export BATCH_SHARE_PERCENT=20
docker compose up -d --no-deps --scale worker=3 worker
python3 scripts/ingress-share.py --share 20 --profiles campaign10k --out .local/ingress-share/campaign.json
docker compose down -v
```
