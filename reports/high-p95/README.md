# High-priority latency during a 10,000-SMS burst

## Results

The per-priority p95 values cover the interval from client create start to persisted provider acceptance. The early high share counts durable send claims in the first 30 s, starting at the first claim in each trial.

| Trial | High/normal/low weights | High p95 (s) | Normal p95 (s) | Low p95 (s) | Accepted/s, wall clock | Peak POSTs/rolling s | Early high share |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [A1](a1.json) | 6:3:1 | 75.58 | 88.28 | 109.22 | 89.91 | 94 | 47.6% |
| [B1](b1.json) | 8:1:1 | 61.62 | 112.17 | 111.45 | 86.40 | 94 | 55.6% |
| [B2](b2.json) | 8:1:1 | 61.53 | 113.28 | 106.50 | 86.61 | 94 | 58.0% |
| [A2](a2.json) | 6:3:1 | 90.51 | 95.46 | 115.84 | 84.78 | 94 | 42.9% |
| [C1](c1.json) | 12:2:1 | 73.55 | 102.17 | 117.96 | 83.36 | 95 | 51.2% |
| [C2](c2.json) | 12:2:1 | 66.08 | 96.04 | 113.41 | 86.43 | 95 | 49.3% |

Each trial reached 10,000 provider acceptances. Across 60,000 IDs, the simulator recorded exactly one POST per ID, with zero receipt lookup errors. The 100 SMS/rolling-second quota held in all six trials.

The observed high p95 for 8:1:1 was below 64 s in both trials. The normal p95 rose to 112.17 and 113.28 s, while the production weights produced 88.28 and 95.46 s. The exploratory 12:2:1 cycle retained a larger normal than low weight. It missed the high target twice at 73.55 and 66.08 s. Its normal p95 was lower than either 8:1:1 result. The 6:3:1 baseline itself varied by 14.93 s between runs. Two repeats per policy on a shared local machine establish feasibility under this workload. A production tail-latency target needs repeated representative traffic including standalone calls, provider delays and retries.

High p95 time from database creation to the durable send claim ranged from 61.46 to 90.45 s. High p95 time from claim to persisted provider acceptance ranged from 25.3 to 26.0 ms. This puts most of the measured delay before the provider send. That interval includes CDC, Kafka, local scheduling and quota admission. The query measures the two percentiles independently. [Stage results](stages.json) and [SQL](stages.sql).

## Method

The [earlier 64.10 s figure](../bench/comparison.md) used the v1.0.0 worker and the mixed-priority, SMS-only burst. This comparison uses the permanent ingress-lane worker at `ad607d365736`. All 10,000 items in each trial use the batch endpoint. Ten simultaneous requests carry 1,000 notifications each, cycling through high, normal and low. There are 3,334 high, 3,333 normal and 3,333 low notifications. The local provider adds 20 ms before replying. Three worker replicas each run 24 handlers with a shared rolling budget of 100 SMS attempts/s. The within-priority batch weight stays at 20% throughout. No standalone traffic competes in this experiment.

Only the priority dispatch cycle changes in the candidate source trees. The existing reader cap scales with the number of positions assigned to a priority. A uses six high, three normal and one low position, with reader caps of 144/72/24. B uses 8:1:1, with caps of 192/24/24. C uses 12:2:1, with caps of 288/48/24. B and C both have a nominal 80% high share. C preserves a larger service weight for normal than for low. The measurement captures the combined dispatch and reader-window effect. Each worker schedules from the currently eligible records it has fetched. Partition assignments, refill timing and in-flight work can move the observed global share away from the nominal cycle. The service in the repository continues to use 6:3:1. [Worker scheduler and poll limits](../../internal/stream/worker.go).

The first four trials run in A, B, B, A order. Two exploratory C trials follow on the same isolated Compose project with the same PostgreSQL, broker, CDC, Redis, API and recording provider. Each trial recreates all three workers, warms them with 300 SMS and allows rolling reservations to expire. The timed run then resets only the isolated provider's counters. Terminal latency starts immediately before making each create request and ends at the persisted provider acceptance timestamp. Observed terminal throughput includes final client polling. The provider counts actual POST arrivals. The harness checks every notification ID for zero or multiple POSTs after the timed portion.

## Capacity bound

With 3,334 high notifications, nearest-rank p95 is the 3,168th. The Redis limiter spaces SMS admissions at least 10 ms apart. Even if high received every slot from the start, 3,167 intervals would take 31.67 s before the 3,168th admission, plus provider and processing time. This is an optimistic bound for simultaneous arrivals and an idle limiter. The ten client start times differ slightly. Sustained service to competing priorities and actual throughput below 100/s both raise the practical floor. Finishing at least 3,168 high messages inside 64 s requires about 49.5 high acceptances/s over that interval. For the entire 10,000-SMS batch, the same pacing puts the earliest final admission about 100 s after the first. Additional replicas still share the same SMS budget. [Redis admission code](../../internal/stream/limiter.go).

## Reproduce

Use an isolated Compose project and free host ports. The burst harness resets provider counters. The candidate patch applies to the baseline revision and is used only in a temporary build tree.

```sh
export COMPOSE_PROJECT_NAME=notify-high-review
export API_PORT=38080 PROVIDER_PORT=38090 POSTGRES_PORT=35432 REDIS_PORT=36379 KAFKA_PORT=39092 CONNECT_PORT=38083
export PROVIDER_URL=http://provider:8081/send PROVIDER_IDEMPOTENT=false PROVIDER_LATENCY=20ms
export WORKER_CONCURRENCY=24 BATCH_SHARE_PERCENT=20 OTEL_EXPORTER_OTLP_ENDPOINT=
trialdir=$(mktemp -d)
mkdir "$trialdir/base" "$trialdir/candidate" "$trialdir/tiered"
git archive ad607d365736 | tar -xf - -C "$trialdir/base"
git archive ad607d365736 | tar -xf - -C "$trialdir/candidate"
git archive ad607d365736 | tar -xf - -C "$trialdir/tiered"
git -C "$trialdir/tiered" apply --unidiff-zero "$PWD/reports/high-p95/weights-1221.patch"
git -C "$trialdir/candidate" apply --unidiff-zero "$PWD/reports/high-p95/weights-811.patch"
docker build -t notification-system:review-631 "$trialdir/base"
docker build -t notification-system:review-811 "$trialdir/candidate"
docker build -t notification-system:review-1221 "$trialdir/tiered"
export APP_IMAGE=notification-system:review-631
docker compose up -d --no-build --scale worker=3 --wait
go build -o "$trialdir/loadtest" ./cmd/loadtest
for trial in a1 b1 b2 a2 c1 c2; do
    case "$trial" in
        a*) export APP_IMAGE=notification-system:review-631 ;;
        b*) export APP_IMAGE=notification-system:review-811 ;;
        c*) export APP_IMAGE=notification-system:review-1221 ;;
    esac
    docker compose up -d --no-deps --force-recreate --scale worker=3 worker
    scripts/require-local-provider.sh
    "$trialdir/loadtest" -api "http://127.0.0.1:$API_PORT" -provider "http://127.0.0.1:$PROVIDER_PORT" \
        -scenario burst -total 300 -batch 100 -deadline 90s > "$trialdir/$trial-warm.json"
    sleep 1.2
    "$trialdir/loadtest" -api "http://127.0.0.1:$API_PORT" -provider "http://127.0.0.1:$PROVIDER_PORT" \
        -scenario burst -total 10000 -batch 1000 -deadline 10m -audit-receipts -out "$trialdir/$trial.json"
    run_id=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["runId"])' "$trialdir/$trial.json")
    docker compose exec -T postgres psql -U notify -d notify -v run_id="$run_id" \
        < reports/high-p95/stages.sql > "$trialdir/$trial-stages.txt"
done
docker compose down -v
```
