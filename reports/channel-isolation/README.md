# Push delivery during an SMS backlog

## Results

The standalone ingress path served sporadic push probes in roughly a quarter second with either worker. Moving already-fetched jobs into channel-specific admission lanes did not reduce tail latency on the shared batch path. SMS throughput during probes also fell in both candidate trials. The candidate remains an experiment.

| Trial | Batch push p95, idle (ms) | Batch push p95, backlog (s) | Single push p95, idle (ms) | Single push p95, backlog (ms) | SMS POSTs/s during probes |
| --- | ---: | ---: | ---: | ---: | ---: |
| A1, current | 213.8 | 44.412 | 215.4 | 233.7 | 83.42 |
| B1, candidate | 223.1 | 46.375 | 212.9 | 223.8 | 81.51 |
| B2, candidate | 217.2 | 46.884 | 213.7 | 202.1 | 79.86 |
| A2, current | 199.6 | 43.425 | 205.8 | 229.1 | 83.41 |

Across the four trials, the 96 loaded batch push probes entered the same reader as the campaign. At the four batch p95 probes, respectively 1,302, 1,283, 1,149 and 1,258 earlier SMS on the same partition had yet to reach the durable send claim when the push was created. For those four probes, claim to persisted acceptance took only 23 to 27 ms. The single probes had no campaign SMS at an earlier offset on their topic. Moving quota admission after Kafka fetch cannot move an unread push into the worker's ready set.

Five of the 96 batch probes had no unclaimed SMS at a lower offset on their particular partition when created and still took 24 to 32 seconds. The worker has one batch reader for all three partitions, and a new poll waits for outstanding work from the previous poll. Per-partition order explains only part of the measured wait. The experiment does not timestamp individual fetches or the acquisition of worker slots.

Every one of 16,416 API-accepted notification IDs was audited. The provider recorded one POST for each of 16,384 accepted deliveries and zero for all 32 cancelled IDs. No delivery was unknown. Rolling one-second maxima were 88 SMS and 23 push POSTs across the four loaded trials. Two repetitions per image describe this local workload. [Candidate patch](channel-admission.patch) and [candidate validation](candidate-checks.txt).

## Workload

One worker with 24 delivery slots, the default priority and ingress weights, PostgreSQL, Debezium, Kafka, Redis and a local recording provider with 20 ms latency. All notifications use normal priority. Each of the batch and single Kafka topics has three partitions. Only the worker image changes between trials. The two variants run in A, B, B, A order. The HTTP API, broker, database and provider stay up; each trial restarts and warms the worker.

Four concurrent batch requests admit 4,000 SMS. After 50 SMS provider calls, 24 pairs of push probes arrive one second apart. One push in every pair uses the batch endpoint (a one-item batch behind the SMS campaign). Its twin uses the standalone endpoint (the independent normal-priority single reader). Each pair arrives with more than 300 SMS still outstanding at the provider. The separate idle calibration uses 24 pairs 400 ms apart, which keeps both push workloads comfortably below quota. There are no SMS during calibration.

Eight additional batch items (four SMS and four push) are created and immediately cancelled while the SMS backlog is active. A successful cancellation must yield a durable cancelled status and zero provider POSTs for that ID. At completion, the runner reconciles every API admission with its database row, its original Kafka record and its independently queried provider POST count. It checks that both consumer groups committed through the log end on all six source partitions. The provider independently measures rolling one-second arrival peaks by channel.

## Reading the measurements

Latency starts at database creation and ends at persisted acceptance. Each per-lane p95 is the nearest-rank percentile of 24 probe IDs. Each probe has two stage measurements: creation to the durable send claim, and claim to persisted acceptance. The first interval includes CDC, waiting for Kafka consumption, local scheduling, database work and quota admission. The second includes the provider round trip and persistence. Neither directly measures time between worker-slot assignment and a quota permit.

For every probe the raw JSON includes the source topic (implied by its lane), partition, offset, number of campaign records at lower offsets in that partition, and how many of those had yet to start sending when the push was created. The generator queries Kafka after the timed interval. SMS throughput is the change in the provider's received SMS count divided by the observed time between the first and last probe samples, while the campaign was still backlogged.

Compare A with B **within each ingress lane**. The single and batch probes share a priority and a worker pool but have different Kafka readers and different dispatch weights under contention. Their latency difference alone cannot isolate the cost of a worker waiting for a permit. The idle cadence of 2.5 pairs/s gives a low-load reference; all loaded A and B trials use exactly one pair/s.

## Reproduce

Run `make channel-isolation` from the project root. To use prebuilt worker images, run:

```sh
python3 -B scripts/channel-isolation.py \
  --baseline-image notification-system:channel-base-ed4257c \
  --candidate-image notification-system:channel-admission-ed4257c \
  --out-dir .local/channel-isolation
```

The harness provisions its own Compose project with loopback ports and removes it and its volumes on exit. [A1](a1.json), [B1](b1.json), [B2](b2.json) and [A2](a2.json) include the exact worker image digest and the per-ID audit totals.
