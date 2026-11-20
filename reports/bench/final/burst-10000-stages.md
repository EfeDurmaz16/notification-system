# Stage latency and dispatch on the fixed reference

Run `D2BJ7DVHVKNTPJ436OSB3RBV4I` used three worker replicas, 10 concurrent
1,000-SMS requests, and the local 20 ms provider. All 10,000 notifications had
complete first-attempt status history and reached provider acceptance.

| Priority | Count | Pending to sending p95 | Sending to accepted p95 | Pending to accepted p50 | Pending to accepted p95 | Pending to accepted p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| All | 10,000 | 108.39 s | 25.95 ms | 56.73 s | 108.42 s | 114.34 s |
| High | 3,334 | 109.12 s | 25.71 ms | 57.51 s | 109.14 s | 114.56 s |
| Normal | 3,333 | 105.24 s | 26.03 ms | 54.05 s | 105.26 s | 110.70 s |
| Low | 3,333 | 109.61 s | 26.02 ms | 57.66 s | 109.64 s | 114.99 s |

Most waiting precedes the send claim. Claim-to-accept time includes the
provider HTTP round trip and database updates. The 108.39 s pre-claim interval
also includes propagation, waiting, scheduling, and rate limiting. These
boundaries cannot isolate broker latency or justify attributing the entire
interval to Kafka.

| Seconds since first claim | High claims | Normal claims | Low claims |
| --- | ---: | ---: | ---: |
| 0 to 10 | 328 | 316 | 258 |
| 10 to 20 | 320 | 290 | 285 |
| 20 to 30 | 305 | 286 | 295 |
| 0 to 30 | 953 | 892 | 838 |

At least 2,224 notifications per priority were already pending when the first
send claim occurred. At 30 s, 2,381 high, 2,441 normal, and 2,495 low were
still pending. The observed 35.5% / 33.2% / 31.2% admissions in these first
30 s did not deliver 6:3:1 calendar-time service across replicas. A database
backlog need not be visible in each worker's bounded fetch queue.

[Full stage percentiles](burst-10000-stages.tsv) ·
[Dispatch windows and pending counts](burst-10000-dispatch.tsv) ·
[Client-side run](burst-10000.json) ·
[Definitions and reproduction](../../../docs/persistence.md#stage-latency-after-a-load-run)
