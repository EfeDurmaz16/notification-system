# Where 10,000 burst notifications spent their time

Three worker replicas processed ten simultaneous 1,000-item SMS batches from run
`XY23S4ONV253LS3SWPWQ22QLII`. PostgreSQL recorded all three first-attempt
stages for every notification. All 10,000 reached provider acceptance. The query
ran after the timed workloads finished.

| Priority | Count | Pending to sending p95 | Sending to accepted p95 | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| All | 10,000 | 109.41 s | 25.97 ms | 56.89 s | 109.43 s | 114.50 s |
| High | 3,334 | 106.45 s | 25.93 ms | 54.76 s | 106.48 s | 111.02 s |
| Normal | 3,333 | 104.27 s | 26.00 ms | 52.25 s | 104.29 s | 110.51 s |
| Low | 3,333 | 113.65 s | 25.92 ms | 64.73 s | 113.67 s | 115.30 s |

Most elapsed time accumulated before the send claim. This interval includes CDC,
Kafka, dispatch, and shared channel-quota waiting. At most 100 starts per second
on one channel already implies roughly 100 seconds to dispatch this burst; this
measurement alone cannot attribute the remaining overhead to any single
component. The synthetic provider's configured delay was 20 ms.

Low-priority acceptance had a higher p95 than high and normal. Normal finished
slightly faster than high at p95 in this trial. Local weighted dispatch chooses
among jobs each replica can currently see; these results do not establish a
global priority ordering or service ratio.

The client-side [run](burst-10000.json) measured 86.22 observed terminal
notifications per elapsed second, including the last poll. The table uses
database trigger times at pending, sending, and accepted. Time before the pending
trigger and client polling lag are outside these durations.

[All stage percentiles in milliseconds](burst-10000-stages.tsv) ·
[Read-only SQL](../../scripts/latency-breakdown.sql) ·
[Measurement boundaries](../../docs/persistence.md#stage-latency-after-a-load-run)

## Priority dispatch while backlogged

Starting at the first database send claim, the first 10 s contained **300 high,
392 normal and 215 low** claims. Across the first 30 s the counts were **886,
960 and 759**. More than 2,300 notifications remained pending for each class at
30 s. This observed distribution differs from the local scheduler's configured
6:3:1 weights. [Window counts](burst-10000-dispatch.tsv)
