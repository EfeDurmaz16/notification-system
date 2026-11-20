# Local notification experiments

All rows below are measurements from the files in this directory. The provider is local, with a synthetic delay. Rates exclude setup time.

| Run | Offered | API accepted | Terminal | API p95 ms | Persisted terminal p95 ms | Observed terminal p95 ms | Terminal/s | Drain s | Max SMS attempts in 1 s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| burst-10000 | 10000 | 10000 | 10000 | 213.8 | 109605.9 | 109720.8 | 86.2 | 115.77 | 95 |
| burst-1w | 1000 | 1000 | 1000 | 32.8 | 12408.2 | 12538.9 | 76.7 | 13.01 | 90 |
| burst-3w | 1000 | 1000 | 1000 | 53.4 | 11097.1 | 11310.3 | 84.7 | 11.76 | 92 |
| failures | 30 | 30 | 30 | 8.0 | 261.2 | 505.7 | 23.9 | 0.25 | 28 |
| load | 600 | 600 | 600 | 6.7 | 285.0 | 506.2 | 49.6 | 0.30 | 59 |
| stress | 1200 | 1200 | 1200 | 7.6 | 4989.0 | 5004.7 | 81.4 | 5.47 | 93 |

## Outcomes

- **burst-10000**: accepted=10000. Provider accepted 10000, rejected 0, lost responses 0. Outstanding at deadline 0. Failed checks: none.
- **burst-1w**: accepted=1000. Provider accepted 1000, rejected 0, lost responses 0. Outstanding at deadline 0. Failed checks: none.
- **burst-3w**: accepted=1000. Provider accepted 1000, rejected 0, lost responses 0. Outstanding at deadline 0. Failed checks: none.
- **failures**: accepted=10, failed=10, unknown=10. Provider accepted 20, rejected 10, lost responses 10. Outstanding at deadline 0. Failed checks: none.
- **load**: accepted=600. Provider accepted 600, rejected 0, lost responses 0. Outstanding at deadline 0. Failed checks: none.
- **stress**: accepted=1200. Provider accepted 1200, rejected 0, lost responses 0. Outstanding at deadline 0. Failed checks: none.

## Method

- The 1,000-item burst is one request, so its API percentiles describe that single call. The 10,000-item burst is ten simultaneous batches of 1,000. Replica counts are recorded in each JSON file.
- Load cases submit batches at a fixed arrival rate; stress uses three equal-population stages at 1x, 2x, and 4x base rate. See scheduling delay in the JSON to identify generator lag.
- Terminal latency starts before the create request. Persisted latency ends at the database status timestamp when available. Observed latency includes polling delay. Observed terminal throughput includes the ramp and the final poll.
- Peak SMS rate is calculated from every provider handler arrival, using a sliding one-second window. Separate retries count again. In the failures case the provider accepts the synthetic unknown notifications, then deliberately loses their responses.
- Expired deadlines and quota violations remain visible in the raw reports. Queueing and timing from a local run do not establish production capacity.

## Queueing and priority

The largest observed backlog is sampled once per second and equals API acknowledgments minus client-observed terminal states, including any work awaiting its next status poll. The latency columns use persisted terminal times.

| Experiment | Highest sampled backlog | High p95 ms | Normal p95 ms | Low p95 ms |
| --- | ---: | ---: | ---: | ---: |
| burst-10000 | 9939 | 106663.2 | 104508.9 | 113887.6 |
| burst-1w | 956 | 12422.6 | 11445.7 | 12728.7 |
| burst-3w | 947 | 10072.0 | 11380.2 | 11140.0 |
| failures | 1 | 252.5 | 252.5 | 262.6 |
| load | 15 | 287.8 | 293.9 | 273.3 |
| stress | 407 | 4838.0 | 4617.7 | 5268.7 |
