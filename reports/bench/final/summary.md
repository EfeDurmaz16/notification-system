# Local notification experiments

All rows below are measurements from the files in this directory. The provider is local, with a synthetic delay. Rates exclude setup time.

| Run | Offered | API accepted | Terminal | API p95 ms | Persisted terminal p95 ms | Observed terminal p95 ms | Terminal/s | Drain s | Max SMS attempts in 1 s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| burst-10000 | 10000 | 10000 | 10000 | 201.7 | 108613.3 | 108710.0 | 86.2 | 115.76 | 94 |
| burst-1w | 1000 | 1000 | 1000 | 57.7 | 11327.1 | 11568.5 | 82.9 | 12.01 | 90 |
| burst-3w | 1000 | 1000 | 1000 | 36.1 | 11139.4 | 11295.1 | 84.7 | 11.76 | 91 |
| failures | 30 | 30 | 30 | 9.3 | 279.8 | 510.9 | 23.8 | 0.25 | 20 |
| load | 600 | 600 | 600 | 7.2 | 285.6 | 507.5 | 48.7 | 0.50 | 58 |
| mixed | 3000 | 3000 | 3000 | 93.2 | 11534.9 | 11598.1 | 242.9 | 12.26 | 92 |
| stress | 1200 | 1200 | 1200 | 7.9 | 4969.2 | 5006.6 | 82.0 | 5.37 | 92 |

## Outcomes

- **burst-10000**: accepted=10000. Provider accepted 10000, rejected 0, lost responses 0. Outstanding at deadline 0. Failed checks: none.
- **burst-1w**: accepted=1000. Provider accepted 1000, rejected 0, lost responses 0. Outstanding at deadline 0. Failed checks: none.
- **burst-3w**: accepted=1000. Provider accepted 1000, rejected 0, lost responses 0. Outstanding at deadline 0. Failed checks: none.
- **failures**: accepted=10, failed=10, unknown=10. Provider accepted 20, rejected 10, lost responses 10. Outstanding at deadline 0. Failed checks: none.
- **load**: accepted=600. Provider accepted 600, rejected 0, lost responses 0. Outstanding at deadline 0. Failed checks: none.
- **mixed**: accepted=3000. Provider accepted 3000, rejected 0, lost responses 0. Outstanding at deadline 0. Failed checks: none.
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
| burst-10000 | 9940 | 109322.7 | 105494.3 | 109853.4 |
| burst-1w | 946 | 11218.5 | 10819.2 | 11636.9 |
| burst-3w | 947 | 10476.6 | 10732.8 | 11477.8 |
| failures | 4 | 269.1 | 280.8 | 279.8 |
| load | 15 | 285.2 | 283.4 | 285.6 |
| mixed | 2835 | 10940.5 | 11162.1 | 11916.9 |
| stress | 408 | 4352.5 | 4955.4 | 5275.0 |

## Independent channel quotas

Three-channel burst across independent budgets, with each priority represented on each channel.

| Channel | Requests received | Peak requests in a sliding second |
| --- | ---: | ---: |
| email | 1000 | 91 |
| push | 1000 | 91 |
| sms | 1000 | 92 |
