# Local notification experiments

All rows below are measurements from the files in this directory. The provider is local, with a synthetic delay. Rates exclude setup time.

| Run | Offered | API accepted | Terminal | API p95 ms | Persisted terminal p95 ms | Observed terminal p95 ms | Terminal/s | Drain s | Max SMS attempts in 1 s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| burst-10000 | 10000 | 10000 | 10000 | 187.7 | 111469.8 | 111694.8 | 84.8 | 117.76 | 94 |
| burst-1w | 1000 | 1000 | 1000 | 55.7 | 11571.7 | 11817.6 | 81.2 | 12.26 | 89 |
| burst-3w | 1000 | 1000 | 1000 | 51.6 | 11180.7 | 11307.8 | 84.7 | 11.76 | 92 |
| failures | 30 | 30 | 30 | 43.1 | 254.6 | 294.8 | 23.8 | 0.25 | 26 |
| load | 600 | 600 | 600 | 7.6 | 285.3 | 508.7 | 48.7 | 0.50 | 60 |
| mixed | 3000 | 3000 | 3000 | 38.2 | 11925.0 | 12043.7 | 234.5 | 12.76 | 91 |
| stress | 1200 | 1200 | 1200 | 8.0 | 5009.2 | 5254.6 | 81.2 | 5.50 | 92 |

## Outcomes

- **burst-10000**: accepted=10000. Provider accepted 10000, rejected 0, lost responses 0. Outstanding at deadline 0. Failed checks: none. 10000 individual IDs audited after the timed run: 0 missing, 0 repeated, 0 audit read errors.
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
| burst-10000 | 9935 | 64100.5 | 91153.2 | 115896.6 |
| burst-1w | 946 | 7551.1 | 9248.7 | 12044.6 |
| burst-3w | 949 | 8628.3 | 9599.1 | 11592.7 |
| failures | 0 | 266.0 | 180.1 | 254.6 |
| load | 18 | 287.2 | 284.5 | 279.9 |
| mixed | 2829 | 8302.7 | 10012.5 | 12350.0 |
| stress | 422 | 3602.6 | 5644.4 | 4825.3 |

## Independent channel quotas

Three-channel burst across independent budgets, with each priority represented on each channel.

| Channel | Requests received | Peak requests in a sliding second |
| --- | ---: | ---: |
| email | 1000 | 93 |
| push | 1000 | 91 |
| sms | 1000 | 91 |
