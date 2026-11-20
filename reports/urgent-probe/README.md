# One urgent SMS behind a running campaign

Tested the v1.0.0 service using three worker replicas, 24 slots per replica, a shared SMS quota of 100 attempts per rolling second and a local provider with a 20 ms delay. Each trial started with a standalone high control. After ten concurrent batches of 1,000 low SMS were admitted and at least 500 campaign POSTs reached the provider, a high SMS and a low control were admitted in a single transaction. Every SMS used the same synthetic recipient.

Times run from PostgreSQL creation to persisted provider acceptance, using the database clock. Outstanding includes campaign jobs that had yet to reach accepted, including in-flight sends.

| Trial | Outstanding at probe | Idle high (ms) | Loaded high (ms) | Paired low (s) | Campaign jobs accepted after high |
|---|---:|---:|---:|---:|---:|
| 1 | 9499 | 134.8 | 464.9 | 107.78 | 9459 |
| 2 | 9501 | 79.3 | 151.4 | 108.82 | 9488 |
| 3 | 9497 | 108.0 | 1000.1 | 114.13 | 9411 |

The urgent probe bypassed most campaign jobs in each trial. In the slowest trial it spent 975.684 ms before the send claim and 24.461 ms from claim to persisted acceptance. These three samples describe a late isolated high message under a low-only backlog. They do not establish a percentile or an SLA.

[Raw results](results.json) and [experiment script](../../scripts/urgent-probe.py). The script expects a healthy, otherwise idle Compose stack, API port 18080, provider port 8090, and no other arrivals. Scale to three replicas before running it and restore the original count afterwards. It issues 30,009 synthetic notifications across the three trials.
