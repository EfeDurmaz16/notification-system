# Failure and recovery experiments

Each run creates a new Docker Compose project with its own PostgreSQL, Kafka, Redis, Debezium, API, provider and two worker replicas. The script removes only that project's containers and volumes. Requests use random operation keys and synthetic recipients. Results are read through the public status API and through an independent HTTP provider recorder.

```sh
python3 scripts/recovery.py --build
python3 scripts/recovery.py --crash
```

The second command deliberately kills both workers during an active send. Allow at least three minutes for the real two-minute abandoned-send timer and container startup. The regular suite includes the following observations. Elapsed times cover the entire scenario, including Docker service control and recovery probes.

| Experiment | Observation |
|---|---|
| Stop both workers, create two items, cancel one, restart | One provider POST. The two items ended accepted and cancelled. |
| Stop Debezium before create | API returned 202. No POST while the connector was stopped. One POST and an accepted status after restart. |
| Stop Kafka before create | Same result. Single-node broker outage and catch-up completed in 50.046 seconds, including the deliberate hold. |
| Stop Redis, create two jobs and cancel one | No POST during the outage. After recovery, one accepted and one cancelled. |
| Stop provider until retries exhaust | One item failed after four dial attempts. A second item entered retry_wait, then succeeded on attempt two after provider restart. |
| Rejected POST vs lost response | Two messages produced two POSTs. The provider rejected one and accepted the other before closing its connection. Statuses were failed and unknown. |
| 300 SMS, two workers | 300 unique acceptances and 300 POSTs. Maximum 92 arrivals in any trailing one-second window, measured by the provider. |

[Recorded recovery cases](recovery.json) include explicit pass/fail and timing for each assertion. The rolling peak is an observation for this fixture and run. Single-node restart tests exercise process and connection loss with volumes intact. Multi-node failover and volume loss require separate deployments and tests.

## Lost acknowledgement and worker death

The slow suite delays the provider response four seconds. After the provider records acceptance and the API reports sending, the harness sends SIGKILL to both worker processes. It confirms sending is still durable, starts fresh workers and waits for the real abandoned-send timeout. The first item became unknown. A second SMS to the same recipient was accepted. Together they produced exactly two provider POSTs. The full case took 124.852 seconds.

A separate run switches both provider and workers to explicit provider idempotency. The provider accepted an item, lost its first HTTP response and replayed the receipt on retry. The API reported accepted. The provider counted two POSTs, one logical acceptance and one deduplicated replay.

[Recorded crash and idempotency cases](recovery-crash.json) contain the measured counts.
