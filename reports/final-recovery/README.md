# Release candidate recovery verification

These reruns target the same application image used for the tuned traffic matrix. Its local Docker image ID is `sha256:3b42f3b05cc46fbb32ddbea3cada2a4a66c0e38e806ac6522e78c3887c90a156`. Both Compose projects use the digest-specific local tag below, with builds disabled. Each gets independent PostgreSQL, Redis, Kafka and CDC state, a recording provider and two worker replicas with concurrency 16 each.

```sh
docker image tag sha256:3b42f3b05cc46fbb32ddbea3cada2a4a66c0e38e806ac6522e78c3887c90a156 notification-system:recovery-3b42f3b05cc4
export APP_IMAGE=notification-system:recovery-3b42f3b05cc4
python3 scripts/recovery.py --report reports/final-recovery/recovery.json
python3 scripts/recovery.py --crash --report reports/final-recovery/recovery-crash.json
```

On a new host, use a locally built image from the release revision in `APP_IMAGE`; a local image ID identifies the exact bytes tested on this machine. The regular run restarts individual services with volumes intact. The crash run kills both workers after provider acceptance and waits for the real two-minute recovery deadline.

Both commands exited zero. Inspection of the API, two workers and provider in both projects confirmed the image ID above. All nine assertions passed.

| Failure or bound | Observation |
|---|---|
| Worker offline with two queued items | One cancelled, one accepted, one provider POST. |
| Connector offline | Zero POSTs before restart, one after restart. |
| Kafka offline | Zero POSTs before restart, one after restart. |
| Redis offline | Zero POSTs while quota authority was unavailable. The remaining item was accepted after restart. |
| Dial failure | One item failed after four attempts. Another recovered on its second attempt with one provider POST. |
| Rejection and lost response | Rejection ended failed. Lost acknowledgement ended unknown. |
| Two workers, 300 SMS | 300 POSTs and 300 accepted; peak 91 arrivals in any trailing second. |
| Workers killed after provider acceptance | First status became unknown. A second notification to the same recipient was accepted. Two POSTs across the two items. Case elapsed 125.422 seconds. |
| Provider replay guarantee enabled | Lost response triggered a second POST with the same key. One acceptance, one deduplicated replay, final status accepted. |

[Seven recovery cases](recovery.json) and [two worker-crash and replay cases](recovery-crash.json) record the independent provider observations and elapsed times.
