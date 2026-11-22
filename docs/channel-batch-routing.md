# Delivery topic routing

A fresh Compose stack routes each priority and ingress lane by channel. At normal priority a standalone email uses `notifications.normal.single.email`, and an SMS batch uses `notifications.normal.batch.sms`. Each topic has its own reader and commits independently. An SMS poll waiting for a permit or a paused endpoint can coexist with an email poll in the same lane. Every reader feeds the same worker pool. Per-channel quotas, priority weighting, the standalone/batch share and cancellation still apply.

Each standalone channel topic has three partitions so up to three replicas can own partitions for one hot channel and priority. Batch channel topics have one partition each. The fetch budget within each priority and lane is divided across the three channel readers, with at least one record per reader. The original shared topics remain readable during rollout.

## What the local scaling test showed

With three workers already running, increasing each SMS batch topic from one to three partitions cut the time for 10,000 SMS from 25.4 to 14.0 seconds. On three partitions per priority, 100,000 SMS took 288.5 seconds with one worker, 174.8 seconds when the queue triggered scaling from one to three, and 170.1 seconds with three workers ready at the start.

The experimental quota allowed 10,000 attempts/s. The scaling run averaged 572 provider acceptances/s. At 20 ms per provider call, sustaining 10,000/s would require at least 200 concurrent calls, while three workers had 72 send slots in total. Each configuration ran once against the local provider. At the delivery quota of 100/s, scaling cut a 3,000-SMS burst from 38.1 to 36.3 seconds.

The worker sent HTTP requests over the Compose network. The [local provider](../cmd/provider/main.go) recorded each successful test SMS, then waited for the configured `PROVIDER_LATENCY=20ms` before returning its receipt. Each request had its own timer. The rate of 572/s includes the queue draining and the client observing completion. For a production target, first measure the actual provider's response times, allowed attempt rate and peak arrival rate by channel. Use target attempts/s × mean HTTP round-trip seconds to estimate the average calls in flight. Increase per-worker concurrency while monitoring database connections and provider throttling. If waiting work outgrows one replica, provision enough partitions for other replicas to share it and scale on queue age. Check high-priority latency and duplicate POSTs during a rebalance before adopting the change.

### Replay the 100,000-SMS comparison

Use `make wave` for the normal rising and falling traffic test at the release quota. To reproduce the partition and high-quota figures above, run the [benchmark fixture](../reports/worker-scaling-replay/) from the repository root. It pins the measured revision, applies a benchmark-only limiter patch to a temporary copy, and uses the local provider with a 20 ms response delay. You need Git history containing that revision, Go, Python 3, Docker Compose, `patch`, and free localhost ports 28820 through 28825.

```sh
sh reports/worker-scaling-replay/run.sh
```

The eleven runs compare one fixed worker, three fixed workers and a queue-driven controller. The 100,000-SMS cases use three partitions per priority and a 10,000-attempts/s experimental quota. Budget at least 20 minutes plus setup. Each run records replica changes, latency, throughput and per-ID receipt checks. The script removes its own Compose project, volumes and image at exit. The script prints the temporary results directory on startup. It contains `summary.csv` and the per-run JSON files.

## Modes

Set `BATCH_ROUTING` on both the API and workers. Workers also publish when promoting scheduled jobs and retries. Existing outbox records retain their original destination. Every mode reads the original shared topics.

| Mode | Batch writes | Standalone writes | Additional reader subscriptions |
| --- | --- | --- | --- |
| `shared` (bare binary default) | Shared | Shared | None |
| `dual-read` | Shared | Shared | Batch by channel |
| `channel` (previous Compose default) | By channel | Shared | Batch by channel |
| `read-isolated` | By channel | Shared | Batch and standalone by channel |
| `isolated` (current Compose default) | By channel | By channel | Batch and standalone by channel |

## Upgrade and rollback

1. Create the new topics with the same retention and permissions as the original delivery topics. In Compose use `docker compose build api` and `docker compose run --rm --no-deps topics`. The provisioning job is idempotent. Subscribe independent consumers that need the complete ready stream to all topic families before enabling writers.
2. From a `channel` deployment, deploy the new workers with `BATCH_ROUTING=read-isolated`. Keep the API in its previous mode. For an older `shared` deployment, first deploy the new workers in `dual-read` and then switch the workers to `read-isolated`. Check assignments using the existing consumer group prefix. For example, run `docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group delivery.normal.single.email --describe --members --verbose`.
3. Switch the API and workers to `isolated`. Old outbox records drain through their original topic and consumer offsets. Fresh creates and due-job promotions use the new topics. Monitor consumer lag per channel, connector progress and oldest pending age.
4. To roll back one generation, switch the API and workers to `read-isolated`. This stops new channel-specific standalone writes while keeping readers available. Wait for transactions from the previous writers to finish, verify CDC has published them, then verify group offsets equal the log end on every new standalone partition. The earlier `channel` binary and settings can then take over.
5. For a further rollback to `shared`, switch both services to `dual-read`. Repeat the publication and offset checks for batch channel topics before switching readers to `shared`. A partition that has never held any records may have no committed offset.

Run `python3 -B scripts/check-routing.py` after each change. It checks live process modes and looks for pending jobs whose current outbox generation would have no compatible reader. Verify CDC separately: zero Kafka lag can also mean events are still waiting in PostgreSQL. The end-to-end rollback exercise is available as `python3 -B scripts/channel-cutover.py`. It starts its own Compose project and checks the old binary, new binary, retry promotion, cancellation, delayed CDC and each notification's provider POST count.
