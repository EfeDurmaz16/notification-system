# Batch routing by delivery channel

Three modes support a staged upgrade. Set `BATCH_ROUTING` on the API and workers. A worker also produces ready events when it promotes scheduled jobs and retries. Existing outbox records keep their original topic.

| Mode | New batch outbox destination | Worker subscriptions |
| --- | --- | --- |
| `shared` (default) | One topic per priority | Shared batch and standalone topics |
| `dual-read` | One topic per priority | Shared batch, channel batch and standalone topics |
| `channel` | One topic per priority and channel | Shared batch, channel batch and standalone topics |

For a fresh channel-routed stack, run:

```sh
BATCH_ROUTING=channel docker compose up --build -d
```

Persist the mode in the deployment environment for subsequent Compose commands. Running without the variable uses `shared`.

## How a notification is read

At normal priority, standalone SMS, email and push still enter `notifications.normal.single`. Channel-routed batches enter `notifications.normal.batch.sms`, `notifications.normal.batch.email` or `notifications.normal.batch.push`. High and low follow the same pattern. Each channel topic has one partition and an independent reader, poll and commit cycle. Three channel partitions per priority replace the three active shared batch partitions. The original three shared partitions stay available to drain pre-upgrade records.

Every reader feeds the same worker pool. Priority weighting, the standalone/batch dispatch share, per-channel quotas, conditional send claims and cancellation rules still apply. The per-priority channel fetch budgets divide the shared batch cap three ways, with a minimum of one record per reader. One active channel uses one source partition per priority for batch traffic. Adding replicas can distribute different partitions but each partition has only one owner in a consumer group.

## Upgrade a running deployment

1. Build the new API/worker image and create the channel topics before starting their readers. With Compose, use `docker compose build api` followed by `docker compose run --rm --no-deps topics`. The topic job is idempotent. Give the channel topics the same retention and permissions as the original topics.
2. Deploy the workers with `BATCH_ROUTING=dual-read` and the existing consumer group prefix. Leave the API writing shared topics. Check membership and assigned partitions for the legacy, channel and standalone delivery groups. With the default prefix, a normal push batch uses group `delivery.normal.batch.push`. For example, inspect `docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group delivery.normal.batch.push --describe --members --verbose`.
3. Switch API and worker processes to `BATCH_ROUTING=channel`. Keep the legacy subscriptions running. The new mode routes fresh batch creates and future promotions by channel. An earlier durable ready record is still consumed on its original topic and group offset. A scheduled job or retry can cross the change of mode because the due transition and its new outbox record share a transaction.
4. Subscribe independent consumers that need the complete ready stream to both topic families and the standalone topics. Monitor per-channel group lag and the oldest pending age during the change.

## Roll back

Switch both API and worker processes to `dual-read`. All new batch writes then target the shared topics while workers continue to drain channel topics. This is a supported steady state.

Before reverting the worker binary or switching it to `shared`, stop all channel-mode writers, including schedulers, and let their open transactions finish. Verify CDC has flushed their final outbox transactions and the channel delivery groups have committed through the log end on every partition. A zero-length partition can have no committed offset. A broker reporting zero consumer lag can still be waiting for records from a delayed connector. Keep the dual-read workers when CDC progress is uncertain.
