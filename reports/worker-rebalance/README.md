# A worker dies with both SMS lanes backlogged

At the exit of one worker, both lanes had 395 pending and five sending notifications. The recording provider accepted all 800 notifications, one POST for every API-returned ID. No ID was missing or sent twice.

| Ingress | Admitted | API accepted | API unknown | Zero provider POSTs | One POST | Multiple POSTs |
|---|---:|---:|---:|---:|---:|---:|
| Single | 400 | 399 | 1 | 0 | 400 | 0 |
| Batch | 400 | 398 | 2 | 0 | 400 | 0 |

The pre-kill provider query already showed one POST for each of the three eventual unknown IDs. At exit, those IDs were in `sending`. They lacked a saved provider message ID and reached `unknown` 120.020 to 120.070 seconds after the send claim. The two survivors made no further POST for those IDs. The instrumentation resolves the provider outcome in this fixture. With an external provider, the application would only have its stored `unknown` and would need provider evidence to reconcile it.

The dead worker owned partition 2 of the batch topic and partition 1 of the single topic. The broker reported those partitions assigned to different survivors 45.13 seconds after SIGKILL. This measures time to observe reassignment. Both consumer groups reached zero lag on all six partitions. The provider counted 800 POSTs at drain and still counted 800 seven seconds later. These observations cover one controlled interruption on revision `ad607d3`.

## Experiment

The experiment starts a fresh local Compose project with three worker replicas, 24 handlers each and the default 20% batch setting. One batch request admits 400 normal-priority SMS. Alongside it, 400 standalone requests admit another 400 SMS with the same payload. Every notification has its own ID. The local provider records acceptance before delaying each successful HTTP response by one second. This widens the interval between a real acceptance and the worker persisting its acknowledgement.

The runner reads Kafka's partition assignments for both consumer groups, locates an owner by its container IP, samples pending and in-flight work, and sends SIGKILL to that container only. The worker stays down. The two surviving workers take its partitions. For each API-returned ID, the runner joins durable state history with that ID's independently queried provider POST count. A separate broker check verifies both groups committed all 800 offsets. A seven-second guard observes the provider for late POSTs after the groups drain.

The simulator has no handset delivery receipts. For this success-only run, all ordinary POSTs reaching the fixture are counted as accepted before the response delay. A worker can die after the database send claim but before the POST reaches the provider, or after provider acceptance but before the acknowledgement is recorded. Without a provider idempotency contract, the two-minute stale-send recovery moves that abandoned claim to `unknown`. The ledger shows which case the fixture supports for each observed ID.

## Reproduce

From the repository root, run the following on a machine with Docker Compose and Python 3. The runner builds its own application image, allocates fresh loopback ports and removes its test project and volumes upon exit.

```sh
python3 -B scripts/worker-rebalance.py --out-dir .local/worker-rebalance
```

The [per-ID ledger](ids.csv) contains all API-acknowledged IDs. An empty `provider_posts_before_kill_sample` field means the ID was outside the pre-kill sample. The fixture's final count is recorded for every ID. The `at_kill` field uses PostgreSQL status trigger timestamps and the victim container's Docker exit timestamp. The [summary](result.json) records observed partition ownership, committed offsets, status counts and provider counters.
