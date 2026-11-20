# Worker verification

`integration.txt` is the local test output against two independent Redis clients and a real Kafka group. Reproduce with the Compose dependencies up:

```sh
TEST_REDIS_ADDR=127.0.0.1:16379 TEST_KAFKA_BROKER=127.0.0.1:19092 go test -race -count=1 -v ./internal/stream
```

The Kafka test uses a unique one-partition test topic and removes it afterwards. A higher offset finishes successfully before a lower offset fails. Both reappear after restarting the client; the following successful batch commits before the next poll. The Redis case delays a subset of requests after acquisition and records the busiest actual one-second dispatch window. This is an invariant check, not a throughput target.

## Design and dependency references

- Three bounded fetchers feed the local weighted dispatcher with independent high, normal and low backlogs. Each uses a group suffixed with its priority. Batches, a shared pool and handler deadlines bound outstanding work. Priority applies to dispatch opportunities with visible work. Already running requests continue.
- Group auto-commit is disabled. Polling blocks rebalances until all fetched offsets finish or are durably quarantined; a failure restarts the reader from committed offsets. Record processing and Kafka group offsets are separated by a crash boundary. Conditional notification state transitions absorb replay. See the [franz-go offset management notes](https://github.com/twmb/franz-go/blob/v1.22.3/docs/producing-and-consuming.md), [group examples](https://github.com/twmb/franz-go/tree/v1.22.3/examples/group_committing) and [release changelog](https://github.com/twmb/franz-go/blob/v1.22.3/CHANGELOG.md).
- Redis server time, an atomic Lua script and one shared channel ledger coordinate all replicas. Admission spacing is at least 10 ms per channel; at most 100 slots stay occupied across provider calls and for a further second after completion. Each attempt has a 30-second deadline and a two-minute safety lease. A Redis error prevents new sends; a failed release leaves the conservative lease. Production durability depends on preserving limiter state across Redis failover and bounded process pauses. See [Redis sliding logs](https://redis.io/tutorials/howtos/ratelimiting/), [Go scripting example](https://github.com/redis/go-redis/tree/v9.23.0/example/lua-scripting) and [client releases](https://github.com/redis/go-redis/releases).
- Unknown event IDs, malformed payloads and future generations are quarantined by source coordinates before offset commit. Raw contact details are never copied to the quarantine topic. Redelivery of a quarantine marker can occur if the process exits before committing the source offset.
