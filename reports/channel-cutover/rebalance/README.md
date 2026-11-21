# Loss of the active batch SMS worker

On 21 November 2026, a local Compose stack ran three workers with 24 handlers each, a one-second synthetic provider delay and `BATCH_ROUTING=channel`. The tested image digest was `sha256:98c3900eed8ceabd9f2fd65e0d72fb5fe94fb900fe9875df5fc660da0b3bd27f`. The service source at build was `3d1cdbb` with channel routing explicitly selected. The completed run recorded checkout `1059c0c`, which includes the updated fault harness.

The test admitted 400 batch and 400 standalone SMS. It cancelled one pending batch ID, then killed the owner of `notifications.normal.batch.sms/0` with SIGKILL. At the kill there were 395 batch and 394 standalone notifications pending. Four batch and six standalone notifications were sending.

| Final application status | IDs | Actual provider POSTs per ID |
| --- | ---: | ---: |
| Accepted | 793 | 1 |
| Unknown | 6 | 1 |
| Cancelled | 1 | 0 |

All six unknown IDs had already been observed at the provider before the kill. Recovery preserved those ambiguous outcomes. The provider saw 799 calls and a rolling one-second peak of 48. The replacement owners committed through offset 400 on the single batch SMS partition. The three standalone partitions committed 127, 121 and 152 records. Every final lag was zero. Aggregate POST counts remained unchanged during the seven-second observation after drain.

Run `BATCH_ROUTING=channel make rebalance` on a Docker host. The [raw report](result.json) records owners, source offsets, kill time and provider counts. The [ID ledger](ids.csv) records the state transitions and POST count of each admission.
