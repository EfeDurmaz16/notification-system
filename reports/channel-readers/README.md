# Reading batch channels independently

The independent readers met the batch push latency target in both matched pairs. SMS throughput fell by 5.65% and 6.26%, exceeding the agreed 5% budget in each pair. The release keeps the shared batch reader. [Decision checks](gate.json).

| Trial | Idle batch push p95 | Loaded batch push p95 | Loaded single push p95 | SMS POSTs/s during probes |
| --- | ---: | ---: | ---: | ---: |
| [A1, shared](a1.json) | 225 ms | 42.978 s | 213 ms | 83.114 |
| [B1, per channel](b1.json) | 210 ms | 0.187 s | 183 ms | 78.416 |
| [B2, per channel](b2.json) | 210 ms | 0.246 s | 209 ms | 77.541 |
| [A2, shared](a2.json) | 229 ms | 45.736 s | 202 ms | 82.717 |

For the two baseline probes at the batch p95, 1,329 and 1,286 earlier SMS on the same partition had yet to be claimed when the push arrived. Those probes waited 42.95 s and 45.71 s before claim. Both candidate p95 probes had zero earlier SMS on their push partition and waited 164 ms and 193 ms before claim. Under independent routing, the SMS reader can fetch at most 24 normal-priority records in one poll. The matched experiment measures the total change from routing and polling together; it did not isolate the cause of the SMS throughput decrease.

Across idle and loaded phases, all 16,416 IDs reconciled. Each of 16,384 accepted IDs had one provider POST, all 32 cancelled IDs had zero, and all active source partitions committed to their log ends. Peak observed arrivals were 87 SMS and 13 push POSTs per rolling second. Email had no traffic in this campaign scenario. The two matched repetitions are local observations with a synthetic provider.

## Workload and decision rule

A four-request campaign admits 4,000 normal-priority SMS. After the provider receives the first 50 SMS, the client offers one pair of push notifications per second for 24 seconds. Each pair has a single request and a one-item batch. Every pair must arrive with more than 300 SMS still waiting for the provider. The client also sends eight batch notifications and immediately attempts to cancel them. A successful cancellation requires zero provider POSTs. A send that wins the race requires a cancellation conflict and exactly one POST.

The baseline uses one batch topic with three partitions per priority. The candidate writes a separate one-partition batch topic for each channel within each priority. Readers poll and commit independently. The candidate also consumes the legacy topic for migration; it is empty in these runs. Both builds use the original single topic with three partitions per priority, one worker with 24 handlers, a 20% batch weight, a shared rolling quota of 100 attempts/s per channel and a local provider with 20 ms latency. At normal priority the baseline can fetch up to 72 batch records at once. The candidate divides that budget across the three channel readers (up to 24 records each).

The experiment runs A1, B1, B2, A2 in that order. Each trial starts with fresh Compose volumes, warms both push ingress lanes and obtains a separate idle calibration with 24 pairs. Push latency runs from database creation to persisted acceptance. The p95 is the 23rd of 24 sorted measurements. SMS throughput is actual provider arrivals between the first and last loaded probe samples, measured while the campaign still has a backlog.

For each matched pair (A1/B1 and A2/B2), the decision rule requires candidate batch push p95 at most 2 s and candidate SMS throughput at least 95% of its baseline. Every API ID must reconcile with its database state, Kafka record, and provider POST count. All nonempty measured partitions must commit their end offsets; an empty partition can have no committed offset. The provider independently measures each channel's peak arrivals in a rolling second. Safe cutover of already queued events is a separate release gate.

## Reproduce

The [experiment revision](https://github.com/EfeDurmaz16/notification-system/tree/97447f4) contains the candidate Compose topic definitions and the matching harness. Start in a full clone and run the following commands. Use a fresh directory for the worktree. The control service source is `88c995c` and the candidate service source is `5e0db44`. Each trial records its worker image digest in the JSON file.

```sh
git fetch origin experiment/channel-batch-readers
git worktree add --detach ../notification-reader-experiment 97447f4
cd ../notification-reader-experiment
git archive 88c995c | docker build -t notification-system:reader-control -
git archive 5e0db44 | docker build -t notification-system:reader-candidate -
python3 -B scripts/channel-isolation.py --compare-readers \
  --baseline-image notification-system:reader-control \
  --candidate-image notification-system:reader-candidate \
  --out-dir .local/results/reader-comparison
```
