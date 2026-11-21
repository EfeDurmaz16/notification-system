# Review the evidence

Begin with the [assessment checklist](assessment-requirements.md) for the implementation and the [verification guide](../docs/verification.md) to run it on your checkout. The experiments below used a synthetic provider. Their latency ends when the service records a provider acknowledgement.

| Question | Recorded observation | Reproduce |
| --- | --- | --- |
| Can a cancellation race cause a send? | When cancellation won the conditional database update, the provider received zero POSTs. A separate hard-stop test found six unresolved IDs that reached the provider before the worker died. [Per-ID audit](https://github.com/EfeDurmaz16/notification-system/blob/4ca75cc/reports/channel-cutover/rebalance/README.md) | `make ci`, `make rebalance` |
| What happens when all three channels stay busy? | A 15-minute run on v1.1.0 sent 36,000 notifications per channel, including deliberate rejections and lost acknowledgements. All 108,000 IDs had one provider POST each. [Versioned release report](https://github.com/EfeDurmaz16/notification-system/releases/download/v1.1.0/notification-system-verification-report.md) | `make sustained-mixed` |
| Can one standalone request pass a campaign? | During a 10,000-SMS campaign, a standalone probe took 100.46 ms and a same-priority one-item batch took 108.312 s. They were created 7.19 ms apart. [Probe timestamps and workload](https://github.com/EfeDurmaz16/notification-system/blob/4ca75cc/reports/ingress-share/README.md) | `python3 scripts/ingress-share.py --help` for workload options |
| Where did batch push wait during an SMS campaign? | Per-channel batch readers reduced push p95 from 42.98 s to 187 ms and 45.74 s to 246 ms. SMS throughput during the probes fell by 5.65% and 6.26%. [Matched trials and selection rule](../docs/decision-history.md#a-push-batch-waiting-behind-an-sms-campaign) | See the [routing guide](../docs/channel-batch-routing.md) for the experiment and migration checks. |

## Historical evidence

The [offline archive](historical-evidence.tar.gz) contains the complete 145-file evidence directory as it stood at commit `4ca75cc`. It includes raw JSON, per-ID CSVs, TDD transcripts, the original index and workload notes. Each run retains its recorded environment. The snapshot commit identifies where the evidence was packaged. Individual runs identify the implementation tested. [Browse the same files on GitHub](https://github.com/EfeDurmaz16/notification-system/tree/4ca75cc/reports)

To inspect one result locally without extracting the whole archive:

```sh
tar -xOzf reports/historical-evidence.tar.gz reports/bench/tuned/summary.md
```
