# Review the evidence

Begin with the [assessment checklist](assessment-requirements.md) for the implementation and the [verification guide](../docs/verification.md) to run it on your checkout. The experiments below used a synthetic provider. Their latency ends when the service records a provider acknowledgement.

## Standalone channel isolation

Each run submitted 30 standalone emails at normal priority in three conditions. The backlog case admitted 1,500 standalone SMS requests first. The cooldown case held 180 SMS in the shared channel limiter for 12 seconds. One worker with 24 slots served all channels. Each column lists two fresh local Compose runs. [Measurements and image IDs](standalone-isolation.json)

| Measurement | Shared standalone topic `4ca75cc` | Per-channel standalone topics `24e727d` |
| --- | --- | --- |
| Email p95 on an idle system | 473 / 548 ms | 434 / 519 ms |
| Email p95 behind the SMS burst | 17.27 / 17.31 s | 562 / 460 ms |
| SMS completions/s across that burst | 81.74 / 81.50 | 77.14 / 77.03 |
| Email p95 with SMS paused | 14.00 / 13.77 s | 442 / 484 ms |
| Emails accepted before the first paused SMS was claimed | 0 of 30 in both runs | 30 of 30 in both runs |

The first email entered with 1,451 to 1,457 SMS outstanding. Across all four runs, each of 7,080 API IDs had one provider POST and one persisted acceptance. The observed peak was 87 POSTs for one channel in a rolling second. SMS burst throughput fell by 5.6% and 5.5% in the two comparisons.

```sh
python3 -B scripts/single-channel-isolation.py --revision 4ca75cc --out .local/results/single-baseline.json
python3 -B scripts/single-channel-isolation.py --revision 24e727d --out .local/results/single-candidate.json
```

## Three-channel sustained run

On November 21, 2026 the implementation at `a97d430` ran for 15 minutes with three worker replicas. The synthetic provider received exactly one POST for each of 108,000 notification IDs. Each channel received 36,000. Per channel, 35,640 were accepted, 180 were deliberately rejected and 180 had deliberately lost acknowledgements. The API recorded the expected outcome for every ID. Accepted p95 was 297.4 ms for email, 297.2 ms for push and 297.6 ms for SMS. Reproduce with `make sustained-mixed`.

In a separate 10,000-SMS burst, the API's create-response p95 was 203 ms. Notification p95 from creation to a persisted outcome was 120.0 seconds. All 10,000 IDs reached acceptance and the provider saw each ID once. The peak was 90 SMS attempts in a rolling second. Reproduce with `make matrix`.

## Other experiments

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
