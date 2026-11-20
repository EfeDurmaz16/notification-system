# A standalone low SMS during a 10,000-SMS campaign

Does a customer waiting for one SMS have to wait for a campaign sent earlier at the same priority? This experiment measures elapsed time from notification creation in PostgreSQL to recording the local provider's acceptance. Handset delivery and HTTP create latency are outside that interval.

Each run starts with an idle standalone control. Ten concurrent requests then submit 1,000 low SMS each through the baseline batch API. After the provider has observed at least 500 campaign attempts, the harness submits two identical low SMS to the selected API. One uses `POST /v1/notifications` and the other uses `POST /v1/batches` with one item. Both use the same synthetic recipient as the campaign. The second trial reverses the submission order.

The baseline routes both requests to `notifications.low`. The experimental API gives the standalone request access to an independent reader by routing its initial ready event to the otherwise idle `notifications.normal` topic. The product priority in PostgreSQL stays `low`. Both variants use the same three workers with 24 slots each, shared SMS quota and local provider. The provider adds 20 ms of latency per successful call.

| Trial | Standalone under load | One-item batch under load | Campaign SMS still outstanding at standalone acceptance | Total campaign time |
|---|---:|---:|---:|---:|
| [Baseline 1](baseline-1.json) | 109.760 s | 107.575 s | 7 | 115.739 s |
| [Separate reader 1](candidate-1.json) | 0.583 s | 108.490 s | 9,445 | 114.430 s |
| [Baseline 2](baseline-2.json) | 116.117 s | 115.693 s | 2 | 122.135 s |
| [Separate reader 2](candidate-2.json) | 0.163 s | 114.075 s | 9,483 | 120.773 s |

The standalone SMS arrived with 9,495 to 9,499 campaign items outstanding. Its time from send claim to recorded acceptance stayed between 22.8 and 24.5 ms across the four runs. The difference came from the wait before the claim. Reversing submission order gave the same qualitative result, including when the standalone request arrived 11.7 ms after the batch control.

Each run recorded 10,000 accepted campaign items, two accepted loaded probes and exactly 10,002 provider POSTs during the loaded phase. The provider recorded one POST per probe ID. The [outbox audit](routing-audit.json) ties each probe's persisted product priority to its actual dispatch topic.

These two pairs demonstrate the effect of independent dispatch for a sparse interactive arrival under bulk load. They are too small to estimate a latency percentile or the throughput cost of sustained interactive demand. The test used spare capacity in a pre-existing reader and established no service guarantee under competing high or normal traffic.

[Reproduction steps](REPRODUCE.md) and [test harness](../../scripts/single-bulk-probe.py).
