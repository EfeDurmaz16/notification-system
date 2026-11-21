# Sustained traffic on SMS, email and push

The experiment offers 40 notifications per second on each of three channels for 15 minutes. Every 250 ms the client submits a batch of 30, with ten notifications per channel. All three priorities appear on each channel. Three worker replicas each have 24 delivery slots. The local HTTP provider waits 20 ms before responding and has no idempotency guarantee.

The input deliberately gives one in every 200 notifications on each channel a permanent rejection and gives another one a lost acknowledgement after the provider has accepted it. At the offered rate, the expected five-minute averages per channel are 39.6 confirmed acceptances/s, 0.2 failures/s and 0.2 unknown outcomes/s. Small differences can arise from changing queue delay and sample boundaries. A new stack starts with an empty history, so the rate ramps for the first five minutes.

## Observed results

The verified run started at 12:00 UTC on November 21, 2026. In 900.010 seconds all 108,000 admitted notifications reached their specified terminal states (120.00/s). All 3,600 batch creates returned HTTP 202, with an 8.50 ms API p95. A per-ID audit found exactly one provider POST for each of 108,000 IDs, zero lookup errors and zero mismatched final outcomes. The provider recorded 107,460 acceptances, including the 540 whose acknowledgements it deliberately dropped.

| Channel | Admitted | Confirmed | Failed | Unknown | Peak POSTs in 1 s | Creation to confirmed p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SMS | 36,000 | 35,640 | 180 | 180 | 50 | 291.803 ms | 325.644 ms |
| Email | 36,000 | 35,640 | 180 | 180 | 50 | 292.192 ms | 325.380 ms |
| Push | 36,000 | 35,640 | 180 | 180 | 50 | 291.246 ms | 325.152 ms |

After five minutes of traffic and before arrivals stopped, 20 samples across the three channels showed 39.567 to 39.617 confirmed acceptances/s, 0.197 to 0.203 failures/s and exactly 0.2 unknowns/s. In the independent status-history check, each channel reported 11,878 confirmed, 60 failed and 60 unknown during the trailing 300 seconds. All nine series fell within the bounds from concurrent database reads. The maximum sampled queue depth was 30. It returned to zero after admissions stopped.

## Reproduce

```sh
python3 -B scripts/sustained-mixed.py --out .local/results/sustained-mixed
```

Use an empty output directory. The runner builds the current checkout, assigns loopback ports and provisions a separate Compose project with fresh volumes. Cleanup removes those containers, volumes and the generated image tag. The run samples `/metrics` roughly every 30 seconds and polls API batch receipts for terminal status. After timing ends, it looks up the actual POST count for every notification ID at the provider.

## Evidence

- [Load generator](load.json) records admission, terminal outcomes, latency percentiles, aggregate provider counters, the highest POST count in any rolling second on each channel and the per-ID receipt audit.
- [Channel results](channels.csv) compare the final status of each durable notification with its planned outcome. Latency runs from database creation to persisted provider acknowledgement and covers confirmed acceptances on that channel.
- [Rates](rates.csv) are live API responses. [Timeline](timeline.csv) shows cumulative admissions and observed terminal receipts every second. Their difference includes client polling delay.
- [Window cross-check](window-check.json) compares one mature scrape with independently counted terminal events while inserts and expirations continue.
- [Environment](environment.json) identifies the service image and settings used. One local run measures short-term service and covers several complete five-minute windows. Memory leaks, long-running maintenance, production provider behavior and day-long traffic patterns need separate evidence.

## Initial run

The [first recording](first-run/load.json) began at 11:42 UTC on November 21, 2026. All 108,000 notifications reached their expected terminal counts with 108,000 provider POSTs. The eight-reader audit made 108,000 individual GET requests after timing ended. It returned 94,122 readable counts of one and 13,878 read errors, causing the run to fail its per-ID assertion. A lookup error leaves that ID unverified. The original reader retained only the number of errors. It also exited before the per-channel latency query ran. The [initial timeline](first-run/timeline.csv), [rates](first-run/rates.csv), [window check](first-run/window-check.json) and [settings](first-run/environment.json) are retained here.

The revised reader checks each ID sequentially with HTTP keep-alive and records the first lookup error if one occurs. The runner saves its channel breakdown before checking the load generator exit code. Each Compose project and its data volumes were exclusive to its run. Other local containers shared the host, including a separate million-row database experiment during the initial workload. These are absolute measurements under the observed host conditions.
