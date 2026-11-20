# Effect of weighting Kafka fetch windows

Two consecutive local experiments each submitted ten simultaneous batches of 1,000 SMS. Both used three worker replicas, 24 worker slots per replica, a 20 ms synthetic provider and a shared 100-attempt sliding-second channel quota. Each input contained the same distribution of high, normal and low priorities. Each image ran the complete load, stress, fault and mixed-channel matrix.

| Measure | Equal fetch windows | Weighted fetch windows |
| --- | ---: | ---: |
| Create response p95 across 10 batches | 201.7 ms | 187.7 ms |
| Create start to durable acceptance p95 | 108.61 s | 111.47 s |
| Observed terminal notifications per second | 86.23 | 84.78 |
| Peak actual SMS attempts in one sliding second | 94 | 94 |
| Time to last terminal observation | 115.97 s | 117.95 s |

## Where the first 30 seconds went

The anchor is the first durable send claim for each run. Both deployments selected jobs using the same local 6:3:1 cycle. The reference exposed up to 48 records to the scheduler per priority reader. The tuned readers exposed up to 144 high, 72 normal and 24 low records. This changes how often the scheduler can see pending work from each priority while the readers wait for their batches to commit.

| Priority | Reference claims | Tuned claims | Reference terminal p95 | Tuned terminal p95 |
| --- | ---: | ---: | ---: | ---: |
| high | 953 | 1518 | 109.32 s | 64.10 s |
| normal | 892 | 825 | 105.49 s | 91.15 s |
| low | 838 | 366 | 109.85 s | 115.90 s |

## Correctness and interpretation

- Each run reported 10,000 durable provider acceptances and 10,000 provider POSTs. The independent post-run audit checked 10,000 IDs on the tuned provider: 0 missing, 0 with multiple POSTs, 0 lookup errors.
- In this matched pair, the change in high-priority p95 was -45.22 s (-41.4%). The change in low-priority p95 was +6.04 s. Low-priority jobs continued to receive sends in the early window.
- Tuned DB-trigger p95 was 111.30 s from pending to sending, and 26.16 ms from sending to accepted. The former includes CDC, Kafka, scheduling, quota waiting and the claim. The latter includes the HTTP exchange and durable completion. Stage percentiles are computed independently.
- One run per image on a shared local host cannot establish a statistically significant production speedup. The single-channel quota strongly constrains aggregate throughput. These measurements describe provider acceptance; recipient-device delivery is outside the fixture contract.

[Equal-window results](final/summary.md) · [Weighted-window results](tuned/summary.md) · [Reference early claims](final/burst-10000-dispatch.tsv) · [Tuned early claims](tuned/burst-10000-dispatch.tsv) · [Reproduce the matrix](../../scripts/bench.sh)
