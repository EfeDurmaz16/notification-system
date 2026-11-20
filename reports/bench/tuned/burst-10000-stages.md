# Stage latency after tuning priority fetch windows

Run `GKSQHDVP2S7VG2RJ7KAQL2IFIZ` submitted 10,000 SMS notifications in ten
1,000-item requests. PostgreSQL recorded complete pending, sending, and accepted
boundaries for all 10,000 first attempts.

| Priority | Count | Pending to sending p95 | Sending to accepted p95 | Pending to accepted p50 | Pending to accepted p95 | Pending to accepted p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| All | 10,000 | 111.30 s | 26.16 ms | 55.40 s | 111.32 s | 116.50 s |
| High | 3,334 | 63.90 s | 24.97 ms | 33.54 s | 63.92 s | 67.91 s |
| Normal | 3,333 | 90.98 s | 26.38 ms | 58.97 s | 91.00 s | 94.09 s |
| Low | 3,333 | 115.70 s | 27.14 ms | 95.51 s | 115.73 s | 117.29 s |

| Seconds since first claim | High claims | Normal claims | Low claims |
| --- | ---: | ---: | ---: |
| 0 to 10 | 441 | 307 | 156 |
| 10 to 20 | 530 | 262 | 113 |
| 20 to 30 | 547 | 256 | 97 |
| 0 to 30 | 1,518 | 825 | 366 |

In the first 30 seconds, high-priority notifications received 56.0% of send
claims (35.5% in the fixed reference), normal 30.5%, and low 13.5%. The
backlog at that boundary was 1,816 high, 2,508 normal, and 2,967 low. A larger
share of early service shifted latency from high to low. Overall p95 increased
from 108.42 s to 111.32 s. These are two local observations, not a global
fairness guarantee. The first 10 s also include batch ingestion and CDC startup
for this run; the table includes two later 10 s slices for comparison.

[Raw stage samples by percentile](burst-10000-stages.tsv) ·
[Dispatch windows and pending counts](burst-10000-dispatch.tsv) ·
[Client-side run](burst-10000.json) ·
[Fixed reference](../final/burst-10000-stages.md) ·
[Measurement boundaries](../../../docs/persistence.md#stage-latency-after-a-load-run)
