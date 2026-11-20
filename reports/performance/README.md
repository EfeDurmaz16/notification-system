# Local microbenchmarks

Go 1.26.4, macOS arm64, Apple M5 Pro. Each row shows the median of five samples from `make performance RESULTS=reports/performance`. Packages ran serially. The load matrix had finished. The fault suite was active separately on the host, including a stale-send wait and service restarts.

| Operation | ns/op | Bytes/op | Allocations/op |
|---|---:|---:|---:|
| Compute percentiles over 1,000 synthetic values | 9629 | 0 | 0 |
| Update the mock provider rolling window | 11.57 | 47 | 0 |
| Parse and validate an SMS payload | 718.6 | 1088 | 13 |
| Execute the in-process delivery path | 3429 | 6471 | 80 |
| Build and serialize an outgoing request | 479.8 | 1161 | 13 |

The delivery benchmark uses an in-memory store and a synthetic HTTP round trip. The request-building row is a component of that path. Compare service-level queueing and provider latency with the [end-to-end matrix](../bench/) and inspect the [five raw samples](bench.txt) before drawing conclusions.
