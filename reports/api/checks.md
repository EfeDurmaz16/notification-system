# API checks

Go 1.26.4, darwin/arm64, Apple M5 Pro. Tests use a repository stub to isolate HTTP semantics. WebSocket tests run a loopback HTTP server and real RFC 6455 client. PostgreSQL and the delivery pipeline have separate integration suites.

| Command | Observation |
| --- | --- |
| `go test -race -cover -count=1 ./internal/api` | Passed in 1.646 s, 82.8% statement coverage. |
| `go vet ./internal/api` | Passed. |
| `go test ./internal/api -bench=BenchmarkValidateSMS -benchmem -run='^$' -count=3` | 722.7, 736.7, 706.4 ns/op. 1088-1089 B/op, 13 allocations/op. |

The first WebSocket test attempt could not bind a loopback socket in the desktop sandbox. The rerun above included the socket test and race detector.

Cases include a partially valid batch, canonical JSON replay, conflicting content, an all-invalid replayable receipt, template substitution, schedule decoding, character limits, pagination and filtering errors, cancellation conflict, missing resources, metrics, WebSocket snapshots, terminal closure, and cross-origin rejection. The microbenchmark validates an SMS input in memory and does not measure DB commits, queueing, or provider latency.

## Ambiguous JSON regression

A reordered pair of `priority` and `Priority` keys had one canonical hash and two possible decoded priorities. The first test run failed for both create routes. A bounded JSON walk now rejects duplicate decoded keys, case aliases of known schema fields, nesting deeper than 64, and unpaired Unicode surrogate escapes. These request-level errors return 400 before a key is reserved. Valid JSON member order and whitespace still normalize to the same hash. Data validation and replay use the persisted receipt to determine whether the API accepted any item.

Focused regressions passed in 0.327 s. A fresh `go test -race -cover -count=1 ./internal/api` passed in 1.642 s with 85.9% statement coverage. See [red](idempotency-red.txt) and [green](idempotency-green.txt).
