# Delivery correctness evidence

The first test pauses the worker immediately before its conditional claim. A cancellation changes the modeled state to cancelled and the worker resumes. An actual local HTTP server counts POST requests.

| Stage | Change | Command | Observation |
| --- | --- | --- | --- |
| Red | Baseline ignored the boolean returned by the claim. | `go test -count=1 -v engine.go engine_test.go` in `internal/delivery` | One provider POST after cancellation. See [01-red.txt](01-red.txt). |
| Green | Return immediately when another transition won. | Same command | Zero POSTs and final state cancelled. See [02-green.txt](02-green.txt). |
| Policy | Added transport behavior, status persistence and provider retry boundaries. | `go test -race -cover -count=1 -v ./internal/delivery` | [03-policy.txt](03-policy.txt) records all observed states and statement coverage. |

Reproduce the cancellation test against the current code with `go test -run TestCancellationWinsZeroProviderPOSTs -count=1 -v ./internal/delivery`. These tests isolate the delivery engine with an in-memory store. PostgreSQL and end-to-end tests separately exercise the production state transitions.

## Provider contract references

- [Webhook.site Requests API](https://docs.webhook.site/api/requests.html) describes request capture and configured responses. Capture alone provides no external idempotency contract.
- [Go HTTP Transport](https://pkg.go.dev/net/http#Transport) documents conditions under which the standard transport can replay requests. Provider POST bodies are not rewindable and redirects are disabled.
- [Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests) illustrates a provider-defined key retention window. Enable idempotent-provider mode only for an upstream that explicitly supports it and configure the corresponding window.
