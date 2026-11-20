# Webhook.site provider

Run `scripts/webhook-smoke.sh` for one live outbound notification. The script creates a disposable capture URL configured for JSON and HTTP 202, calls the production delivery engine with a synthetic email addressed to the reserved domain `example.invalid`, and verifies the captured wire body and returned provider message ID. The capture expires in 600 seconds; the test also requests deletion on exit. The engine uses an in-memory state recorder in this smoke test.

The production worker takes its capture URL from `PROVIDER_URL`. Keep `PROVIDER_IDEMPOTENT=false` for Webhook.site. For a manual run, configure a fresh token through the [token API](https://docs.webhook.site/api/tokens.html) with a 202 status, content type `application/json`, and response body containing `messageId`, `status: accepted`, and an RFC 3339 `timestamp`. Supply its URL through the worker environment.

The configured response acknowledges provider acceptance. A constant simulated message ID is valid for the single probe. The [request history](https://docs.webhook.site/api/requests.html) indicates receipt by the capture endpoint. The demo provides no recipient delivery receipts or provider idempotency contract. Use the local simulated provider for load tests.
