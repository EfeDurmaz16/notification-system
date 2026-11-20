# API examples

Use the local API after starting Compose. The examples use synthetic recipients.

```sh
API=http://127.0.0.1:8080
curl -fsS "$API/healthz"
```

## Create and replay a notification

```sh
key=$(uuidgen)
body='{"recipient":"+12025550147","channel":"sms","priority":"high","content":"Your pickup is ready."}'
curl -isS "$API/v1/notifications" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $key" --data "$body"
```

A successful response is HTTP 202 with `id` and `batchId`. Repeat the call with the same key and body after a lost response. Reusing a persisted key for different input produces HTTP 409. Keys share one namespace in this single-tenant assessment service. The `X-Correlation-ID` response header identifies the HTTP request in logs.

## Submit a partially valid batch

```sh
curl -isS "$API/v1/batches" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" --data '{
    "notifications": [
      {"recipient":"demo@example.com","channel":"email","priority":"normal","template":"Hello {{name}}","variables":{"name":"Ari"}},
      {"recipient":"invalid","channel":"sms","priority":"low","content":"Sale starts today."}
    ]
  }'
```

The first item receives an ID. The second returns a validation error at index 1. Replay the entire original batch with the same key to recover its accepted IDs. To submit corrected input, use a new key and send only corrected items. Each batch contains 1 to 1000 items and each request body is limited to 5 MiB.

## Schedule and cancel

```sh
curl -isS "$API/v1/notifications" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" --data '{
    "recipient":"demo-device-1","channel":"push","priority":"normal",
    "content":"Review your weekly summary.","scheduledAt":"2099-04-15T12:00:00Z"
  }'
# Copy the returned IDs into your shell.
curl -isS -X POST "$API/v1/notifications/$id/cancel"
curl -fsS "$API/v1/notifications/$id"
curl -fsS "$API/v1/batches/$batchId"
```

Cancellation returns HTTP 204 when successful. A dispatch that has already started can return HTTP 409. Treat provider status `accepted` as an acknowledgement from the provider. Treat `unknown` as an unresolved attempt that requires reconciliation.

## List, filter and watch

```sh
curl -fsS "$API/v1/notifications?channel=sms&status=accepted&limit=20&offset=0"
curl -fsSG "$API/v1/notifications" \
  --data-urlencode 'from=2026-01-01T00:00:00Z' \
  --data-urlencode 'to=2026-12-31T23:59:59Z'
curl -fsS "$API/metrics"
```

Use a WebSocket client to open `ws://127.0.0.1:8080/v1/notifications/{id}/events`. Frames are current notification snapshots. Reconnect with a fresh status read after interruption. Independent notifications for the same recipient may complete in different orders.

## Validate the contract

```sh
./scripts/validate-openapi.sh
python3 scripts/check-contract.py
```

The first command validates the OpenAPI model, references and examples using a pinned Go tool. The second creates synthetic scheduled work against the running local service, captures its real responses, and validates those bodies against the schemas of the matching OpenAPI operations.
