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

## Watch a retry in the local simulator

Use the default Compose provider. Its triggers are prefixes in the notification's `content`. Each run below uses a fresh UUID in both the content and create key, so the simulator treats it as a fresh throttle scenario.

```sh
key=$(uuidgen)
id=$(curl -fsS "$API/v1/notifications" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $key" \
  --data "{\"recipient\":\"+12025550147\",\"channel\":\"sms\",\"priority\":\"normal\",\"content\":\"[mock:busy-once] $key\"}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -fsS "$API/v1/notifications/$id/history"
```

Repeat the last command after the second provider attempt. The `items` array records transitions in order, including `retry_wait` with `providerHttpStatus: 429` on attempt 1 and `accepted` with `providerHttpStatus: 202` on attempt 2. The current snapshot is available at `GET /v1/notifications/{id}`. History pages contain up to 50 retained changes. If a response contains `nextAfter`, pass its value as `?after=<nextAfter>` to fetch the next page.

| Content prefix | Simulator behavior | Outcome with default settings |
| --- | --- | --- |
| `[mock:busy-once]` | Rejects the first matching channel, recipient and content with 429 and `Retry-After: 1` | `retry_wait`, then `accepted` |
| `[mock:reject]` | Rejects the recipient with 400 | `failed` |
| `[mock:unknown]` | Accepts the POST and drops the connection before replying | `unknown` |
| `[mock:gateway-after-accept]` | Accepts the POST, then returns 503 and `Retry-After: 1` | `unknown` |
| `[mock:empty-ack]` | Accepts the POST and returns an empty 200 | `unknown` |
| `[mock:malformed-ack]` | Accepts the POST and returns a 202 without the required receipt fields | `unknown` |

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

Each API process admits 32 concurrent WebSockets by default (set `WS_MAX_SUBSCRIPTIONS` to 1–1024).
After the initial lookup, all sockets on a replica share one query every 250 ms for the distinct
notification IDs with active subscriptions. Slow clients receive the latest snapshot. When all places are in use, the handshake returns HTTP 503 with `Retry-After: 1`.
Wait at least that many seconds and reconnect to the same ID, or use the ordinary status endpoint.
The first frame contains the latest durable state, including a terminal result reached while disconnected.

## Continue a list without skipping through old rows

Request `GET /v1/notifications?channel=email&limit=50`. When the response contains `nextCursor`, pass it as `cursor` on the next request with the same filters. Omit `offset`. The cursor seeks directly after the last creation time and ID, including when several rows have the same creation time. Newer arrivals appear on a fresh first page. A status filter uses each row's current state at the time of the request.

## Validate the contract

```sh
./scripts/validate-openapi.sh
python3 scripts/check-contract.py
```

The first command validates the OpenAPI model, references and examples using a pinned Go tool. The second creates synthetic scheduled work against the running local service, captures its real responses, and validates those bodies against the schemas of the matching OpenAPI operations.
