# Verify an external provider acknowledgement

Run this command from the checkout with Docker Compose v2, Python 3 and outbound internet access:

```sh
scripts/webhook-smoke.sh
```

It builds the service in a fresh Compose project with isolated data and ports. It configures a disposable Webhook.site endpoint, submits one email to `assessment-smoke@example.invalid` through the local create API, then queries that ID until the worker persists the configured provider message ID. It also reads the external request history and checks that exactly one POST arrived with the same `X-Notification-ID`, recipient, channel and content. On success the script prints `PASS`, removes the disposable stack and requests deletion of the capture. The token has a ten-minute expiry and is omitted from the output.

## Configure a capture by hand

1. Create a fresh Webhook.site URL. Set its response status to **202** and its content type to **application/json**. Paste the following response body. A fixed ID and timestamp make a reproducible receipt for a single probe. The assessment's `ISO8601` stands for a formatted datetime. [Configuration fields](https://docs.webhook.site/api/tokens.html#create-token)

   ```json
   {
     "messageId": "00000000-0000-4000-8000-000000000001",
     "status": "accepted",
     "timestamp": "2026-11-21T12:00:00Z"
   }
   ```

2. On an **empty demo stack**, start Compose with the capture URL. Existing queued or scheduled work on a reused stack would reach this endpoint too. Keep the URL private.

   ```sh
   export PROVIDER_URL='https://webhook.site/<your-uuid>'
   PROVIDER_IDEMPOTENT=false docker compose up --build -d --wait
   ```

3. Run the [single-create example](../README.md#run-one-notification) starting at the `api=` line. Read `GET /v1/notifications/{id}` until it returns `status=accepted` and the configured `messageId`. A default empty HTTP 200 or a malformed 202 leaves the outcome `unknown`. Finding a matching capture proves the POST reached Webhook.site. The configured acknowledgement supplies the evidence used for acceptance. [Investigating an unknown ID](operations.md#checking-an-unknown-notification-in-webhooksite)

4. Restore the local provider before running any load tests:

   ```sh
   unset PROVIDER_URL
   docker compose up -d --wait
   ```

Webhook.site provides a capture endpoint. Its configured reply does not document provider-side idempotency or recipient delivery. The [request history API](https://docs.webhook.site/api/requests.html) is useful when investigating an unresolved attempt.

## Other provider contracts

Keep `PROVIDER_ACK_MODE=assessment` for this exercise. Set `PROVIDER_ACK_MODE=message_id` for a provider whose contract explicitly defines a nonempty JSON `messageId` on a 2xx response as proof of acceptance. Both modes issue the same POST. The mode controls how the worker reads the response.

| Response | `assessment` | `message_id` |
|---|---|---|
| 202, complete accepted receipt with a valid timestamp | Accepted | Accepted |
| 200, complete accepted receipt | Unknown | Accepted |
| 201, `{"messageId":"m1"}` | Unknown | Accepted |
| 202, ID, accepted status and literal timestamp `"ISO8601"` | Unknown | Accepted |
| Any 2xx with an empty body, blank ID or explicit rejected status | Unknown | Unknown |

The table assumes an ordinary provider without an idempotency guarantee. An HTTP capture with the right notification ID can support an investigation. Only the configured receipt contract can confirm acceptance. See [operator instructions](operations.md#checking-an-unknown-notification-in-webhooksite).
