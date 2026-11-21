# Webhook.site integration smoke

## Full Compose delivery, 2026-11-21

`scripts/webhook-smoke.sh` built and ran the review working tree in an isolated Compose project. The Go API admitted one synthetic email, PostgreSQL recorded it, Debezium published the outbox event, and a worker submitted it to a real, disposable Webhook.site endpoint. A separate HTTP read of the notification returned the configured provider message ID. The image manifest from this run was `sha256:a86d97e7a3ef10444b9e74cc3f9dd604b0b3a3272ebcded63cc114d701909781`.

| Observation | Result |
| --- | --- |
| Local notification ID | `fbf98b7a-194b-4ba5-a56d-ee354c4912ac` |
| Create API response | HTTP 202 |
| Webhook.site request history | Exactly one POST, with the same notification ID in the header |
| External request body | Exact match for the synthetic email recipient, channel and content |
| External acknowledgement | Configured HTTP 202, accepted status, provider message ID and timestamp |
| Subsequent status API read | `accepted`, persisted message ID matches the provider receipt |
| Cleanup | Token deletion returned HTTP 204. Compose removed the temporary containers and volumes. |

```text
PASS notification_id=fbf98b7a-194b-4ba5-a56d-ee354c4912ac api_create=202 api_outcome=accepted provider_posts=1 message_id_verified=true payload_verified=true
Disposable token cleanup: HTTP 204
```

The package uses a different disposable URL for each probe. [Setup and reproduction](../docs/webhook-site.md) · [Probe source](../scripts/webhook-compose.py)

## Earlier engine-only check, 2026-11-20

The production Go HTTP delivery client sent one synthetic email to a disposable token at 19:34:49 UTC. The test used an in-memory state recorder, verified the outbound JSON and configured receipt, and deleted the token with HTTP 204. It passed in 1.85 seconds. The newer run above additionally exercises API admission, persistent state, queue processing and status lookup.
