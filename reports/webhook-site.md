# Webhook.site integration smoke

Observed at 2026-11-20 19:34:49 UTC (22:34:49 Istanbul). The local Go test invoked the production delivery engine and inspected the disposable endpoint's request history.

| Check | Observed |
|---|---|
| Command | `scripts/webhook-smoke.sh` |
| External provider requests captured | 1 POST |
| Outbound JSON | Matched expected `to`, `channel`, and `content` |
| Provider result | HTTP 202, valid accepted body, expected provider message ID |
| Application outcome | `accepted` |
| Token cleanup | HTTP 204 |
| Test | PASS, 1.85 s |

The recipient was synthetic. The capture URL was held in memory for the run. This check exercises the external HTTP provider path; the local Compose suite covers database persistence and asynchronous delivery.
