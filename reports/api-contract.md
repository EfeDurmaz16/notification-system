# API contract verification

Tested on 20 November 2026 on macOS arm64, Go 1.26.4, PostgreSQL 16 and an isolated loopback API. The API used its actual migrations and HTTP handler. Synthetic records were scheduled in 2099 so no external provider was contacted.

| Check | Result |
| --- | --- |
| `./scripts/validate-openapi.sh` | Passed. Pinned kin-openapi v0.153.0 validated OpenAPI 3.0.3, references, defaults and examples. |
| `python3 scripts/check-contract.py --url http://127.0.0.1:18087` | Passed after rebuilding the final all-invalid batch receipt change. Ten live JSON bodies passed their response schemas. Two cancellations returned 204 with empty bodies. |
| Negative control | A numeric ID in a deliberately incomplete Notification example failed schema validation with "value must be a string" and missing-field errors. |

The live sample covers creation, replay with the same response, changed-body conflict, scheduled state, lookup after cancellation, partial batch acceptance, live batch lookup, all-invalid batch errors, list filtering and health. Full delivery pipeline and performance measurements are recorded separately.
