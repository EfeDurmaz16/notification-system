# Verification evidence

| Evidence | Command or run | Result |
|---|---|---|
| [First live smoke](compose-smoke.txt) | `make smoke API_PORT=18080` | Cancelled ID produced zero provider requests after its due time. Replayed accepted item produced one. The client observed scheduled and cancelled WebSocket snapshots. |
| [Local compile and race gate](local-check.txt) | `make check` | Formatting, vet, tests, race detector and native binaries passed. Live integrations are opt-in under this target. |
| [Local full CI](local.txt) | `API_PORT=18080 make ci` | Passed against Compose: versioned SQL migrations, DB and Kafka redelivery tests, Redis coordination, real HTTP delivery, WebSocket and 10 live JSON response contract checks. |
| [First hosted CI](https://github.com/EfeDurmaz16/notification-system/actions/runs/44803997682) | GitHub Actions, revision `d37dd3d`, Ubuntu 24.04 | Both jobs passed. The integration job exercised the full stack and a 300-item bounded burst. |

Host-side Go was 1.26.4 on macOS arm64. Test provider traffic and database credentials in the transcript belong to the local synthetic environment. The first hosted run predates the final release. Release tags run the complete hosted checks again before publishing artifacts.
