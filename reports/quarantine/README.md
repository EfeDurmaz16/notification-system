# Kafka quarantine regression

Run `make quarantine` to provision a disposable Compose project with the recording HTTP provider. The fixture creates the normal single-notification topic with one partition so the invalid record and the following valid event always share a partition. The worker, outbox connector and quarantine topic use the normal application code.

| Input on source partition 0 | Expected evidence |
|---|---|
| Invalid ready event at offset 0 | The only record in `notifications.invalid` has key and value `notifications.normal.single/0/0`. The provider receives zero POSTs. The broker reports committed offset 1, end offset 1 and lag zero. |
| Valid notification submitted through the API | CDC publishes its ID and attempt at offset 1. The API reports `accepted`. The provider records exactly one POST for that ID. The broker reports committed offset 2, end offset 2 and lag zero. |

The [recorded local run](result.json) contains the broker coordinates, offsets, API notification ID and provider counts. GitHub's integration job runs the same command and uploads its JSON result. This check covers malformed input during uninterrupted processing. A worker failure between quarantine publication and source offset commit can produce another marker with the same source coordinates.
