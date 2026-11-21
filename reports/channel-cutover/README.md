# Batch routing cutover and rollback

A disposable Compose stack admitted 16 normal-priority notifications across SMS, email and push. The baseline API and worker came from revision `88c995c`. The candidate kept the existing standalone and shared batch readers and added separate readers for channel-specific batch topics.

| Transition | What the test observed |
| --- | --- |
| Legacy producer to dual readers | The old API admitted four batch notifications with workers offline. The dual reader delivered three and skipped the cancelled ID. |
| Switch batch writers with CDC stopped | The old and new APIs wrote both topic formats to the outbox. The candidate scheduler promoted an old scheduled push to its push topic. A confirmed pre-send dial failure on the old shared email route was retried on the email topic. The second attempt made the only provider POST for that ID. |
| Restore the legacy writer | Three split batch records and one new shared record accumulated while CDC was stopped. Dual readers consumed both formats after CDC resumed. |
| Restore the legacy worker | The runner compared expected outbox event counts with Kafka end and committed offsets for nine normal-priority partitions across five topics. All published records had been consumed. The original worker then delivered a new SMS written by the original API. |

Every accepted ID had a provider receipt and exactly one provider POST. The cancelled ID had zero. All 15 POSTs were accepted, provider deduplications were zero, and the application had no unknown IDs. The test checked the outbox topic for each attempt, including the unchanged standalone push route. The [raw report](result.json) contains the image IDs, per-notification audit and offsets immediately before the reader downgrade.

Run on the candidate checkout with Go application images built by Docker:

```sh
python3 -B scripts/channel-cutover.py --out .local/results/channel-cutover.json
```

The script builds the baseline image from the archived Git revision and the candidate from the checkout. It creates its own ports and volumes, uses the local recording provider and removes its stack and image tags on exit. This small cutover checks routing compatibility, cancellation, scheduling and a safe retry. Load, multi-replica failure and latency isolation are separate experiments.
