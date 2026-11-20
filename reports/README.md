# Verification and review guide

The release candidate was exercised with a real PostgreSQL, Kafka, Debezium and Redis stack, plus a local HTTP provider. The external provider contract was checked separately with one synthetic Webhook.site request.

| Question | Evidence |
|---|---|
| Does a successful cancellation prevent a provider POST? | [Initial red and green tests](tdd/README.md), [PostgreSQL concurrent claims](store/postgres-integration.txt), [live Compose smoke](ci/compose-smoke.txt) |
| What did we actually observe under load? | [Before/after priority experiment](bench/comparison.md), [release candidate measurements](bench/tuned/summary.md), [per-ID audit on 10,000 messages](bench/tuned/burst-10000.json), [reference measurements before priority tuning](bench/final/summary.md) |
| How much time did work spend before the send claim versus at the provider? | [SQL stage breakdown](bench/tuned/burst-10000-stages.tsv), [measurement definitions](../docs/persistence.md#stage-latency-after-a-load-run) |
| What happens if a dependency stops or a worker dies after acceptance? | [Fault sequence and original results](recovery.md), [release candidate rerun](final-recovery/README.md) |
| Can an independent client safely replay? | [Request-body identity regression](api/checks.md), [later provider rejection after a lost acknowledgment](tdd/README.md) |
| How expensive are validation and serialization inside Go? | [Five samples per benchmark](performance/README.md) |
| Is the API contract usable? | [Live OpenAPI contract](api-contract.md), [API examples](../docs/api-examples.md), [real external capture](webhook-site.md) |
| Can I trace a message? | [OTLP trace from the API through the worker to HTTP](trace.md), [operational queries](../docs/tracing.md) |
| Can I reproduce the checks? | [Local and GitHub CI](ci/README.md), [full verification commands](../docs/verification.md), [tested container inventory](infra/README.md) |

## Reading the measurements

The three result directories correspond to successive runs. `bench/` holds the initial measurements. `bench/final/` is the matched baseline after API and retry correctness fixes, before tuning Kafka fetch windows. `bench/tuned/` contains measurements with the weighted fetch windows. Each includes its own environment and raw JSON files. Percentiles reported in the summary use the population for that run. Single-batch create timing represents one observation.

In the tuned 10,000-SMS burst, the service accepted all ten 1,000-item requests, reached provider acceptance for every item, and the provider counted exactly one POST for **each** of the 10,000 IDs. The receipt audit ran after the timed interval. The provider observed at most 94 SMS requests in a rolling second. This demonstrates the behavior in this run with a synthetic 20 ms provider and persistent single-host infrastructure.

One controlled failure run produced 10 accepted, 10 failed and 10 unknown statuses. The fixture actually accepted the ambiguous ten before dropping its responses. Their unresolved status is intentional and observable.
