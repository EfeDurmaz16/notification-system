# Worker shutdown after provider acceptance

Six normal-priority SMS were admitted while the only worker was offline. The test waited for all six Kafka records to be published, then started one worker with one send slot. The local provider recorded acceptance immediately and delayed its HTTP response by three seconds. Once the first ID had one provider POST and was still `sending`, the test issued `docker compose stop worker`. Five IDs were pending. A replacement worker started only after the original container exited.

| Observation | [Before](before.json) | [After](after.json) |
| --- | ---: | ---: |
| In-flight ID at worker exit | unknown, no receipt | accepted, receipt persisted |
| Remaining pending IDs at worker exit | 5 | 5 |
| Provider POSTs for those five at worker exit | 0 | 0 |
| Time spent in Compose stop | 0.340 s | 2.983 s |
| Worker exit code | 0 | 0 |
| Final application states | 5 accepted, 1 unknown | 6 accepted |
| Final provider POSTs, verified for each ID | 1 each | 1 each |

With the fix, the interrupted ID had the same message ID before and after replacement. The provider accepted all six distinct notifications in each run. All three source partitions had zero lag by the end. An empty partition may have no committed offset. The JSON files include the worker image digest, all notification IDs, provider POST counts and source offsets. The test uses a non-idempotent fixture with a response delay shorter than the five-second HTTP timeout.

## Scale-down with a backlog

The [original traffic wave](../traffic-wave/scripted.json) and the [repeat after the fix](scripted.json) each offered 12,250 SMS across seven 25-second phases, scaling workers from one to three and back to one. In the original run, the provider received one POST for every ID, but one notification ended in `unknown`. In the repeat, all 12,250 IDs ended in `accepted`, each had exactly one provider POST, and all three source partitions drained. The six-ID experiment above isolates the shutdown race. The larger wave checks the outcome under load. Other local work overlapped with the repeat, so the latency difference cannot be attributed to this change.

## Reproduce

```sh
make drain
```

The runner builds the checkout in an isolated Compose project and writes its result under `.local/results/`. To check the regression against a prebuilt image from `0680a9b`, run `python3 -B scripts/worker-drain.py --image IMAGE --report .local/results/before.json`. The original image fails the assertion that its in-flight acknowledgement was persisted. Both runs preserve per-ID outcomes before checking the assertion and remove their test containers and volumes.
