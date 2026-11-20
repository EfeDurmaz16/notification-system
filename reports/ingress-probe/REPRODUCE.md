# Reproduce the ingress competition experiment

Use this branch with Go 1.26.4, Python 3, Docker Compose, and the synthetic provider. Start the local stack on port 18080 with three workers. Stop other traffic for the duration. All scenario traffic uses SMS with product priority `low`. The probe script checks that every worker points at the local provider.

```sh
export API_PORT=18080 WORKER_CONCURRENCY=24 PROVIDER_LATENCY=20ms
export PROVIDER_URL=http://provider:8081/send
docker compose up --build -d --scale worker=3
scripts/require-local-provider.sh
```

The experiment uses the current baseline API at 18080 to create each 10,000-item campaign. Each campaign consists of ten calls with 1,000 notifications each. The probe client sends two additional requests to the selected API, one through each ingress route. Both carry the same low SMS payload. Trial 2 reverses their submission order.

Build the alternate API binary in a temporary copy, retaining all baseline services and the baseline binary. The executable must target the architecture of the local app image. Docker Desktop must be able to bind-mount the temporary directory.

```sh
candidate_dir=$(mktemp -d /tmp/notify-ingress-candidate.XXXXXX)
git archive HEAD | tar -xf - -C "$candidate_dir"
git -C "$candidate_dir" apply "$PWD/reports/ingress-probe/candidate.patch"
app_arch=$(docker image inspect notification-system:local --format '{{.Architecture}}')
(cd "$candidate_dir" && CGO_ENABLED=0 GOOS=linux GOARCH="$app_arch" go build -trimpath -o notify-linux ./cmd/notify)
docker compose run -d --rm --no-deps --name notify-ingress-candidate \
  -p 127.0.0.1:18081:8080 -v "$candidate_dir/notify-linux:/app/notify:ro" api api
curl --fail --retry 8 --retry-connrefused http://127.0.0.1:18081/healthz
```

Run the four commands sequentially. Each result is saved in this directory after the campaign drains.

```sh
python3 scripts/single-bulk-probe.py --variant baseline --trial 1
python3 scripts/single-bulk-probe.py --variant candidate --trial 1
python3 scripts/single-bulk-probe.py --variant baseline --trial 2
python3 scripts/single-bulk-probe.py --variant candidate --trial 2
docker stop notify-ingress-candidate
```

Return the worker count to its pre-test value if it was changed (for example, `docker compose up -d --no-deps --scale worker=1 worker`). Remove the temporary directory when finished.

The alternate API sends the initial ready event of a standalone low notification to the already-consumed, otherwise idle `notifications.normal` topic. The notification row stays `low`. A batch containing just one item still uses `notifications.low`. Both topics use the same worker pool, Redis limiter and provider. With only those two topics active and both populated, the current scheduler considers three normal positions per low position. The run compares queue separation under a low-only campaign. A concurrent normal-priority workload, sustained standalone demand, scheduled jobs and retries require separate tests.

To verify the routing of a recorded probe, copy its notification ID from the JSON result and use the following read-only query. Each probe in this success-only fixture should have one initial outbox row. The SQL audit output is in [routing-audit.json](routing-audit.json).

```sql
SELECT n.id, n.priority, n.channel, n.attempt, n.status, o.aggregatetype AS event_topic
FROM notifications AS n
JOIN outbox AS o ON o.aggregateid = n.id
WHERE n.id = '<probe UUID>';
```
