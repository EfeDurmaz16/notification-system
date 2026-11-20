#!/usr/bin/env bash
# Run the full scenario matrix against an isolated, already running local Compose stack.
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/require-local-provider.sh
# Any replicas created below must use the same synthetic provider.
export PROVIDER_URL=http://provider:8081/send
results=${RESULTS:-.local/experiments}
mkdir -p "$results"
api=${API_URL:-http://127.0.0.1:8080}
provider=${MOCK_PROVIDER_URL:-http://127.0.0.1:8090}
workers=$(docker compose ps --status running -q worker | wc -l | tr -d ' ')
if [[ "$workers" == 0 ]]; then
  echo "Start the isolated local Compose stack before running the experiments." >&2
  exit 1
fi
run_dir=$(mktemp -d)
restore() {
  docker compose up -d --no-deps --scale worker="$workers" worker >/dev/null
  rm -f "$run_dir/loadtest"
  rmdir "$run_dir"
}
trap restore EXIT
go build -o "$run_dir/loadtest" ./cmd/loadtest
{
  printf 'UTC start: '; date -u +%FT%TZ
  go version
  docker version --format '{{.Server.Version}}'
  docker compose version
  git rev-parse HEAD
  docker compose ps --format 'table {{.Service}}\t{{.Image}}\t{{.Status}}'
  docker image inspect notification-system:local --format 'Application image: {{.Id}}'
} > "$results/environment.txt"
exit_code=0
run_test() {
  local name=$1 scenario=$2 replicas=$3
  shift 3
  echo "Running $name"
  if ! "$run_dir/loadtest" -scenario "$scenario" -api "$api" -provider "$provider" -label "$replicas worker replicas" -out "$results/$name.json" "$@" > /dev/null; then
    echo "Some checks failed; see $results/$name.json" >&2
    exit_code=1
  fi
}
for replicas in 1 3; do
  docker compose up -d --no-deps --scale worker="$replicas" worker
  # Let the new group finish a real notification before measuring this topology.
  "$run_dir/loadtest" -scenario burst -total 1 -batch 1 -api "$api" -provider "$provider" -deadline 90s >/dev/null
  run_test "burst-${replicas}w" burst "$replicas"
done
run_test load load 3
run_test stress stress 3
run_test failures failures 3
run_test burst-10000 burst 3 -total 10000 -batch 1000 -deadline 10m -audit-receipts
run_test mixed mixed 3
python3 scripts/summarize-bench.py "$results" > "$results/summary.md"
cat "$results/summary.md"
exit "$exit_code"
