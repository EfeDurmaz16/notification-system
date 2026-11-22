#!/bin/sh
set -eu

source_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(git -C "$source_dir" rev-parse --show-toplevel)
revision=2ec97c3e5b459ff287aa7b3a3fecccb766a6493f
git -C "$repo_dir" cat-file -e "$revision^{commit}"
docker info >/dev/null
export API_PORT=28820 PROVIDER_PORT=28821 POSTGRES_PORT=28822 REDIS_PORT=28823 KAFKA_PORT=28824 CONNECT_PORT=28825
python3 - "$API_PORT" "$PROVIDER_PORT" "$POSTGRES_PORT" "$REDIS_PORT" "$KAFKA_PORT" "$CONNECT_PORT" <<'PY'
import socket
import sys
for port in sys.argv[1:]:
    try:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", int(port)))
    except OSError as error:
        sys.exit(f"Local replay port {port} is unavailable: {error}")
PY

work_dir=$(mktemp -d)
lab="$work_dir/.local/worker-scaling"
mkdir -p "$lab"
printf 'Replay results: %s\n' "$lab"
git -C "$repo_dir" archive "$revision" -o "$lab/source.tar"
tar -xf "$lab/source.tar" -C "$work_dir"
cp "$source_dir/experiment.py" "$source_dir/report.py" "$lab/"

run_id=$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')
export COMPOSE_PROJECT_NAME="notify-partition-replay-$run_id"
export APP_IMAGE="notification-system:partition-replay-$run_id"
export COMPOSE_FILE="$work_dir/docker-compose.yml"
export OTEL_EXPORTER_OTLP_ENDPOINT="" COMPOSE_PROFILES=""
export WORKER_CONCURRENCY=24 PROVIDER_LATENCY=20ms
export BATCH_SHARE_PERCENT=20 BATCH_ROUTING=isolated PROVIDER_IDEMPOTENT=false
export PROVIDER_URL=http://provider:8081/send PROVIDER_ACK_MODE=assessment
export LOG_LEVEL=INFO

cd "$work_dir"
patch -p1 < "$source_dir/limiter.patch"
go build -o "$lab/loadtest" ./cmd/loadtest
docker build -t "$APP_IMAGE" .
cleanup() {
    docker compose down --volumes --remove-orphans
    docker image rm "$APP_IMAGE"
}
trap cleanup EXIT
docker compose up -d --no-build --scale worker=1 --wait
python3 -Bu "$lab/experiment.py"
python3 -B "$lab/report.py"
