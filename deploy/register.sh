#!/bin/sh
set -eu
base=http://connect:8083/connectors/notification-outbox
curl -fsS --retry 10 --retry-all-errors --retry-delay 2 -o /dev/null \
    -X PUT -H 'Content-Type: application/json' --data-binary @/deploy/connector.json "$base/config"

# Both the connector and its PostgreSQL task must reach RUNNING.
for _ in $(seq 1 60); do
    status=$(curl -fsS "$base/status" || true)
    case "$status" in
        *'"state":"FAILED"'*) printf '%s\n' "$status" >&2; exit 1 ;;
    esac
    if printf '%s\n' "$status" | grep -Eq '"tasks":\[\{[^}]*"state":"RUNNING"' &&
       printf '%s\n' "$status" | grep -Eq '"connector":\{[^}]*"state":"RUNNING"'; then
        printf '%s\n' 'Outbox connector and task are running.'
        exit 0
    fi
    sleep 2
done
printf 'Connector did not start: %s\n' "$status" >&2
exit 1
