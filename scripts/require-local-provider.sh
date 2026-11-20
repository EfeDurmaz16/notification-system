#!/usr/bin/env bash
# Verify the worker destination before firing synthetic traffic. Never print a
# configured provider URL: webhook.site tokens can be embedded in the path.
set -euo pipefail
ids=$(docker compose ps --status running -q worker)
if [[ -z "$ids" ]]; then
  echo "A running local delivery worker is required for load experiments." >&2
  exit 1
fi
while IFS= read -r id; do
  local_fixture=$(docker inspect --format '{{range .Config.Env}}{{if eq . "PROVIDER_URL=http://provider:8081/send"}}yes{{end}}{{end}}' "$id")
  if [[ "$local_fixture" != yes ]]; then
    echo "Load experiments require all workers to target the Compose synthetic provider." >&2
    exit 1
  fi
done <<< "$ids"
