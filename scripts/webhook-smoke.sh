#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
RUN_WEBHOOK_SITE_SMOKE=1 go test -v -count=1 -timeout=120s -run '^TestWebhookSiteCapture$' ./tests/webhook
