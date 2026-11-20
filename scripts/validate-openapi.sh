#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
exec go run github.com/getkin/kin-openapi/cmd/validate@v0.153.0 --multi -- openapi.yaml
