#!/usr/bin/env bash
# Build all deliverables from a single checked-in tree.
set -euo pipefail
root=$(git rev-parse --show-toplevel)
cd "$root"
revision=$(git rev-parse HEAD)
short=$(git rev-parse --short=12 HEAD)
prefix="notification-system-$short"
out="$root/dist"
mkdir -p "$out"
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT

git archive --prefix="$prefix/" "$revision" | gzip -n > "$stage/$prefix-source.tar.gz"
tar -xzf "$stage/$prefix-source.tar.gz" -C "$stage"
platform="$(go env GOOS)-$(go env GOARCH)"
bundle="$prefix-$platform"
mkdir -p "$stage/$bundle/bin"
(
  cd "$stage/$prefix"
  CGO_ENABLED=0 go build -mod=readonly -trimpath -buildvcs=false -o "$stage/$bundle/bin/" ./cmd/...
)
printf '%s\n' "$revision" > "$stage/$bundle/REVISION"
tar -czf "$stage/$bundle.tar.gz" -C "$stage" "$bundle"
(
  cd "$stage"
  shasum -a 256 "$prefix-source.tar.gz" "$bundle.tar.gz" > SHA256SUMS
  shasum -a 256 -c SHA256SUMS
)
install -m 644 "$stage/$prefix-source.tar.gz" "$stage/$bundle.tar.gz" "$stage/SHA256SUMS" "$out"
