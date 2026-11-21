#!/usr/bin/env bash
# Build both sides from the same service revision before starting the experiment.
set -euo pipefail

root=$(git rev-parse --show-toplevel)
revision=ed4257ce83f874d47d4848612a055437a3bd65bd
source_dir=$(mktemp -d "${TMPDIR:-/tmp}/notify-channels.XXXXXXXX")
run_tag=${source_dir##*/}
base_image="notification-system:${run_tag}-baseline"
candidate_image="notification-system:${run_tag}-candidate"
cleanup() {
  docker image rm "$base_image" "$candidate_image" >/dev/null 2>&1 || true
  rm -rf -- "$source_dir"
}
trap cleanup EXIT

git -C "$root" archive "$revision" | tar -xf - -C "$source_dir"
docker build --quiet -t "$base_image" "$source_dir"
git -C "$source_dir" apply --unidiff-zero "$root/scripts/fixtures/channel-admission.patch"
docker build --quiet -t "$candidate_image" "$source_dir"
python3 -B "$root/scripts/channel-isolation.py" \
  --baseline-image "$base_image" --candidate-image "$candidate_image" \
  --out-dir "${1:-.local/results/channel-isolation}"
