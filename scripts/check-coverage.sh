#!/usr/bin/env bash
#
# Line-coverage gate for the core module.
#
#   scripts/check-coverage.sh [min-percent]   # default 90
#
# Runs the core test suite with a coverage profile and exits non-zero when total
# statement coverage is below the gate.
set -euo pipefail

min="${1:-90}"

go test -coverprofile=coverage.out ./... >/dev/null
pct="$(go tool cover -func=coverage.out | awk '/^total:/ { gsub(/%/, "", $3); print $3 }')"

printf 'Line coverage: %s%% — gate: %s%%\n' "$pct" "$min"

if awk -v p="$pct" -v m="$min" 'BEGIN { exit (p + 0 >= m + 0) ? 0 : 1 }'; then
  echo "OK: coverage gate met."
else
  echo "FAIL: coverage ${pct}% is below the ${min}% gate." >&2
  exit 1
fi
