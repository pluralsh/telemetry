#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/tests/regression/docker-compose.yml")

cleanup() {
  status=$?
  if (( status != 0 )); then
    "${COMPOSE[@]}" ps || true
    "${COMPOSE[@]}" logs --no-color || true
  fi
  "$ROOT/tests/regression/down.sh"
  exit "$status"
}
trap cleanup EXIT INT TERM

"${COMPOSE[@]}" --profile test build regression-runner
"$ROOT/tests/regression/start.sh"
"${COMPOSE[@]}" --profile test run --rm regression-runner
