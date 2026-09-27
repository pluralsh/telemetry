#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/tests/regression/docker-compose.yml")
urls=(
  http://localhost:19090/-/ready
  http://localhost:18080/-/ready
  http://localhost:18081/-/ready
  http://localhost:18082/-/ready
)
deadline=$((SECONDS + 180))

while (( SECONDS < deadline )); do
  ready=true
  for url in "${urls[@]}"; do
    if ! curl --fail --silent --show-error "$url" >/dev/null 2>&1; then
      ready=false
      break
    fi
  done
  if "$ready"; then
    exit 0
  fi
  sleep 2
done

"${COMPOSE[@]}" ps
"${COMPOSE[@]}" logs --no-color
echo "regression services did not become healthy" >&2
exit 1
