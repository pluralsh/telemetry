#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/tests/regression/docker-compose.yml")
services=(prometheus meter-writer-0 meter-writer-1 meter-reader)
deadline=$((SECONDS + 180))

while (( SECONDS < deadline )); do
  ready=true
  for service in "${services[@]}"; do
    cid="$("${COMPOSE[@]}" ps -q "$service")"
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || true)"
    if [[ "$status" != "healthy" ]]; then
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
