#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/tests/regression/docker-compose.yml")

"${COMPOSE[@]}" up --build -d prometheus minio minio-init meter-writer-0 meter-writer-1 meter-reader
"$ROOT/tests/regression/wait.sh"
