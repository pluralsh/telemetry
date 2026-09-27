#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
docker compose -f "$ROOT/tests/regression/docker-compose.yml" down --volumes --remove-orphans
