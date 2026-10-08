#!/usr/bin/env bash
# Runs a harness command inside the runner container, which joins each
# product's compose network so latency excludes host port forwarding:
#
#   tests/regression/products/runner/run.sh python -m harness.fuzz.bench --duration 30m
#
# FUZZ_*, REGRESSION_*, MIMIR_*, and TEMPO_* variables are forwarded.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$here/../../../.." && pwd)"
export REPO_ROOT

compose=(docker compose --file "$here/docker-compose.yml" --profile bench)
forward=()
for name in $(compgen -e); do
  case "$name" in
    FUZZ_IN_NETWORK) ;;
    FUZZ_* | REGRESSION_* | MIMIR_* | TEMPO_*) forward+=(--env "$name") ;;
  esac
done

"${compose[@]}" build fuzz-runner
exec "${compose[@]}" run --rm "${forward[@]+"${forward[@]}"}" fuzz-runner "$@"
