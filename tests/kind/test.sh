#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER="${KIND_CLUSTER_NAME:-meter-regression}"
NS=meter-regression
LOG_DIR="$ROOT/tests/kind/logs"
mkdir -p "$LOG_DIR"
FORWARD_PIDS=()

cleanup() {
  status=$?
  if (( ${#FORWARD_PIDS[@]} > 0 )); then
    kill "${FORWARD_PIDS[@]}" 2>/dev/null || true
  fi
  if (( status != 0 )); then
    kubectl -n "$NS" get all,configmap,lease -o wide >"$LOG_DIR/resources.log" 2>&1 || true
    kubectl -n "$NS" logs statefulset/meter --all-containers --prefix >"$LOG_DIR/writers.log" 2>&1 || true
    kubectl -n "$NS" logs deployment/meter-reader --all-containers --prefix >"$LOG_DIR/reader.log" 2>&1 || true
  fi
  if [[ "${KEEP_KIND_CLUSTER:-0}" != 1 ]]; then
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

command -v kind >/dev/null
command -v kubectl >/dev/null
kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"
docker build -f "$ROOT/crates/meter-server/Dockerfile" -t meter-regression:local "$ROOT"
kind load docker-image --name "$CLUSTER" meter-regression:local
kubectl apply -f "$ROOT/tests/kind/manifests.yaml"
kubectl -n "$NS" wait --for=condition=complete job/minio-init --timeout=180s
kubectl -n "$NS" rollout status statefulset/meter --timeout=300s
kubectl -n "$NS" rollout status deployment/meter-reader --timeout=300s

start_port_forwards() {
  if (( ${#FORWARD_PIDS[@]} > 0 )); then
    kill "${FORWARD_PIDS[@]}" 2>/dev/null || true
    wait "${FORWARD_PIDS[@]}" 2>/dev/null || true
  fi
  kubectl -n "$NS" port-forward service/meter-writer 28080:8080 >"$LOG_DIR/writer-forward.log" 2>&1 &
  FORWARD_PIDS=("$!")
  kubectl -n "$NS" port-forward service/meter-reader 28082:8080 >"$LOG_DIR/reader-forward.log" 2>&1 &
  FORWARD_PIDS+=("$!")
  for _ in {1..60}; do
    if curl -fsS http://127.0.0.1:28080/-/ready >/dev/null \
      && curl -fsS http://127.0.0.1:28082/-/ready >/dev/null; then
      return 0
    fi
    if ! kill -0 "${FORWARD_PIDS[@]}" 2>/dev/null; then
      echo "kubectl port-forward exited before becoming ready" >&2
      return 1
    fi
    sleep 1
  done
  echo "timed out waiting for local Meter port forwards" >&2
  return 1
}

check_assignment() {
  expected="$1"
  for _ in {1..60}; do
    owners="$(
      kubectl -n "$NS" get configmap meter-shard-assignments \
        -o jsonpath='{.data.assignment\.json}' 2>/dev/null \
        | { grep -o '"owner"' || true; } \
        | wc -l \
        | tr -d ' '
    )"
    leases="$(kubectl -n "$NS" get lease -o name 2>/dev/null | { grep -c 'meter-shard-' || true; })"
    if [[ "$owners" == "$expected" && "$leases" -ge "$expected" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "expected $expected shard assignment ranges" >&2
  return 1
}

BASE_MS=$(( $(date +%s) * 1000 / 60000 * 60000 - 1800000 ))

run_meter_check() {
  stage="$1"
  offset_ms="$2"
  start_port_forwards
  (
    cd "$ROOT"
    REGRESSION_METER_ONLY=1 \
      REGRESSION_RUN_ID="kind-$stage-$BASE_MS" \
      REGRESSION_BASE_MS="$((BASE_MS + offset_ms))" \
      METER_WRITE_URL=http://127.0.0.1:28080/write/ns/regression \
      METER_READ_URL=http://127.0.0.1:28082/read/ns/regression \
      cargo run --locked --package regression
  )
}

check_assignment 1
run_meter_check one-writer 0

kubectl -n "$NS" scale statefulset/meter --replicas=3
kubectl -n "$NS" rollout status statefulset/meter --timeout=300s
check_assignment 3
run_meter_check three-writers 600000

kubectl -n "$NS" scale statefulset/meter --replicas=1
kubectl -n "$NS" rollout status statefulset/meter --timeout=300s
check_assignment 1
run_meter_check one-writer-again 1200000
