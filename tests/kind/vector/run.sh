#!/usr/bin/env bash
# End-to-end test of a sharded Vector cluster on kind: installs the operator
# and the Vector store through the telemetry-stack chart, loads namespaces,
# then changes the cluster (writers added, each splitting a shard for itself;
# a writer killed) and checks every replica returns the same query results
# afterwards, and that removing writers is rejected.
# Finally it uninstalls the release and checks the chart's BucketCleaner
# emptied the store's bucket prefix.
#
#   tests/kind/vector/run.sh
#
# KEEP_KIND_CLUSTER=1 keeps the cluster; SKIP_BUILD=1 reuses the
# vector-e2e:0.0.0-e2e and telemetry-operator:e2e images already built.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
HERE="$ROOT/tests/kind/vector"
CLUSTER="${KIND_CLUSTER_NAME:-vector-e2e}"
NS=vector-e2e
NAME=vector
RELEASE=telemetry-stack
PREFIX=e2e
MC_IMAGE=cgr.dev/chainguard/minio-client@sha256:b8b144ab34694ecea25aa352c4be9de4c26ee2a02701521dce02ee5593c57338
WORK="${VECTOR_E2E_DIR:-$(mktemp -d -t vector-e2e)}"
mkdir -p "$WORK"
PYTHON="${PYTHON:-python3}"
VECTOR_IMAGE=vector-e2e:0.0.0-e2e
OPERATOR_IMAGE=telemetry-operator:e2e
FORWARD_PIDS=()
URLS=""

log() { printf '\n==> %s\n' "$*"; }

stop_forwards() {
  if (( ${#FORWARD_PIDS[@]} > 0 )); then
    kill "${FORWARD_PIDS[@]}" 2>/dev/null || true
    wait "${FORWARD_PIDS[@]}" 2>/dev/null || true
  fi
  FORWARD_PIDS=()
}

cleanup() {
  status=$?
  stop_forwards
  if (( status != 0 )); then
    mkdir -p "$WORK/logs"
    kubectl -n "$NS" get all,vector,vectortopology,bucketcleaner,lease -o wide >"$WORK/logs/resources.log" 2>&1 || true
    kubectl -n "$NS" get bucketcleaner -o yaml >"$WORK/logs/bucketcleaners.yaml" 2>&1 || true
    kubectl -n "$NS" get vectortopology "$NAME-topology" -o yaml >"$WORK/logs/topology.yaml" 2>&1 || true
    for pod in $(kubectl -n "$NS" get pods -l app.kubernetes.io/instance="$NAME" -o name 2>/dev/null); do
      kubectl -n "$NS" logs "$pod" >"$WORK/logs/${pod#pod/}.log" 2>&1 || true
    done
    kubectl -n telemetry-operator logs deployment/telemetry-operator >"$WORK/logs/operator.log" 2>&1 || true
    echo "failed; logs in $WORK/logs" >&2
  fi
  if [[ "${KEEP_KIND_CLUSTER:-0}" != 1 ]]; then
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

for tool in kind kubectl helm docker "$PYTHON"; do command -v "$tool" >/dev/null; done

if [[ "${SKIP_BUILD:-0}" != 1 ]]; then
  log "building images"
  docker build -f "$ROOT/crates/vector/Dockerfile" -t "$VECTOR_IMAGE" "$ROOT"
  docker build -t "$OPERATOR_IMAGE" "$ROOT/go/operator"
fi

log "creating kind cluster $CLUSTER"
kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"
kind load docker-image --name "$CLUSTER" "$VECTOR_IMAGE" "$OPERATOR_IMAGE"

log "installing the operator"
# The chart's CRDs are too large for a Helm release Secret (1 MiB), so the
# chart is rendered and applied server-side instead of installed.
kubectl apply --server-side -f "$ROOT/chart/telemetry-operator/crds"
kubectl create namespace telemetry-operator --dry-run=client -o yaml | kubectl apply -f -
helm template telemetry-operator "$ROOT/chart/telemetry-operator" --namespace telemetry-operator --skip-crds \
  --set image.repository="${OPERATOR_IMAGE%:*}" --set image.tag="${OPERATOR_IMAGE#*:}" --set image.pullPolicy=Never \
  | kubectl apply --server-side -n telemetry-operator -f -
kubectl -n telemetry-operator rollout status deployment/telemetry-operator --timeout=180s

log "installing MinIO, then the Vector store through the telemetry-stack chart"
kubectl apply -f "$HERE/manifests.yaml"
kubectl -n "$NS" wait --for=condition=complete job/minio-init --timeout=180s
helm upgrade --install "$RELEASE" "$ROOT/chart/telemetry-stack" --namespace "$NS" -f "$HERE/values.yaml"
kubectl -n "$NS" wait --for=condition=Ready "bucketcleaner/$NAME-data" --timeout=120s
# The chart's cleaner names only the store; the operator infers the prefix.
[[ "$(kubectl -n "$NS" get bucketcleaner "$NAME-data" -o jsonpath='{.status.subPath}')" == "$PREFIX" ]]

# bucket_objects prints the number of objects under the store's prefix.
bucket_objects() {
  kubectl -n "$NS" run "mc-ls-$RANDOM" --rm -i --quiet --restart=Never --image="$MC_IMAGE" \
    --env="MC_HOST_local=http://vector:vector-e2e-secret@minio:9000" \
    -- ls --recursive "local/vector-e2e/$PREFIX/" 2>/dev/null | grep -c . || true
}

# active_shards prints the topology's active shard ids, space separated.
active_shards() {
  kubectl -n "$NS" get vectortopology "$NAME-topology" -o json 2>/dev/null \
    | "$PYTHON" -c 'import json,sys; print(" ".join(str(s["id"]) for s in json.load(sys.stdin)["spec"]["shards"] if s["state"] == "active"))' 2>/dev/null \
    || true
}

# wait_for_cluster waits until the topology has `$1` active shards, one per
# writer with no split pending, each served by a ready reader group, and only
# those groups run.
wait_for_cluster() {
  want="$1"
  for _ in {1..150}; do
    shards="$(active_shards)"
    count="$(wc -w <<<"$shards" | tr -d ' ')"
    ready="$(kubectl -n "$NS" get vector "$NAME" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
    status_shards="$(kubectl -n "$NS" get vector "$NAME" -o jsonpath='{.status.activeShards}' 2>/dev/null | tr -d '[]' | tr ',' ' ')"
    groups="$(kubectl -n "$NS" get statefulset -l app.kubernetes.io/component=reader -o name 2>/dev/null | wc -l | tr -d ' ')"
    # Distinct writers owning an active shard, or -1 while a shard has no
    # owner or a split is pending.
    owners="$(kubectl -n "$NS" get vectortopology "$NAME-topology" -o json 2>/dev/null \
      | "$PYTHON" -c 'import json,sys; s=[s for s in json.load(sys.stdin)["spec"]["shards"] if s["state"] == "active"]; print(-1 if any(not x.get("owner") or x.get("split_to") for x in s) else len({x["owner"] for x in s}))' 2>/dev/null || echo -1)"
    writers="$(kubectl -n "$NS" get statefulset "$NAME-writer" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)"
    if [[ "$count" == "$want" && "$ready" == True && "$status_shards" == "$shards" && "$groups" == "$want" \
      && "$owners" == "$want" && "$writers" == "$want" ]]; then
      echo "active shards: $shards, one per writer"
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for $want active shards (have '$shards', ready=$ready, groups=$groups, owners=$owners/$writers)" >&2
  return 1
}

# forward port-forwards every replica and sets URLS to their local addresses.
forward() {
  stop_forwards
  # Polled rather than `kubectl wait`, which never returns for a pod deleted
  # while it waits, such as a retired shard's reader being pruned.
  for _ in {1..90}; do
    unready="$(kubectl -n "$NS" get pods -l app.kubernetes.io/instance="$NAME" -o json \
      | "$PYTHON" -c 'import json,sys; print(sum(1 for p in json.load(sys.stdin)["items"] if p["metadata"].get("deletionTimestamp") or not any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))))')"
    [[ "$unready" == 0 ]] && break
    sleep 2
  done
  [[ "$unready" == 0 ]]
  port=29000
  urls=()
  for pod in $(kubectl -n "$NS" get pods -l app.kubernetes.io/instance="$NAME" -o name | sort); do
    port=$((port + 1))
    kubectl -n "$NS" port-forward "$pod" "$port:8080" >"$WORK/forward-$port.log" 2>&1 &
    FORWARD_PIDS+=("$!")
    urls+=("http://127.0.0.1:$port")
    echo "  ${pod#pod/} -> http://127.0.0.1:$port"
  done
  for url in "${urls[@]}"; do
    for _ in {1..60}; do
      curl -fsS "$url/-/ready" >/dev/null 2>&1 && break
      sleep 1
    done
    curl -fsS "$url/-/ready" >/dev/null
  done
  URLS="$(IFS=,; echo "${urls[*]}")"
}

check() { "$PYTHON" "$HERE/check.py" "$@" --urls "$URLS"; }

# expand runs one topology change ($1 is a description, the rest the command),
# then checks every replica still returns the recorded results and accepts
# new writes.
round=0
expand() {
  what="$1"; shift
  want="$1"; shift
  round=$((round + 1))
  log "round $round: recording baseline before $what"
  forward
  check snapshot --baseline "$WORK/baseline-$round.json"
  log "round $round: $what"
  "$@"
  wait_for_cluster "$want"
  forward
  check shards
  log "round $round: verifying results after $what"
  check verify --baseline "$WORK/baseline-$round.json" --timeout 120
  check extend --round "$round" --timeout 60
}

# set_writers sets the Vector's writer replicas to `$1`.
set_writers() {
  kubectl -n "$NS" patch vector "$NAME" --type merge -p "{\"spec\":{\"writer\":{\"replicas\":$1}}}"
}

# add_writer adds one writer; once it is ready the elected writer plans a
# split of the widest shard for it.
add_writer() {
  set_writers "$1"
  for _ in {1..60}; do
    [[ "$(kubectl -n "$NS" get statefulset "$NAME-writer" -o jsonpath='{.spec.replicas}')" == "$1" ]] && break
    sleep 1
  done
  kubectl -n "$NS" rollout status statefulset/"$NAME-writer" --timeout=300s
}

kill_owner() {
  shard="$(active_shards | awk '{print $1}')"
  owner="$(kubectl -n "$NS" get vectortopology "$NAME-topology" -o json \
    | "$PYTHON" -c "import json,sys; print(next(s['owner'] for s in json.load(sys.stdin)['spec']['shards'] if s['id'] == $shard))")"
  echo "deleting $owner, the owner of shard $shard"
  stop_forwards
  # A forced delete skips graceful shutdown, so the shard lease is left to
  # expire as it would after a crash.
  kubectl -n "$NS" delete pod "$owner" --grace-period=0 --force
  sleep 5
}

log "waiting for the initial cluster"
wait_for_cluster 1
forward
check load

log "declaring a namespace with a new NamespaceAuthentication"
pod_states() {
  kubectl -n "$NS" get pods -l app.kubernetes.io/instance="$NAME" \
    -o jsonpath='{range .items[*]}{.metadata.uid}:{.status.containerStatuses[0].restartCount} {end}'
}
pods_before="$(pod_states)"
kubectl -n "$NS" apply -f - <<YAML
apiVersion: telemetry.plural.sh/v1alpha1
kind: NamespaceAuthentication
metadata:
  name: vector-added
spec:
  dataStoreRef: { kind: Vector, name: $NAME }
  namespace: added
  username: other
  permission: write
  secretKeyRef: { name: vector-e2e-other, key: password }
YAML
# The kubelet refreshes the config Secret within about a minute, and servers
# poll it every 10s.
check declared --namespace added --timeout 180
kubectl -n "$NS" delete namespaceauthentication vector-added
[[ "$(pod_states)" == "$pods_before" ]] || { echo "pods restarted: $pods_before -> $(pod_states)" >&2; exit 1; }

expand "adding a second writer" 2 add_writer 2
expand "adding a third writer" 3 add_writer 3
expand "killing a shard owner" 3 kill_owner

log "removing a writer, which is rejected"
set_writers 2
for _ in {1..60}; do
  reason="$(kubectl -n "$NS" get vector "$NAME" -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}')"
  [[ "$reason" == WriterScaleDownRejected ]] && break
  sleep 2
done
[[ "$reason" == WriterScaleDownRejected ]] || { echo "scale-down not rejected (reason $reason)" >&2; exit 1; }
kubectl -n "$NS" get vector "$NAME" -o jsonpath='{.status.conditions[?(@.type=="Ready")].message}'; echo
[[ "$(kubectl -n "$NS" get statefulset "$NAME-writer" -o jsonpath='{.spec.replicas}')" == 3 ]]
set_writers 3
wait_for_cluster 3

log "uninstalling the release"
stop_forwards
before="$(bucket_objects)"
echo "$before objects under $PREFIX/"
(( before > 0 ))
# The BucketCleaner is deleted with the store, and waits for its pods to
# stop before emptying the prefix.
helm uninstall "$RELEASE" --namespace "$NS"
kubectl -n "$NS" wait --for=delete "bucketcleaner/$NAME-data" --timeout=300s
if kubectl -n "$NS" get pods -l app.kubernetes.io/instance="$NAME" -o name | grep -q .; then
  echo "the BucketCleaner finished while Vector pods were still running" >&2
  exit 1
fi
after="$(bucket_objects)"
echo "$after objects under $PREFIX/ after uninstalling"
(( after == 0 ))

log "all rounds passed"
