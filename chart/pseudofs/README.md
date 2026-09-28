# PseudoFS Helm chart

This chart deploys one standalone PseudoFS StatefulSet and a gRPC Service on
port 9093. The replica count is intentionally fixed at one because concurrent
processes must not own the same local store. One process serves multiple
request-selected tenant filesystems through the same SlateDB instance.

## Install

```sh
helm install pseudofs oci://ghcr.io/pluralsh/charts/pseudofs \
  --namespace telemetry \
  --create-namespace
```

The data PVC is enabled by default. Configure its storage class and size with
`persistence.storageClass` and `persistence.size`, or set
`persistence.existingClaim`. Disabling data persistence uses an `emptyDir` and
is intended only for disposable workloads.

The hybrid block cache uses memory and `/var/cache/pseudofs`. The cache volume
defaults to `emptyDir`; enable `cache.persistence.enabled` for a PVC or set
`cache.persistence.existingClaim`. `cache.emptyDir.medium` and
`cache.emptyDir.sizeLimit` configure the ephemeral alternative.

`config.filesystem` maps directly to the PseudoFS filesystem configuration,
including append-chain compaction through `max_append_generations`. The chart
generates `listener` from `service.port`. `config.maxUnaryFileSizeBytes` bounds
memory used by unary file RPCs; use the streaming RPCs for larger files.
`config.maxDecodingMessageBytes` and `config.maxEncodingMessageBytes` render the
server's per-message gRPC limits.

The ServiceAccount is optional. With `serviceAccount.create=false`, the chart
uses `serviceAccount.name` when set and otherwise uses the namespace's default
ServiceAccount. API token automounting is disabled by default.

## Validation

```sh
helm lint ./chart/pseudofs
helm template pseudofs ./chart/pseudofs --namespace telemetry
helm template pseudofs ./chart/pseudofs --namespace telemetry \
  --set cache.persistence.enabled=true \
  --set cache.persistence.size=50Gi \
  --set serviceAccount.create=false \
  --set serviceAccount.name=pseudofs-runtime
```
