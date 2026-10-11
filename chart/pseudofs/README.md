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

The block cache defaults to `write_policy: WriteOnEviction`, so blocks reach
the cache disk only when they leave the memory tier. `WriteOnInsertion` writes
every cached block to disk and costs request latency; see the PseudoFS
configuration docs.

`config.filesystem` maps directly to the PseudoFS filesystem configuration
(`storage`, `metastore_wal_path`, `blobs`, `inline_threshold_bytes`,
`chunk_size_bytes`, `max_file_size_bytes`, `max_append_generations`), and
`config.search` to the Glob and Grep limits, including `max_grep_seconds`.
The schema checks object store, cache, and search fields at install time. The
chart generates `listener` from `service.port`. `config.maxUnaryFileSizeBytes`
bounds memory used by unary file RPCs; use the streaming RPCs for larger files.
`config.maxDecodingMessageBytes` and `config.maxEncodingMessageBytes` render the
server's per-message gRPC limits.

## Remote object storage

With an `Aws`, `Azure`, or `Gcp` store the chart sets
`metastore_wal_path` to `<persistence.mountPath>/wal`, so `WRITTEN`
acknowledgements wait on the persistent volume rather than the bucket; set it
to `""` to keep the WAL in the bucket. Keep data persistence enabled: blobs and
the WAL are acknowledged before they reach the bucket.

```yaml
config:
  filesystem:
    storage:
      object_store:
        type: Aws
        region: us-east-1
        bucket: my-pseudofs
serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/pseudofs
```

`config.filesystem.storage.path` is the filesystem's root prefix: the
metastore is kept under `<path>/metastore` and blobs under `<path>/blobs`, in
`blobs.object_store` when set and the storage object store otherwise.
[`examples/s3-subpath-values.yaml`](examples/s3-subpath-values.yaml) is a
minimal S3 setup under one prefix of a shared bucket.

Without workload identity, provide credentials as environment variables from a
Secret, for example `extraEnvFrom: [{secretRef: {name: pseudofs-s3}}]` with
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` keys. Azure reads
`AZURE_STORAGE_ACCOUNT_KEY` (or the other `AZURE_STORAGE_*` variables) and GCP
reads `GOOGLE_SERVICE_ACCOUNT_KEY`.

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
helm template pseudofs ./chart/pseudofs --namespace telemetry \
  --set config.filesystem.storage.object_store.type=Aws \
  --set config.filesystem.storage.object_store.region=us-east-1 \
  --set config.filesystem.storage.object_store.bucket=my-pseudofs
```
