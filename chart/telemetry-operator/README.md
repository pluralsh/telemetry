# Telemetry Operator Helm chart

This chart installs the Kubebuilder-based Telemetry Operator and its `Meter`
and `NamespaceAuthentication` CRDs. The operator watches all namespaces and
manages the workloads and credentials declared by those resources.

## Install

```sh
helm install telemetry-operator oci://ghcr.io/pluralsh/charts/telemetry-operator \
  --version 0.1.0 \
  --namespace telemetry-system \
  --create-namespace
```

Set `image.repository` and `image.tag` when using a private or development
build:

```sh
helm upgrade --install telemetry-operator oci://ghcr.io/pluralsh/charts/telemetry-operator \
  --version 0.1.0 \
  --namespace telemetry-system \
  --create-namespace \
  --set image.repository=registry.example.com/telemetry-operator \
  --set image.tag=v1.0.0
```

The CRDs in `crds/` are installed before templated resources. Helm does not
upgrade or delete CRDs automatically; review CRD schema changes before chart
upgrades and remove CRDs manually only after preserving any custom resources.

## Configuration

- `replicaCount` defaults to `1`; leader election is enabled by default.
- `image.repository`, `image.tag`, and `image.pullPolicy` select the controller
  image. An empty tag uses the chart `appVersion`.
- `imagePullSecrets` configures private registry credentials.
- `serviceAccount` and `rbac` control identity and generated permissions.
- `metrics.enabled` defaults to `false`; `bindAddress` and `secure` configure
  the listener when enabled.
- `resources`, pod metadata, security contexts, scheduling constraints,
  runtime classes, and probe timings are configurable in `values.yaml`.
- `additionalArgs` appends command-line arguments to the manager.

Metrics are disabled by default, so the chart has no cert-manager dependency.
When secure metrics are enabled, arrange TLS and scraping configuration
appropriate for the cluster; this chart intentionally does not install a
metrics Service or certificate resources.

The current manager implementation only supports cluster-wide watches, so
`watchClusterWide` must remain `true`. If `rbac.create=false`, provide an
equivalent ClusterRole/ClusterRoleBinding and leader-election Role/RoleBinding.

## Meter workload storage

Each Meter writer and reader receives per-replica `ReadWriteOnce` claims by
default: 10Gi for `dataVolume` and 20Gi for `cacheVolume`, using the cluster's
default StorageClass. Override either claim with a Kubernetes-native PVC spec,
or explicitly opt into ephemeral storage:

```yaml
spec:
  writer:
    cacheVolume:
      persistentVolumeClaim:
        storageClassName: fast
        accessModes: [ReadWriteOnce]
        resources:
          requests:
            storage: 50Gi
  reader:
    cacheVolume:
      emptyDir:
        sizeLimit: 20Gi
```

Persistent claims can be expanded by increasing their requested storage. The
operator patches existing claims and orphan-recreates the StatefulSet so future
replicas use the new template. Shrinks and other immutable claim changes are
rejected.
