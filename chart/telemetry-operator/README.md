# Telemetry Operator Helm chart

This chart installs the Kubebuilder-based Telemetry Operator and its `Meter`,
`Line`, and `NamespaceAuthentication` CRDs. The operator watches all namespaces and
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
- `defaultProductVersion` selects the image tag for managed resources that omit
  `spec.version`. An empty value also uses the chart `appVersion`, keeping the
  operator and datastore release versions aligned.
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

## Meter and Line images and scheduling
For managed `Meter` and `Line` resources, `spec.version` is the canonical
product image tag. It accepts SemVer 2.0 without a leading `v`, including
prerelease and build metadata. Deprecated `spec.image.tag` remains an alias
with the same validation; `spec.version` takes precedence and admission
rejects conflicting values. Omitting both uses `defaultProductVersion`, which
defaults to the chart `appVersion`.
`spec.image.repository` defaults to the product's `ghcr.io/pluralsh/...`
repository, and `spec.image.pullPolicy` defaults to `IfNotPresent`.

Sharded resources default to three writer replicas and two reader replicas.
`spec.writer.replicas` and `spec.reader.replicas` override those defaults.
Standalone resources safely run exactly one writer and no reader; other
standalone replica values are rejected.

Each writer and reader supports first-class `nodeSelector` and Kubernetes
`tolerations`, plus the full `podTemplate` escape hatch. First-class node
selector keys override matching `podTemplate.spec.nodeSelector` keys.
Tolerations are merged, with first-class entries replacing template entries
that have the same key, operator, and effect, so duplicates are not emitted.
Nodes own taints; workloads configure tolerations for those taints.

## Workload storage

Each Meter or Line writer and reader receives per-replica `ReadWriteOnce` claims by
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

Line defaults to `ghcr.io/pluralsh/line`, HTTP port 3100, and gRPC port 9091.
Its ingress routes `/write/ns` to writers and `/read/ns` to readers so the
remaining path segment is the Line namespace required by its Loki-compatible
API. Line rejects `spec.ingress.pathPrefix` because its server routes are fixed.

Managed Meter and Line namespace HTTP APIs deny anonymous access by default.
Omitting `spec.config.auth.unauthenticated` renders `false`; explicitly set it
to `true` only for workloads that intentionally allow anonymous reads and
writes. Health and readiness endpoints remain public.

## Object-store authentication

Meter supports AWS S3 and S3-compatible endpoints, Azure Blob Storage, and
Google Cloud Storage. Provider credentials in a `Meter` resource use
`SecretKeySelector` fields and are injected directly into the managed
containers, rather than copied into generated configuration. Omit explicit
credentials to use ambient identity such as AWS IRSA, Azure managed/workload
identity, or Google application default credentials. Configure cloud identity
annotations on the Meter-managed ServiceAccount with
`spec.serviceAccount.annotations`.

## Meter ingress

Set `spec.ingress.enabled`, `hostname`, and optionally `ingressClass`, metadata,
and TLS settings to create an Ingress for a Meter. TLS defaults to the
`<meter-name>-tls` Secret. Set the optional `pathPrefix`, such as `/meter`, when
sharing a hostname. The operator routes `{pathPrefix}/write` to writers and
`{pathPrefix}/read` to readers; both routes target the same Service for a
standalone Meter. The server handles the prefix directly, so no Ingress rewrite
is required. Health, readiness, and metrics endpoints remain unprefixed.
