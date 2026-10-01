# Telemetry Operator

## Description
The Telemetry Operator manages `Metrics` metric stores, `Logs` log stores, `Traces`
trace stores, and their `NamespaceAuthentication` credentials. It renders product configuration, creates
internal credentials, and reconciles the Services, StatefulSets, and
namespace-scoped RBAC required by standalone or sharded deployments.

The canonical product image tag is `spec.version`. It accepts SemVer 2.0 values
such as `1.2.3` or `1.2.3-rc.1+build.7`; a leading `v` is rejected to match the
repository's published image tags. The deprecated `spec.image.tag` remains an
alias for existing resources and accepts the same SemVer syntax.
`spec.version` takes precedence, and admission rejects resources that set both
fields to different values. When neither is set, the operator uses the
`--default-product-version` value supplied at startup. Release builds default
that flag to their own version.
`spec.image.repository` defaults to
`ghcr.io/pluralsh/metrics`, `ghcr.io/pluralsh/logs`, or `ghcr.io/pluralsh/traces`, and
`spec.image.pullPolicy` defaults to `IfNotPresent`. These first-class image
settings override image values in the product container inside `podTemplate`.

All products use `/write` and `/read` Ingress routing prefixes. Their public
APIs continue below `/write/ns/{namespace}` and `/read/ns/{namespace}`. All
three products support `spec.ingress.pathPrefix` for sharing a hostname, and
their servers handle the prefix directly.

Each writer or reader workload accepts `replicas`, `nodeSelector`,
`tolerations`, `podTemplate`, `dataVolume`, and `cacheVolume`. Sharded
workloads default to one writer replica and two reader replicas; explicit
non-negative replica counts override those defaults. Standalone mode always
uses one writer replica and no reader workload. Admission rejects any other
standalone replica configuration.

In sharded mode, `spec.writer.replicas` remains user intent. Scale-up is applied
to the writer StatefulSet immediately so the Rust shard coordinator can observe
the new ordinal and advance the authoritative `<name>-writer-shard-map`.
Scale-down is held at the ShardMap shard count because every shard referenced
by a routing epoch must stay writable.
If that ShardMap is missing or its ownership metadata is uncertain, the
operator conservatively retains the current StatefulSet replica count. Product
status exposes effective and ready writer replicas, ShardMap count/generation,
and `WriterScaling` / `WriterScalingBlocked` conditions.
A sharded product is not `Ready` until writer intent equals the authoritative
shard count.

The operator intentionally does not inject a writer `preStop` drain command.
The products currently expose health/readiness endpoints and coordinate
per-shard drain through ShardMap and Lease transitions, but do not expose a
stable, authenticated, bounded whole-process drain endpoint or CLI command.
Calling an inferred endpoint during pod termination could bypass the Rust
coordinator or hang termination. A hook can be added once such a protocol is
explicitly supported with a bounded timeout and idempotent semantics.

`nodeSelector` is merged over `podTemplate.spec.nodeSelector`, so first-class
keys win. `tolerations` are merged with `podTemplate.spec.tolerations`; a
first-class entry replaces a template entry with the same key, operator, and
effect, preventing duplicates, while unrelated entries are retained. Pod
taints are not configurable because taints belong to nodes; use tolerations to
schedule onto tainted nodes. The full `podTemplate` remains available for
other Kubernetes pod settings.

A volume selects exactly one of `emptyDir` or
`persistentVolumeClaim`. When omitted, data and cache use per-replica
`ReadWriteOnce` persistent claims of 10Gi and 20Gi respectively, with the
cluster's default StorageClass. Explicit `emptyDir` volumes remain supported,
including optional `sizeLimit`. Persistent claims are mounted through fixed
`data` and `cache` StatefulSet claim templates. Claim sizes may be expanded;
shrinking or changing immutable claim properties is rejected without deleting
existing PVCs.

The operator creates one ServiceAccount per Metrics, Logs, or Traces and assigns it to every
managed workload. Use `spec.serviceAccount.annotations` for cloud identity
integrations such as AWS IRSA, Azure workload identity, or GKE workload
identity.

`spec.ingress` can expose any product through a standard Kubernetes Ingress:

```yaml
spec:
  ingress:
    enabled: true
    hostname: metrics.example.com
    ingressClass: nginx
    # Optional when sharing a hostname with another application.
    pathPrefix: /metrics
    metadata:
      annotations:
        cert-manager.io/cluster-issuer: letsencrypt
      labels:
        app.kubernetes.io/part-of: telemetry
    tls:
      enabled: true
      # Defaults to <metrics-name>-tls when omitted.
      secretName: metrics-tls
```

The operator routes `{pathPrefix}/write` to the writer Service and
`{pathPrefix}/read` to the reader Service for every product. Both routes target
the same Service for standalone instances. The product serves the prefix
directly, so the Ingress must not strip or rewrite it. Health, readiness, and
metrics endpoints remain unprefixed and are not exposed by these routes.
Disabling ingress deletes the operator-owned Ingress.

This module is part of the repository's `go/` workspace. The supported
installation method is the published OCI Helm chart. The generated
[CRD API reference](docs/api.md) documents the supported resources.

## Getting Started

### Prerequisites
- Go 1.27
- Kubebuilder 4.6
- Docker for image builds
- kubectl and access to a Kubernetes 1.25+ cluster

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/operator:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/operator:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
Create the password and S3 credential Secrets referenced by the sample, then
apply the resources:

```sh
kubectl create secret generic prometheus-basic-auth --from-literal=password=change-me
kubectl create secret generic metrics-s3 \
  --from-literal=access-key-id=change-me \
  --from-literal=secret-access-key=change-me
kubectl apply -k config/samples/
```

Metrics object stores support AWS S3 (including custom S3-compatible endpoints),
Azure Blob Storage, and Google Cloud Storage. Static credentials are always
referenced from Kubernetes Secrets and injected directly into Metrics containers;
they are not copied into the generated Metrics configuration. Credential
references can be omitted to use ambient cloud identity such as AWS IRSA, Azure
managed/workload identity, or Google application default credentials. See the
[CRD API reference](docs/api.md) for each provider's endpoint and authentication
fields and the [object-store authentication guide](docs/object-store-authentication.md)
for complete examples.

Metrics, Logs, and Traces namespace HTTP APIs are authenticated by default. Omitted
`spec.config.auth.unauthenticated` renders `false`; set it to `true` only when
anonymous reads and writes are intentional. Health and readiness remain public.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/operator:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/operator/<tag or branch>/dist/install.yaml
```

### By providing a Helm Chart

Install the maintained repository chart:

```sh
helm upgrade --install telemetry-operator oci://ghcr.io/pluralsh/charts/telemetry-operator \
  --version 0.1.0 \
  --namespace telemetry-system --create-namespace
```

After changing API markers, run `make manifests crd-docs` and copy
`config/crd/bases/*.yaml` to `../../chart/telemetry-operator/crds/`.

## Contributing
Run `make generate manifests crd-docs fmt vet test` before submitting changes. The test
target runs focused unit tests and envtest integration tests; Kind e2e tests
remain a separate `make test-e2e` target.

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

