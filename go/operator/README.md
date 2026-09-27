# Telemetry Operator

## Description
The Telemetry Operator manages `Meter` datastores and their
`NamespaceAuthentication` credentials. It renders Meter configuration, creates
internal credentials, and reconciles the Services, StatefulSets, and
namespace-scoped RBAC required by standalone or sharded deployments.

Each writer or reader workload accepts `replicas`, `podTemplate`, `dataVolume`,
and `cacheVolume`. A volume selects exactly one of `emptyDir` or
`persistentVolumeClaim`. When omitted, data and cache use per-replica
`ReadWriteOnce` persistent claims of 10Gi and 20Gi respectively, with the
cluster's default StorageClass. Explicit `emptyDir` volumes remain supported,
including optional `sizeLimit`. Persistent claims are mounted through fixed
`data` and `cache` StatefulSet claim templates. Claim sizes may be expanded;
shrinking or changing immutable claim properties is rejected without deleting
existing PVCs.

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
kubectl create secret generic meter-s3 \
  --from-literal=access-key-id=change-me \
  --from-literal=secret-access-key=change-me
kubectl apply -k config/samples/
```

Meter object stores support AWS S3 (including custom S3-compatible endpoints),
Azure Blob Storage, and Google Cloud Storage. Static credentials are always
referenced from Kubernetes Secrets and injected directly into Meter containers;
they are not copied into the generated Meter configuration. Credential
references can be omitted to use ambient cloud identity such as AWS IRSA, Azure
managed/workload identity, or Google application default credentials. See the
[CRD API reference](docs/api.md) for each provider's endpoint and authentication
fields and the [object-store authentication guide](docs/object-store-authentication.md)
for complete examples.

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

