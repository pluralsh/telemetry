# telemetry-stack

This chart creates the core `Metrics`, `Logs`, and `Traces` resources managed by
the [telemetry-operator](../telemetry-operator). Install the operator before
this chart.

The defaults reproduce the demo stack: sharded resources backed by the
`plrl-telemetry-demo` S3 bucket in `us-east-2`, one writer and one reader per
product, and path-based ingress at `telemetry.plrldemo.onplural.sh`.

```sh
helm upgrade --install telemetry-operator \
  oci://ghcr.io/pluralsh/charts/telemetry-operator \
  --namespace telemetry-system --create-namespace

helm upgrade --install telemetry-stack \
  oci://ghcr.io/pluralsh/charts/telemetry-stack \
  --namespace telemetry-system
```

Override `global.storage` and `global.ingress` once for all products.
Anything under a product's `spec` is merged over those global defaults.
Products can be enabled or disabled independently with
`products.<metrics|logs|traces|vector>.enabled`.

## Vector

`products.vector` creates a `Vector` store, off by default. It shares
`global.storage` and `global.ingress` like the other products: the API is
served at `/vector/api/v1/...` on the shared hostname, and in-cluster through
the `vector` Service on port 8080. Change `spec.ingress.pathPrefix` to move
it, or set `spec.ingress.enabled: false` to keep it in-cluster only. In the
default `Sharded` mode, queries are eventually consistent.

Its namespaces come from `authentications`, like the other products' (see
below): each entry declares a namespace and the credentials that may use it.
`write` credentials can create, configure, delete and write to the
namespace, and `read` credentials can query it. Requests for undeclared
namespaces get 404 unless `spec.config.auth.unauthenticated` is set.

```yaml
products:
  vector:
    enabled: true
    spec:
      writer:
        replicas: 2
      reader:
        replicas: 1   # per shard
    authentications:
      - name: vector-search
        namespace: search
        username: plrl
        permission: write
    schemas:
      - name: search   # the namespace, unless `namespace` is set
        schema:
          body: { type: string, full_text_search: true, embed: true }
          year: { type: int }
```

Each `schemas` entry creates a `NamespaceSchema`, which the servers merge
into the namespace's attribute schema, so its types and full-text or
embedding options hold from the first write.

## Bucket cleanup

With `bucketCleaners.enabled`, the chart creates a `BucketCleaner` named
`{name}-data` for every enabled product. It references the product through
`dataStoreRef`, and the operator takes the bucket and storage path from the
product's storage settings. When the release is uninstalled, or a product is
disabled, the operator deletes every object under that path. It first waits
for the product and its pods to be gone, so no replica writes after the
cleanup. Add more prefixes under `bucketCleaners.additional`:

```yaml
bucketCleaners:
  enabled: true
  additional:
    - name: logs-archive
      subPath: logs-archive
      # objectStore defaults to global.storage.objectStore
    - name: legacy-metrics
      # bucket and path come from the Metrics resource
      dataStoreRef:
        kind: Metrics
        name: legacy
```

Cleanup is irreversible and needs the telemetry-operator running when the
release is removed. The object store must be `Aws`, `Azure`, or `Gcp`, and
credential Secrets it references must live in the release namespace (or the
operator's own cloud identity must be able to list and delete objects).

## Namespace authentication

Authentication entries live beside each product. They default to the shared
Secret configured by `authSecret`:

```yaml
authSecret:
  create: true
  name: telemetry-auth
  username: plrl
  password: supplied-by-your-secret-values-pipeline

products:
  metrics:
    authentications:
      - name: metrics-write
        namespace: default
        username: plrl
        permission: write
      - name: metrics-read
        namespace: default
        username: grafana
        permission: read
        secretKeyRef:
          name: grafana-telemetry-auth
          key: password
```

For separate credentials per telemetry namespace, create additional managed
Secrets and select one on each authentication entry:

```yaml
authSecrets:
  - name: team-a-telemetry-auth
    username: team-a
    password: supplied-by-your-secret-values-pipeline
  - name: team-b-telemetry-auth
    username: team-b
    password: supplied-by-your-secret-values-pipeline

products:
  logs:
    authentications:
      - name: team-a-logs-write
        namespace: team-a
        username: team-a
        permission: write
        secretKeyRef:
          name: team-a-telemetry-auth
          key: password
      - name: team-b-logs-write
        namespace: team-b
        username: team-b
        permission: write
        secretKeyRef:
          name: team-b-telemetry-auth
          key: password
```

The chart does not query cluster state or generate credentials. A password is
required for every chart-managed Secret, keeping GitOps rendering deterministic;
provide it through your encrypted values pipeline. Add equivalent entries under
`metrics` and `traces`. An entry may also reference any pre-existing Secret in
the chart's release namespace without listing it under `authSecrets`.
