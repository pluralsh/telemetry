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

Override `global.storage` and `global.ingress` once for all three products.
Anything under a product's `spec` is merged over those global defaults.
Products can be disabled independently with
`products.<metrics|logs|traces>.enabled`.

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
