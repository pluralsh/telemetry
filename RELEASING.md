# Releasing

Telemetry uses one release version for its datastore images and Kubernetes
operator. Push a SemVer tag with a leading `v`, for example:

```sh
git tag v0.2.0
git push origin v0.2.0
```

The tag publishes `ghcr.io/pluralsh/meter`,
`ghcr.io/pluralsh/line`, and `ghcr.io/pluralsh/telemetry-operator` with the
version `0.2.0`. Track will join the same release when its server and operator
resource are implemented.

The Helm charts retain independent package versions. During a tagged release,
the chart releaser increments each chart version, sets its `appVersion` to the
release version, publishes the OCI chart, and opens a pull request containing
the metadata changes.

The telemetry-operator chart uses `appVersion` for both the operator image tag
and the default managed product image tag. Users can override these separately
with `image.tag` and `defaultProductVersion`; a `Meter` or `Line` can always pin
its own image with `spec.version`.
