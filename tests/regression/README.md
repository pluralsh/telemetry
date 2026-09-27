# Meter regression suite

This black-box suite sends the same deterministic Prometheus remote-write
Snappy/protobuf fixture to stock Prometheus and a two-writer Meter cluster. The
fixture is deliberately posted only to `meter-writer-0`; series mapped to the
second contiguous shard range therefore exercise authenticated internal gRPC
forwarding. A separate reader opens every shard `DbReader` from shared MinIO
storage.

Run the complete Docker suite:

```sh
tests/regression/test.sh
```

`start.sh`, `wait.sh`, and `down.sh` are also usable independently. The test
script always tears down volumes and prints service logs on failure. Host ports
are Prometheus `19090`, writers `18080`/`18081`, reader `18082`, and MinIO
`19000`.

The Rust runner compares normalized/sorted Prometheus results. Numeric values
use relative tolerance `1e-9` with absolute floor `1e-12`. It covers instant
and range queries, selectors, regex, aggregation, counter rate, offsets, binary
joins, discovery and metadata APIs, empty/boundary behavior, OTLP equivalence,
namespace isolation, authorization, idempotency, read-only rejection, and
reader freshness.

The optional Kubernetes scenario is not part of normal Cargo tests:

```sh
tests/kind/test.sh
```

It creates a disposable kind cluster, builds and loads the local image,
validates lease-backed assignment and forwarding, scales writers `1 -> 3 -> 1`,
and checks reads after each handoff. Set `KEEP_KIND_CLUSTER=1` to retain a
failed cluster; failure logs are written beneath `tests/kind/logs`.
