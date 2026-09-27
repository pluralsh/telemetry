# Regression harness

Pytest owns lifecycle, readiness, diagnostics, and cleanup for each product.
Docker state is isolated by Compose project and removed after every suite. Tests
skip with a clear reason when Docker or Compose is unavailable.

Install and run:

```sh
mise install
mise exec -- python -m pip install -r tests/regression/requirements.txt
mise exec -- python -m pytest tests/regression/test_unit
mise exec -- python -m pytest tests/regression/test_live
```

Use `--extended` to include restart and time-based retention checks. The core
suite keeps those slower checks out of pull-request latency:

```sh
mise exec -- python -m pytest tests/regression/test_live --extended
```

## Meter

`products/meter/` owns Prometheus, MinIO, Meter compose/config assets. The
host-side Python suite uses product-specific modules in `harness/meter/` for
deterministic fixtures, Prometheus remote-write protobuf and Snappy encoding,
OTLP protobuf, authentication, HTTP calls, and response normalization. The live
tests compare PromQL and discovery results against Prometheus using relative
tolerance `1e-9` with absolute floor `1e-12`, and cover sharding, forwarding,
the Basic/JWT authorization matrix, OTLP metadata, idempotency, namespace
isolation, and reader freshness.

The optional Meter-only entrypoint used by the kind handoff scenario is:

```sh
PYTHONPATH=tests/regression python -m harness.meter
```

## Line

`products/line/` pins Loki 3.5.5 by multi-architecture digest and builds a local
single-node Line image. Identical current-time Loki JSON, raw Snappy protobuf,
and OTLP JSON fixtures are sent to both products. The suite canonicalizes and
compares Loki log and metric endpoint envelopes, while keeping Line's
`| match` BM25 extension in Line-only assertions.

Coverage includes out-of-order entries, sparse streams, cold/warm BM25,
periodic visibility, durable restart, and retention expiry. Line's checked-in
configuration publishes accepted writes every one second; the live test polls
at 100 ms and fails if visibility exceeds three seconds. Restart and retention
run with `--extended`. Line stores a logical expiry deadline in page metadata,
so reads stop returning expired pages independently of SlateDB compaction;
physical TTL remains enabled for eventual reclamation.

Host ports are Loki `13100`, Line `13101`, retention Line `13102`, Prometheus
`19090`, Meter writers `18080`/`18081`, Meter reader `18082`, and MinIO `19000`.

The optional Kubernetes scenario is not part of normal Cargo tests:

```sh
tests/kind/test.sh
```

It creates a disposable kind cluster, builds and loads the local image,
validates lease-backed assignment and forwarding, scales writers `1 -> 3 -> 1`,
and checks reads after each handoff. Set `KEEP_KIND_CLUSTER=1` to retain a
failed cluster; failure logs are written beneath `tests/kind/logs`.
