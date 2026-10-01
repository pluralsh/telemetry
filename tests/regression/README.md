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

Compose builds product images with `--build` by default. Set
`REGRESSION_BUILD=0` to reuse prebuilt `telemetry-regression/<product>-server:local`
images instead; CI builds those per product in parallel through buildx with
the GitHub Actions layer cache.

## Metrics

`products/metrics/` owns Prometheus, MinIO, Metrics compose/config assets. The
host-side Python suite uses product-specific modules in `harness/metrics/` for
deterministic fixtures, Prometheus remote-write protobuf and Snappy encoding,
OTLP protobuf, authentication, HTTP calls, and response normalization. The live
tests compare PromQL and discovery results against Prometheus using relative
tolerance `1e-9` with absolute floor `1e-12`, and cover sharding, forwarding,
the Basic/JWT authorization matrix, OTLP metadata, idempotency, namespace
isolation, and reader freshness. Native histograms are written as remote-write
integer histograms (exponential schemas with a counter reset, and custom
buckets) and as an OTLP exponential histogram at scale 10, then compared as
full bucket layouts through selectors, `rate`/`increase`, aggregation, and the
`histogram_*` functions.

The optional Metrics-only entrypoint used by the kind handoff scenario is:

```sh
PYTHONPATH=tests/regression python -m harness.metrics
```

## Logs

`products/logs/` pins Loki 3.5.5 by multi-architecture digest and builds a local
single-node Logs image. Identical current-time Loki JSON, raw Snappy protobuf,
and OTLP JSON fixtures are sent to both products. The suite canonicalizes and
compares Loki log and metric endpoint envelopes, while keeping Logs'
`| match` BM25 extension in Logs-only assertions.

Coverage includes out-of-order entries, sparse streams, cold/warm BM25,
periodic visibility, durable restart, and retention expiry. Logs' checked-in
configuration publishes accepted writes every one second; the live test polls
at 100 ms and fails if visibility exceeds three seconds. Restart and retention
run with `--extended`. Logs stores a logical expiry deadline in page metadata,
so reads stop returning expired pages independently of SlateDB compaction;
physical TTL remains enabled for eventual reclamation.

## Traces

`products/traces/` runs Tempo 2.8.2 as the semantic oracle and a two-writer,
one-reader Traces deployment over deterministic local MinIO storage. The Python
modules in `harness/traces/` build typed, deterministic OTLP fixtures, encode
OTLP protobuf and Zipkin JSON, exercise OTLP HTTP and gRPC, and normalize only
ordering and API-envelope differences before comparing traces.

The live suite compares trace-by-ID, search, typed TraceQL predicates, tag
names and values, and aggregate non-metrics TraceQL behavior. It also covers
static-shard forwarding, Basic/JWT authorization, namespace isolation, reader
mode, and Zipkin ingestion. Reader restart and short-retention persistence
checks run with `--extended`. Traces exposes Jaeger collector gRPC on host ports
`14251` and `14252`; the topology keeps those routes available for collector
compatibility even though the host harness currently exercises OTLP and Zipkin.

Host ports are Loki `13100`, Logs `13101`, retention Logs `13102`, Prometheus
`19090`, Metrics writers `18080`/`18081`, Metrics reader `18082`, and Metrics MinIO
`19000`. Traces uses Tempo `13200`, writers `13201`/`13202`, reader `13203`,
retention `13204`, OTLP gRPC `14317`/`14318`/`14319`, Tempo OTLP HTTP `14320`,
Zipkin `19411`, and Jaeger collector gRPC `14251`/`14252`.

The optional Kubernetes scenario is not part of normal Cargo tests:

```sh
tests/kind/test.sh
```

It creates a disposable kind cluster, builds and loads the local image,
validates lease-backed assignment and forwarding, scales writers `1 -> 3 -> 1`,
and checks reads after each handoff. Set `KEEP_KIND_CLUSTER=1` to retain a
failed cluster; failure logs are written beneath `tests/kind/logs`.
