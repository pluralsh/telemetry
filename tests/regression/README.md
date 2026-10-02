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

## Differential fuzzing

`harness/fuzz/` is a time-boxed differential fuzzer that runs alongside the
regression suites. A shared runner, recorder, and transport drive one module per
product: `logs.py` compares LogQL against Loki, `metrics.py` compares PromQL
against Prometheus, and `traces.py` compares TraceQL and the search, trace,
and tag APIs against Tempo. Each round loads a freshly randomized dataset into
both sides, waits until the round's sentinel data is visible on both, then runs
a randomized batch of queries over the accumulated data. Datasets vary in
shape: wide and deep cardinality, bursts, sparse data, out-of-order timestamps,
odd Unicode and escaping, NaN and ±Inf, broken and native histograms, and
clock-skewed or fan-out traces. Queries are generated recursively, so the
fuzzer can surface degenerate behaviour in ingestion as well as in queries.

Fuzzing is opt-in and too heavy for pull requests. `.github/workflows/fuzz.yml`
runs it nightly per product with a 30 minute budget, and it can also be started
manually with a different duration, seed, product list, or logs backend. To run
it locally:

```sh
# pytest wrapper (skipped without --fuzz)
FUZZ_DURATION=5m mise exec -- python -m pytest \
  tests/regression/test_fuzz/test_fuzz_logs.py --fuzz -s
# direct entrypoint; exits 1 when the run fails
FUZZ_DURATION=5m PYTHONPATH=tests/regression \
  mise exec -- python -m harness.fuzz metrics
```

`FUZZ_DURATION` (default `30m`; accepts forms like `90s` or `1h30m`) limits the
fuzzing loop only, not stack startup or image builds. When the budget runs
out, the round in progress stops early and a short reserve is kept for writing
the report, so larger fuzzes only need a longer duration. A failing run can be
reproduced with `FUZZ_SEED`: data and queries for every round are derived from
the seed, although wall-clock time still shifts the data windows.

| Variable | Default | Purpose |
| --- | --- | --- |
| `FUZZ_SEED` | random | Seed for data and query generation |
| `FUZZ_SCALE` | `1` | Multiplier on data volume per round |
| `FUZZ_WINDOW` | 20m / 30m / 10m | Time span covered by each round's data |
| `FUZZ_QUERIES_PER_ROUND` | `40-160` | Queries per round, as a fixed count or a range |
| `FUZZ_MAX_ROUNDS`, `FUZZ_MAX_CASES` | `0` (unlimited) | Hard caps in addition to the duration |
| `FUZZ_REQUEST_TIMEOUT` | `30s` | Per-request timeout; implementation timeouts fail the run |
| `FUZZ_VISIBILITY_TIMEOUT` | `90s` | How long to wait for a round's data to become visible |
| `FUZZ_LATENCY_RATIO`, `FUZZ_LATENCY_FLOOR_MS` | `10`, `250` | A case is a latency outlier when the implementation is more than this many times slower than the oracle and the gap exceeds this many ms |
| `FUZZ_RECHECK` | `1` | Re-run mismatches once against both sides to detect flaky oracles |
| `FUZZ_FAIL_ON` | `mismatch,impl_error,impl_timeout,unstable_impl,ingest` | Outcomes that fail the run |
| `FUZZ_OUTPUT_DIR` | `target/fuzz/<product>-<run id>` | Report location |
| `FUZZ_RECORD_DATA` | `1` | Store each round's generated dataset (gzip) |
| `FUZZ_LOGS_STORAGE` | `s3` | Logs implementation backend: `s3` (MinIO) or `local` |
| `FUZZ_LOGS_CONFIG` | `regression` | Logs implementation config (S3 storage only): `regression` (60s segments, 16 KiB/128-row pages, 2m discovery rollups, fast compaction, to exercise segment, page and rollup boundaries) or `production` (crate defaults: 1h segments, 1 MiB pages, 24h rollups) |
| `FUZZ_LOGS_DUPLICATE_RATE` | `0.02` | Fraction of log entries re-sent with an identical timestamp and line |
| `FUZZ_TRACES_DUPLICATE_RATE` | `0` | Fraction of spans re-sent in a later request. Opt-in: Tempo returns the copies until it compacts the trace, while Traces deduplicates them at query time |
| `FUZZ_TRACES_ORACLE` | `tempo` | Traces oracle: `tempo` (local-disk blocks; with the default frontend settings recent data is served from ingester memory) or `tempo-s3` (blocks in the shared MinIO, search and tag lookups read only flushed blocks, ingesters drop flushed blocks after 15s; the like-for-like object-store comparison) |
| `FUZZ_TRACES_CONFIG` | `regression` | Traces implementation configs: `regression` (60s segments, two-trace pages, IO concurrency 2, to exercise page and segment boundaries) or `production` (crate defaults: 1h segments, 1 MiB/4 MiB pages of up to 1024 traces, IO concurrency 128) |
| `FUZZ_LOGS_UNALIGNED_RATE` | `0.5` | Fraction of LogQL metric range queries sent unaligned to their step, exercising frontend step alignment |
| `TEMPO_IMAGE` | `grafana/tempo:2.10.8` | Tempo oracle for traces fuzzing; the live regression suite keeps `2.8.2`, which returns nothing for negated structural operators when the left side matches no spans |
| `FUZZ_METRICS_ORACLE` | `prometheus` | Metrics oracle: `prometheus` (local-disk TSDB), `mimir` (Mimir 3.2.1 on the shared MinIO, stock read path serving recent data from ingester memory), or `mimir-blocks` (flushes each round to MinIO and reads only through the store-gateway, the like-for-like object-store comparison; adds ~30s of visibility wait per round). `MIMIR_QUERY_ENGINE=prometheus` swaps Mimir's default MQE engine for the Prometheus engine |
| `FUZZ_METRICS_STRICT_NAME` | `0` | Compare `__name__` even where Prometheus drops it after functions (Metrics keeps it; the live suite ignores this too) |

Each run writes `summary.md` and `summary.json`, which contain per-family
outcome counts, oracle and implementation latency percentiles, latency ratios,
and the failure reasons. The output directory also holds `cases.jsonl` (one
line per query, with both latencies), `ingest.jsonl`, `rounds.jsonl`,
`artifacts/` (full request and both responses for each non-matching case or
latency outlier), and `data/` (generated datasets). Cases where both sides hit
a result limit, or where either side fails a recheck against itself, are
counted separately and do not count as mismatches.

By default the fuzzer starts dedicated Compose stacks: project `logs-fuzz`
(Loki on `13110`, S3-backed Logs over MinIO on `13111`, local-disk Logs on
`13101`), `metrics-fuzz`, and `traces-fuzz`, which reuse the regression ports
listed above. To fuzz deployed or S3-backed environments, set
`FUZZ_STACK=external` and point each role at an endpoint:

```sh
FUZZ_STACK=external \
FUZZ_METRICS_ORACLE_READ_URL=https://prometheus.example \
FUZZ_METRICS_ORACLE_WRITE_URL=https://prometheus.example \
FUZZ_METRICS_IMPL_WRITE_URL=https://metrics.example/write/ns/fuzz \
FUZZ_METRICS_IMPL_WRITE_AUTHORIZATION="Bearer $TOKEN" \
FUZZ_METRICS_IMPL_READ_URL=https://metrics.example/read/ns/fuzz \
FUZZ_METRICS_IMPL_READ_HEADERS='{"X-Extra": "1"}' \
FUZZ_DURATION=2h PYTHONPATH=tests/regression python -m harness.fuzz metrics
```

The roles are `ORACLE_READ`, `ORACLE_WRITE`, `IMPL_READ`, and `IMPL_WRITE`.
Each accepts `_URL`, `_AUTHORIZATION` (set it to an empty value to drop the
default credential), and `_HEADERS` (a JSON object). All generated data carries
a `fuzz_run` label or a `fuzz.run` resource attribute, and every data query is
scoped to it, so a shared long-lived stack can be fuzzed repeatedly. Metadata
endpoints that cannot be scoped (log label names and values, trace tag names
and values) only run when `FUZZ_ISOLATED=1`, which is the default for Compose
stacks. External oracles need the same write paths as the local ones:
Prometheus must run with `--web.enable-remote-write-receiver`, and Loki must
have `reject_old_samples: false` and ingestion limits as permissive as
`products/logs/loki-fuzz.yaml`.

## Kubernetes

The optional Kubernetes scenario is not part of normal Cargo tests:

```sh
tests/kind/test.sh
```

It creates a disposable kind cluster, builds and loads the local image,
validates lease-backed assignment and forwarding, scales writers `1 -> 3 -> 1`,
and checks reads after each handoff. Set `KEEP_KIND_CLUSTER=1` to retain a
failed cluster; failure logs are written beneath `tests/kind/logs`.
