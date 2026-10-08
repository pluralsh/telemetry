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
`19090`, Metrics writers `18080`/`18081`, Metrics reader `18082`, standalone
Metrics fuzz target `18083`, and Metrics MinIO
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
| `FUZZ_SCENARIO` | `recent` | `recent` (each product's default oracle) or `historical` (every oracle answers from object storage, behind a MinIO latency proxy; see below) |
| `MINIO_FIRST_BYTE_MS` | `15` | Historical scenario: delay added before the first byte of every MinIO response |
| `FUZZ_PIN_CPUS` | `1` | Pin shared services, the runner, the implementation, and the oracle to disjoint cpusets (needs at least 4 Docker CPUs); `0` disables |
| `FUZZ_CPU_RANGE` | unset | Confine pinning to these cores (such as `6-11`); set per lane by `bench --lanes` |
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
| `FUZZ_HISTORY_DIR` | unset (off) | Also record a compact benchmark entry under this directory (relative paths are from the repository root), e.g. `documentation/benchmarks/fuzz` |
| `FUZZ_HISTORY_HOST_LABEL` | unset | Short machine name stored in history entries |
| `FUZZ_HISTORY_BATCH` | unset | Groups entries from one benchmark; set by `harness.fuzz.bench` |
| `FUZZ_LOGS_STORAGE` | `s3` | Logs implementation backend: `s3` (MinIO) or `local` |
| `FUZZ_LOGS_CONFIG` | `regression` | Logs implementation config (S3 storage only): `regression` (60s segments, 16 KiB/128-row pages, 2m discovery rollups, fast compaction, to exercise segment, page and rollup boundaries) or `production` (the operator's defaults: 512 MiB memory + 10 GiB disk block cache, 128 MiB metadata cache, `applied` writes flushed every 10s, 1h segments, 1 MiB/8192-row pages, 24h rollups) |
| `FUZZ_LOGS_DUPLICATE_RATE` | `0.02` | Fraction of log entries re-sent with an identical timestamp and line |
| `FUZZ_TRACES_DUPLICATE_RATE` | `0` | Fraction of spans re-sent in a later request. Opt-in: Tempo returns the copies until it compacts the trace, while Traces deduplicates them at query time |
| `FUZZ_TRACES_ORACLE` | `tempo` | Traces oracle: `tempo` (local-disk blocks; with the default frontend settings recent data is served from ingester memory) or `tempo-s3` (blocks in the shared MinIO, search and tag lookups read only flushed blocks, ingesters drop flushed blocks after 15s; the like-for-like object-store comparison) |
| `FUZZ_TRACES_CONFIG` | `regression` | Traces implementation: `regression` (two writers and a reader with 60s segments, two-trace pages, IO concurrency 2, to exercise sharding, page and segment boundaries) or `production` (one standalone process and shard over MinIO, like the single-binary Tempo, with the operator's defaults: 512 MiB memory + 10 GiB disk block cache, 128 MiB metadata cache, `applied` writes flushed every 10s, 1h segments and 1 MiB/4 MiB pages of up to 1024 traces) |
| `FUZZ_LOGS_UNALIGNED_RATE` | `0.5` | Fraction of LogQL metric range queries sent unaligned to their step, exercising frontend step alignment |
| `TEMPO_IMAGE` | `grafana/tempo:2.10.8` | Tempo oracle for traces fuzzing; the live regression suite keeps `2.8.2`, which returns nothing for negated structural operators when the left side matches no spans |
| `FUZZ_METRICS_ORACLE` | `prometheus` | Metrics oracle: `prometheus` (local-disk TSDB), `mimir` (Mimir 3.2.1 on the shared MinIO, stock read path serving recent data from ingester memory), or `mimir-blocks` (flushes each round to MinIO and reads only through the store-gateway, the like-for-like object-store comparison; adds ~30s of visibility wait per round). Mimir runs with its ingester postings-for-matchers caches off, since with query sharding they hide series created within the last 10s. Where MQE answers a query Prometheus rejects for duplicate series, the case is `inconclusive`. `MIMIR_QUERY_ENGINE=prometheus` swaps Mimir's default MQE engine for the Prometheus engine |
| `FUZZ_METRICS_STORAGE` | `s3` | Metrics under test: `s3` (two writers and a reader over MinIO) or `local` (one standalone process on a local-disk volume with the same caches, durability, and flush interval, so a Prometheus comparison leaves out the object store) |
| `FUZZ_METRICS_CONFIG` | `regression` | Metrics implementation (S3 storage only): `regression` (two writers and a reader with 64 MiB caches and durable writes flushed every second, to exercise sharding) or `production` (one standalone process and shard over MinIO, like the single-process oracles, with the operator's defaults: 512 MiB memory + 10 GiB disk block cache, 128 MiB metadata cache, `applied` writes flushed every 10s and a 256 MiB reader cache) |
| `FUZZ_METRICS_STRICT_NAME` | `0` | Compare `__name__` even where Prometheus drops it after functions (Metrics keeps it; the live suite ignores this too) |

Each run writes `summary.md` and `summary.json`, which contain per-family
outcome counts, oracle and implementation latency percentiles, latency ratios,
and the failure reasons. The output directory also holds `cases.jsonl` (one
line per query, with both latencies), `ingest.jsonl`, `rounds.jsonl`,
`artifacts/` (full request and both responses for each non-matching case or
latency outlier), and `data/` (generated datasets). Cases where both sides hit
a result limit, or where either side fails a recheck against itself, are
counted separately and do not count as mismatches.

### Benchmark history

With `FUZZ_HISTORY_DIR` set, both entrypoints also store a summary-level
entry (git revision, host and Docker resources, settings, outcomes, latency,
and CPU/memory, but no cases or data) and regenerate the history index. The
checked-in history lives in
[`documentation/benchmarks/fuzz`](../../documentation/benchmarks/fuzz/README.md);
see [`documentation/benchmarks`](../../documentation/benchmarks/README.md) for
the layout, how to read it, and `--lanes` for concurrent runs. To benchmark
every product with consistent settings and record history:

```sh
PYTHONPATH=tests/regression mise exec -- \
  python -m harness.fuzz.bench --duration 30m --products logs,metrics,traces
# import a finished run's output directory, e.g. a CI artifact
PYTHONPATH=tests/regression mise exec -- \
  python -m harness.fuzz.history record target/fuzz/metrics
# regenerate README.md, index.json, and per-entry pages; --check for CI
PYTHONPATH=tests/regression mise exec -- python -m harness.fuzz.history rebuild
```

For benchmark numbers, run the harness inside Docker so both sides are reached
over the compose network rather than through host port forwarding (which on
Docker Desktop adds a proxy hop to every request):

```sh
tests/regression/products/runner/run.sh \
  python -m harness.fuzz.bench --duration 30m --products logs,metrics,traces
```

The runner mounts the Docker socket and the repository at its host path, sets
`FUZZ_IN_NETWORK=1`, joins each product's `<project>_default` network after
`up`, and forwards `FUZZ_*`, `REGRESSION_*`, `MIMIR_*`, and `TEMPO_*`
variables. `run.json` records `network` as `compose-network` or `host-ports`;
only compare runs with the same mode.

`bench` defaults its history directory to `FUZZ_HISTORY_DIR`, then
`documentation/benchmarks/fuzz`, writes full outputs to
`target/fuzz/bench-<timestamp>/<product>`, continues after a failing
product, and exits non-zero if any failed. `--scenarios recent,historical`
runs every product once per scenario, into `<product>-<scenario>`.
`bench` sets `FUZZ_<PRODUCT>_CONFIG` from `--config`, `production` by default
so numbers reflect a deployed configuration; `--config regression` with a short
`--duration` is the quick correctness pass over page, segment and cache
boundaries.

The `historical` scenario compares reads of data that only exists in object
storage. Metrics uses `mimir-blocks` and traces uses `tempo-s3`. Logs switches
Loki to `products/logs/loki-fuzz-historical.yaml`, which stores chunks in MinIO,
flushes idle chunks within about a second, and stops querying ingesters; the
harness flushes Loki after every round and waits until no chunks remain in
memory. Loki 3.5's series API cannot see flushed streams until their TSDB index
ships (every 15 minutes), so the logs fuzz skips series cases there. In every
product, MinIO moves to `:9010` and `products/latency-proxy/proxy.py` takes over
`minio:9000` inside MinIO's network namespace, delaying the first byte of each
response by `MINIO_FIRST_BYTE_MS` for both sides.

Every round also times a trivial readiness request against each service and
reports it as the "Request floor" in `summary.md`, the per-request overhead
below which latency comparisons are noise.

To profile a run's queries natively, without HTTP or Docker, convert its
output into replay input and run the crate's ignored `profile_fuzz_cases`
test, which ingests the rounds once into `PROFILE_STORE` and writes one JSON
line per case to `PROFILE_OUT`:

```sh
PYTHONPATH=tests/regression mise exec -- \
  python -m harness.fuzz.replay metrics target/fuzz/metrics /tmp/replay
PROFILE_CASES=/tmp/replay PROFILE_STORE=/tmp/replay-store \
PROFILE_OUT=/tmp/replay.jsonl PROFILE_REPS=3 \
  cargo test -p plural-metrics --features remote-write --release --lib \
  profile::profile_fuzz_cases -- --ignored
```

Logs and traces work the same way, with `-p plural-logs` or `-p plural-traces`
and the test path `db::query::profile::profile_fuzz_cases`.

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
