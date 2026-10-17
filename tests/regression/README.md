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
Zipkin `19411`, and Jaeger collector gRPC `14251`/`14252`. The PseudoFS fuzz
stack serves gRPC on `19093`.

## Reader groups

Each live suite also runs readers as reader groups: one reader per storage
shard that opens only that shard and reads the other one from its group over
the internal `ShardReader` gRPC service. Metrics and Traces start
`<product>-reader-0` and `<product>-reader-1` (profile `reader-groups`) next to
the reader that opens every shard, then send the full oracle comparison
through both groups. `test_logs_sharded.py` runs Logs as two static-shard
writers over MinIO with two reader groups (profile `sharded`) and compares
log, limit and direction, metric, and metadata queries with Loki. Each suite
also stops one group and checks that the other fails the query instead of
returning partial results, then recovers.

Reader-group ports are Metrics `18085`/`18086`, Traces `13205`/`13206`, and
Logs writers `13103`/`13104` and readers `13105`/`13106`.

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

`pseudofs.py` has no external oracle: it checks PseudoFS over gRPC against an
in-process reference model (`harness/pseudofs/model.py`) of the filesystem,
glob, and grep semantics, so its reports carry implementation latency only.
Rounds build trees with odd names, inline/blob/streamed files of every
durability, binary and image signatures, append logs that compact, renames,
deletes, cross-tenant writes, and invalid mutations; every mutation's result is
checked against the model as it is applied. Some rounds also run race
scenarios in a scratch tenant (overwrites, appends, unlink and replacing
renames against concurrent readers), which must never expose torn content,
lost appends, or errors other than `NOT_FOUND`. Every
`FUZZ_PSEUDOFS_CRASH_EVERY` rounds the server is SIGKILLed and restarted after a
`WRITTEN` write, and later queries check that everything acknowledged
survived. Grep results past `max_matches` may legally come from any files the
concurrent search reached, so they are accepted when they keep walk order and
each file's matches are a prefix of the model's.

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
| `FUZZ_SCENARIO` | `recent` | `recent` (each product's default oracle), `historical` (every oracle answers from object storage, behind a MinIO latency proxy), or `cold` (a fixed battery over a day of object-store data, each query against freshly restarted processes with empty caches; logs, metrics and traces only); see below |
| `MINIO_FIRST_BYTE_MS` | `15` (historical), `20` (cold) | Object-store scenarios: delay added before the first byte of every MinIO response |
| `FUZZ_COLD_SPAN` | `22h` | Cold scenario: time the dataset spans, ending at the load; at least `1h`, and keep it under the 24h retention of production Logs and Traces |
| `FUZZ_COLD_REPEATS` | `3` | Cold scenario: cold measurements of every battery query per side |
| `FUZZ_COLD_SETTLE` | `5s` | Cold scenario: least pause after a restarted service is ready; the harness then also waits until it moves less than 64 KiB to or from object storage over 2s (at most 60s longer), so only metadata polling is left |
| `FUZZ_COLD_WARM` | `1` | Cold scenario: re-send each query once to the now-warm process and report it as `warm` |
| `FUZZ_COLD_FAMILIES` | unset (all) | Cold scenario: comma-separated family-name prefixes to run, such as `log.,labels` |
| `FUZZ_COLD_ROLLUPS` | `1` | Cold scenario: run the Logs and the standalone Metrics and Traces servers' rollups to completion while settling, as the oracles' compactors are; `0` measures the unrolled hour segments |
| `FUZZ_COLD_COMPACTION` | `1` | Cold scenario: while settling, wait until Logs' SlateDB compaction is quiescent and run Loki's compactor (which otherwise first runs 10 minutes after a start, longer than any cold process lives); `0` measures whatever layout loading left |
| `FUZZ_COLD_TIMELINE` | `0` | Cold scenario: `1` makes the latency proxy record every object-store request (client, method, key, `Range`, status, bytes, start/first-byte/end ms) and writes each measurement's startup and query requests, with their dependent-round-trip `waves`, to `cold_timeline.jsonl` |
| `FUZZ_COLD_IMPL_STARTS` | unset | Cold scenario, standalone Metrics and Traces (`FUZZ_<PRODUCT>_CONFIG=production`): measure the implementation once per comma-separated start, each on a fresh process: `off` (startup cache warming disabled, as before it was on by default), `ready` (query as soon as it is ready, while warming runs), `warm` (after `telemetry_cache_warmer_runs_total{mode="startup"}` counts the pass, then the usual idle wait), `idle` (the default behaviour). The last start is the measurement of record; `summary.json` reports each under `cold.impl_starts` |
| `FUZZ_COLD_READY_TIMEOUT` | `5m` | Cold scenario: how long a restarted service may take to report ready |
| `FUZZ_COLD_QUIESCE`, `FUZZ_COLD_QUIESCE_TIMEOUT` | `45s`, `15m` | Cold scenario: before measuring, both sides must upload less than 1 MiB per window this long; fail after the timeout |
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
| `FUZZ_PSEUDOFS_CONFIG` | `regression` | PseudoFS implementation: `regression` (4 KiB inline threshold, 64 KiB chunks, 64 MiB blob cache, 2 upload workers, compaction every 8 appends, to exercise inline/blob, eviction and compaction boundaries), `production` (the operator's defaults over MinIO), or `local` (production settings on a local-disk object store, without MinIO) |
| `FUZZ_PSEUDOFS_CRASH_EVERY` | `4` (compose), `0` (external) | SIGKILL and restart the server after every this many rounds; `0` disables |
| `FUZZ_PSEUDOFS_RACE_RATE` | `0.5` | Fraction of rounds that also run a race scenario |
| `FUZZ_PSEUDOFS_CONCURRENCY` | `8` | Concurrent writers in parallel load batches |
| `FUZZ_PSEUDOFS_MAX_FILE_MIB`, `FUZZ_PSEUDOFS_MAX_DATA_MIB` | 24/1536 (regression), 96/3072 (production) | Largest generated file, and the dataset size past which a round prunes the oldest trees |
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
runs every product once per scenario, into `<product>-<scenario>`; `cold`
runs only for logs, metrics and traces.
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

The `cold` scenario measures first-query latency over a realistic amount of
object-store data instead of fuzzing. It runs against the same oracles as
`historical` (Loki with `products/logs/loki-fuzz-cold.yaml`, `mimir-blocks`,
and `tempo-s3`), loads `FUZZ_COLD_SPAN` of backdated data hour by hour
(at `FUZZ_SCALE=1`: about 1M logfmt entries over 144 streams; 2840 metric
series, including 2000 short-lived high-cardinality ones, at a 30s interval,
about 2.7M samples; 30k traces of 1-12 spans), then:

1. Flushes both sides to MinIO: Loki flushes its ingester and ships the TSDB
   index on a graceful stop (each run checks that every day's index table is
   in the bucket), Mimir flushes its ingester and compacts with a 30s
   interval and deletes compacted-away blocks at once, and Tempo is flushed
   after every hour, so each hour is its own block, as if ingested live.
   The Logs and the standalone Metrics and Traces servers are restarted once
   with their rollup task running every 10s instead of every 5 minutes
   (which no measured process lives for), and the harness waits for two
   successful passes (`telemetry_rollup_passes_total`; Logs counts no
   passes, so it is restarted with rollup logging on and the harness waits
   until every rollup segment it started is sealed and none started for
   25s), so 24h queries read the window and day rollups a long-running
   server would have made rather than a day of hour segments.
   `FUZZ_COLD_ROLLUPS=0` skips this. Logs then has to show a quiescent
   SlateDB compaction for 30s (`slatedb_db_sst_count`, `l0_sst_count`,
   `sorted_run_count`, `sst_view_count` and
   `storage_segment_compactions_total` unchanged, nothing being compacted,
   under 1 MiB uploaded), and Loki is restarted once with its compactor
   running every 10s until two runs succeeded
   (`loki_boltdb_shipper_compact_tables_operation_total`), since it
   otherwise first runs 10 minutes after a start. Both sides' object layout
   before and after settling is written to `cold_settle.json`.
   `FUZZ_COLD_COMPACTION=0` skips both.
2. Waits until each side uploads less than 1 MiB per `FUZZ_COLD_QUIESCE`, so
   no flush or compaction is still running.
3. Checks exact counts on both sides (entries, samples per metric, one
   sentinel trace per hour, and trace lookups by ID across the span).
4. Runs every battery query `FUZZ_COLD_REPEATS` times per side in seeded
   random order. Before each measurement the service is stopped, its data
   volume (Loki, Mimir, Tempo) is emptied, and it is recreated, so in-memory
   caches, local block copies, and the implementation's disk cache (in the
   container's `/tmp`) start empty. The harness waits for readiness, for
   `FUZZ_COLD_SETTLE`, and until startup object-store traffic is down to
   metadata polling, then sends the query. The other side stays idle.

The latency proxy counts requests, response bytes and uploaded bytes per
client IP and method (`--stats-port`, `9020`), so each measured query
reports the GETs and bytes it read from object storage, and startup traffic
is reported separately. Answers are still compared and classified as in the
fuzz. Results land in `summary.json` and `summary.md` under `cold`, one line
per measurement in `cold.jsonl`, and the history entry under `results.cold`.
Cold restarts need `FUZZ_STACK=compose`. A measurement takes 20-50 seconds,
mostly restarts (Loki needs about 11s to become ready, Tempo 16s), so with
the defaults logs takes about 25 minutes, metrics 15 and traces 30;
`FUZZ_COLD_REPEATS=1` takes about a third of that. If the budget runs out,
the remaining measurements are skipped and noted in the summary. Only startup warming the operator does by default counts: Metrics
and Traces warm SST filters and indexes in the background once started
(readiness does not wait, so the idle wait above usually covers the pass;
`FUZZ_COLD_IMPL_STARTS` measures around it), Logs does not warm on startup,
and every continuous cache warmer's first pass after an unwarmed start only
records the SSTs already live.

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
