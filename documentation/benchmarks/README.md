# Benchmarks

## Differential fuzz benchmark

The fuzz benchmark runs the regression harness's differential fuzzer
([`tests/regression/harness/fuzz/`](../../tests/regression/harness/fuzz/)) for
each database next to its reference system on one Docker host:

| Product | Implementation under test | Oracle |
| --- | --- | --- |
| Logs | single-node Logs over MinIO (`FUZZ_LOGS_STORAGE=s3`) or local disk | Loki 3.5.5 |
| Metrics | two writers and a reader over MinIO (`FUZZ_METRICS_STORAGE=s3`) or one standalone process on local disk (`local`) | Prometheus (local TSDB), Mimir, or Mimir store-gateway only (`FUZZ_METRICS_ORACLE`) |
| Traces | two writers and a reader over MinIO | Tempo 2.10 on local disk or MinIO (`FUZZ_TRACES_ORACLE`) |

Each round loads the same randomized dataset into both sides, waits until it
is visible on both, and sends the same randomized queries to both. A run
measures two things:

- **Correctness.** Every query is classified as `match`, `mismatch`,
  `inconclusive` (a difference the API contract allows, such as both sides
  truncating at a limit), an error or timeout on either side, or unstable
  (one side disagrees with itself on a recheck). Mismatches, implementation
  errors and timeouts, unstable implementation answers, and rejected writes
  fail the run.
- **Performance.** Oracle and implementation latency percentiles for every
  answered query, overall and per query family, the per-query
  implementation/oracle latency ratio, write latency, and container CPU and
  memory per role (`impl`, `oracle`, and `shared` object storage), sampled
  from the Docker Engine API every two seconds.

Results are kept in [`fuzz/`](fuzz/README.md): a generated overview with the
newest result per product, a per-product history table, and one entry per run.

## Running it

From the repository root, with Docker running and the Python dependencies
installed (see [`tests/regression/README.md`](../../tests/regression/README.md)):

```sh
PYTHONPATH=tests/regression mise exec -- python -m harness.fuzz.bench --duration 30m
```

This runs logs, metrics (once against Prometheus and once against Mimir), and
traces with the same settings, each in a fresh Compose stack, records a
history entry per run under `documentation/benchmarks/fuzz/history/`, and
regenerates the index. It keeps going when a run fails and exits non-zero at
the end if any did. Options:

| Option | Default | Purpose |
| --- | --- | --- |
| `--products` | `logs,metrics,traces` | Products to run, in order |
| `--scenarios` | `FUZZ_SCENARIO`, else `recent` | Scenarios every product runs in |
| `--metrics-oracles` | `FUZZ_METRICS_ORACLE`, else `prometheus,mimir` | One recent-scenario metrics run per oracle; historical always uses `mimir-blocks` |
| `--duration` | `FUZZ_DURATION`, else `30m` | Fuzzing budget per run (excludes image builds and stack startup) |
| `--lanes` | `1` | Runs at once, each on its own slice of the Docker CPUs |
| `--seed` | random per run | Shared `FUZZ_SEED`, to repeat a benchmark on the same data |
| `--history-dir` | `FUZZ_HISTORY_DIR`, else `documentation/benchmarks/fuzz` | History root |
| `--output-root` | `target/fuzz/<batch>` | Full run outputs (cases, artifacts, data), one directory per run |
| `--cooldown` | `10s` | Pause before a lane starts its next run |

Every other `FUZZ_*` variable passes through, so variants are benchmarked the
same way, for example `FUZZ_TRACES_ORACLE=tempo-s3`. Set
`FUZZ_HISTORY_HOST_LABEL` to a short name for the machine (such as
`m3-max-orbstack`) to make entries easier to compare.

Plan for roughly the budget per run plus a few minutes of stack startup each.
Most of a run's wall time is spent waiting for each round to become visible
(production configs flush the write buffer every 10s), not on queries, so a
shorter `--duration` mostly means a smaller dataset rather than the same
measurement sooner; ratios drift as data grows, so only compare runs with the
same budget.

`--lanes 2` halves the wall time. Lanes get disjoint, equal CPU ranges
(`FUZZ_CPU_RANGE`, recorded in each entry's `cpu_layout`), so on 12 Docker
CPUs each side gets 2 pinned cores instead of 5. Most services average well
under half a core, but Tempo bursts past 2, so compare only runs with the
same lane count. Two runs of the same product never overlap, in-network runs
each get their own runner container, and images are built once before the
first lane starts. Lanes share memory and disk: with production configs a
logs stack (Logs and Loki) peaks near 3 GiB after 10 minutes and keeps
growing, traces near 1.5 GiB and metrics near 1 GiB, so give the Docker VM at
least 8 GiB for two lanes. Compose binds the ports listed in the
regression README (Loki `13110`, Logs `13111`/`13101`, Prometheus `19090`,
Mimir `19009`, Metrics `18080`-`18083`, MinIO `19000`, Tempo
`13200`/`14320`, Traces `13201`-`13203`), so stop other regression or fuzz
stacks first. With one lane, give the Docker VM at least 4 CPUs and 8 GiB of
memory.

Single products, the pytest wrapper, and CI can record history too, by
setting `FUZZ_HISTORY_DIR`. It is off by default, so ordinary fuzz runs never
write into the repository:

```sh
FUZZ_HISTORY_DIR=documentation/benchmarks/fuzz FUZZ_DURATION=30m \
  PYTHONPATH=tests/regression mise exec -- python -m harness.fuzz metrics
```

A run that has already finished (for example a downloaded CI artifact) can be
imported from its output directory; git and host details then describe the
importing machine:

```sh
PYTHONPATH=tests/regression mise exec -- \
  python -m harness.fuzz.history record target/fuzz/metrics
```

After adding, removing, or editing entries by hand, regenerate the derived
files. Generation is deterministic, and `--check` fails when they are stale:

```sh
PYTHONPATH=tests/regression mise exec -- python -m harness.fuzz.history rebuild
PYTHONPATH=tests/regression mise exec -- python -m harness.fuzz.history rebuild --check
```

## Reading the history

```
fuzz/README.md                       newest result per product and variant, recent runs
fuzz/index.json                      key metrics of every entry, for the documentation site
fuzz/history/<product>/README.md     every run of one product, newest first
fuzz/history/<product>/<name>.json   one run (the source of truth)
fuzz/history/<product>/<name>.md     the same run rendered
```

Entry names are `<YYYY-MM-DD>T<HHMM>Z-<commit>[-dirty]-<seed>`, using the UTC
start time, the first ten characters of the commit, and the seed in hex.
`-dirty` means the working tree had uncommitted changes to tracked files, so
the commit alone does not reproduce the build. Entries from one `bench`
invocation share a `batch` value.

Each JSON entry has `schema_version` 1 and holds:

- `product`, `variant` (the settings that make runs incomparable, such as
  `oracle=prometheus`), `started_at`, `finished_at`, `run_id`, `batch`.
- `git`: `commit`, `short`, `branch`, `dirty`.
- `host`: OS, architecture, CPU model, cores, memory, Python, and the Docker
  engine's version, CPUs, and memory (on macOS the Docker VM is the real
  limit); `ci` when run in GitHub Actions.
- `config`: duration, seed, scale, window, queries per round, timeouts,
  latency thresholds, `fail_on`, and stack; `settings`: the product's
  oracle, storage, and implementation config; `environment`: the
  benchmark-relevant variables that were set; `images`: image and ID per
  service.
- `results`: status and failure reasons, rounds, cases, outcome counts,
  overall and per-family latency percentiles with ratios, ingest latency,
  and per-role and per-service CPU and memory.

Per-case data, response bodies, and datasets stay in the run's output
directory and are not copied into the history.

In the tables, `mismatch` counts genuine semantic differences, and
`non-match` counts every case that was not a clean match (including both
sides erroring on an invalid query, which is expected). Latency columns are
`implementation / oracle` milliseconds over cases both sides answered.
`ratio p50` is the median per-query implementation/oracle ratio, so values
below `1.00x` mean the implementation is usually faster. CPU is mean cores
over the run and memory is the p95 working set (page cache excluded, as in
`docker stats`).

## Caveats

- **One shared host.** Both systems, object storage, and the fuzzer share the
  same machine and Docker VM, so absolute numbers depend on the hardware and
  on whatever else is running. Compare runs from the same host label, and
  treat small differences as noise. Requests alternate which side goes first
  to cancel out warm-cache bias.
- **Oracles are not like-for-like.** The default Prometheus oracle serves
  from a local-disk TSDB and memory while the implementation reads object
  storage; `FUZZ_METRICS_STORAGE=local` runs Metrics standalone on local disk
  with otherwise identical settings, the like-for-like Prometheus comparison.
  `FUZZ_METRICS_ORACLE=mimir-blocks` and `FUZZ_TRACES_ORACLE=tempo-s3`
  are the object-store comparisons. Loki and the default Tempo serve recent
  data from ingester memory. Treat each variant as its own series.
- **Implementation configs.** The default `regression` configs use tiny
  segments and pages to exercise boundaries, which costs latency;
  `FUZZ_LOGS_CONFIG=production` and `FUZZ_TRACES_CONFIG=production` use crate
  defaults and are the fairer performance comparison.
- **Randomized workloads.** Seeds change the data shapes and query mix, so
  per-family numbers move between runs even on the same code. Pass `--seed`
  to compare two builds on the same workload.
- **Small datasets.** Each round holds minutes to tens of minutes of data at
  `FUZZ_SCALE=1`; latency here reflects query planning and small-scan
  overheads more than large-scale throughput.
