# Deployed scalability runner

This standalone Rust package generates product-shaped write traffic against an
already deployed Telemetry installation. It is intentionally excluded from the
repository Cargo workspace and standard regression tests because it persists
large amounts of data and can saturate production infrastructure.

Use a dedicated namespace and object-store location with an explicit retention
or cleanup policy. The required `--allow-production-write` flag is an
acknowledgement, not a sandbox: the runner sends real writes to the supplied
URL.

## Run it

Build and run the package explicitly from the repository root:

```shell
cargo run --release --manifest-path tests/scalability/Cargo.toml -- \
  --url https://telemetry.example/write/ns/scale/loki/api/v1/push \
  --duration-seconds 1800 \
  --warmup-seconds 120 \
  --concurrency 32 \
  --target-rate 18000 \
  --output line-18k.json \
  --allow-production-write \
  line
```

The URL must include the namespace and complete product route:

- Line: `https://.../write/ns/scale/loki/api/v1/push`
- Meter: `https://.../write/ns/scale/api/v1/write`
- Track: `https://.../write/ns/scale/v1/traces`

If the deployment requires a bearer token, keep it out of command history:

```shell
export TELEMETRY_SCALE_TOKEN=...
cargo run --release --manifest-path tests/scalability/Cargo.toml -- \
  --url https://telemetry.example/write/ns/scale/api/v1/write \
  --bearer-token-env TELEMETRY_SCALE_TOKEN \
  --allow-production-write \
  meter
```

Basic authentication is also read from environment variables:

```shell
export TELEMETRY_SCALE_USERNAME=scale-writer
export TELEMETRY_SCALE_PASSWORD=...
cargo run --release --manifest-path tests/scalability/Cargo.toml -- \
  --url https://telemetry.example/write/ns/scale/loki/api/v1/push \
  --basic-username-env TELEMETRY_SCALE_USERNAME \
  --basic-password-env TELEMETRY_SCALE_PASSWORD \
  --allow-production-write \
  line
```

Both Basic options are required together and cannot be combined with
`--bearer-token-env`.

Run `... -- --help` or `... -- <common options> line --help` for every option.

## Workload profiles

Each workload creates a unique run ID in labels and attributes unless
`--run-id` is supplied:

- `line` sends Loki JSON. Its default 1,600-entry requests use 128 streams per
  request, rotate through 10,000 streams, and mix approximately 70% 250-byte,
  25% 1 KiB, and 5% 4 KiB log entries.
- `meter` sends Snappy-compressed Prometheus remote-write protobuf. Its default
  request has 5,000 samples rotating through four million active series.
  `--churn-percent` adds one-use label values to the requested percentage.
- `track` sends OTLP/HTTP protobuf. Its default request has 2,000 spans, five
  spans per trace, and a 256-byte payload attribute per span.

Initial single-writer rate checks matching the capacity-planning guide are:

```shell
# Line: repeat at 18,000 and 36,000 entries/s
cargo run --release --manifest-path tests/scalability/Cargo.toml -- \
  --url https://telemetry.example/write/ns/scale/loki/api/v1/push \
  --target-rate 18000 --allow-production-write line

# Meter: repeat at 250,000 and 500,000 samples/s
cargo run --release --manifest-path tests/scalability/Cargo.toml -- \
  --url https://telemetry.example/write/ns/scale/api/v1/write \
  --target-rate 250000 --allow-production-write meter

# Track: repeat at 30,000 and 60,000 spans/s
cargo run --release --manifest-path tests/scalability/Cargo.toml -- \
  --url https://telemetry.example/write/ns/scale/v1/traces \
  --target-rate 30000 --allow-production-write track
```

`--target-rate 0` is closed-loop saturation mode: every worker sends its next
request as soon as the previous request completes. Sweep concurrency and
product-specific batch sizes rather than treating one saturation run as a
capacity result. For sustained limits, run at least 30 minutes after warmup and
continue until compaction is in steady state.

The target rate is global across workers and is expressed in entries, samples,
or spans per second—not requests per second. A target below one request's event
count naturally results in requests spaced more than one second apart.

## Interpreting results

The runner prints a JSON report and can also write it with `--output`. Key
fields include:

- `successful_events_per_second`: compare this with `requested_target_rate`.
- `successful_payload_mib_per_second`: encoded HTTP body throughput. It is not
  raw semantic telemetry size or SlateDB logical-write throughput.
- `latency_ms`: successful-request p50, p90, p95, p99, and maximum latency.
- `failed_requests` and `errors`: transport failures and non-success HTTP
  statuses.
- `client_payload_build_cpu_seconds`: aggregate client CPU time spent
  generating and encoding bodies.

If payload-build CPU approaches elapsed time multiplied by available
load-generator cores, the client is limiting the result. Use a larger
load-generator host or several processes/hosts with distinct run IDs, then sum
their successful event rates.

The report does not collect service, Kubernetes, SlateDB, or object-store
telemetry. Correlate every run with writer CPU and RSS, network traffic, L0 and
compaction backlog, write stalls, durability/visibility lag, cache behavior,
and object-store GET/PUT rates and throttling. Also run representative reads
and an online shard split at the recommended 65% operating point before
adopting a scaling limit.
