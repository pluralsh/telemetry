use std::{
    collections::BTreeMap,
    env,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use clap::{Args, Parser, Subcommand};
use hdrhistogram::Histogram;
use prost::Message;
use reqwest::{
    Client,
    header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderValue},
};
use serde::Serialize;
use tokio::{task::JoinSet, time::Instant as TokioInstant};
use uuid::Uuid;

const NANOS_PER_SECOND: u128 = 1_000_000_000;

#[derive(Parser, Debug)]
#[command(
    about = "Run opt-in scalability workloads against a deployed Telemetry install",
    version
)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    #[command(subcommand)]
    product: ProductArgs,
}

#[derive(Args, Clone, Debug, Serialize)]
struct CommonArgs {
    /// Full product ingestion URL, including namespace and route.
    #[arg(long)]
    url: String,

    /// Measured phase duration in seconds.
    #[arg(long, default_value_t = 300)]
    duration_seconds: u64,

    /// Unmeasured warmup duration in seconds.
    #[arg(long, default_value_t = 30)]
    warmup_seconds: u64,

    /// Concurrent HTTP request workers.
    #[arg(long, default_value_t = 16)]
    concurrency: usize,

    /// Target product events per second. Zero runs closed-loop at maximum rate.
    #[arg(long, default_value_t = 0)]
    target_rate: u64,

    /// Per-request timeout in seconds.
    #[arg(long, default_value_t = 30)]
    timeout_seconds: u64,

    /// Environment variable containing a bearer token.
    #[arg(long)]
    bearer_token_env: Option<String>,

    /// Environment variable containing a Basic authentication username.
    #[arg(long, requires = "basic_password_env")]
    basic_username_env: Option<String>,

    /// Environment variable containing a Basic authentication password.
    #[arg(long, requires = "basic_username_env")]
    basic_password_env: Option<String>,

    /// Write the JSON report to this path in addition to stdout.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Stable prefix used in generated labels. Defaults to a random UUID.
    #[arg(long)]
    run_id: Option<String>,

    /// Required acknowledgement that the target will receive persistent data.
    #[arg(long, default_value_t = false)]
    allow_production_write: bool,
}

#[derive(Subcommand, Clone, Debug)]
enum ProductArgs {
    /// Loki JSON log ingestion workload.
    Line(LineArgs),
    /// Prometheus remote-write metrics workload.
    Meter(MeterArgs),
    /// OTLP/HTTP protobuf trace ingestion workload.
    Track(TrackArgs),
}

#[derive(Args, Clone, Debug, Serialize)]
struct LineArgs {
    /// Log entries in each HTTP request.
    #[arg(long, default_value_t = 1_600)]
    events_per_request: usize,

    /// Streams represented in each request.
    #[arg(long, default_value_t = 128)]
    streams_per_request: usize,

    /// Stable stream population rotated through by requests.
    #[arg(long, default_value_t = 10_000)]
    active_streams: u64,
}

#[derive(Args, Clone, Debug, Serialize)]
struct MeterArgs {
    /// Samples in each remote-write request.
    #[arg(long, default_value_t = 5_000)]
    events_per_request: usize,

    /// Stable series population rotated through by requests.
    #[arg(long, default_value_t = 4_000_000)]
    active_series: u64,

    /// Percentage of samples assigned a one-use churn label, from 0 to 100.
    #[arg(long, default_value_t = 0.0)]
    churn_percent: f64,
}

#[derive(Args, Clone, Debug, Serialize)]
struct TrackArgs {
    /// Spans in each OTLP request.
    #[arg(long, default_value_t = 2_000)]
    events_per_request: usize,

    /// Spans assigned to each trace.
    #[arg(long, default_value_t = 5)]
    spans_per_trace: usize,

    /// Approximate payload bytes added as one span attribute.
    #[arg(long, default_value_t = 256)]
    attribute_bytes: usize,
}

impl ProductArgs {
    fn name(&self) -> &'static str {
        match self {
            Self::Line(_) => "line",
            Self::Meter(_) => "meter",
            Self::Track(_) => "track",
        }
    }

    fn event_name(&self) -> &'static str {
        match self {
            Self::Line(_) => "entries",
            Self::Meter(_) => "samples",
            Self::Track(_) => "spans",
        }
    }

    fn events_per_request(&self) -> usize {
        match self {
            Self::Line(args) => args.events_per_request,
            Self::Meter(args) => args.events_per_request,
            Self::Track(args) => args.events_per_request,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.events_per_request() == 0 {
            bail!("events-per-request must be greater than zero");
        }
        match self {
            Self::Line(args) => {
                if args.streams_per_request == 0 || args.active_streams == 0 {
                    bail!("Line stream counts must be greater than zero");
                }
            }
            Self::Meter(args) => {
                if args.active_series == 0
                    || !args.churn_percent.is_finite()
                    || !(0.0..=100.0).contains(&args.churn_percent)
                {
                    bail!("Meter active-series must be positive and churn-percent must be 0..=100");
                }
            }
            Self::Track(args) => {
                if args.spans_per_trace == 0 {
                    bail!("Track spans-per-trace must be greater than zero");
                }
            }
        }
        Ok(())
    }

    fn build_payload(&self, request_id: u64, run_id: &str) -> Result<Payload> {
        match self {
            Self::Line(args) => line_payload(args, request_id, run_id),
            Self::Meter(args) => meter_payload(args, request_id, run_id),
            Self::Track(args) => track_payload(args, request_id, run_id),
        }
    }
}

#[derive(Debug)]
struct Payload {
    body: Vec<u8>,
    content_type: &'static str,
    content_encoding: Option<&'static str>,
}

struct Stats {
    requests: AtomicU64,
    successful_requests: AtomicU64,
    failed_requests: AtomicU64,
    attempted_events: AtomicU64,
    successful_events: AtomicU64,
    attempted_bytes: AtomicU64,
    successful_bytes: AtomicU64,
    client_build_nanos: AtomicU64,
    latency_micros: Mutex<Histogram<u64>>,
    errors: Mutex<BTreeMap<String, u64>>,
}

impl Stats {
    fn new() -> Result<Self> {
        Ok(Self {
            requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            attempted_events: AtomicU64::new(0),
            successful_events: AtomicU64::new(0),
            attempted_bytes: AtomicU64::new(0),
            successful_bytes: AtomicU64::new(0),
            client_build_nanos: AtomicU64::new(0),
            latency_micros: Mutex::new(Histogram::new_with_bounds(1, 600_000_000, 3)?),
            errors: Mutex::new(BTreeMap::new()),
        })
    }

    fn error(&self, key: String) {
        self.failed_requests.fetch_add(1, Ordering::Relaxed);
        *self
            .errors
            .lock()
            .expect("error lock poisoned")
            .entry(key)
            .or_default() += 1;
    }
}

#[derive(Serialize)]
struct Report {
    product: String,
    event_unit: String,
    run_id: String,
    url: String,
    duration_seconds: f64,
    requested_target_rate: u64,
    concurrency: usize,
    events_per_request: usize,
    attempted_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    attempted_events: u64,
    successful_events: u64,
    attempted_payload_bytes: u64,
    successful_payload_bytes: u64,
    successful_requests_per_second: f64,
    successful_events_per_second: f64,
    successful_payload_mib_per_second: f64,
    client_payload_build_cpu_seconds: f64,
    latency_ms: LatencyReport,
    errors: BTreeMap<String, u64>,
    workload: serde_json::Value,
}

#[derive(Serialize)]
struct LatencyReport {
    p50: f64,
    p90: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    validate(&cli)?;
    let run_id = cli
        .common
        .run_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
    let headers = headers(&cli.common)?;
    let client = Client::builder()
        .default_headers(headers)
        .pool_max_idle_per_host(cli.common.concurrency)
        .timeout(Duration::from_secs(cli.common.timeout_seconds))
        .build()
        .context("building HTTP client")?;

    if cli.common.warmup_seconds > 0 {
        eprintln!(
            "warming {} with {} workers for {}s",
            cli.product.name(),
            cli.common.concurrency,
            cli.common.warmup_seconds
        );
        let warmup = run_phase(
            &client,
            &cli.common,
            &cli.product,
            &run_id,
            Duration::from_secs(cli.common.warmup_seconds),
        )
        .await?;
        if warmup.successful_requests.load(Ordering::Relaxed) == 0 {
            bail!(
                "warmup completed without a successful request: {:?}",
                warmup.errors.lock().expect("error lock poisoned")
            );
        }
    }

    eprintln!(
        "measuring {} for {}s at {} {}/s (0 means unlimited)",
        cli.product.name(),
        cli.common.duration_seconds,
        cli.common.target_rate,
        cli.product.event_name()
    );
    let started = Instant::now();
    let stats = run_phase(
        &client,
        &cli.common,
        &cli.product,
        &run_id,
        Duration::from_secs(cli.common.duration_seconds),
    )
    .await?;
    let elapsed = started.elapsed();
    let report = report(&cli, run_id, elapsed, &stats)?;
    let encoded = serde_json::to_string_pretty(&report)?;
    println!("{encoded}");
    if let Some(path) = &cli.common.output {
        std::fs::write(path, format!("{encoded}\n"))
            .with_context(|| format!("writing report to {}", path.display()))?;
    }
    if report.successful_requests == 0 {
        bail!("measurement completed without a successful request");
    }
    Ok(())
}

fn validate(cli: &Cli) -> Result<()> {
    if !cli.common.allow_production_write {
        bail!(
            "--allow-production-write is required because this tool persists high-volume test data"
        );
    }
    if cli.common.duration_seconds == 0 || cli.common.concurrency == 0 {
        bail!("duration-seconds and concurrency must be greater than zero");
    }
    if cli.common.bearer_token_env.is_some()
        && (cli.common.basic_username_env.is_some() || cli.common.basic_password_env.is_some())
    {
        bail!("bearer and Basic authentication options are mutually exclusive");
    }
    cli.product.validate()
}

fn headers(common: &CommonArgs) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    if let Some(name) = &common.bearer_token_env {
        let token = env::var(name).with_context(|| format!("reading bearer token from {name}"))?;
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))
                .context("bearer token is not a valid HTTP header value")?,
        );
    } else if let (Some(username_name), Some(password_name)) =
        (&common.basic_username_env, &common.basic_password_env)
    {
        let username = env::var(username_name).with_context(|| {
            format!("reading Basic authentication username from {username_name}")
        })?;
        let password = env::var(password_name).with_context(|| {
            format!("reading Basic authentication password from {password_name}")
        })?;
        let value = basic_authorization_value(&username, &password)?;
        headers.insert(AUTHORIZATION, value);
    }
    Ok(headers)
}

fn basic_authorization_value(username: &str, password: &str) -> Result<HeaderValue> {
    let credentials = BASE64_STANDARD.encode(format!("{username}:{password}"));
    let mut value = HeaderValue::from_str(&format!("Basic {credentials}"))
        .context("Basic credentials are not a valid HTTP header value")?;
    value.set_sensitive(true);
    Ok(value)
}

async fn run_phase(
    client: &Client,
    common: &CommonArgs,
    product: &ProductArgs,
    run_id: &str,
    duration: Duration,
) -> Result<Arc<Stats>> {
    let stats = Arc::new(Stats::new()?);
    let request_number = Arc::new(AtomicU64::new(0));
    let start = TokioInstant::now();
    let deadline = start + duration;
    let mut workers = JoinSet::new();

    for _ in 0..common.concurrency {
        let client = client.clone();
        let common = common.clone();
        let product = product.clone();
        let run_id = run_id.to_owned();
        let stats = Arc::clone(&stats);
        let request_number = Arc::clone(&request_number);
        workers.spawn(async move {
            loop {
                let sequence = request_number.fetch_add(1, Ordering::Relaxed);
                if common.target_rate > 0 {
                    let event_offset =
                        u128::from(sequence).saturating_mul(product.events_per_request() as u128);
                    let offset_nanos = event_offset.saturating_mul(NANOS_PER_SECOND)
                        / u128::from(common.target_rate);
                    let Ok(offset_nanos) = u64::try_from(offset_nanos) else {
                        break;
                    };
                    let scheduled = start + Duration::from_nanos(offset_nanos);
                    if scheduled >= deadline {
                        break;
                    }
                    tokio::time::sleep_until(scheduled).await;
                } else if TokioInstant::now() >= deadline {
                    break;
                }

                let build_started = Instant::now();
                let payload = match product.build_payload(sequence, &run_id) {
                    Ok(payload) => payload,
                    Err(error) => {
                        stats.error(format!("payload: {error:#}"));
                        continue;
                    }
                };
                stats.client_build_nanos.fetch_add(
                    build_started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                    Ordering::Relaxed,
                );
                let events = product.events_per_request() as u64;
                let bytes = payload.body.len() as u64;
                stats.requests.fetch_add(1, Ordering::Relaxed);
                stats.attempted_events.fetch_add(events, Ordering::Relaxed);
                stats.attempted_bytes.fetch_add(bytes, Ordering::Relaxed);

                let request_started = Instant::now();
                let mut request = client
                    .post(&common.url)
                    .header(CONTENT_TYPE, payload.content_type)
                    .header("x-request-id", format!("scale-{run_id}-{sequence}"))
                    .body(payload.body);
                if let Some(encoding) = payload.content_encoding {
                    request = request.header(CONTENT_ENCODING, encoding);
                }
                if product.name() == "meter" {
                    request = request.header("x-prometheus-remote-write-version", "0.1.0");
                }
                let response = request.send().await;
                let latency = request_started.elapsed().as_micros().max(1) as u64;
                let _ = stats
                    .latency_micros
                    .lock()
                    .expect("latency lock poisoned")
                    .record(latency);
                match response {
                    Ok(response) if response.status().is_success() => {
                        stats.successful_requests.fetch_add(1, Ordering::Relaxed);
                        stats.successful_events.fetch_add(events, Ordering::Relaxed);
                        stats.successful_bytes.fetch_add(bytes, Ordering::Relaxed);
                    }
                    Ok(response) => {
                        let status = response.status();
                        let body = response
                            .text()
                            .await
                            .unwrap_or_else(|error| format!("<body error: {error}>"));
                        stats.error(format!(
                            "http {}: {}",
                            status.as_u16(),
                            truncate(&body, 256)
                        ));
                    }
                    Err(error) => stats.error(format!("transport: {error}")),
                }
            }
        });
    }
    while let Some(result) = workers.join_next().await {
        result.context("scalability worker panicked")?;
    }
    Ok(stats)
}

fn report(cli: &Cli, run_id: String, elapsed: Duration, stats: &Stats) -> Result<Report> {
    let seconds = elapsed.as_secs_f64();
    let successful_requests = stats.successful_requests.load(Ordering::Relaxed);
    let successful_events = stats.successful_events.load(Ordering::Relaxed);
    let successful_bytes = stats.successful_bytes.load(Ordering::Relaxed);
    let histogram = stats.latency_micros.lock().expect("latency lock poisoned");
    let percentile = |quantile| histogram.value_at_quantile(quantile) as f64 / 1_000.0;
    let workload = match &cli.product {
        ProductArgs::Line(args) => serde_json::to_value(args)?,
        ProductArgs::Meter(args) => serde_json::to_value(args)?,
        ProductArgs::Track(args) => serde_json::to_value(args)?,
    };
    Ok(Report {
        product: cli.product.name().to_owned(),
        event_unit: cli.product.event_name().to_owned(),
        run_id,
        url: cli.common.url.clone(),
        duration_seconds: seconds,
        requested_target_rate: cli.common.target_rate,
        concurrency: cli.common.concurrency,
        events_per_request: cli.product.events_per_request(),
        attempted_requests: stats.requests.load(Ordering::Relaxed),
        successful_requests,
        failed_requests: stats.failed_requests.load(Ordering::Relaxed),
        attempted_events: stats.attempted_events.load(Ordering::Relaxed),
        successful_events,
        attempted_payload_bytes: stats.attempted_bytes.load(Ordering::Relaxed),
        successful_payload_bytes: successful_bytes,
        successful_requests_per_second: successful_requests as f64 / seconds,
        successful_events_per_second: successful_events as f64 / seconds,
        successful_payload_mib_per_second: successful_bytes as f64 / seconds / (1024.0 * 1024.0),
        client_payload_build_cpu_seconds: stats.client_build_nanos.load(Ordering::Relaxed) as f64
            / 1_000_000_000.0,
        latency_ms: LatencyReport {
            p50: percentile(0.50),
            p90: percentile(0.90),
            p95: percentile(0.95),
            p99: percentile(0.99),
            max: histogram.max() as f64 / 1_000.0,
        },
        errors: stats.errors.lock().expect("error lock poisoned").clone(),
        workload,
    })
}

fn truncate(value: &str, maximum: usize) -> String {
    value.chars().take(maximum).collect()
}

#[derive(Serialize)]
struct LokiPush {
    streams: Vec<LokiStream>,
}

#[derive(Serialize)]
struct LokiStream {
    stream: BTreeMap<&'static str, String>,
    values: Vec<[String; 2]>,
}

fn line_payload(args: &LineArgs, request_id: u64, run_id: &str) -> Result<Payload> {
    let stream_count = args.streams_per_request.min(args.events_per_request);
    let mut streams = Vec::with_capacity(stream_count);
    let now = unix_nanos();
    for local_stream in 0..stream_count {
        let stream_id =
            (request_id * stream_count as u64 + local_stream as u64) % args.active_streams;
        let mut labels = BTreeMap::new();
        labels.insert("app", format!("scale-app-{}", stream_id % 100));
        labels.insert("cluster", "scale-cluster".to_owned());
        labels.insert("environment", "benchmark".to_owned());
        labels.insert("host", format!("host-{}", stream_id % 1_000));
        labels.insert("run_id", run_id.to_owned());
        let mut values = Vec::new();
        let mut event = local_stream;
        while event < args.events_per_request {
            let bytes: usize = match event % 20 {
                0 => 4_096,
                1..=5 => 1_024,
                _ => 250,
            };
            let prefix = format!(
                r#"{{"level":"info","request":{},"stream":{},"message":""#,
                request_id, stream_id
            );
            let padding = "x".repeat(bytes.saturating_sub(prefix.len() + 2));
            values.push([
                (now + event as u128).to_string(),
                format!("{prefix}{padding}\"}}"),
            ]);
            event += stream_count;
        }
        streams.push(LokiStream {
            stream: labels,
            values,
        });
    }
    Ok(Payload {
        body: serde_json::to_vec(&LokiPush { streams })?,
        content_type: "application/json",
        content_encoding: None,
    })
}

#[derive(Clone, PartialEq, Message)]
struct RemoteWriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<RemoteTimeSeries>,
}

#[derive(Clone, PartialEq, Message)]
struct RemoteTimeSeries {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<RemoteLabel>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<RemoteSample>,
}

#[derive(Clone, PartialEq, Message)]
struct RemoteLabel {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, Copy, PartialEq, Message)]
struct RemoteSample {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(int64, tag = "2")]
    timestamp: i64,
}

fn meter_payload(args: &MeterArgs, request_id: u64, run_id: &str) -> Result<Payload> {
    let timestamp = unix_millis();
    let churn_threshold = (args.churn_percent * 100.0).round() as u64;
    let mut timeseries = Vec::with_capacity(args.events_per_request);
    for offset in 0..args.events_per_request {
        let ordinal = request_id
            .saturating_mul(args.events_per_request as u64)
            .saturating_add(offset as u64);
        let series_id = ordinal % args.active_series;
        let mut labels = vec![
            remote_label("__name__", format!("scale_metric_{}", series_id % 100)),
            remote_label("cluster", "scale-cluster"),
            remote_label("environment", "benchmark"),
            remote_label("instance", format!("instance-{}", series_id % 100_000)),
            remote_label("job", format!("job-{}", series_id % 1_000)),
            remote_label("run_id", run_id),
            remote_label("zone", format!("zone-{}", series_id % 3)),
        ];
        if ordinal % 10_000 < churn_threshold {
            labels.push(remote_label("churn", format!("{request_id}-{offset}")));
        }
        timeseries.push(RemoteTimeSeries {
            labels,
            samples: vec![RemoteSample {
                value: (ordinal % 10_000) as f64 / 10.0,
                timestamp,
            }],
        });
    }
    let mut encoded = Vec::with_capacity(args.events_per_request * 100);
    RemoteWriteRequest { timeseries }.encode(&mut encoded)?;
    Ok(Payload {
        body: snap::raw::Encoder::new()
            .compress_vec(&encoded)
            .context("Snappy-compressing remote-write payload")?,
        content_type: "application/x-protobuf",
        content_encoding: Some("snappy"),
    })
}

fn remote_label(name: impl Into<String>, value: impl Into<String>) -> RemoteLabel {
    RemoteLabel {
        name: name.into(),
        value: value.into(),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ExportTraceServiceRequest {
    #[prost(message, repeated, tag = "1")]
    resource_spans: Vec<ResourceSpans>,
}

#[derive(Clone, PartialEq, Message)]
struct ResourceSpans {
    #[prost(message, optional, tag = "1")]
    resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    scope_spans: Vec<ScopeSpans>,
}

#[derive(Clone, PartialEq, Message)]
struct Resource {
    #[prost(message, repeated, tag = "1")]
    attributes: Vec<KeyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct ScopeSpans {
    #[prost(message, repeated, tag = "2")]
    spans: Vec<OtlpSpan>,
}

#[derive(Clone, PartialEq, Message)]
struct OtlpSpan {
    #[prost(bytes = "vec", tag = "1")]
    trace_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    span_id: Vec<u8>,
    #[prost(string, tag = "5")]
    name: String,
    #[prost(enumeration = "SpanKind", tag = "6")]
    kind: i32,
    #[prost(fixed64, tag = "7")]
    start_time_unix_nano: u64,
    #[prost(fixed64, tag = "8")]
    end_time_unix_nano: u64,
    #[prost(message, repeated, tag = "9")]
    attributes: Vec<KeyValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum SpanKind {
    Unspecified = 0,
    Internal = 1,
    Server = 2,
}

#[derive(Clone, PartialEq, Message)]
struct KeyValue {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(message, optional, tag = "2")]
    value: Option<AnyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct AnyValue {
    #[prost(oneof = "any_value::Value", tags = "1")]
    value: Option<any_value::Value>,
}

mod any_value {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Value {
        #[prost(string, tag = "1")]
        StringValue(String),
    }
}

fn track_payload(args: &TrackArgs, request_id: u64, run_id: &str) -> Result<Payload> {
    let now = unix_nanos() as u64;
    let mut spans = Vec::with_capacity(args.events_per_request);
    let padding = "x".repeat(args.attribute_bytes);
    for offset in 0..args.events_per_request {
        let ordinal = request_id
            .saturating_mul(args.events_per_request as u64)
            .saturating_add(offset as u64);
        let trace_ordinal = ordinal / args.spans_per_trace as u64;
        let mut trace_id = Vec::with_capacity(16);
        trace_id.extend_from_slice(&stable_hash(run_id).to_be_bytes());
        trace_id.extend_from_slice(&trace_ordinal.to_be_bytes());
        spans.push(OtlpSpan {
            trace_id,
            span_id: ordinal.to_be_bytes().to_vec(),
            name: format!("scale-operation-{}", ordinal % 100),
            kind: SpanKind::Server as i32,
            start_time_unix_nano: now.saturating_add(offset as u64 * 1_000),
            end_time_unix_nano: now
                .saturating_add(offset as u64 * 1_000)
                .saturating_add(1_000_000),
            attributes: vec![
                string_kv("deployment.environment", "benchmark"),
                string_kv("http.request.method", "GET"),
                string_kv("scale.payload", padding.clone()),
                string_kv("scale.run_id", run_id),
                string_kv(
                    "service.instance.id",
                    format!("instance-{}", ordinal % 10_000),
                ),
            ],
        });
    }
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string_kv(
                        "service.name",
                        format!("scale-service-{}", request_id % 100),
                    ),
                    string_kv("scale.run_id", run_id),
                ],
            }),
            scope_spans: vec![ScopeSpans { spans }],
        }],
    };
    let mut body = Vec::with_capacity(args.events_per_request * (args.attribute_bytes + 200));
    request.encode(&mut body)?;
    Ok(Payload {
        body,
        content_type: "application/x-protobuf",
        content_encoding: None,
    })
}

fn string_kv(key: impl Into<String>, value: impl Into<String>) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
    }
}

fn stable_hash(value: &str) -> u64 {
    value.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    })
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn unix_millis() -> i64 {
    unix_nanos().min(i64::MAX as u128) as i64 / 1_000_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payloads_are_nonempty_and_report_expected_encodings() {
        let line = line_payload(
            &LineArgs {
                events_per_request: 20,
                streams_per_request: 4,
                active_streams: 10,
            },
            1,
            "test",
        )
        .unwrap();
        assert_eq!(line.content_type, "application/json");
        assert!(line.body.len() > 10_000);

        let meter = meter_payload(
            &MeterArgs {
                events_per_request: 10,
                active_series: 100,
                churn_percent: 10.0,
            },
            1,
            "test",
        )
        .unwrap();
        assert_eq!(meter.content_encoding, Some("snappy"));
        assert!(
            !snap::raw::Decoder::new()
                .decompress_vec(&meter.body)
                .unwrap()
                .is_empty()
        );

        let track = track_payload(
            &TrackArgs {
                events_per_request: 10,
                spans_per_trace: 5,
                attribute_bytes: 256,
            },
            1,
            "test",
        )
        .unwrap();
        assert_eq!(track.content_type, "application/x-protobuf");
        assert!(track.body.len() > 2_500);
    }

    #[test]
    fn validation_rejects_unsafe_or_empty_runs() {
        let common = CommonArgs {
            url: "http://localhost".to_owned(),
            duration_seconds: 1,
            warmup_seconds: 0,
            concurrency: 1,
            target_rate: 0,
            timeout_seconds: 1,
            bearer_token_env: None,
            basic_username_env: None,
            basic_password_env: None,
            output: None,
            run_id: None,
            allow_production_write: false,
        };
        let cli = Cli {
            common,
            product: ProductArgs::Line(LineArgs {
                events_per_request: 1,
                streams_per_request: 1,
                active_streams: 1,
            }),
        };
        assert!(validate(&cli).is_err());
    }

    #[test]
    fn basic_auth_uses_rfc_7617_encoding() {
        let value = basic_authorization_value("scale-user", "p@ss:word").unwrap();
        assert_eq!(
            value.to_str().unwrap(),
            "Basic c2NhbGUtdXNlcjpwQHNzOndvcmQ="
        );
        assert!(value.is_sensitive());
    }
}
