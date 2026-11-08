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

    /// Ramps the measured phase linearly from this many events per second to
    /// `--target-rate`; the warmup runs at this rate. Zero holds
    /// `--target-rate` throughout.
    #[arg(long, default_value_t = 0)]
    ramp_from_rate: u64,

    /// Adds a timeline of the measured phase to the report, in windows of
    /// this many seconds. Zero leaves it out.
    #[arg(long, default_value_t = 0)]
    timeline_seconds: u64,

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

    /// Seconds to stamp generated events in the past, so the target's
    /// rollup windows close while they still receive writes.
    #[arg(long, default_value_t = 0)]
    backdate_seconds: u64,
}

/// Nanoseconds generated events are stamped in the past.
static BACKDATE_NANOS: AtomicU64 = AtomicU64::new(0);

#[derive(Subcommand, Clone, Debug)]
enum ProductArgs {
    /// Loki JSON log ingestion workload.
    Logs(LogsArgs),
    /// Prometheus remote-write metrics workload.
    Metrics(MetricsArgs),
    /// OTLP/HTTP protobuf trace ingestion workload.
    Traces(TracesArgs),
}

#[derive(Args, Clone, Debug, Serialize)]
struct LogsArgs {
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
struct MetricsArgs {
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
struct TracesArgs {
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
            Self::Logs(_) => "logs",
            Self::Metrics(_) => "metrics",
            Self::Traces(_) => "traces",
        }
    }

    fn event_name(&self) -> &'static str {
        match self {
            Self::Logs(_) => "entries",
            Self::Metrics(_) => "samples",
            Self::Traces(_) => "spans",
        }
    }

    fn events_per_request(&self) -> usize {
        match self {
            Self::Logs(args) => args.events_per_request,
            Self::Metrics(args) => args.events_per_request,
            Self::Traces(args) => args.events_per_request,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.events_per_request() == 0 {
            bail!("events-per-request must be greater than zero");
        }
        match self {
            Self::Logs(args) => {
                if args.streams_per_request == 0 || args.active_streams == 0 {
                    bail!("Logs stream counts must be greater than zero");
                }
            }
            Self::Metrics(args) => {
                if args.active_series == 0
                    || !args.churn_percent.is_finite()
                    || !(0.0..=100.0).contains(&args.churn_percent)
                {
                    bail!(
                        "Metrics active-series must be positive and churn-percent must be 0..=100"
                    );
                }
            }
            Self::Traces(args) => {
                if args.spans_per_trace == 0 {
                    bail!("Traces spans-per-trace must be greater than zero");
                }
            }
        }
        Ok(())
    }

    fn build_payload(&self, request_id: u64, run_id: &str) -> Result<Payload> {
        match self {
            Self::Logs(args) => logs_payload(args, request_id, run_id),
            Self::Metrics(args) => metrics_payload(args, request_id, run_id),
            Self::Traces(args) => traces_payload(args, request_id, run_id),
        }
    }
}

#[derive(Debug)]
struct Payload {
    body: Vec<u8>,
    content_type: &'static str,
    content_encoding: Option<&'static str>,
}

/// When open-loop requests are due: events offered grow at `from` events
/// per second, rising linearly to `to` by `over`.
#[derive(Clone, Copy, Debug)]
struct Schedule {
    from: f64,
    to: f64,
    over: f64,
}

impl Schedule {
    fn hold(rate: u64) -> Self {
        Self {
            from: rate as f64,
            to: rate as f64,
            over: 1.0,
        }
    }

    fn ramp(common: &CommonArgs, over: Duration) -> Self {
        if common.ramp_from_rate == 0 {
            return Self::hold(common.target_rate);
        }
        Self {
            from: common.ramp_from_rate as f64,
            to: common.target_rate as f64,
            over: over.as_secs_f64().max(f64::MIN_POSITIVE),
        }
    }

    /// The rate offered `at` seconds in.
    fn rate(&self, at: f64) -> f64 {
        self.from + (self.to - self.from) * (at / self.over).min(1.0)
    }

    /// Seconds in at which the `events`th event is due: the inverse of
    /// `from·t + (to - from)·t² / 2·over`.
    fn due(&self, events: f64) -> f64 {
        let slope = (self.to - self.from) / self.over;
        if slope.abs() < f64::EPSILON {
            return events / self.from;
        }
        let ramped = (self.from * self.from + 2.0 * slope * events).max(0.0).sqrt();
        let at = (ramped - self.from) / slope;
        if at <= self.over {
            at
        } else {
            let at_end = self.from * self.over + slope * self.over * self.over / 2.0;
            self.over + (events - at_end) / self.to
        }
    }
}

/// One window of the measured phase, by when its requests were sent.
struct Window {
    attempted_events: u64,
    successful_events: u64,
    rejected_requests: u64,
    failed_requests: u64,
    latency_micros: Histogram<u64>,
}

#[derive(Serialize)]
struct WindowReport {
    start_seconds: u64,
    offered_events_per_second: f64,
    attempted_events_per_second: f64,
    successful_events_per_second: f64,
    rejected_requests: u64,
    failed_requests: u64,
    p99_ms: f64,
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
    window_seconds: u64,
    windows: Mutex<Vec<Window>>,
}

impl Stats {
    fn new() -> Result<Self> {
        Self::with_windows(0, Duration::ZERO)
    }

    fn with_windows(window_seconds: u64, duration: Duration) -> Result<Self> {
        let windows = if window_seconds == 0 {
            Vec::new()
        } else {
            (0..duration.as_secs().div_ceil(window_seconds).max(1))
                .map(|_| {
                    Ok(Window {
                        attempted_events: 0,
                        successful_events: 0,
                        rejected_requests: 0,
                        failed_requests: 0,
                        latency_micros: Histogram::new_with_bounds(1, 600_000_000, 3)?,
                    })
                })
                .collect::<Result<_>>()?
        };
        Ok(Self {
            window_seconds,
            windows: Mutex::new(windows),
            ..Self::empty()?
        })
    }

    fn empty() -> Result<Self> {
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
            window_seconds: 0,
            windows: Mutex::new(Vec::new()),
        })
    }

    /// Records a request sent `sent` into the phase in its window.
    fn window(&self, sent: Duration, record: impl FnOnce(&mut Window)) {
        if self.window_seconds == 0 {
            return;
        }
        let mut windows = self.windows.lock().expect("window lock poisoned");
        let index = usize::try_from(sent.as_secs() / self.window_seconds).unwrap_or(usize::MAX);
        if let Some(window) = windows.get_mut(index) {
            record(window);
        }
    }

    fn timeline(&self, schedule: Option<Schedule>) -> Vec<WindowReport> {
        let width = self.window_seconds as f64;
        self.windows
            .lock()
            .expect("window lock poisoned")
            .iter()
            .enumerate()
            .map(|(index, window)| {
                let start = index as f64 * width;
                WindowReport {
                    start_seconds: index as u64 * self.window_seconds,
                    offered_events_per_second: schedule
                        .map_or(0.0, |schedule| schedule.rate(start + width / 2.0)),
                    attempted_events_per_second: window.attempted_events as f64 / width,
                    successful_events_per_second: window.successful_events as f64 / width,
                    rejected_requests: window.rejected_requests,
                    failed_requests: window.failed_requests,
                    p99_ms: window.latency_micros.value_at_quantile(0.99) as f64 / 1_000.0,
                }
            })
            .collect()
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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    timeline: Vec<WindowReport>,
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
    BACKDATE_NANOS.store(
        cli.common.backdate_seconds.saturating_mul(1_000_000_000),
        Ordering::Relaxed,
    );
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
        let warmup = Duration::from_secs(cli.common.warmup_seconds);
        let rate = if cli.common.ramp_from_rate > 0 {
            cli.common.ramp_from_rate
        } else {
            cli.common.target_rate
        };
        let warmup = run_phase(
            &client,
            &cli.common,
            &cli.product,
            &run_id,
            (rate > 0).then(|| Schedule::hold(rate)),
            Arc::new(Stats::new()?),
            warmup,
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
    let duration = Duration::from_secs(cli.common.duration_seconds);
    let schedule = (cli.common.target_rate > 0).then(|| Schedule::ramp(&cli.common, duration));
    let started = Instant::now();
    let stats = run_phase(
        &client,
        &cli.common,
        &cli.product,
        &run_id,
        schedule,
        Arc::new(Stats::with_windows(cli.common.timeline_seconds, duration)?),
        duration,
    )
    .await?;
    let elapsed = started.elapsed();
    let report = report(&cli, run_id, elapsed, &stats, schedule)?;
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
    schedule: Option<Schedule>,
    stats: Arc<Stats>,
    duration: Duration,
) -> Result<Arc<Stats>> {
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
                if let Some(schedule) = schedule {
                    let events = sequence as f64 * product.events_per_request() as f64;
                    let Ok(offset) = Duration::try_from_secs_f64(schedule.due(events)) else {
                        break;
                    };
                    let scheduled = start + offset;
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
                let sent = start.elapsed();
                stats.window(sent, |window| window.attempted_events += events);
                let mut request = client
                    .post(&common.url)
                    .header(CONTENT_TYPE, payload.content_type)
                    .header("x-request-id", format!("scale-{run_id}-{sequence}"))
                    .body(payload.body);
                if let Some(encoding) = payload.content_encoding {
                    request = request.header(CONTENT_ENCODING, encoding);
                }
                if product.name() == "metrics" {
                    request = request.header("x-prometheus-remote-write-version", "0.1.0");
                }
                let response = request.send().await;
                let latency = request_started.elapsed().as_micros().max(1) as u64;
                let _ = stats
                    .latency_micros
                    .lock()
                    .expect("latency lock poisoned")
                    .record(latency);
                stats.window(sent, |window| {
                    let _ = window.latency_micros.record(latency);
                    match &response {
                        Ok(response) if response.status().is_success() => {
                            window.successful_events += events;
                        }
                        Ok(response) if response.status().as_u16() == 429 => {
                            window.rejected_requests += 1;
                        }
                        _ => window.failed_requests += 1,
                    }
                });
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

fn report(
    cli: &Cli,
    run_id: String,
    elapsed: Duration,
    stats: &Stats,
    schedule: Option<Schedule>,
) -> Result<Report> {
    let seconds = elapsed.as_secs_f64();
    let successful_requests = stats.successful_requests.load(Ordering::Relaxed);
    let successful_events = stats.successful_events.load(Ordering::Relaxed);
    let successful_bytes = stats.successful_bytes.load(Ordering::Relaxed);
    let histogram = stats.latency_micros.lock().expect("latency lock poisoned");
    let percentile = |quantile| histogram.value_at_quantile(quantile) as f64 / 1_000.0;
    let workload = match &cli.product {
        ProductArgs::Logs(args) => serde_json::to_value(args)?,
        ProductArgs::Metrics(args) => serde_json::to_value(args)?,
        ProductArgs::Traces(args) => serde_json::to_value(args)?,
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
        timeline: stats.timeline(schedule),
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

fn logs_payload(args: &LogsArgs, request_id: u64, run_id: &str) -> Result<Payload> {
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
            let padding = filler(
                request_id ^ (event as u64).rotate_left(32),
                bytes.saturating_sub(prefix.len() + 2),
            );
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

fn metrics_payload(args: &MetricsArgs, request_id: u64, run_id: &str) -> Result<Payload> {
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

fn traces_payload(args: &TracesArgs, request_id: u64, run_id: &str) -> Result<Payload> {
    let now = unix_nanos() as u64;
    let mut spans = Vec::with_capacity(args.events_per_request);
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
                string_kv("scale.payload", filler(ordinal, args.attribute_bytes)),
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

const FILLER_WORDS: &[&str] = &[
    "request", "completed", "failed", "retrying", "connection", "timeout", "user", "session",
    "cache", "miss", "hit", "upstream", "downstream", "status", "handler", "worker", "queue",
    "latency", "bytes", "read", "write", "commit", "rollback", "token", "expired", "refresh",
    "pool", "acquired", "released", "batch", "flushed", "received", "sent", "path", "method",
    "GET", "POST", "PUT", "200", "404", "500", "ms", "id", "trace", "span", "error", "warn",
    "info", "debug", "client", "server", "region", "shard", "replica", "lease", "renewed",
];

/// `bytes` of log-like text, deterministic in `seed`: common words with a
/// small number every eighth and a unique id every 25th, about the
/// request and trace ids a real line carries. It compresses about as well
/// as real log messages and span attributes rather than as a run of one
/// byte, and adds as many distinct terms to a full-text index.
fn filler(seed: u64, bytes: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut text = String::with_capacity(bytes + 24);
    while text.len() < bytes {
        let pick = next();
        match pick % 200 {
            0..8 => text.push_str(&format!("{:016x}", next())),
            8..33 => text.push_str(&(next() % 1_000).to_string()),
            _ => text.push_str(FILLER_WORDS[(pick >> 8) as usize % FILLER_WORDS.len()]),
        }
        text.push(' ');
    }
    text.truncate(bytes);
    text
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
        .saturating_sub(u128::from(BACKDATE_NANOS.load(Ordering::Relaxed)))
}

fn unix_millis() -> i64 {
    unix_nanos().min(i64::MAX as u128) as i64 / 1_000_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filler_compresses_like_text() {
        let text = filler(7, 64 * 1024);
        assert_eq!(text.len(), 64 * 1024);
        assert_eq!(text, filler(7, 64 * 1024));
        assert_ne!(text, filler(8, 64 * 1024));
        let compressed = snap::raw::Encoder::new()
            .compress_vec(text.as_bytes())
            .unwrap();
        let ratio = text.len() as f64 / compressed.len() as f64;
        assert!((1.3..4.0).contains(&ratio), "snappy ratio {ratio}");
    }

    #[test]
    fn payloads_are_nonempty_and_report_expected_encodings() {
        let line = logs_payload(
            &LogsArgs {
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

        let metrics = metrics_payload(
            &MetricsArgs {
                events_per_request: 10,
                active_series: 100,
                churn_percent: 10.0,
            },
            1,
            "test",
        )
        .unwrap();
        assert_eq!(metrics.content_encoding, Some("snappy"));
        assert!(
            !snap::raw::Decoder::new()
                .decompress_vec(&metrics.body)
                .unwrap()
                .is_empty()
        );

        let traces = traces_payload(
            &TracesArgs {
                events_per_request: 10,
                spans_per_trace: 5,
                attribute_bytes: 256,
            },
            1,
            "test",
        )
        .unwrap();
        assert_eq!(traces.content_type, "application/x-protobuf");
        assert!(traces.body.len() > 2_500);
    }

    #[test]
    fn validation_rejects_unsafe_or_empty_runs() {
        let common = CommonArgs {
            url: "http://localhost".to_owned(),
            duration_seconds: 1,
            warmup_seconds: 0,
            concurrency: 1,
            target_rate: 0,
            ramp_from_rate: 0,
            timeline_seconds: 0,
            timeout_seconds: 1,
            bearer_token_env: None,
            basic_username_env: None,
            basic_password_env: None,
            output: None,
            run_id: None,
            allow_production_write: false,
            backdate_seconds: 0,
        };
        let cli = Cli {
            common,
            product: ProductArgs::Logs(LogsArgs {
                events_per_request: 1,
                streams_per_request: 1,
                active_streams: 1,
            }),
        };
        assert!(validate(&cli).is_err());
    }

    #[test]
    fn a_ramp_offers_its_events_on_a_linear_rate() {
        let ramp = Schedule {
            from: 100.0,
            to: 300.0,
            over: 10.0,
        };
        // 2000 events by the end of the ramp, then 300 a second.
        assert!((ramp.due(0.0)).abs() < 1e-9);
        assert!((ramp.due(2_000.0) - 10.0).abs() < 1e-9);
        assert!((ramp.due(2_300.0) - 11.0).abs() < 1e-9);
        assert!((ramp.due(1_000.0) - (-100.0 + 50_000f64.sqrt()) / 20.0).abs() < 1e-9);
        assert!((ramp.rate(5.0) - 200.0).abs() < 1e-9);
        let hold = Schedule::hold(50);
        assert!((hold.due(100.0) - 2.0).abs() < 1e-9);
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
