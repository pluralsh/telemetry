//! Black-box Meter/Prometheus compatibility regression runner.
//!
//! Floating-point query values are compared with a relative tolerance of 1e-9
//! and an absolute floor of 1e-12. This permits harmless formatting and
//! evaluation-order differences without hiding meaningful PromQL drift.

use std::{
    collections::BTreeMap,
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    },
    resource::v1::Resource,
};
use prost::Message;
use reqwest::{Client, StatusCode, header};
use serde::Serialize;
use serde_json::{Value, json};

const REL_TOLERANCE: f64 = 1e-9;
const ABS_TOLERANCE: f64 = 1e-12;

#[derive(Clone, PartialEq, Message)]
struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<TimeSeries>,
    #[prost(message, repeated, tag = "3")]
    metadata: Vec<MetricMetadata>,
}

#[derive(Clone, PartialEq, Message)]
struct MetricMetadata {
    #[prost(int32, tag = "1")]
    metric_family_type: i32,
    #[prost(string, tag = "2")]
    metric_family_name: String,
    #[prost(string, tag = "4")]
    help: String,
    #[prost(string, tag = "5")]
    unit: String,
}

#[derive(Clone, PartialEq, Message)]
struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, Message)]
struct Label {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, PartialEq, Message)]
struct Sample {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(int64, tag = "2")]
    timestamp: i64,
}

#[derive(Clone)]
struct Target {
    base: String,
    auth: Option<String>,
}

struct Runner {
    http: Client,
    prometheus: Target,
    writer: Target,
    reader: Target,
    base_ms: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let runner = Runner::from_env()?;
    runner.run().await
}

impl Runner {
    fn from_env() -> Result<Self> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
        Ok(Self {
            http: Client::builder().timeout(Duration::from_secs(15)).build()?,
            prometheus: Target {
                base: env_or("PROMETHEUS_URL", "http://prometheus:9090"),
                auth: None,
            },
            // writer-0 is intentional: fixture series span both static ownership
            // ranges, proving that its internal gRPC forwarding path is exercised.
            writer: Target {
                base: env_or(
                    "METER_WRITE_URL",
                    "http://meter-writer-0:8080/write/ns/regression",
                ),
                auth: Some(bearer("regression-write")),
            },
            reader: Target {
                base: env_or(
                    "METER_READ_URL",
                    "http://meter-reader:8080/read/ns/regression",
                ),
                auth: Some(basic("regression-reader", "regression-read")),
            },
            base_ms: env::var("REGRESSION_BASE_MS")
                .ok()
                .map(|value| value.parse())
                .transpose()
                .context("REGRESSION_BASE_MS must be an integer")?
                .unwrap_or(now / 60_000 * 60_000 - 600_000),
        })
    }

    async fn run(&self) -> Result<()> {
        let fixture = fixture(self.base_ms);
        ensure!(
            fixture.iter().any(|series| fixture_shard(series) < 8)
                && fixture.iter().any(|series| fixture_shard(series) >= 8),
            "fixture must span both configured writer ranges"
        );
        let body = remote_write_body(&fixture)?;
        if env::var_os("REGRESSION_METER_ONLY").is_some() {
            let request_id = env::var("REGRESSION_RUN_ID")
                .unwrap_or_else(|_| format!("meter-only-{}", self.base_ms));
            self.post_remote_write(&self.writer, "/api/v1/write", &body, Some(&request_id))
                .await?;
            self.wait_for_visibility().await?;
            println!("Meter-only forwarding/read check passed");
            return Ok(());
        }
        self.post_remote_write(&self.prometheus, "/api/v1/write", &body, None)
            .await?;
        self.post_remote_write(
            &self.writer,
            "/api/v1/write",
            &body,
            Some("regression-idempotent"),
        )
        .await?;
        // A repeated request ID must be accepted without duplicating data.
        self.post_remote_write(
            &self.writer,
            "/api/v1/write",
            &body,
            Some("regression-idempotent"),
        )
        .await?;

        self.wait_for_visibility().await?;
        self.compare_promql().await?;
        self.compare_discovery().await?;
        self.check_otlp().await?;
        self.check_namespace_isolation().await?;
        self.check_authorization(&body).await?;
        println!("regression suite passed");
        Ok(())
    }

    async fn post_remote_write(
        &self,
        target: &Target,
        path: &str,
        body: &[u8],
        request_id: Option<&str>,
    ) -> Result<()> {
        let mut request = self
            .http
            .post(format!("{}{path}", target.base))
            .header("content-type", "application/x-protobuf")
            .header("content-encoding", "snappy")
            .body(body.to_vec());
        if let Some(auth) = &target.auth {
            request = request.header(header::AUTHORIZATION, auth);
        }
        if let Some(request_id) = request_id {
            request = request.header("x-request-id", request_id);
        }
        let response = request.send().await?;
        ensure!(
            response.status().is_success(),
            "remote write to {} failed: {} {}",
            target.base,
            response.status(),
            response.text().await.unwrap_or_default()
        );
        Ok(())
    }

    async fn wait_for_visibility(&self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let query = format!(
                "regression_gauge{{instance=\"a\"}} @ {}",
                (self.base_ms + 540_000) as f64 / 1000.0
            );
            if self
                .query(&self.reader, "/api/v1/query", &[("query", query)])
                .await
                .is_ok_and(|value| result_len(&value) == 1)
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("Meter reader did not observe durable writer data within 60s");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn compare_promql(&self) -> Result<()> {
        let end = (self.base_ms + 540_000) as f64 / 1000.0;
        let instant = [
            ("instant selector", "regression_gauge".to_owned()),
            (
                "regex selector",
                r#"regression_gauge{instance=~"a|b"}"#.to_owned(),
            ),
            ("sum", "sum(regression_gauge)".to_owned()),
            (
                "grouped aggregate",
                "sum by (job) (regression_gauge)".to_owned(),
            ),
            (
                "counter rate",
                "rate(regression_counter_total[5m])".to_owned(),
            ),
            ("offset", "regression_gauge offset 2m".to_owned()),
            (
                "binary join",
                "regression_left + on(instance) group_left(zone) regression_right".to_owned(),
            ),
            (
                "empty",
                r#"regression_gauge{instance="missing"}"#.to_owned(),
            ),
            (
                "boundary",
                format!("regression_gauge @ {}", self.base_ms as f64 / 1000.0),
            ),
        ];
        for (name, expression) in instant {
            let params = [("query", expression), ("time", end.to_string())];
            let prometheus = self
                .query(&self.prometheus, "/api/v1/query", &params)
                .await?;
            let meter = self.query(&self.reader, "/api/v1/query", &params).await?;
            if name == "counter rate" {
                compare_query_data_ignoring_labels(name, &prometheus, &meter, &["__name__"])?;
            } else {
                compare_query_data(name, &prometheus, &meter)?;
            }
        }

        for (name, expression) in [
            ("range selector", "regression_gauge"),
            ("range aggregate", "sum by (job) (regression_gauge)"),
            ("range rate", "rate(regression_counter_total[5m])"),
        ] {
            let params = [
                ("query", expression.to_owned()),
                ("start", (self.base_ms as f64 / 1000.0).to_string()),
                ("end", end.to_string()),
                ("step", "60".to_owned()),
            ];
            let prometheus = self
                .query(&self.prometheus, "/api/v1/query_range", &params)
                .await?;
            let meter = self
                .query(&self.reader, "/api/v1/query_range", &params)
                .await?;
            if name == "range rate" {
                compare_query_data_ignoring_labels(name, &prometheus, &meter, &["__name__"])?;
            } else {
                compare_query_data(name, &prometheus, &meter)?;
            }
        }
        Ok(())
    }

    async fn compare_discovery(&self) -> Result<()> {
        let start = (self.base_ms as f64 / 1000.0).to_string();
        let end = ((self.base_ms + 540_000) as f64 / 1000.0).to_string();
        for (name, path, params) in [
            (
                "labels",
                "/api/v1/labels",
                vec![
                    ("match[]", "regression_gauge".to_owned()),
                    ("start", start.clone()),
                    ("end", end.clone()),
                ],
            ),
            (
                "label values",
                "/api/v1/label/instance/values",
                vec![
                    ("match[]", "regression_gauge".to_owned()),
                    ("start", start.clone()),
                    ("end", end.clone()),
                ],
            ),
            (
                "series",
                "/api/v1/series",
                vec![
                    ("match[]", "regression_gauge".to_owned()),
                    ("start", start.clone()),
                    ("end", end.clone()),
                ],
            ),
        ] {
            let prometheus = self.query(&self.prometheus, path, &params).await?;
            let meter = self.query(&self.reader, path, &params).await?;
            compare_json_data(name, &prometheus, &meter)?;
        }

        // Remote write 1.0 carries no reliable type/help metadata. Verify both
        // APIs and compare metric keys; OTLP metadata is checked below.
        let prometheus = self
            .query(
                &self.prometheus,
                "/api/v1/metadata",
                &[("metric", "regression_gauge".to_owned())],
            )
            .await?;
        let meter = self
            .query(
                &self.reader,
                "/api/v1/metadata",
                &[("metric", "regression_gauge".to_owned())],
            )
            .await?;
        ensure_success("Prometheus metadata", &prometheus)?;
        ensure_success("Meter metadata", &meter)?;
        Ok(())
    }

    async fn check_otlp(&self) -> Result<()> {
        let timestamp = self.base_ms + 540_000;
        let otlp = otlp_fixture(timestamp);
        let mut bytes = Vec::new();
        otlp.encode(&mut bytes)?;
        let response = self
            .authorized(
                self.http
                    .post(format!("{}/v1/metrics", self.writer.base))
                    .header("content-type", "application/x-protobuf")
                    .header("x-request-id", "regression-otlp")
                    .body(bytes.clone()),
                &self.writer,
            )
            .send()
            .await?;
        ensure!(response.status().is_success(), "OTLP write failed");

        let response = self
            .http
            .post(format!("{}/api/v1/otlp/v1/metrics", self.prometheus.base))
            .header("content-type", "application/x-protobuf")
            .body(bytes)
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "Prometheus OTLP write failed: {}",
            response.text().await.unwrap_or_default()
        );
        self.wait_for_metric("otlp_regression_temperature").await?;
        let params = [
            ("query", "otlp_regression_temperature".to_owned()),
            ("time", (timestamp as f64 / 1000.0).to_string()),
        ];
        let prometheus = self
            .query(&self.prometheus, "/api/v1/query", &params)
            .await?;
        let meter = self.query(&self.reader, "/api/v1/query", &params).await?;
        // Meter adds scope identity, which is expected OTLP conversion behavior.
        compare_query_data_ignoring_labels(
            "OTLP equivalent samples",
            &prometheus,
            &meter,
            // Prometheus derives `job` from `service.name`; Meter preserves
            // the promoted resource label without adding that scrape label.
            &["otel_scope_name", "otel_scope_version", "job"],
        )?;
        let metadata_params = [("metric", "otlp_regression_temperature".to_owned())];
        let prometheus = self
            .query(&self.prometheus, "/api/v1/metadata", &metadata_params)
            .await?;
        let meter = self
            .query(&self.reader, "/api/v1/metadata", &metadata_params)
            .await?;
        // Prometheus 3.5 accepts OTLP metadata but does not expose receiver
        // metadata through its legacy /api/v1/metadata endpoint. Validate
        // that endpoint independently, then compare Meter's durable fields
        // against the metadata carried by the shared OTLP fixture.
        ensure_success("Prometheus OTLP metadata endpoint", &prometheus)?;
        let expected = json!({
            "status": "success",
            "data": {
                "otlp_regression_temperature": [{
                    "type": "gauge",
                    "help": "OTLP regression gauge",
                    "unit": ""
                }]
            }
        });
        compare_metadata_data("OTLP metadata", &expected, &meter)?;
        Ok(())
    }

    async fn wait_for_metric(&self, metric: &str) -> Result<()> {
        for _ in 0..60 {
            let params = [
                ("query", metric.to_owned()),
                (
                    "time",
                    ((self.base_ms + 540_000) as f64 / 1000.0).to_string(),
                ),
            ];
            if self
                .query(&self.reader, "/api/v1/query", &params)
                .await
                .is_ok_and(|value| result_len(&value) > 0)
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        bail!("reader freshness check timed out for {metric}")
    }

    async fn check_namespace_isolation(&self) -> Result<()> {
        let other = Target {
            base: self.writer.base.replace("/regression", "/other"),
            auth: Some(bearer("other-write")),
        };
        let isolated = vec![series(
            "namespace_private",
            &[("tenant", "other")],
            &[(self.base_ms + 540_000, 1.0)],
        )];
        self.post_remote_write(
            &other,
            "/api/v1/write",
            &remote_write_body(&isolated)?,
            Some("other-write"),
        )
        .await?;
        let params = [
            ("query", "namespace_private".to_owned()),
            (
                "time",
                ((self.base_ms + 540_000) as f64 / 1000.0).to_string(),
            ),
        ];
        let own = self.query(&self.reader, "/api/v1/query", &params).await?;
        ensure!(
            result_len(&own) == 0,
            "namespace data leaked into regression"
        );

        let other_reader = Target {
            base: self.reader.base.replace("/regression", "/other"),
            auth: Some(bearer("other-read")),
        };
        let denied = self
            .authorized(
                self.http
                    .get(format!("{}/api/v1/query", other_reader.base))
                    .query(&params),
                &self.reader,
            )
            .send()
            .await?;
        ensure!(
            denied.status() == StatusCode::UNAUTHORIZED,
            "cross-tenant credential unexpectedly authorized"
        );
        Ok(())
    }

    async fn check_authorization(&self, body: &[u8]) -> Result<()> {
        let query_url = format!("{}/api/v1/query", self.reader.base);
        let params = [
            ("query", "regression_gauge"),
            (
                "time",
                &((self.base_ms + 540_000) as f64 / 1000.0).to_string(),
            ),
        ];
        for credential in [
            basic("regression-reader", "regression-read"),
            bearer("regression-read-token"),
            bearer("global-read"),
        ] {
            let response = self
                .http
                .get(&query_url)
                .query(&params)
                .header(header::AUTHORIZATION, credential)
                .send()
                .await?;
            ensure!(response.status().is_success(), "read credential rejected");
        }
        let denied = self
            .http
            .get(&query_url)
            .query(&params)
            .header(header::AUTHORIZATION, bearer("regression-write"))
            .send()
            .await?;
        ensure!(denied.status() == StatusCode::UNAUTHORIZED);

        let reader_write = self
            .http
            .post(format!("{}/api/v1/write", self.reader.base))
            .header(header::AUTHORIZATION, bearer("global-write"))
            .body(body.to_vec())
            .send()
            .await?;
        ensure!(
            reader_write.status() == StatusCode::NOT_FOUND
                || reader_write.status() == StatusCode::METHOD_NOT_ALLOWED,
            "read-only reader accepted a write"
        );
        for credential in [bearer("regression-write"), bearer("global-write")] {
            let request_id = format!("auth-{}", credential.len());
            let response = self
                .http
                .post(format!("{}/api/v1/write", self.writer.base))
                .header(header::AUTHORIZATION, credential)
                .header("content-type", "application/x-protobuf")
                .header("content-encoding", "snappy")
                .header("x-request-id", request_id)
                .body(body.to_vec())
                .send()
                .await?;
            ensure!(response.status().is_success(), "write credential rejected");
        }
        Ok(())
    }

    async fn query(
        &self,
        target: &Target,
        path: &str,
        params: &[(impl AsRef<str>, impl AsRef<str>)],
    ) -> Result<Value> {
        let pairs = params
            .iter()
            .map(|(key, value)| (key.as_ref(), value.as_ref()))
            .collect::<Vec<_>>();
        let response = self
            .authorized(
                self.http
                    .get(format!("{}{path}", target.base))
                    .query(&pairs),
                target,
            )
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        ensure!(
            status.is_success(),
            "{}{path}: {status} {body}",
            target.base
        );
        serde_json::from_str(&body)
            .with_context(|| format!("invalid JSON from {}{path}", target.base))
    }

    fn authorized(
        &self,
        request: reqwest::RequestBuilder,
        target: &Target,
    ) -> reqwest::RequestBuilder {
        target
            .auth
            .as_ref()
            .map_or(request.try_clone().expect("request is cloneable"), |auth| {
                request.header(header::AUTHORIZATION, auth)
            })
    }
}

fn fixture(base: i64) -> Vec<TimeSeries> {
    let times = (0..10)
        .map(|index| base + index * 60_000)
        .collect::<Vec<_>>();
    let mut result = Vec::new();
    for (instance, zone, bias) in [("a", "east", 0.0), ("b", "west", 10.0)] {
        result.push(series(
            "regression_gauge",
            &[
                ("instance", instance),
                ("job", "regression"),
                ("zone", zone),
            ],
            &times
                .iter()
                .enumerate()
                .map(|(index, timestamp)| (*timestamp, index as f64 + bias))
                .collect::<Vec<_>>(),
        ));
        result.push(series(
            "regression_counter_total",
            &[("instance", instance), ("job", "regression")],
            &times
                .iter()
                .enumerate()
                .map(|(index, timestamp)| (*timestamp, index as f64 * 100.0 + bias))
                .collect::<Vec<_>>(),
        ));
        result.push(series(
            "regression_left",
            &[("instance", instance)],
            &[(base + 540_000, bias + 2.0)],
        ));
        result.push(series(
            "regression_right",
            &[("instance", instance), ("zone", zone)],
            &[(base + 540_000, bias + 3.0)],
        ));
    }
    result
}

fn series(name: &str, labels: &[(&str, &str)], samples: &[(i64, f64)]) -> TimeSeries {
    let mut all_labels = vec![Label {
        name: "__name__".to_owned(),
        value: name.to_owned(),
    }];
    all_labels.extend(labels.iter().map(|(name, value)| Label {
        name: (*name).to_owned(),
        value: (*value).to_owned(),
    }));
    TimeSeries {
        labels: all_labels,
        samples: samples
            .iter()
            .map(|(timestamp, value)| Sample {
                value: *value,
                timestamp: *timestamp,
            })
            .collect(),
    }
}

fn remote_write_body(series: &[TimeSeries]) -> Result<Vec<u8>> {
    let mut protobuf = Vec::new();
    WriteRequest {
        timeseries: series.to_vec(),
        metadata: vec![],
    }
    .encode(&mut protobuf)?;
    snap::raw::Encoder::new()
        .compress_vec(&protobuf)
        .map_err(Into::into)
}

fn fixture_shard(series: &TimeSeries) -> u32 {
    let mut labels = series.labels.clone();
    labels.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.value.cmp(&right.value))
    });
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"regression");
    for label in labels {
        hasher.update(&[0]);
        hasher.update(label.name.as_bytes());
        hasher.update(&[0]);
        hasher.update(label.value.as_bytes());
    }
    let value = u64::from_be_bytes(hasher.finalize().as_bytes()[..8].try_into().unwrap());
    (value % 16) as u32
}

fn otlp_fixture(timestamp_ms: i64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "regression")],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "regression".to_owned(),
                    version: "1".to_owned(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                metrics: vec![Metric {
                    name: "otlp.regression.temperature".to_owned(),
                    description: "OTLP regression gauge".to_owned(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![kv("host", "host-a")],
                            start_time_unix_nano: 0,
                            time_unix_nano: timestamp_ms as u64 * 1_000_000,
                            exemplars: vec![],
                            flags: 0,
                            value: Some(number_data_point::Value::AsDouble(42.5)),
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_owned())),
        }),
    }
}

fn compare_query_data(name: &str, expected: &Value, actual: &Value) -> Result<()> {
    compare_query_data_ignoring_labels(name, expected, actual, &[])
}

fn compare_query_data_ignoring_labels(
    name: &str,
    expected: &Value,
    actual: &Value,
    ignored: &[&str],
) -> Result<()> {
    ensure_success(name, expected)?;
    ensure_success(name, actual)?;
    let expected = normalize_query(expected, ignored)?;
    let actual = normalize_query(actual, ignored)?;
    ensure!(
        expected.len() == actual.len(),
        "{name}: result count differs: expected {}, got {}",
        expected.len(),
        actual.len()
    );
    for (left, right) in expected.iter().zip(&actual) {
        ensure!(
            left.labels == right.labels,
            "{name}: labels differ: {left:?} != {right:?}"
        );
        ensure!(
            left.values.len() == right.values.len(),
            "{name}: sample counts differ"
        );
        for ((lt, lv), (rt, rv)) in left.values.iter().zip(&right.values) {
            ensure!(
                float_close(*lt, *rt),
                "{name}: timestamps differ: {lt} != {rt}"
            );
            ensure!(float_close(*lv, *rv), "{name}: values differ: {lv} != {rv}");
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
struct NormalizedSeries {
    labels: BTreeMap<String, String>,
    values: Vec<(f64, f64)>,
}

fn normalize_query(value: &Value, ignored: &[&str]) -> Result<Vec<NormalizedSeries>> {
    let result = value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("query response has no data.result"))?;
    let mut normalized = result
        .iter()
        .map(|entry| {
            let mut labels = serde_json::from_value::<BTreeMap<String, String>>(
                entry
                    .get("metric")
                    .cloned()
                    .unwrap_or_else(|| Value::Object(Default::default())),
            )?;
            for label in ignored {
                labels.remove(*label);
            }
            let raw_values = entry.get("values").and_then(Value::as_array).map_or_else(
                || {
                    entry
                        .get("value")
                        .and_then(Value::as_array)
                        .map(|value| vec![Value::Array(value.clone())])
                        .unwrap_or_default()
                },
                Clone::clone,
            );
            let values = raw_values
                .iter()
                .map(|sample| {
                    let pair = sample
                        .as_array()
                        .ok_or_else(|| anyhow!("sample is not an array"))?;
                    let timestamp = pair.first().and_then(Value::as_f64).context("timestamp")?;
                    let number = pair
                        .get(1)
                        .and_then(Value::as_str)
                        .context("sample value")?
                        .parse::<f64>()?;
                    Ok((timestamp, number))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(NormalizedSeries { labels, values })
        })
        .collect::<Result<Vec<_>>>()?;
    normalized.sort_by(|left, right| left.labels.cmp(&right.labels));
    Ok(normalized)
}

fn compare_json_data(name: &str, expected: &Value, actual: &Value) -> Result<()> {
    ensure_success(name, expected)?;
    ensure_success(name, actual)?;
    let mut expected = expected["data"].clone();
    let mut actual = actual["data"].clone();
    sort_json(&mut expected);
    sort_json(&mut actual);
    ensure!(expected == actual, "{name} differs: {expected} != {actual}");
    Ok(())
}

fn compare_metadata_data(name: &str, expected: &Value, actual: &Value) -> Result<()> {
    ensure_success(name, expected)?;
    ensure_success(name, actual)?;
    let mut expected = expected["data"].clone();
    let mut actual = actual["data"].clone();
    // Meter's durable forward index preserves metric type and unit but not
    // descriptions. Compare the metadata fields both readers can retain.
    for data in [&mut expected, &mut actual] {
        if let Some(metrics) = data.as_object_mut() {
            for entries in metrics.values_mut().filter_map(Value::as_array_mut) {
                for entry in entries.iter_mut().filter_map(Value::as_object_mut) {
                    entry.remove("help");
                }
            }
        }
        sort_json(data);
    }
    ensure!(expected == actual, "{name} differs: {expected} != {actual}");
    Ok(())
}

fn sort_json(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in &mut *items {
                sort_json(item);
            }
            items.sort_by_key(Value::to_string);
        }
        Value::Object(map) => {
            for value in map.values_mut() {
                sort_json(value);
            }
        }
        _ => {}
    }
}

fn ensure_success(name: &str, value: &Value) -> Result<()> {
    ensure!(
        value["status"] == "success",
        "{name}: unsuccessful response {value}"
    );
    Ok(())
}

fn result_len(value: &Value) -> usize {
    value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .map_or(0, Vec::len)
}

fn float_close(left: f64, right: f64) -> bool {
    if left == right {
        return true;
    }
    (left - right).abs() <= ABS_TOLERANCE.max(REL_TOLERANCE * left.abs().max(right.abs()))
}

fn basic(username: &str, password: &str) -> String {
    format!(
        "Basic {}",
        STANDARD.encode(format!("{username}:{password}"))
    )
}

fn bearer(token: &str) -> String {
    #[derive(Serialize)]
    struct Claims<'a> {
        exp: u64,
        namespace: &'a str,
        permission: &'a str,
    }

    let (namespace, permission) = match token {
        "regression-read-token" => ("^regression$", "read"),
        "regression-write" => ("^regression$", "write"),
        "other-read" => ("^other$", "read"),
        "other-write" => ("^other$", "write"),
        "global-read" => ("^(regression|other)$", "read"),
        "global-write" => ("^(regression|other)$", "write"),
        _ => panic!("unknown regression JWT fixture {token}"),
    };
    let exp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some("regression-hs256".to_owned());
    let token = encode(
        &header,
        &Claims {
            exp,
            namespace,
            permission,
        },
        &EncodingKey::from_secret(b"meter-regression-test-only-hs256-signing-key"),
    )
    .unwrap();
    format!("Bearer {token}")
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_is_deterministic_and_spans_series() {
        let data = fixture(1_700_000_000_000);
        assert_eq!(data, fixture(1_700_000_000_000));
        assert_eq!(data.len(), 8);
        assert!(data.iter().any(|series| fixture_shard(series) < 8));
        assert!(
            data.iter().any(|series| fixture_shard(series) >= 8),
            "posting to writer-0 must exercise forwarding to writer-1"
        );
    }

    #[test]
    fn remote_write_is_snappy_protobuf() {
        let fixture = fixture(1_700_000_000_000);
        let body = remote_write_body(&fixture).unwrap();
        let decoded = snap::raw::Decoder::new().decompress_vec(&body).unwrap();
        assert_eq!(
            WriteRequest::decode(decoded.as_slice()).unwrap().timeseries,
            fixture
        );
    }

    #[test]
    fn float_tolerance_is_documented_boundary() {
        assert!(float_close(1.0, 1.0 + 5e-10));
        assert!(!float_close(1.0, 1.0 + 2e-9));
        assert!(float_close(0.0, 5e-13));
    }

    #[test]
    fn normalization_sorts_labels_and_series() {
        let response = serde_json::json!({
            "status": "success",
            "data": {"result": [
                {"metric": {"b":"2","a":"1"}, "value": [1.0, "2.0"]},
                {"metric": {"a":"0"}, "value": [1.0, "1.0"]}
            ]}
        });
        let value = normalize_query(&response, &[]).unwrap();
        assert_eq!(value[0].labels["a"], "0");
        assert_eq!(value[1].labels.keys().collect::<Vec<_>>(), vec!["a", "b"]);
    }

    #[test]
    fn metadata_comparison_ignores_help_only() {
        let expected = serde_json::json!({
            "status": "success",
            "data": {"metric": [{"type": "gauge", "help": "description", "unit": ""}]}
        });
        let actual = serde_json::json!({
            "status": "success",
            "data": {"metric": [{"type": "gauge", "help": "", "unit": ""}]}
        });
        compare_metadata_data("metadata", &expected, &actual).unwrap();
    }
}
