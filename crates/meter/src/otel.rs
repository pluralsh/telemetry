//! OpenTelemetry metrics to Series conversion.
//!
//! This module provides [`OtelConverter`], which converts an OTLP
//! `ExportMetricsServiceRequest` into `Vec<Series>` suitable for
//! `TimeSeriesDb::write()`.

use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{KeyValue, any_value},
    metrics::v1::{
        AggregationTemporality, DataPointFlags, exponential_histogram_data_point, metric,
        number_data_point,
    },
};

use crate::error::Error;
use crate::histogram::{
    Bucket, CounterResetHint, FloatHistogram, MAX_EXPONENTIAL_SCHEMA, MIN_EXPONENTIAL_SCHEMA,
};
use crate::model::{HistogramSample, Label, MetricType, STALE_NAN, Sample, Series, Temporality};

/// Zero threshold the upstream OTLP translator assigns to exponential
/// histograms; OTLP's default of 0 would otherwise render a degenerate zero
/// bucket that disagrees with Prometheus.
const DEFAULT_OTLP_ZERO_THRESHOLD: f64 = 1e-128;

/// Configuration for [`OtelConverter`].
#[derive(Debug, Clone)]
pub struct OtelConfig {
    /// Include resource attributes as labels on every series. Default: `true`.
    pub include_resource_attrs: bool,
    /// Include scope attributes as labels on every series. Default: `true`.
    pub include_scope_attrs: bool,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            include_resource_attrs: true,
            include_scope_attrs: true,
        }
    }
}

/// Converts OTLP `ExportMetricsServiceRequest` into `Vec<Series>`.
///
/// The converter walks the OTLP hierarchy (ResourceMetrics → ScopeMetrics →
/// Metric → data points) and decomposes each OTEL metric type into
/// Prometheus-compatible series following the
/// [OTLP Prometheus compatibility spec](https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/).
pub struct OtelConverter {
    config: OtelConfig,
}

impl OtelConverter {
    /// Creates a new converter with the given configuration.
    pub fn new(config: OtelConfig) -> Self {
        Self { config }
    }

    /// Decompose an OTLP export request into Series.
    pub fn convert(&self, request: &ExportMetricsServiceRequest) -> Result<Vec<Series>, Error> {
        let mut result = Vec::new();
        for rm in &request.resource_metrics {
            self.convert_resource_metrics(rm, &mut result);
        }
        Ok(result)
    }

    /// Convert a single `ResourceMetrics` — extracts resource-level labels,
    /// then delegates each `ScopeMetrics` entry.
    fn convert_resource_metrics(
        &self,
        rm: &opentelemetry_proto::tonic::metrics::v1::ResourceMetrics,
        result: &mut Vec<Series>,
    ) {
        let resource_labels = if self.config.include_resource_attrs {
            rm.resource
                .as_ref()
                .map(|r| collect_labels(&r.attributes))
                .unwrap_or_default()
        } else {
            vec![]
        };

        for sm in &rm.scope_metrics {
            self.convert_scope_metrics(sm, &resource_labels, result);
        }
    }

    /// Convert a single `ScopeMetrics` — builds the base label set
    /// (resource + scope), then dispatches each metric by type.
    fn convert_scope_metrics(
        &self,
        sm: &opentelemetry_proto::tonic::metrics::v1::ScopeMetrics,
        resource_labels: &[Label],
        result: &mut Vec<Series>,
    ) {
        let mut base_labels = resource_labels.to_vec();

        if let Some(s) = sm.scope.as_ref() {
            if !s.name.is_empty() {
                base_labels.push(Label::new("otel_scope_name", &s.name));
            }
            if !s.version.is_empty() {
                base_labels.push(Label::new("otel_scope_version", &s.version));
            }
            if self.config.include_scope_attrs {
                base_labels.extend(collect_labels(&s.attributes));
            }
        }

        for metric in &sm.metrics {
            self.convert_metric(metric, &base_labels, result);
        }
    }

    /// Dispatch a single `Metric` to the appropriate type-specific converter.
    fn convert_metric(
        &self,
        metric: &opentelemetry_proto::tonic::metrics::v1::Metric,
        base_labels: &[Label],
        result: &mut Vec<Series>,
    ) {
        let name = &metric.name;
        let mut ctx = SeriesCollector {
            unit: &metric.unit,
            description: &metric.description,
            base_labels,
            result,
        };

        match &metric.data {
            Some(metric::Data::Gauge(g)) => {
                self.convert_gauge(name, &g.data_points, &mut ctx);
            }
            Some(metric::Data::Sum(s)) => {
                self.convert_sum(
                    name,
                    &s.data_points,
                    s.aggregation_temporality,
                    s.is_monotonic,
                    &mut ctx,
                );
            }
            Some(metric::Data::Histogram(h)) => {
                self.convert_histogram(name, &h.data_points, h.aggregation_temporality, &mut ctx);
            }
            Some(metric::Data::ExponentialHistogram(eh)) => {
                self.convert_exp_histogram(
                    name,
                    &eh.data_points,
                    eh.aggregation_temporality,
                    &mut ctx,
                );
            }
            Some(metric::Data::Summary(s)) => {
                self.convert_summary(name, &s.data_points, &mut ctx);
            }
            None => {}
        }
    }
}

/// Accumulates `Series` from a single OTLP metric, carrying the fields that are
/// constant across all data points (unit, description, base labels).
struct SeriesCollector<'a> {
    unit: &'a str,
    description: &'a str,
    base_labels: &'a [Label],
    result: &'a mut Vec<Series>,
}

impl SeriesCollector<'_> {
    fn push(
        &mut self,
        name: &str,
        metric_type: MetricType,
        point_labels: &[Label],
        extra_labels: &[Label],
        timestamp_ms: i64,
        value: f64,
    ) {
        self.push_series(
            name,
            metric_type,
            point_labels,
            extra_labels,
            vec![Sample::new(timestamp_ms, value)],
            Vec::new(),
        );
    }

    fn push_histogram(
        &mut self,
        name: &str,
        metric_type: MetricType,
        point_labels: &[Label],
        sample: HistogramSample,
    ) {
        self.push_series(
            name,
            metric_type,
            point_labels,
            &[],
            Vec::new(),
            vec![sample],
        );
    }

    fn push_series(
        &mut self,
        name: &str,
        metric_type: MetricType,
        point_labels: &[Label],
        extra_labels: &[Label],
        samples: Vec<Sample>,
        histograms: Vec<HistogramSample>,
    ) {
        let mut labels = Vec::with_capacity(
            1 + self.base_labels.len() + point_labels.len() + extra_labels.len(),
        );
        labels.push(Label::metric_name(name));
        labels.extend_from_slice(self.base_labels);
        labels.extend_from_slice(point_labels);
        labels.extend_from_slice(extra_labels);

        self.result.push(Series {
            labels,
            metric_type: Some(metric_type),
            unit: if self.unit.is_empty() {
                None
            } else {
                Some(self.unit.to_string())
            },
            description: if self.description.is_empty() {
                None
            } else {
                Some(self.description.to_string())
            },
            samples,
            histograms,
        });
    }
}

fn sanitize_name(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '.' | '-' => out.push('_'),
            _ if ch.is_ascii_alphanumeric() || ch == '_' => out.push(ch),
            _ => {} // strip
        }
    }
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

fn normalize_unit(unit: &str) -> Option<String> {
    if unit.is_empty() || unit == "1" {
        return None;
    }
    if unit.starts_with('{') && unit.ends_with('}') {
        return None;
    }
    let suffix = match unit {
        "s" => "seconds".to_string(),
        "ms" => "milliseconds".to_string(),
        "us" => "microseconds".to_string(),
        "ns" => "nanoseconds".to_string(),
        "By" => "bytes".to_string(),
        "KBy" => "kilobytes".to_string(),
        "MBy" => "megabytes".to_string(),
        "GBy" => "gigabytes".to_string(),
        "TBy" => "terabytes".to_string(),
        other => sanitize_name(other),
    };
    Some(suffix)
}

fn build_metric_name(name: &str, unit: &str, is_monotonic_counter: bool) -> String {
    let mut result = sanitize_name(name);

    if let Some(suffix) = normalize_unit(unit)
        && !result.ends_with(&format!("_{}", suffix))
    {
        result.push('_');
        result.push_str(&suffix);
    }

    if is_monotonic_counter && !result.ends_with("_total") {
        result.push_str("_total");
    }

    result
}

fn format_float(v: f64) -> String {
    if v.fract() == 0.0 && v.is_finite() {
        // Use i64 formatting when the value fits, otherwise fall back to f64
        // which avoids silent saturation for values beyond i64 range.
        let i = v as i64;
        if i as f64 == v {
            return format!("{i}");
        }
    }
    format!("{v}")
}

fn kv_to_label(kv: &KeyValue) -> Option<Label> {
    let key = sanitize_name(&kv.key);
    let value = kv.value.as_ref().and_then(|v| v.value.as_ref())?;
    let string_val = match value {
        any_value::Value::StringValue(s) => s.clone(),
        any_value::Value::IntValue(i) => i.to_string(),
        any_value::Value::DoubleValue(d) => d.to_string(),
        any_value::Value::BoolValue(b) => b.to_string(),
        _ => return None,
    };
    Some(Label::new(key, string_val))
}

fn collect_labels(kvs: &[KeyValue]) -> Vec<Label> {
    kvs.iter().filter_map(kv_to_label).collect()
}

fn to_temporality(t: i32) -> Temporality {
    if t == AggregationTemporality::Cumulative as i32 {
        Temporality::Cumulative
    } else if t == AggregationTemporality::Delta as i32 {
        Temporality::Delta
    } else {
        Temporality::Unspecified
    }
}

// Per-type conversion methods.
impl OtelConverter {
    fn convert_gauge(
        &self,
        name: &str,
        data_points: &[opentelemetry_proto::tonic::metrics::v1::NumberDataPoint],
        ctx: &mut SeriesCollector<'_>,
    ) {
        let metric_name = build_metric_name(name, ctx.unit, false);
        for dp in data_points {
            let value = match dp.value {
                Some(number_data_point::Value::AsDouble(v)) => v,
                Some(number_data_point::Value::AsInt(v)) => v as f64,
                None => continue,
            };
            let timestamp_ms = (dp.time_unix_nano / 1_000_000) as i64;
            let point_labels = collect_labels(&dp.attributes);
            ctx.push(
                &metric_name,
                MetricType::Gauge,
                &point_labels,
                &[],
                timestamp_ms,
                value,
            );
        }
    }

    fn convert_sum(
        &self,
        name: &str,
        data_points: &[opentelemetry_proto::tonic::metrics::v1::NumberDataPoint],
        temporality: i32,
        is_monotonic: bool,
        ctx: &mut SeriesCollector<'_>,
    ) {
        let temp = to_temporality(temporality);

        if temp == Temporality::Delta {
            tracing::warn!(metric = name, "dropping delta temporality sum");
            return;
        }

        let (metric_type, is_counter) = if is_monotonic {
            (
                MetricType::Sum {
                    monotonic: true,
                    temporality: temp,
                },
                true,
            )
        } else {
            (MetricType::Gauge, false)
        };

        let metric_name = build_metric_name(name, ctx.unit, is_counter);

        for dp in data_points {
            let value = match dp.value {
                Some(number_data_point::Value::AsDouble(v)) => v,
                Some(number_data_point::Value::AsInt(v)) => v as f64,
                None => continue,
            };
            let timestamp_ms = (dp.time_unix_nano / 1_000_000) as i64;
            let point_labels = collect_labels(&dp.attributes);
            ctx.push(
                &metric_name,
                metric_type,
                &point_labels,
                &[],
                timestamp_ms,
                value,
            );
        }
    }

    fn convert_histogram(
        &self,
        name: &str,
        data_points: &[opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint],
        temporality: i32,
        ctx: &mut SeriesCollector<'_>,
    ) {
        let temp = to_temporality(temporality);
        let metric_type = MetricType::Histogram { temporality: temp };
        let base_name = build_metric_name(name, ctx.unit, false);
        let bucket_name = format!("{}_bucket", base_name);
        let sum_name = format!("{}_sum", base_name);
        let count_name = format!("{}_count", base_name);

        for dp in data_points {
            let timestamp_ms = (dp.time_unix_nano / 1_000_000) as i64;
            let point_labels = collect_labels(&dp.attributes);

            // Bucket series — cumulative counts
            let mut cumulative: u64 = 0;
            for (i, bound) in dp.explicit_bounds.iter().enumerate() {
                cumulative += dp.bucket_counts.get(i).copied().unwrap_or(0);
                let le_label = [Label::new("le", format_float(*bound))];
                ctx.push(
                    &bucket_name,
                    metric_type,
                    &point_labels,
                    &le_label,
                    timestamp_ms,
                    cumulative as f64,
                );
            }

            // +Inf bucket
            cumulative += dp
                .bucket_counts
                .get(dp.explicit_bounds.len())
                .copied()
                .unwrap_or(0);
            let inf_label = [Label::new("le", "+Inf")];
            ctx.push(
                &bucket_name,
                metric_type,
                &point_labels,
                &inf_label,
                timestamp_ms,
                cumulative as f64,
            );

            // _sum
            if let Some(sum) = dp.sum {
                ctx.push(
                    &sum_name,
                    metric_type,
                    &point_labels,
                    &[],
                    timestamp_ms,
                    sum,
                );
            }

            // _count
            ctx.push(
                &count_name,
                metric_type,
                &point_labels,
                &[],
                timestamp_ms,
                dp.count as f64,
            );
        }
    }

    /// Convert an OTLP ExponentialHistogram to a Prometheus native histogram
    /// series, following the upstream OTLP translator.
    ///
    /// OTLP scale maps to the native schema; scales above
    /// [`MAX_EXPONENTIAL_SCHEMA`] are downscaled by merging buckets, and
    /// scales below [`MIN_EXPONENTIAL_SCHEMA`] cannot be represented and are
    /// dropped. OTLP bucket `k` covers `(base^k, base^(k+1)]` while native
    /// bucket `k` covers `(base^(k-1), base^k]`, hence the `+ 1` on indexes.
    /// Delta temporality points become gauge histograms.
    fn convert_exp_histogram(
        &self,
        name: &str,
        data_points: &[opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint],
        temporality: i32,
        ctx: &mut SeriesCollector<'_>,
    ) {
        let temp = to_temporality(temporality);
        let metric_type = MetricType::ExponentialHistogram { temporality: temp };
        let metric_name = build_metric_name(name, ctx.unit, false);

        for dp in data_points {
            let timestamp_ms = (dp.time_unix_nano / 1_000_000) as i64;
            let point_labels = collect_labels(&dp.attributes);
            if dp.flags & DataPointFlags::NoRecordedValueMask as u32 != 0 {
                ctx.push(
                    &metric_name,
                    metric_type,
                    &point_labels,
                    &[],
                    timestamp_ms,
                    f64::from_bits(STALE_NAN),
                );
                continue;
            }
            if dp.scale < MIN_EXPONENTIAL_SCHEMA {
                tracing::warn!(
                    metric = name,
                    scale = dp.scale,
                    "dropping exponential histogram point with unsupported scale"
                );
                continue;
            }
            let scale_down = (dp.scale - MAX_EXPONENTIAL_SCHEMA).max(0) as u32;
            let buckets = |side: &Option<exponential_histogram_data_point::Buckets>| {
                let mut out: Vec<Bucket> = Vec::new();
                let Some(side) = side else {
                    return out;
                };
                for (i, &count) in side.bucket_counts.iter().enumerate() {
                    if count == 0 {
                        continue;
                    }
                    let index = ((side.offset + i as i32) >> scale_down) + 1;
                    match out.last_mut() {
                        Some(last) if last.index == index => last.count += count as f64,
                        _ => out.push(Bucket {
                            index,
                            count: count as f64,
                        }),
                    }
                }
                out
            };
            let histogram = FloatHistogram {
                counter_reset_hint: if temp == Temporality::Delta {
                    CounterResetHint::Gauge
                } else {
                    CounterResetHint::Unknown
                },
                schema: dp.scale - scale_down as i32,
                zero_threshold: if dp.zero_threshold > 0.0 {
                    dp.zero_threshold
                } else {
                    DEFAULT_OTLP_ZERO_THRESHOLD
                },
                zero_count: dp.zero_count as f64,
                count: dp.count as f64,
                sum: dp.sum.unwrap_or(0.0),
                positive: buckets(&dp.positive),
                negative: buckets(&dp.negative),
                custom_values: Default::default(),
            };
            ctx.push_histogram(
                &metric_name,
                metric_type,
                &point_labels,
                HistogramSample::new(timestamp_ms, histogram),
            );
        }
    }

    fn convert_summary(
        &self,
        name: &str,
        data_points: &[opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint],
        ctx: &mut SeriesCollector<'_>,
    ) {
        let base_name = build_metric_name(name, ctx.unit, false);
        let sum_name = format!("{}_sum", base_name);
        let count_name = format!("{}_count", base_name);

        for dp in data_points {
            let timestamp_ms = (dp.time_unix_nano / 1_000_000) as i64;
            let point_labels = collect_labels(&dp.attributes);

            // Per-quantile series
            for q in &dp.quantile_values {
                let q_label = [Label::new("quantile", format_float(q.quantile))];
                ctx.push(
                    &base_name,
                    MetricType::Summary,
                    &point_labels,
                    &q_label,
                    timestamp_ms,
                    q.value,
                );
            }

            // _sum
            ctx.push(
                &sum_name,
                MetricType::Summary,
                &point_labels,
                &[],
                timestamp_ms,
                dp.sum,
            );

            // _count
            ctx.push(
                &count_name,
                MetricType::Summary,
                &point_labels,
                &[],
                timestamp_ms,
                dp.count as f64,
            );
        }
    }
}

#[cfg(test)]
mod tests;
