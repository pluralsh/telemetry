//! OpenTelemetry metrics to Series conversion.
//!
//! This module provides [`OtelConverter`], which converts an OTLP
//! `ExportMetricsServiceRequest` into `Vec<Series>` suitable for
//! `TimeSeriesDb::write()`.

use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{KeyValue, any_value},
    metrics::v1::{AggregationTemporality, metric, number_data_point},
};

use crate::error::Error;
use crate::model::{Label, MetricType, Sample, Series, Temporality};

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
            samples: vec![Sample::new(timestamp_ms, value)],
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

    /// Convert an OTLP ExponentialHistogram to classic Prometheus `_bucket`/`_sum`/`_count` series.
    ///
    /// Only positive buckets and `zero_count` are converted to `le`-style buckets. Negative
    /// buckets (representing negative measurement values) cannot be expressed as classic
    /// Prometheus buckets; the OTLP spec maps them to Prometheus Native Histograms instead.
    /// Negative observations are still reflected in `_count` and the `+Inf` bucket.
    fn convert_exp_histogram(
        &self,
        name: &str,
        data_points: &[opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint],
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

            // Convert exponential buckets to classic Prometheus le-style buckets.
            //
            // Negative buckets (for negative measurement values) are not converted.
            // The OTLP spec maps ExponentialHistograms to Prometheus Native Histograms
            // which can represent negative ranges natively; classic le-buckets cannot.
            // Negative bucket counts are still included in _count and +Inf.
            let base = 2_f64.powf(2_f64.powi(-dp.scale));

            let mut explicit_bounds = Vec::new();
            let mut cumulative_counts = Vec::new();

            // Start cumulative from zero_count so that the first positive bucket
            // includes observations in the zero range.
            let mut cumulative: u64 = dp.zero_count;

            // Positive buckets.
            if let Some(ref positive) = dp.positive {
                let offset = positive.offset;
                for (i, &count) in positive.bucket_counts.iter().enumerate() {
                    let boundary = base.powf((offset + i as i32 + 1) as f64);
                    cumulative += count;
                    explicit_bounds.push(boundary);
                    cumulative_counts.push(cumulative);
                }
            }

            // Emit bucket series.
            for (bound, cum_count) in explicit_bounds.iter().zip(cumulative_counts.iter()) {
                let le_label = [Label::new("le", format_float(*bound))];
                ctx.push(
                    &bucket_name,
                    metric_type,
                    &point_labels,
                    &le_label,
                    timestamp_ms,
                    *cum_count as f64,
                );
            }

            // +Inf bucket.
            let inf_label = [Label::new("le", "+Inf")];
            ctx.push(
                &bucket_name,
                metric_type,
                &point_labels,
                &inf_label,
                timestamp_ms,
                dp.count as f64,
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
