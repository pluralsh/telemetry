use serde::de::{self, Visitor};
use serde::ser::{SerializeSeq, SerializeStruct, SerializeTuple};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::error::QueryError;
use crate::histogram::FloatHistogram;
use crate::model::{self, InstantSample, Labels, QueryValue, RangeSample};

/// Convert a `QueryError` into a Prometheus-style `ErrorResponse`.
pub(crate) fn query_error_response(err: QueryError) -> ErrorResponse {
    match err {
        QueryError::InvalidQuery(msg) => ErrorResponse::bad_data(msg),
        QueryError::Execution(msg) => ErrorResponse::execution(msg),
        QueryError::Timeout => ErrorResponse::timeout("query timed out"),
        QueryError::Storage(msg) => ErrorResponse::internal(msg),
    }
}

/// Convert an instant query result into a Prometheus `QueryResponse`.
pub fn query_value_to_response(result: Result<QueryValue, QueryError>) -> QueryResponse {
    match result {
        Ok(QueryValue::Scalar {
            timestamp_ms,
            value,
        }) => QueryResponse {
            status: "success".to_string(),
            data: Some(QueryResult {
                result_type: "scalar".to_string(),
                result: QueryResultValue::Scalar(timestamp_ms, value),
            }),
            error: None,
            error_type: None,
            trace: None,
        },
        Ok(QueryValue::Vector(samples)) => {
            let result: Vec<VectorSeries> = samples.into_iter().map(VectorSeries).collect();

            QueryResponse {
                status: "success".to_string(),
                data: Some(QueryResult {
                    result_type: "vector".to_string(),
                    result: QueryResultValue::Vector(result),
                }),
                error: None,
                error_type: None,
                trace: None,
            }
        }
        Ok(QueryValue::Matrix(range_samples)) => {
            let result: Vec<MatrixSeries> = range_samples.into_iter().map(MatrixSeries).collect();

            QueryResponse {
                status: "success".to_string(),
                data: Some(QueryResult {
                    result_type: "matrix".to_string(),
                    result: QueryResultValue::Matrix(result),
                }),
                error: None,
                error_type: None,
                trace: None,
            }
        }
        Err(e) => {
            let err = query_error_response(e);
            QueryResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
                trace: None,
            }
        }
    }
}

/// Convert a range query result into a Prometheus `QueryRangeResponse`.
pub fn range_result_to_response(
    result: Result<Vec<RangeSample>, QueryError>,
) -> QueryRangeResponse {
    match result {
        Ok(range_samples) => {
            let result: Vec<MatrixSeries> = range_samples.into_iter().map(MatrixSeries).collect();

            QueryRangeResponse {
                status: "success".to_string(),
                data: Some(QueryRangeResult {
                    result_type: "matrix".to_string(),
                    result,
                }),
                error: None,
                error_type: None,
                trace: None,
            }
        }
        Err(e) => {
            let err = query_error_response(e);
            QueryRangeResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
                trace: None,
            }
        }
    }
}

/// Convert a series listing result into a Prometheus `SeriesResponse`.
pub(crate) fn series_to_response(
    result: Result<Vec<crate::model::Labels>, QueryError>,
    limit: Option<usize>,
) -> SeriesResponse {
    match result {
        Ok(mut data) => {
            // Sort for consistent output — Labels is Ord (sorted by name, then value)
            data.sort();

            if let Some(limit) = limit {
                data.truncate(limit);
            }

            SeriesResponse {
                status: "success".to_string(),
                data: Some(data),
                error: None,
                error_type: None,
            }
        }
        Err(e) => {
            let err = query_error_response(e);
            SeriesResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
            }
        }
    }
}

/// Convert a labels result into a Prometheus `LabelsResponse`.
pub(crate) fn labels_to_response(
    result: Result<Vec<String>, QueryError>,
    limit: Option<usize>,
) -> LabelsResponse {
    match result {
        Ok(mut data) => {
            if let Some(limit) = limit {
                data.truncate(limit);
            }
            LabelsResponse {
                status: "success".to_string(),
                data: Some(data),
                error: None,
                error_type: None,
            }
        }
        Err(e) => {
            let err = query_error_response(e);
            LabelsResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
            }
        }
    }
}

/// Convert a label values result into a Prometheus `LabelValuesResponse`.
pub(crate) fn label_values_to_response(
    result: Result<Vec<String>, QueryError>,
    limit: Option<usize>,
) -> LabelValuesResponse {
    match result {
        Ok(mut data) => {
            if let Some(limit) = limit {
                data.truncate(limit);
            }
            LabelValuesResponse {
                status: "success".to_string(),
                data: Some(data),
                error: None,
                error_type: None,
            }
        }
        Err(e) => {
            let err = query_error_response(e);
            LabelValuesResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
            }
        }
    }
}

/// Convert a metadata result into a Prometheus `MetadataResponse`.
pub(crate) fn metadata_to_response(
    result: Result<Vec<model::MetricMetadata>, QueryError>,
    limit: Option<usize>,
    limit_per_metric: Option<usize>,
) -> MetadataResponse {
    match result {
        Ok(entries) => {
            let mut data: HashMap<String, Vec<WireMetricMetadata>> = HashMap::new();
            for m in entries {
                let name = m.metric_name.clone();
                data.entry(name).or_default().push(WireMetricMetadata(m));
            }

            if let Some(limit) = limit {
                data = data.into_iter().take(limit).collect();
            }

            if let Some(limit_per_metric) = limit_per_metric {
                for entries in data.values_mut() {
                    entries.truncate(limit_per_metric);
                }
            }

            MetadataResponse {
                status: "success".to_string(),
                data: Some(data),
                error: None,
                error_type: None,
            }
        }
        Err(e) => {
            let err = query_error_response(e);
            MetadataResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Prometheus wire-format serialization helpers
// ---------------------------------------------------------------------------

/// Serializes an `(i64, f64)` sample as the Prometheus JSON tuple
/// `[timestamp_secs, "value_string"]`.
struct PromSample(i64, f64);

impl Serialize for PromSample {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tup = serializer.serialize_tuple(2)?;
        tup.serialize_element(&(self.0 as f64 / 1000.0))?;
        tup.serialize_element(&PromFloat(self.1))?;
        tup.end()
    }
}

/// A sample value serialized as its Prometheus JSON string
/// ([`common::display::prometheus_json_float`]) without a heap allocation:
/// range results carry one per point.
struct PromFloat(f64);

impl Serialize for PromFloat {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(common::display::FloatText::prometheus_json(self.0).as_str())
    }
}

fn parse_float(s: &str) -> Result<f64, std::num::ParseFloatError> {
    match s {
        "+Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        _ => s.parse(),
    }
}

/// Serializes a native histogram sample as Prometheus'
/// `[timestamp_secs, {"count", "sum", "buckets": [[rule, lower, upper, count]]}]`.
/// Bucket rule: 0 = `(lower, upper]`, 1 = `[lower, upper)`, 2 = open,
/// 3 = closed. Empty buckets are omitted.
struct PromHistogramSample<'a>(i64, &'a FloatHistogram);

impl Serialize for PromHistogramSample<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tup = serializer.serialize_tuple(2)?;
        tup.serialize_element(&(self.0 as f64 / 1000.0))?;
        tup.serialize_element(&PromHistogram(self.1))?;
        tup.end()
    }
}

struct PromHistogram<'a>(&'a FloatHistogram);

impl Serialize for PromHistogram<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let h = self.0;
        let buckets: Vec<_> = h
            .all_buckets()
            .into_iter()
            .filter(|b| b.count != 0.0)
            .collect();
        let mut s = serializer.serialize_struct("Histogram", 3)?;
        s.serialize_field("count", &PromFloat(h.count))?;
        s.serialize_field("sum", &PromFloat(h.sum))?;
        if !buckets.is_empty() {
            let custom = h.uses_custom_buckets();
            let wire: Vec<WireBucket> = buckets
                .iter()
                .map(|b| {
                    let (lower_inclusive, upper_inclusive) = if custom {
                        (b.lower == f64::NEG_INFINITY, true)
                    } else {
                        (b.lower <= 0.0, b.upper >= 0.0)
                    };
                    let rule = match (lower_inclusive, upper_inclusive) {
                        (false, true) => 0,
                        (true, false) => 1,
                        (false, false) => 2,
                        (true, true) => 3,
                    };
                    WireBucket(rule, b.lower, b.upper, b.count)
                })
                .collect();
            s.serialize_field("buckets", &wire)?;
        }
        s.end()
    }
}

struct WireBucket(u8, f64, f64, f64);

impl Serialize for WireBucket {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tup = serializer.serialize_tuple(4)?;
        tup.serialize_element(&self.0)?;
        tup.serialize_element(&PromFloat(self.1))?;
        tup.serialize_element(&PromFloat(self.2))?;
        tup.serialize_element(&PromFloat(self.3))?;
        tup.end()
    }
}

struct PromHistogramSamples<'a>(&'a [(i64, Arc<FloatHistogram>)]);

impl Serialize for PromHistogramSamples<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for (ts_ms, h) in self.0 {
            seq.serialize_element(&PromHistogramSample(*ts_ms, h))?;
        }
        seq.end()
    }
}

/// Wire shape of a histogram sample, for deserializing responses. Buckets
/// are not reconstructed: the round trip keeps only `count` and `sum`.
#[derive(Deserialize)]
struct WireHistogram {
    count: String,
    sum: String,
}

impl WireHistogram {
    fn into_histogram<E: de::Error>(self) -> Result<FloatHistogram, E> {
        Ok(FloatHistogram {
            count: parse_float(&self.count).map_err(E::custom)?,
            sum: parse_float(&self.sum).map_err(E::custom)?,
            ..FloatHistogram::default()
        })
    }
}

/// Thin wrapper to serialize `&[(i64, f64)]` as a JSON array of `PromSample`
/// without collecting into an intermediate `Vec`.
struct PromSamples<'a>(&'a [(i64, f64)]);

impl Serialize for PromSamples<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for &(ts_ms, value) in self.0 {
            seq.serialize_element(&PromSample(ts_ms, value))?;
        }
        seq.end()
    }
}

// ---------------------------------------------------------------------------
// Error response
// ---------------------------------------------------------------------------

/// Error response matching Prometheus API format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub status: String, // "error"
    #[serde(rename = "errorType")]
    pub error_type: String,
    pub error: String,
}

impl ErrorResponse {
    pub fn new(error_type: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            status: "error".to_string(),
            error_type: error_type.into(),
            error: error.into(),
        }
    }

    pub fn bad_data(error: impl Into<String>) -> Self {
        Self::new("bad_data", error)
    }

    pub fn execution(error: impl Into<String>) -> Self {
        Self::new("execution", error)
    }

    pub fn internal(error: impl Into<String>) -> Self {
        Self::new("internal", error)
    }

    pub fn timeout(error: impl Into<String>) -> Self {
        Self::new("timeout", error)
    }
}

// ---------------------------------------------------------------------------
// EXPLAIN response
// ---------------------------------------------------------------------------

/// Dry-run EXPLAIN response for `/api/v1/query[?explain=true]` and
/// `/api/v1/query_range[?explain=true]`. Mirrors the shape of
/// [`QueryResponse`] so the HTTP handler can unify error rendering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<crate::promql::plan::ExplainResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

/// Wrap an [`ExplainResult`](crate::promql::plan::ExplainResult)
/// (or error) into an [`ExplainResponse`].
pub fn explain_result_to_response(
    result: Result<crate::promql::plan::ExplainResult, QueryError>,
) -> ExplainResponse {
    match result {
        Ok(data) => ExplainResponse {
            status: "success".to_string(),
            data: Some(data),
            error: None,
            error_type: None,
        },
        Err(e) => {
            let err = query_error_response(e);
            ExplainResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// /api/v1/query (instant query)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<QueryResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    /// Populated when the request enabled per-query tracing (either via
    /// `?trace=true` or `tracing.enabled` in the server config). Absent
    /// otherwise so the wire shape stays unchanged for normal callers.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub trace: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub result: QueryResultValue,
}

/// Instant query result — a scalar, vector, or matrix.
///
/// `Scalar` holds the raw `(timestamp_ms, value)` and serializes using
/// `PromSample` so the wire format is `[timestamp_secs, "value_string"]`.
#[derive(Debug, Clone)]
pub enum QueryResultValue {
    Scalar(i64, f64),
    Vector(Vec<VectorSeries>),
    Matrix(Vec<MatrixSeries>),
}

impl Serialize for QueryResultValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            QueryResultValue::Scalar(ts_ms, value) => {
                PromSample(*ts_ms, *value).serialize(serializer)
            }
            QueryResultValue::Vector(v) => v.serialize(serializer),
            QueryResultValue::Matrix(m) => m.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for QueryResultValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Prometheus JSON: scalar = [f64, string], vector = [{metric, value}, ...]
        let raw = serde_json::Value::deserialize(deserializer)?;
        if let Some(arr) = raw.as_array() {
            if arr.len() == 2 && arr[0].is_number() {
                // Scalar: [timestamp_secs, "value_string"]
                let ts_secs = arr[0]
                    .as_f64()
                    .ok_or_else(|| de::Error::custom("expected f64"))?;
                let val_str = arr[1]
                    .as_str()
                    .ok_or_else(|| de::Error::custom("expected string"))?;
                let value: f64 = parse_float(val_str).map_err(de::Error::custom)?;
                Ok(QueryResultValue::Scalar(
                    (ts_secs * 1000.0).round() as i64,
                    value,
                ))
            } else if arr
                .first()
                .is_some_and(|v| v.get("values").is_some() || v.get("histograms").is_some())
            {
                // Matrix: array of objects with "values" key
                let m: Vec<MatrixSeries> =
                    serde_json::from_value(raw).map_err(de::Error::custom)?;
                Ok(QueryResultValue::Matrix(m))
            } else {
                // Vector: array of objects with "value" key
                let v: Vec<VectorSeries> =
                    serde_json::from_value(raw).map_err(de::Error::custom)?;
                Ok(QueryResultValue::Vector(v))
            }
        } else {
            Err(de::Error::custom("expected array"))
        }
    }
}

// ---------------------------------------------------------------------------
// /api/v1/query_range (range query)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRangeResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<QueryRangeResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    /// See [`QueryResponse::trace`].
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub trace: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRangeResult {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub result: Vec<MatrixSeries>,
}

// ---------------------------------------------------------------------------
// MatrixSeries — newtype over RangeSample
// ---------------------------------------------------------------------------

/// Newtype over `RangeSample` that serializes as the Prometheus wire format:
/// `{ "metric": {...}, "values": [[ts, "val"], ...] }`.
#[derive(Debug, Clone)]
pub struct MatrixSeries(pub RangeSample);

impl Serialize for MatrixSeries {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let series = &self.0;
        let mut s = serializer.serialize_struct("MatrixSeries", 3)?;
        s.serialize_field("metric", &series.labels)?;
        // Prometheus omits `values` for histogram-only series.
        if !series.samples.is_empty() || series.histograms.is_empty() {
            s.serialize_field("values", &PromSamples(&series.samples))?;
        }
        if !series.histograms.is_empty() {
            s.serialize_field("histograms", &PromHistogramSamples(&series.histograms))?;
        }
        s.end()
    }
}

impl<'de> Deserialize<'de> for MatrixSeries {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Repr {
            metric: Labels,
            #[serde(default)]
            values: Vec<(f64, String)>,
            #[serde(default)]
            histograms: Vec<(f64, WireHistogram)>,
        }
        let repr = Repr::deserialize(deserializer)?;
        let samples = repr
            .values
            .into_iter()
            .map(|(ts_secs, val_str)| {
                let value: f64 = parse_float(&val_str).unwrap_or(f64::NAN);
                ((ts_secs * 1000.0).round() as i64, value)
            })
            .collect();
        let histograms = repr
            .histograms
            .into_iter()
            .map(|(ts_secs, h)| {
                Ok((
                    (ts_secs * 1000.0).round() as i64,
                    Arc::new(h.into_histogram::<D::Error>()?),
                ))
            })
            .collect::<Result<_, D::Error>>()?;
        Ok(MatrixSeries(RangeSample {
            labels: repr.metric,
            samples,
            histograms,
        }))
    }
}

// ---------------------------------------------------------------------------
// VectorSeries — newtype over InstantSample
// ---------------------------------------------------------------------------

/// Newtype over `InstantSample` that serializes as the Prometheus wire format:
/// `{ "metric": {...}, "value": [ts, "val"] }`.
#[derive(Debug, Clone)]
pub struct VectorSeries(pub InstantSample);

impl Serialize for VectorSeries {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("VectorSeries", 2)?;
        s.serialize_field("metric", &self.0.labels)?;
        match &self.0.histogram {
            Some(h) => {
                s.serialize_field("histogram", &PromHistogramSample(self.0.timestamp_ms, h))?
            }
            None => s.serialize_field("value", &PromSample(self.0.timestamp_ms, self.0.value))?,
        }
        s.end()
    }
}

impl<'de> Deserialize<'de> for VectorSeries {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct VectorSeriesVisitor;

        impl<'de> Visitor<'de> for VectorSeriesVisitor {
            type Value = VectorSeries;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a VectorSeries object with metric and value fields")
            }

            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut metric: Option<Labels> = None;
                let mut value: Option<(f64, String)> = None;
                let mut histogram: Option<(f64, WireHistogram)> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "metric" => metric = Some(map.next_value()?),
                        "value" => value = Some(map.next_value()?),
                        "histogram" => histogram = Some(map.next_value()?),
                        _ => {
                            let _ = map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }

                let metric = metric.ok_or_else(|| de::Error::missing_field("metric"))?;
                if let Some((ts_secs, h)) = histogram {
                    return Ok(VectorSeries(InstantSample {
                        labels: metric,
                        timestamp_ms: (ts_secs * 1000.0).round() as i64,
                        value: f64::NAN,
                        histogram: Some(Arc::new(h.into_histogram()?)),
                    }));
                }
                let (ts_secs, val_str) = value.ok_or_else(|| de::Error::missing_field("value"))?;
                let val: f64 = parse_float(&val_str).map_err(de::Error::custom)?;

                Ok(VectorSeries(InstantSample {
                    labels: metric,
                    timestamp_ms: (ts_secs * 1000.0).round() as i64,
                    value: val,
                    histogram: None,
                }))
            }
        }

        deserializer.deserialize_struct("VectorSeries", &["metric", "value"], VectorSeriesVisitor)
    }
}

// ---------------------------------------------------------------------------
// Other response types (unchanged)
// ---------------------------------------------------------------------------

/// Response for /api/v1/series (series listing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<Labels>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

/// Response for /api/v1/labels (label names)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelsResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

/// Response for /api/v1/label/{name}/values (label values)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelValuesResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

/// Response for /api/v1/metadata (metric metadata)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<HashMap<String, Vec<WireMetricMetadata>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

/// Newtype over `model::MetricMetadata` that serializes as the Prometheus wire
/// format: `{"type": "gauge", "help": "...", "unit": "..."}`.
#[derive(Debug, Clone, PartialEq)]
pub struct WireMetricMetadata(pub model::MetricMetadata);

impl Serialize for WireMetricMetadata {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("MetricMetadata", 3)?;
        s.serialize_field(
            "type",
            self.0
                .metric_type
                .as_ref()
                .map(|t| t.as_str())
                .unwrap_or(""),
        )?;
        s.serialize_field("help", self.0.description.as_deref().unwrap_or(""))?;
        s.serialize_field("unit", self.0.unit.as_deref().unwrap_or(""))?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for WireMetricMetadata {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Repr {
            #[serde(rename = "type", default)]
            metric_type: String,
            #[serde(default)]
            help: String,
            #[serde(default)]
            unit: String,
        }
        let repr = Repr::deserialize(deserializer)?;
        let metric_type = if repr.metric_type.is_empty() {
            None
        } else {
            Some(match repr.metric_type.as_str() {
                "gauge" => model::MetricType::Gauge,
                "counter" => model::MetricType::Sum {
                    monotonic: true,
                    temporality: model::Temporality::Unspecified,
                },
                "histogram" => model::MetricType::Histogram {
                    temporality: model::Temporality::Unspecified,
                },
                "summary" => model::MetricType::Summary,
                _ => model::MetricType::Gauge,
            })
        };
        Ok(WireMetricMetadata(model::MetricMetadata {
            metric_name: String::new(),
            metric_type,
            description: if repr.help.is_empty() {
                None
            } else {
                Some(repr.help)
            },
            unit: if repr.unit.is_empty() {
                None
            } else {
                Some(repr.unit)
            },
        }))
    }
}

/// Response for /federate (federation endpoint)
#[derive(Debug, Clone)]
pub struct FederateResponse {
    pub content_type: String, // "text/plain; version=0.0.4"
    pub body: Vec<u8>,        // Prometheus text format
}

#[cfg(test)]
mod tests;
