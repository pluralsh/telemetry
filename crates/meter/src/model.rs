//! Core data types for OpenData TimeSeries.
//!
//! This module defines the fundamental data structures used in the public API,
//! including labels for series identification, samples for data points, and
//! series for batched ingestion.

use crate::histogram::FloatHistogram;
use crate::util::hour_bucket_in_epoch_minutes;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Series ID (unique within a time bucket)
pub(crate) type SeriesId = u32;
/// Series fingerprint (hash of label set)
pub(crate) type SeriesFingerprint = u128;
/// Time bucket (minutes since UNIX epoch)
pub(crate) type BucketStart = u32;
/// Time bucket size (1-15, exponential: 1=1h, 2=2h, 3=4h, 4=8h, etc. = 2^(n-1) hours)
pub(crate) type BucketSize = u8;

/// NormalNaN is a quiet NaN (same as f64::NAN)
pub const NORMAL_NAN: u64 = 0x7ff8000000000001;

/// StaleNaN is a signaling NaN used as a staleness marker in Prometheus.
///
/// This value indicates that a time series is no longer being scraped or updated.
/// It's a signaling NaN (MSB of mantissa is 0) chosen with leading zeros to allow
/// for future extensions. The value 2 (rather than 1) makes it easier to distinguish
/// from NormalNaN during debugging.
pub const STALE_NAN: u64 = 0x7ff0000000000002;

/// Check if a float value is the special StaleNaN marker.
///
/// # Example
///
/// ```
/// use meter::{is_stale_nan, STALE_NAN};
///
/// let stale = f64::from_bits(STALE_NAN);
/// assert!(is_stale_nan(stale));
/// assert!(!is_stale_nan(f64::NAN));
/// assert!(!is_stale_nan(42.0));
/// ```
pub fn is_stale_nan(v: f64) -> bool {
    v.to_bits() == STALE_NAN
}

/// A label is a key-value pair that identifies a time series.
///
/// # Naming
///
/// - The metric name is stored with key `__name__`
/// - Label names and values can be any valid UTF-8 string
/// - Labels starting with `__` are reserved for internal use
///
/// # Prometheus Compatibility
///
/// For Prometheus compatibility, label names should match `[a-zA-Z_][a-zA-Z0-9_]*`,
/// but this is not enforced by the API.
///
/// # Example
///
/// ```
/// use meter::Label;
///
/// let label = Label::new("env", "production");
/// let name = Label::metric_name("http_requests_total");
/// ```
//TODO(rohan): make this impl PartialOrd/Ord so it can be sorted easily
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Label {
    /// The label name (key).
    pub name: String,
    /// The label value.
    pub value: String,
}

impl Label {
    /// Creates a new label with the given name and value.
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }

    /// Creates a metric name label (`__name__`).
    ///
    /// This is a convenience method for creating the special label that
    /// identifies the metric name.
    pub fn metric_name(name: impl Into<String>) -> Self {
        Self::new("__name__", name)
    }
}

/// Canonical ordering for `Label`: first by `name`, then by `value`.
///
/// This ordering ensures consistent sorting of label sets, which is required
/// for fingerprint computation and series identification.
impl Ord for Label {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name
            .cmp(&other.name)
            .then_with(|| self.value.cmp(&other.value))
    }
}

/// Delegates to [`Ord::cmp`] for total ordering.
impl PartialOrd for Label {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A single data point in a time series.
///
/// Samples represent individual measurements at specific points in time.
/// The timestamp is in milliseconds since the Unix epoch, and the value
/// is a 64-bit floating point number.
///
/// ## Special Values
///
/// Prometheus uses special NaN values for signaling:
/// - [`STALE_NAN`]: Marks a series as stale (no longer being scraped)
/// - Regular NaN: Represents missing or invalid data
///
/// Use [`is_stale_nan`] to check if a value is the staleness marker.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    /// Timestamp in milliseconds since Unix epoch.
    ///
    /// Uses `i64` (following chrono/protobuf conventions) to support pre-1970 dates.
    pub timestamp_ms: i64,

    /// The sample value.
    ///
    /// May be NaN or ±Inf for special cases.
    pub value: f64,
}

impl Sample {
    /// Creates a new sample with the given timestamp and value.
    pub fn new(timestamp_ms: i64, value: f64) -> Self {
        Self {
            timestamp_ms,
            value,
        }
    }

    /// Creates a sample with the current timestamp.
    ///
    pub fn now(value: f64) -> Self {
        Self::new(common::time::now_ms(), value)
    }
}

/// A native histogram observation at a point in time.
///
/// A stale native histogram is represented by a float [`STALE_NAN`] sample
/// instead, which the query engine treats identically.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSample {
    /// Timestamp in milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    pub histogram: FloatHistogram,
}

impl HistogramSample {
    pub fn new(timestamp_ms: i64, histogram: FloatHistogram) -> Self {
        Self {
            timestamp_ms,
            histogram,
        }
    }
}

/// Float and native histogram samples of one series, each ascending by
/// timestamp. A timestamp appears in at most one of the two.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct SeriesData {
    pub floats: Vec<Sample>,
    pub histograms: Vec<HistogramSample>,
}

impl SeriesData {
    pub(crate) fn len(&self) -> usize {
        self.floats.len() + self.histograms.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.floats.is_empty() && self.histograms.is_empty()
    }

    /// Keeps samples with `start_ms < timestamp <= end_ms`.
    pub(crate) fn retain_range(&mut self, start_ms: i64, end_ms: i64) {
        let in_range = |ts: i64| ts > start_ms && ts <= end_ms;
        self.floats.retain(|s| in_range(s.timestamp_ms));
        self.histograms.retain(|h| in_range(h.timestamp_ms));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Temporality {
    Cumulative,
    Delta,
    Unspecified,
}

/// The type of a metric.
///
/// This enum represents the two fundamental metric types in time series data:
///
/// - **Gauge**: A value that can go up or down (e.g., temperature, memory usage)
/// - **Sum**: A monotonically increasing value (e.g., request count, bytes sent)
/// - **Histogram**: A value that can go up or down (e.g., temperature, memory usage)
/// - **ExponentialHistogram**: A value that can go up or down (e.g., temperature, memory usage)
/// - **Summary**: A value that can go up or down (e.g., temperature, memory usage)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricType {
    Gauge,
    Sum {
        monotonic: bool,
        temporality: Temporality,
    },
    Histogram {
        temporality: Temporality,
    },
    ExponentialHistogram {
        temporality: Temporality,
    },
    Summary,
}

impl MetricType {
    pub fn as_str(&self) -> &str {
        match self {
            MetricType::Gauge => "gauge",
            MetricType::Sum {
                monotonic: true, ..
            } => "counter",
            MetricType::Sum {
                monotonic: false, ..
            } => "gauge",
            MetricType::Histogram { .. } => "histogram",
            MetricType::ExponentialHistogram { .. } => "histogram",
            MetricType::Summary => "summary",
        }
    }
}

/// A time series with its identifying labels and data points.
///
/// A series represents a single stream of timestamped values.
///
/// # Identity and Metadata
///
/// A series is uniquely identified by its labels, which include the metric name
/// stored as `__name__`. The `metric_type`, `unit`, and `description` fields are
/// metadata with last-write-wins semantics.
///
/// # Example
///
/// ```
/// use meter::{Series, Label, Sample};
///
/// let series = Series::new(
///     "http_requests_total",
///     vec![Label::new("method", "GET")],
///     vec![Sample::new(1700000000000, 1.0)],
/// );
///
/// // Or use the builder:
/// let series = Series::builder("http_requests_total")
///     .label("method", "GET")
///     .sample(1700000000000, 1.0)
///     .build();
///
/// assert_eq!(series.name(), "http_requests_total");
/// ```
#[derive(Debug, Clone)]
pub struct Series {
    /// Labels identifying this series, including `__name__` for the metric name.
    pub labels: Vec<Label>,

    // --- Metadata (last-write-wins) ---
    /// The type of metric (gauge or counter).
    pub metric_type: Option<MetricType>,

    /// Unit of measurement (e.g., "bytes", "seconds").
    pub unit: Option<String>,

    /// Human-readable description of the metric.
    pub description: Option<String>,

    // --- Data ---
    /// Float samples to write.
    pub samples: Vec<Sample>,

    /// Native histogram samples to write. A timestamp present in both
    /// `samples` and `histograms` resolves to the histogram.
    pub histograms: Vec<HistogramSample>,
}

impl Series {
    /// Creates a new series with the given name, labels, and samples.
    ///
    /// The metric name is stored as a `__name__` label and prepended to the
    /// provided labels.
    ///
    /// # Panics
    ///
    /// Panics if `labels` contains a `__name__` label. The metric name should
    /// only be provided via the `name` parameter.
    pub fn new(name: impl Into<String>, labels: Vec<Label>, samples: Vec<Sample>) -> Self {
        assert!(
            !labels.iter().any(|l| l.name == "__name__"),
            "labels must not contain __name__; use the name parameter instead"
        );
        let mut all_labels = Vec::with_capacity(labels.len() + 1);
        all_labels.push(Label::metric_name(name));
        all_labels.extend(labels);
        Self {
            labels: all_labels,
            metric_type: None,
            unit: None,
            description: None,
            samples,
            histograms: Vec::new(),
        }
    }

    /// Returns the metric name (value of the `__name__` label).
    ///
    /// # Panics
    ///
    /// Panics if the series was constructed without a `__name__` label.
    /// This should never happen when using the provided constructors.
    pub fn name(&self) -> &str {
        self.labels
            .iter()
            .find(|l| l.name == "__name__")
            .map(|l| l.value.as_str())
            .expect("Series must have a __name__ label")
    }

    /// Creates a builder for constructing a series.
    ///
    /// The builder provides a fluent API for creating series with
    /// labels, samples, and metadata fields.
    ///
    /// # Arguments
    ///
    /// * `name` - The metric name.
    pub fn builder(name: impl Into<String>) -> SeriesBuilder {
        SeriesBuilder::new(name)
    }

    /// Total float and histogram samples.
    pub(crate) fn sample_count(&self) -> usize {
        self.samples.len() + self.histograms.len()
    }

    /// Splits the samples into one series per key of their timestamps, each
    /// sharing this series' labels and metadata. The last group takes the
    /// owned fields, so the common single-group case is never cloned.
    pub(crate) fn partition_by<K: Eq + std::hash::Hash, E>(
        self,
        mut key: impl FnMut(i64) -> Result<K, E>,
    ) -> Result<Vec<(K, Series)>, E> {
        let Series {
            labels,
            metric_type,
            unit,
            description,
            samples,
            histograms,
        } = self;
        let mut groups: HashMap<K, (Vec<Sample>, Vec<HistogramSample>)> = HashMap::new();
        for sample in samples {
            groups
                .entry(key(sample.timestamp_ms)?)
                .or_default()
                .0
                .push(sample);
        }
        for sample in histograms {
            groups
                .entry(key(sample.timestamp_ms)?)
                .or_default()
                .1
                .push(sample);
        }
        let mut groups: Vec<_> = groups.into_iter().collect();
        let Some((last_key, (last_samples, last_histograms))) = groups.pop() else {
            return Ok(Vec::new());
        };
        let mut partitions = Vec::with_capacity(groups.len() + 1);
        for (key, (samples, histograms)) in groups {
            partitions.push((
                key,
                Series {
                    labels: labels.clone(),
                    metric_type,
                    unit: unit.clone(),
                    description: description.clone(),
                    samples,
                    histograms,
                },
            ));
        }
        partitions.push((
            last_key,
            Series {
                labels,
                metric_type,
                unit,
                description,
                samples: last_samples,
                histograms: last_histograms,
            },
        ));
        Ok(partitions)
    }
}

/// Builder for constructing [`Series`] instances.
///
/// Provides a fluent API for creating series with labels, samples,
/// and metadata fields.
#[derive(Debug, Clone)]
pub struct SeriesBuilder {
    labels: Vec<Label>,
    metric_type: Option<MetricType>,
    unit: Option<String>,
    description: Option<String>,
    samples: Vec<Sample>,
    histograms: Vec<HistogramSample>,
}

impl SeriesBuilder {
    fn new(name: impl Into<String>) -> Self {
        Self {
            labels: vec![Label::metric_name(name)],
            metric_type: None,
            unit: None,
            description: None,
            samples: Vec::new(),
            histograms: Vec::new(),
        }
    }

    /// Adds a label to the series.
    ///
    /// # Panics
    ///
    /// Panics if `name` is `__name__`. The metric name should only be provided
    /// via [`Series::builder()`].
    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        assert!(
            name != "__name__",
            "cannot add __name__ label; use Series::builder(name) instead"
        );
        self.labels.push(Label::new(name, value));
        self
    }

    /// Sets the metric type.
    pub fn metric_type(mut self, metric_type: MetricType) -> Self {
        self.metric_type = Some(metric_type);
        self
    }

    /// Sets the unit of measurement.
    pub fn unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    /// Sets the description.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Adds a sample with the given timestamp and value.
    pub fn sample(mut self, timestamp_ms: i64, value: f64) -> Self {
        self.samples.push(Sample::new(timestamp_ms, value));
        self
    }

    /// Adds a sample with the current timestamp.
    pub fn sample_now(mut self, value: f64) -> Self {
        self.samples.push(Sample::now(value));
        self
    }

    /// Adds a native histogram sample with the given timestamp.
    pub fn histogram(mut self, timestamp_ms: i64, histogram: FloatHistogram) -> Self {
        self.histograms
            .push(HistogramSample::new(timestamp_ms, histogram));
        self
    }

    /// Builds the series.
    pub fn build(self) -> Series {
        Series {
            labels: self.labels,
            metric_type: self.metric_type,
            unit: self.unit,
            description: self.description,
            samples: self.samples,
            histograms: self.histograms,
        }
    }
}

/// An ordered set of labels identifying a series.
///
/// `Labels` wraps a sorted `Vec<Label>` and provides convenience accessors
/// for looking up label values and the metric name. This is the type returned
/// by read/query APIs to identify each result series.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Labels(Vec<Label>);

impl Labels {
    /// Creates an empty `Labels`.
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// Creates a new `Labels` from a vec of labels.
    pub fn new(labels: Vec<Label>) -> Self {
        Self(labels)
    }

    /// Returns the number of labels.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if there are no labels.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the value of the label with the given name, if present.
    // TODO: labels are sorted, could use binary_search_by for O(log n)
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|l| l.name == name)
            .map(|l| l.value.as_str())
    }

    /// Returns the metric name (value of the `__name__` label).
    ///
    /// Returns `""` if no `__name__` label is present.
    pub fn metric_name(&self) -> &str {
        self.get("__name__").unwrap_or("")
    }

    /// Iterates over the labels.
    pub fn iter(&self) -> impl Iterator<Item = &Label> {
        self.0.iter()
    }
}

impl Ord for Labels {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Labels {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Serialize for Labels {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for label in &self.0 {
            map.serialize_entry(&label.name, &label.value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Labels {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LabelsVisitor;

        impl<'de> Visitor<'de> for LabelsVisitor {
            type Value = Labels;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a map of label name to label value")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut access: M) -> Result<Labels, M::Error> {
                let mut labels = Vec::with_capacity(access.size_hint().unwrap_or(0));
                while let Some((name, value)) = access.next_entry::<String, String>()? {
                    labels.push(Label { name, value });
                }
                labels.sort();
                Ok(Labels(labels))
            }
        }

        deserializer.deserialize_map(LabelsVisitor)
    }
}

impl From<Labels> for HashMap<String, String> {
    fn from(labels: Labels) -> Self {
        labels.0.into_iter().map(|l| (l.name, l.value)).collect()
    }
}

impl From<HashMap<String, String>> for Labels {
    fn from(map: HashMap<String, String>) -> Self {
        let mut labels: Vec<Label> = map
            .into_iter()
            .map(|(name, value)| Label { name, value })
            .collect();
        labels.sort();
        Self(labels)
    }
}

/// The result of an instant PromQL query.
///
/// PromQL expressions evaluate to either a scalar (e.g. `1+1`), a
/// vector of time series samples (e.g. `http_requests_total`), or a
/// matrix of range samples (e.g. `http_requests_total[5m]`).
#[derive(Debug, Clone)]
pub enum QueryValue {
    Scalar { timestamp_ms: i64, value: f64 },
    Vector(Vec<InstantSample>),
    Matrix(Vec<RangeSample>),
}

impl QueryValue {
    /// Convert into the most general representation (`Vec<RangeSample>`).
    ///
    /// - `Scalar` becomes a single `RangeSample` with empty labels and one sample.
    /// - `Vector` becomes one `RangeSample` per instant sample (each with one point).
    /// - `Matrix` is returned as-is.
    pub fn into_matrix(self) -> Vec<RangeSample> {
        match self {
            QueryValue::Scalar {
                timestamp_ms,
                value,
            } => vec![RangeSample {
                labels: Labels::empty(),
                samples: vec![(timestamp_ms, value)],
                histograms: Vec::new(),
            }],
            QueryValue::Vector(samples) => samples
                .into_iter()
                .map(|s| match s.histogram {
                    Some(h) => RangeSample {
                        labels: s.labels,
                        samples: Vec::new(),
                        histograms: vec![(s.timestamp_ms, h)],
                    },
                    None => RangeSample {
                        labels: s.labels,
                        samples: vec![(s.timestamp_ms, s.value)],
                        histograms: Vec::new(),
                    },
                })
                .collect(),
            QueryValue::Matrix(range_samples) => range_samples,
        }
    }
}

/// A single series value at a point in time.
///
/// Returned by instant (point-in-time) PromQL queries.
#[derive(Debug, Clone)]
pub struct InstantSample {
    /// The labels identifying this series.
    pub labels: Labels,
    /// Timestamp in milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    /// The sample value. Meaningless when `histogram` is set.
    pub value: f64,
    /// Set when the sample is a native histogram rather than a float.
    pub histogram: Option<Arc<FloatHistogram>>,
}

/// A series with values over a time range.
///
/// Returned by range PromQL queries.
#[derive(Debug, Clone)]
pub struct RangeSample {
    /// The labels identifying this series.
    pub labels: Labels,
    /// Timestamp-value pairs, ordered by timestamp.
    /// Each tuple is `(timestamp_ms, value)`.
    pub samples: Vec<(i64, f64)>,
    /// Native histogram samples, ordered by timestamp. A step holds either
    /// a float or a histogram, never both.
    pub histograms: Vec<(i64, Arc<FloatHistogram>)>,
}

/// Metadata for a metric.
///
/// This is the canonical public API type for metric metadata. It uses typed
/// fields (e.g. `Option<MetricType>`) rather than raw strings.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricMetadata {
    /// The metric name.
    pub metric_name: String,
    /// The metric type, if known.
    pub metric_type: Option<MetricType>,
    /// Human-readable description of the metric.
    pub description: Option<String>,
    /// Unit of measurement (e.g., "bytes", "seconds").
    pub unit: Option<String>,
}

/// Options for PromQL query evaluation.
///
/// Provides tuning knobs that apply to both instant and range queries.
/// Use `Default::default()` for Prometheus-compatible defaults.
#[derive(Debug, Clone)]
pub struct QueryOptions {
    /// How far back to look for a sample when evaluating at a given timestamp.
    ///
    /// Defaults to 5 minutes (the Prometheus staleness delta).
    pub lookback_delta: Duration,

    /// Maximum number of concurrent cache-miss metadata reads (inverted index
    /// and forward index) during query pipeline execution. Cache hits are free
    /// and do not consume a permit.
    pub metadata_concurrency: usize,

    /// Maximum number of concurrent cache-miss sample reads during query
    /// pipeline execution. Cache hits are free and do not consume a permit.
    pub sample_concurrency: usize,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            lookback_delta: Duration::from_secs(5 * 60),
            metadata_concurrency: 64,
            sample_concurrency: 384,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TimeBucket {
    pub(crate) start: BucketStart,
    pub(crate) size: BucketSize,
}

impl TimeBucket {
    pub(crate) fn hour(start: BucketStart) -> Self {
        Self { start, size: 1 }
    }

    pub(crate) fn round_to_hour(time: SystemTime) -> crate::error::Result<Self> {
        let bucket = hour_bucket_in_epoch_minutes(time)?;
        Ok(Self::hour(bucket))
    }

    pub(crate) fn size_in_mins(&self) -> u32 {
        (self.size.pow(2) * 60) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_label() {
        let label = Label::new("env", "prod");
        assert_eq!(label.name, "env");
        assert_eq!(label.value, "prod");
    }

    #[test]
    fn should_create_metric_name_label() {
        let label = Label::metric_name("http_requests");
        assert_eq!(label.name, "__name__");
        assert_eq!(label.value, "http_requests");
    }

    #[test]
    fn should_create_sample() {
        let sample = Sample::new(1700000000000, 42.5);
        assert_eq!(sample.timestamp_ms, 1700000000000);
        assert_eq!(sample.value, 42.5);
    }

    #[test]
    fn should_create_sample_now() {
        let before = common::time::now_ms();
        let sample = Sample::now(100.0);
        let after = common::time::now_ms();

        assert!(sample.timestamp_ms >= before);
        assert!(sample.timestamp_ms <= after);
        assert_eq!(sample.value, 100.0);
    }

    #[test]
    fn should_build_series_with_builder() {
        let series = Series::builder("cpu_usage")
            .label("host", "server1")
            .sample(1000, 0.5)
            .sample(2000, 0.6)
            .build();

        assert_eq!(series.name(), "cpu_usage");
        // labels includes __name__ + host
        assert_eq!(series.labels.len(), 2);
        assert_eq!(series.labels[0], Label::metric_name("cpu_usage"));
        assert_eq!(series.labels[1], Label::new("host", "server1"));
        assert_eq!(series.samples.len(), 2);
        assert_eq!(series.samples[0].value, 0.5);
        assert_eq!(series.samples[1].value, 0.6);
    }

    #[test]
    fn should_create_series_with_new() {
        let series = Series::new(
            "http_requests",
            vec![Label::new("method", "GET")],
            vec![Sample::new(1000, 1.0)],
        );

        assert_eq!(series.name(), "http_requests");
        // labels includes __name__ + method
        assert_eq!(series.labels.len(), 2);
        assert_eq!(series.labels[0], Label::metric_name("http_requests"));
        assert_eq!(series.labels[1], Label::new("method", "GET"));
    }

    #[test]
    #[should_panic(expected = "labels must not contain __name__")]
    fn should_panic_when_new_labels_contain_name() {
        Series::new(
            "http_requests",
            vec![Label::metric_name("other_name")],
            vec![],
        );
    }

    #[test]
    #[should_panic(expected = "cannot add __name__ label")]
    fn should_panic_when_builder_adds_name_label() {
        Series::builder("http_requests")
            .label("__name__", "other_name")
            .build();
    }

    #[test]
    fn into_matrix_from_scalar() {
        let qv = QueryValue::Scalar {
            timestamp_ms: 5000,
            value: 42.0,
        };
        let result = qv.into_matrix();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].labels, Labels::empty());
        assert_eq!(result[0].samples, vec![(5000, 42.0)]);
    }

    #[test]
    fn into_matrix_from_vector() {
        let qv = QueryValue::Vector(vec![
            InstantSample {
                labels: Labels::new(vec![Label::metric_name("cpu")]),
                timestamp_ms: 1000,
                value: 1.0,
                histogram: None,
            },
            InstantSample {
                labels: Labels::new(vec![Label::metric_name("mem")]),
                timestamp_ms: 2000,
                value: 2.0,
                histogram: None,
            },
        ]);
        let result = qv.into_matrix();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].labels.get("__name__").unwrap(), "cpu");
        assert_eq!(result[0].samples, vec![(1000, 1.0)]);
        assert_eq!(result[1].labels.get("__name__").unwrap(), "mem");
        assert_eq!(result[1].samples, vec![(2000, 2.0)]);
    }

    #[test]
    fn into_matrix_from_matrix_is_identity() {
        let range_samples = vec![
            RangeSample {
                labels: Labels::new(vec![Label::metric_name("cpu")]),
                samples: vec![(1000, 1.0), (2000, 2.0)],
                histograms: Vec::new(),
            },
            RangeSample {
                labels: Labels::new(vec![Label::metric_name("mem")]),
                samples: vec![(3000, 3.0)],
                histograms: Vec::new(),
            },
        ];
        let qv = QueryValue::Matrix(range_samples.clone());
        let result = qv.into_matrix();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].labels, range_samples[0].labels);
        assert_eq!(result[0].samples, range_samples[0].samples);
        assert_eq!(result[1].labels, range_samples[1].labels);
        assert_eq!(result[1].samples, range_samples[1].samples);
    }

    #[test]
    fn into_matrix_empty() {
        let qv = QueryValue::Matrix(vec![]);
        assert!(qv.into_matrix().is_empty());

        let qv = QueryValue::Vector(vec![]);
        assert!(qv.into_matrix().is_empty());
    }
}
