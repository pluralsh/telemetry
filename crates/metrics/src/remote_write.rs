//! Prometheus Remote Write protocol handler.
//!
//! Implements both Remote Write 1.0 (`prometheus.WriteRequest`) and 2.0
//! (`io.prometheus.write.v2.Request`), including native histograms:
//! <https://prometheus.io/docs/specs/prw/remote_write_spec/>
//! <https://prometheus.io/docs/specs/prw/remote_write_spec_2_0/>

use prost::Message;

use crate::error::Error;
use crate::histogram::{
    CUSTOM_BUCKETS_SCHEMA, CounterResetHint, FloatHistogram, MIN_EXPONENTIAL_SCHEMA, Span,
};
use crate::model::{HistogramSample, Label, MetricType, STALE_NAN, Sample, Series, Temporality};
use crate::util::Result;

// ============================================================================
// Protobuf message types (Remote Write 1.0)
// ============================================================================

/// WriteRequest is the top-level message for remote write requests.
#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

/// TimeSeries represents a single time series with labels and samples.
#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<ProtobufLabel>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<ProtobufSample>,
    #[prost(message, repeated, tag = "4")]
    pub histograms: Vec<ProtobufHistogram>,
}

/// ProtobufLabel is a name-value pair for metric identification.
/// Named ProtobufLabel to avoid conflict with crate::series::Label.
#[derive(Clone, PartialEq, Message)]
pub struct ProtobufLabel {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// Sample holds a value and timestamp for a time series data point.
/// Named ProtobufSample to avoid conflict with crate::model::Sample.
#[derive(Clone, PartialEq, Message)]
pub struct ProtobufSample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

/// A native histogram sample; the same wire message in 1.0 and 2.0.
///
/// Integer histograms carry delta-encoded bucket counts, float histograms
/// absolute counts; the variant of `count` selects which.
#[derive(Clone, PartialEq, Message)]
pub struct ProtobufHistogram {
    #[prost(oneof = "histogram_count::Count", tags = "1, 2")]
    pub count: Option<histogram_count::Count>,
    #[prost(double, tag = "3")]
    pub sum: f64,
    #[prost(sint32, tag = "4")]
    pub schema: i32,
    #[prost(double, tag = "5")]
    pub zero_threshold: f64,
    #[prost(oneof = "histogram_count::ZeroCount", tags = "6, 7")]
    pub zero_count: Option<histogram_count::ZeroCount>,
    #[prost(message, repeated, tag = "8")]
    pub negative_spans: Vec<ProtobufBucketSpan>,
    #[prost(sint64, repeated, tag = "9")]
    pub negative_deltas: Vec<i64>,
    #[prost(double, repeated, tag = "10")]
    pub negative_counts: Vec<f64>,
    #[prost(message, repeated, tag = "11")]
    pub positive_spans: Vec<ProtobufBucketSpan>,
    #[prost(sint64, repeated, tag = "12")]
    pub positive_deltas: Vec<i64>,
    #[prost(double, repeated, tag = "13")]
    pub positive_counts: Vec<f64>,
    #[prost(int32, tag = "14")]
    pub reset_hint: i32,
    #[prost(int64, tag = "15")]
    pub timestamp: i64,
    #[prost(double, repeated, tag = "16")]
    pub custom_values: Vec<f64>,
}

pub mod histogram_count {
    #[derive(Clone, Copy, PartialEq, prost::Oneof)]
    pub enum Count {
        #[prost(uint64, tag = "1")]
        CountInt(u64),
        #[prost(double, tag = "2")]
        CountFloat(f64),
    }

    #[derive(Clone, Copy, PartialEq, prost::Oneof)]
    pub enum ZeroCount {
        #[prost(uint64, tag = "6")]
        ZeroCountInt(u64),
        #[prost(double, tag = "7")]
        ZeroCountFloat(f64),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct ProtobufBucketSpan {
    #[prost(sint32, tag = "1")]
    pub offset: i32,
    #[prost(uint32, tag = "2")]
    pub length: u32,
}

// ============================================================================
// Protobuf message types (Remote Write 2.0)
// ============================================================================

pub mod v2 {
    use super::ProtobufHistogram;

    /// `io.prometheus.write.v2.Request`. Label names and values, help and
    /// unit strings are references into `symbols`, whose first entry is "".
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Request {
        #[prost(string, repeated, tag = "4")]
        pub symbols: Vec<String>,
        #[prost(message, repeated, tag = "5")]
        pub timeseries: Vec<TimeSeries>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct TimeSeries {
        #[prost(uint32, repeated, tag = "1")]
        pub labels_refs: Vec<u32>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, repeated, tag = "3")]
        pub histograms: Vec<ProtobufHistogram>,
        #[prost(message, optional, tag = "5")]
        pub metadata: Option<Metadata>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Sample {
        #[prost(double, tag = "1")]
        pub value: f64,
        #[prost(int64, tag = "2")]
        pub timestamp: i64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Metadata {
        #[prost(int32, tag = "1")]
        pub r#type: i32,
        #[prost(uint32, tag = "3")]
        pub help_ref: u32,
        #[prost(uint32, tag = "4")]
        pub unit_ref: u32,
    }

    pub const METRIC_TYPE_COUNTER: i32 = 1;
    pub const METRIC_TYPE_GAUGE: i32 = 2;
    pub const METRIC_TYPE_HISTOGRAM: i32 = 3;
    pub const METRIC_TYPE_GAUGE_HISTOGRAM: i32 = 4;
    pub const METRIC_TYPE_SUMMARY: i32 = 5;
}

// ============================================================================
// Conversion logic
// ============================================================================

/// Remote-write protobuf message carried by a request body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    V1,
    V2,
}

impl Protocol {
    /// Resolves the `Content-Type` header. A missing header or one without a
    /// `proto` parameter means 1.0; an unknown `proto` is `None` (the spec
    /// asks receivers to answer 415).
    pub fn from_content_type(content_type: Option<&str>) -> Option<Self> {
        let Some(content_type) = content_type else {
            return Some(Self::V1);
        };
        let proto = content_type
            .split(';')
            .skip(1)
            .filter_map(|param| param.split_once('='))
            .find(|(key, _)| key.trim().eq_ignore_ascii_case("proto"))
            .map(|(_, value)| value.trim().trim_matches('"'));
        match proto {
            None | Some("prometheus.WriteRequest") => Some(Self::V1),
            Some("io.prometheus.write.v2.Request") => Some(Self::V2),
            Some(_) => None,
        }
    }
}

/// A decoded remote-write request plus the per-type counts reported back to
/// 2.0 senders.
#[derive(Debug, Default)]
pub struct RemoteWriteBatch {
    pub series: Vec<Series>,
    pub samples: usize,
    pub histograms: usize,
}

impl RemoteWriteBatch {
    fn push(&mut self, series: Series) {
        if series.samples.is_empty() && series.histograms.is_empty() {
            return;
        }
        self.samples += series.samples.len();
        self.histograms += series.histograms.len();
        self.series.push(series);
    }
}

/// Converts a wire histogram. Stale markers become float [`STALE_NAN`]
/// samples, which the engine treats as ending either sample type.
fn convert_histogram(h: ProtobufHistogram) -> Result<std::result::Result<HistogramSample, Sample>> {
    if crate::model::is_stale_nan(h.sum) {
        return Ok(Err(Sample::new(h.timestamp, f64::from_bits(STALE_NAN))));
    }
    let invalid =
        |message: String| Error::InvalidInput(format!("invalid native histogram: {message}"));
    let spans = |spans: &[ProtobufBucketSpan]| -> Vec<Span> {
        spans
            .iter()
            .map(|s| Span {
                offset: s.offset,
                length: s.length,
            })
            .collect()
    };
    let (positive_spans, negative_spans) = (spans(&h.positive_spans), spans(&h.negative_spans));
    let (count, zero_count, positive, negative) = match h.count {
        Some(histogram_count::Count::CountFloat(count)) => {
            let zero_count = match h.zero_count {
                Some(histogram_count::ZeroCount::ZeroCountFloat(z)) => z,
                Some(histogram_count::ZeroCount::ZeroCountInt(z)) => z as f64,
                None => 0.0,
            };
            (
                count,
                zero_count,
                FloatHistogram::buckets_from_spans(&positive_spans, &h.positive_counts),
                FloatHistogram::buckets_from_spans(&negative_spans, &h.negative_counts),
            )
        }
        count => {
            let count = match count {
                Some(histogram_count::Count::CountInt(count)) => count as f64,
                _ => 0.0,
            };
            let zero_count = match h.zero_count {
                Some(histogram_count::ZeroCount::ZeroCountInt(z)) => z as f64,
                Some(histogram_count::ZeroCount::ZeroCountFloat(z)) => z,
                None => 0.0,
            };
            (
                count,
                zero_count,
                FloatHistogram::buckets_from_delta_spans(&positive_spans, &h.positive_deltas),
                FloatHistogram::buckets_from_delta_spans(&negative_spans, &h.negative_deltas),
            )
        }
    };
    let counter_reset_hint = match h.reset_hint {
        1 => CounterResetHint::CounterReset,
        2 => CounterResetHint::NotCounterReset,
        3 => CounterResetHint::Gauge,
        _ => CounterResetHint::Unknown,
    };
    if h.schema < MIN_EXPONENTIAL_SCHEMA && h.schema != CUSTOM_BUCKETS_SCHEMA {
        return Err(invalid(format!("unsupported schema {}", h.schema)));
    }
    let mut histogram = FloatHistogram {
        counter_reset_hint,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count,
        count,
        sum: h.sum,
        positive: positive.map_err(invalid)?,
        negative: negative.map_err(invalid)?,
        custom_values: h.custom_values.into(),
    };
    histogram.normalize();
    histogram.validate().map_err(invalid)?;
    Ok(Ok(HistogramSample::new(h.timestamp, histogram)))
}

fn series_data(
    samples: impl IntoIterator<Item = Sample>,
    histograms: Vec<ProtobufHistogram>,
) -> Result<(Vec<Sample>, Vec<HistogramSample>)> {
    let mut samples: Vec<Sample> = samples.into_iter().collect();
    let mut native = Vec::with_capacity(histograms.len());
    for h in histograms {
        match convert_histogram(h)? {
            Ok(histogram) => native.push(histogram),
            Err(stale) => samples.push(stale),
        }
    }
    Ok((samples, native))
}

/// Convert a 1.0 WriteRequest into series.
///
/// TimeSeries with neither samples nor histograms are dropped: label
/// registration happens during ingestion, so they would have nothing to
/// query.
pub fn convert_write_request(request: WriteRequest) -> Result<RemoteWriteBatch> {
    let mut batch = RemoteWriteBatch::default();
    for ts in request.timeseries {
        let labels = ts
            .labels
            .into_iter()
            .map(|l| Label::new(l.name, l.value))
            .collect();
        let (samples, histograms) = series_data(
            ts.samples
                .into_iter()
                .map(|s| Sample::new(s.timestamp, s.value)),
            ts.histograms,
        )?;
        batch.push(Series {
            labels,
            // 1.0 carries no type information per series.
            metric_type: Some(MetricType::Gauge),
            unit: None,
            description: None,
            samples,
            histograms,
        });
    }
    Ok(batch)
}

/// Convert a 2.0 Request into series, resolving symbol references.
pub fn convert_v2_request(request: v2::Request) -> Result<RemoteWriteBatch> {
    let symbols = &request.symbols;
    let symbol = |reference: u32| -> Result<&str> {
        symbols
            .get(reference as usize)
            .map(String::as_str)
            .ok_or_else(|| {
                Error::InvalidInput(format!(
                    "symbol reference {reference} out of range for {} symbols",
                    symbols.len()
                ))
            })
    };
    let mut batch = RemoteWriteBatch::default();
    for ts in request.timeseries {
        if ts.labels_refs.len() % 2 != 0 {
            return Err(Error::InvalidInput(
                "labels_refs must hold name/value reference pairs".to_string(),
            ));
        }
        let (pairs, _) = ts.labels_refs.as_chunks::<2>();
        let labels = pairs
            .iter()
            .map(|&[name, value]| Ok(Label::new(symbol(name)?, symbol(value)?)))
            .collect::<Result<Vec<_>>>()?;
        let metadata = ts.metadata.unwrap_or_default();
        let metric_type = match metadata.r#type {
            v2::METRIC_TYPE_COUNTER => MetricType::Sum {
                monotonic: true,
                temporality: Temporality::Cumulative,
            },
            v2::METRIC_TYPE_HISTOGRAM => MetricType::Histogram {
                temporality: Temporality::Cumulative,
            },
            v2::METRIC_TYPE_GAUGE_HISTOGRAM => MetricType::Histogram {
                temporality: Temporality::Unspecified,
            },
            v2::METRIC_TYPE_SUMMARY => MetricType::Summary,
            _ => MetricType::Gauge,
        };
        let non_empty = |reference: u32| -> Result<Option<String>> {
            let value = symbol(reference)?;
            Ok((!value.is_empty()).then(|| value.to_string()))
        };
        let (samples, histograms) = series_data(
            ts.samples
                .into_iter()
                .map(|s| Sample::new(s.timestamp, s.value)),
            ts.histograms,
        )?;
        batch.push(Series {
            labels,
            metric_type: Some(metric_type),
            unit: non_empty(metadata.unit_ref)?,
            description: non_empty(metadata.help_ref)?,
            samples,
            histograms,
        });
    }
    Ok(batch)
}

/// Parse a snappy-compressed protobuf remote-write body.
pub fn parse_remote_write(body: &[u8], protocol: Protocol) -> Result<RemoteWriteBatch> {
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|e| Error::InvalidInput(format!("Snappy decompression failed: {}", e)))?;
    let decode_error =
        |e: prost::DecodeError| Error::InvalidInput(format!("Protobuf decode failed: {}", e));
    match protocol {
        Protocol::V1 => convert_write_request(
            WriteRequest::decode(decompressed.as_slice()).map_err(decode_error)?,
        ),
        Protocol::V2 => {
            convert_v2_request(v2::Request::decode(decompressed.as_slice()).map_err(decode_error)?)
        }
    }
}

#[cfg(test)]
mod tests;
