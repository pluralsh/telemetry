//! Prometheus Remote Write 1.0 protocol handler.
//!
//! Implements the Prometheus Remote Write 1.0 specification:
//! <https://prometheus.io/docs/specs/prw/remote_write_spec/>

use prost::Message;

use crate::error::Error;
use crate::model::{Label, MetricType, Sample, Series};
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

// ============================================================================
// Conversion logic
// ============================================================================

/// Convert a WriteRequest into a `Vec<Series>`.
///
/// Each TimeSeries in the WriteRequest produces one Series containing all its samples.
/// TimeSeries with no samples are filtered out - this is intentional because:
/// - Prometheus remote write 1.0 doesn't carry exemplars or histograms in empty samples
/// - Label registration happens during ingestion when samples are present
/// - Empty timeseries would create Series with no data to query
pub fn convert_write_request(request: WriteRequest) -> Vec<Series> {
    let total_timeseries = request.timeseries.len();
    let mut skipped_empty = 0usize;

    let result: Vec<Series> = request
        .timeseries
        .into_iter()
        .filter_map(|ts| {
            // Skip timeseries with no samples
            if ts.samples.is_empty() {
                skipped_empty += 1;
                return None;
            }
            let labels: Vec<Label> = ts
                .labels
                .into_iter()
                .map(|l| Label::new(l.name, l.value))
                .collect();

            let samples: Vec<Sample> = ts
                .samples
                .into_iter()
                .map(|s| Sample::new(s.timestamp, s.value))
                .collect();

            Some(Series {
                labels,
                metric_type: Some(MetricType::Gauge), // Default to Gauge since type info not in 1.0
                unit: None,
                description: None,
                samples,
            })
        })
        .collect();

    if skipped_empty > 0 {
        tracing::debug!(
            total = total_timeseries,
            skipped = skipped_empty,
            kept = result.len(),
            "Filtered out timeseries with no samples"
        );
    }

    result
}

/// Parse a snappy-compressed protobuf WriteRequest.
pub fn parse_remote_write(body: &[u8]) -> Result<Vec<Series>> {
    // Decompress snappy (block format)
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|e| Error::InvalidInput(format!("Snappy decompression failed: {}", e)))?;

    // Decode protobuf
    let request = WriteRequest::decode(decompressed.as_slice())
        .map_err(|e| Error::InvalidInput(format!("Protobuf decode failed: {}", e)))?;

    Ok(convert_write_request(request))
}
