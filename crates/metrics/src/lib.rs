//! Metrics is a namespace-aware, Prometheus-compatible time-series database.
//!
//! The storage and PromQL engine are adapted from OpenData TimeSeries under
//! the MIT license. See the repository's `THIRD_PARTY_NOTICES.md`.

#![allow(dead_code)]

mod active_series;
mod config;
mod delta;
mod discovery;
pub(crate) mod error;
mod flusher;
pub mod histogram;
mod index;
mod minitsdb;
pub(crate) mod model;
mod postings_cache;
mod promql;
mod query;
mod reader;
#[cfg(feature = "remote-write")]
pub mod remote_write;
pub mod routing;
mod serde;
mod sharded;
mod storage;
#[cfg(test)]
mod test_utils;
mod timeseries;
mod tsdb;
mod tsdb_metrics;
mod util;

#[cfg(feature = "otel")]
pub mod otel;
#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use common::namespace::{Namespace, NamespaceError};
pub use config::Config;
pub use error::{Error, QueryError, Result};
pub use histogram::{Bucket, CounterResetHint, FloatHistogram};
pub use model::{
    HistogramSample, InstantSample, Label, Labels, MetricMetadata, MetricType, QueryOptions,
    QueryValue, RangeSample, STALE_NAN, Sample, Series, SeriesBuilder, Temporality, is_stale_nan,
};
#[cfg(feature = "otel")]
pub use otel::{OtelConfig, OtelConverter};
pub use promql::response::{
    QueryRangeResponse, QueryResponse, query_value_to_response, range_result_to_response,
};
pub use reader::TimeSeriesDbReader;
pub use sharded::{MetricsShard, ShardedMetrics, ShardedTimeseries};
pub use sharding::ShardingOptions;
pub use timeseries::{TimeSeriesDb, Visibility};
pub use util::{parse_duration, parse_timestamp};

/// Persisted SlateDB segment extractor identifier.
pub const SEGMENT_EXTRACTOR_NAME: &str = storage::segment_extractor::EXTRACTOR_NAME;
