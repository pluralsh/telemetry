//! Meter is a namespace-aware, Prometheus-compatible time-series database.
//!
//! The storage and PromQL engine are adapted from OpenData TimeSeries under
//! the MIT license. See the repository's `THIRD_PARTY_NOTICES.md`.

#![allow(dead_code)]

mod active_series;
mod config;
mod delta;
pub(crate) mod error;
mod flusher;
mod index;
mod minitsdb;
pub(crate) mod model;
mod promql;
mod query;
mod reader;
#[cfg(feature = "remote-write")]
pub mod remote_write;
mod routing;
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
pub use model::{
    InstantSample, Label, Labels, MetricMetadata, MetricType, QueryOptions, QueryValue,
    RangeSample, STALE_NAN, Sample, Series, SeriesBuilder, Temporality, is_stale_nan,
};
#[cfg(feature = "otel")]
pub use otel::{OtelConfig, OtelConverter};
pub use reader::TimeSeriesDbReader;
pub use sharded::{ShardedMeter, ShardedTimeseries, ShardingOptions};
pub use timeseries::{TimeSeriesDb, Visibility};

/// Persisted SlateDB segment extractor identifier used by migration preflight.
pub const SEGMENT_EXTRACTOR_NAME: &str = storage::segment_extractor::EXTRACTOR_NAME;
