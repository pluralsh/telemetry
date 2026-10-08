//! Logs' single-node log storage core.
//!
//! Logs stores immutable multi-stream objects of compressed row blocks and
//! namespace/time-segment-scoped indexes in SlateDB and evaluates LogQL
//! locally over bounded block reads.

mod analyzer;
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod bench_support;
mod codec;
mod compaction;
mod config;
mod db;
mod error;
pub mod logql;
mod merge;
mod model;
mod object;
mod query;
pub mod routing;
mod search;
mod sharded;

pub use analyzer::{Analyzer, LogAnalyzer};
pub use common::namespace::{MAX_NAMESPACE_LEN, Namespace, NamespaceError};
pub use config::{
    CompactionConfig, Config, DEFAULT_BLOCK_CACHE_CAPACITY_BYTES, DEFAULT_RETENTION, PageConfig,
};
pub use db::{Durability, LogDb, WriteReport};
pub use error::{Error, Result};
pub use model::{Field, Fields, Label, Labels, LogBatch, LogEntry, LogRow};
pub use query::{
    DEFAULT_INSTANT_LOG_LOOKBACK_NS, Direction, LogStream, MatrixSeries, QueryOptions,
    QueryRequest, QueryResult, Sample, VectorSample,
};
pub use sharded::ShardedLogs;
pub use sharding::ShardingOptions;

/// Persisted SlateDB segment extractor identifier.
pub const SEGMENT_EXTRACTOR_NAME: &str = codec::SEGMENT_EXTRACTOR_NAME;
