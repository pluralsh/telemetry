//! Line's single-node log storage core.
//!
//! Line stores immutable compressed pages and namespace/time-segment-scoped
//! indexes in SlateDB and evaluates LogQL locally over bounded page scans.

mod analyzer;
mod codec;
mod compaction;
mod config;
mod db;
mod error;
pub mod logql;
mod model;
mod page;
mod query;
mod routing;
mod search;
mod sharded;

pub use analyzer::{Analyzer, LogAnalyzer};
pub use common::namespace::{MAX_NAMESPACE_LEN, Namespace, NamespaceError};
pub use config::{CompactionConfig, Config, PageConfig};
pub use db::{Durability, LogDb, WriteReport};
pub use error::{Error, Result};
pub use model::{Field, Fields, Label, Labels, LogBatch, LogEntry, LogRow};
pub use page::{BlockMetadata, Page, PageBuilder};
pub use query::{
    DEFAULT_INSTANT_LOG_LOOKBACK_NS, Direction, LogStream, MatrixSeries, QueryOptions,
    QueryRequest, QueryResult, Sample, VectorSample,
};
pub use sharded::{ShardedLine, ShardingOptions};

/// Persisted SlateDB segment extractor identifier.
pub const SEGMENT_EXTRACTOR_NAME: &str = codec::SEGMENT_EXTRACTOR_NAME;
