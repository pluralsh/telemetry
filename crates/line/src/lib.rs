//! Line's single-node log storage core.
//!
//! Line stores immutable compressed pages and namespace/time-segment-scoped
//! indexes in SlateDB and evaluates LogQL locally over bounded page scans.

mod codec;
mod config;
mod db;
mod error;
pub mod logql;
mod model;
mod namespace;
mod page;
mod query;
mod search;
mod segment;
mod sharded;

pub use config::{Config, PageConfig};
pub use db::{Durability, LogDb, WriteReport};
pub use error::{Error, Result};
pub use model::{Field, Fields, Label, Labels, LogBatch, LogEntry, LogRow};
pub use namespace::{MAX_NAMESPACE_LEN, Namespace, NamespaceError};
pub use page::{BlockMetadata, Page, PageBuilder};
pub use query::{
    DEFAULT_INSTANT_LOG_LOOKBACK_NS, Direction, LogStream, MatrixSeries, QueryOptions,
    QueryRequest, QueryResult, Sample, VectorSample,
};
pub use sharded::{ShardedLine, ShardingOptions};
