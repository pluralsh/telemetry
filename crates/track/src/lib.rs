//! Track's single-node OTLP trace storage core.
//!
//! Track stores typed `ResourceSpans` in bounded immutable compressed pages,
//! with namespace/time-segment routing, direct trace locators, and exact typed
//! scalar attribute posting fragments over SlateDB.

mod codec;
mod config;
mod db;
mod error;
mod model;
mod namespace;
mod otlp;
mod page;
mod segment;
mod sharded;
pub mod traceql;

pub use config::{Config, PageConfig};
pub use db::{Durability, TraceDb, WriteReport};
pub use error::{Error, Result};
pub use model::{
    AttributeMatcher, AttributeScope, AttributeValue, SegmentId, Trace, TraceBatch, TraceId,
};
pub use namespace::{MAX_NAMESPACE_LEN, Namespace, NamespaceError};
pub use otlp::{trace_batches, trace_batches_from_resource_spans};
pub use page::{Page, PageBuilder, TraceDirectoryEntry};
pub use segment::TraceSegmentExtractor;
pub use sharded::{ShardedTrack, ShardingOptions};
pub use traceql::{
    MatchedSpan, QueryOptions, QueryPlan, StaticValue as TraceQlValue, TraceQlResult,
};
