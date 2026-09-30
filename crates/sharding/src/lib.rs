//! Shard planning, runtime ownership, and the per-process shard facade
//! shared by Telemetry products.
//!
//! The crate is product-neutral: products supply routing keys, batch
//! splitting, storage databases, and wire forwarding; this crate owns shard
//! ownership, the open shard set, and local/remote write dispatch.

mod backend;
mod hash_range;
mod model;
mod options;
mod planner;
mod router;
mod runtime;
pub mod server;
mod shard_set;
mod traits;

#[cfg(feature = "kubernetes")]
pub mod kubernetes;

pub use backend::{
    FakeAssignmentStore, FakeLeaseBackend, LeaseEvent, StandaloneAssignmentStore,
    StandaloneLeaseBackend, StaticOwnerResolver,
};
pub use hash_range::{
    HashRange, HashRangeAssignment, HashRangeMap, HashValue, ROUTING_SLOT_BITS, ROUTING_SLOT_COUNT,
    RoutingError, RoutingGeneration, RoutingSlot, hash_routing_key,
};
pub use model::{
    Assignment, AssignmentGeneration, AssignmentState, DEFAULT_IO_CONCURRENCY_LIMIT,
    DEFAULT_SHARDS, EpochPolicy, ModelError, Owner, RoutingEpoch, ShardId, ShardMap, ShardRange,
};
pub use options::ShardingOptions;
pub use planner::{PlanError, balanced_contiguous};
pub use router::{
    ForwardError, LocalWriteGuard, RouteError, RoutedShardLifecycle, RouterLimits, WriteRouter,
    shard_request_id,
};
pub use runtime::{ManagerError, OwnershipManager, OwnershipManagerConfig, OwnershipState};
pub use shard_set::{ShardDatabase, ShardOpener, ShardRole, ShardSet, ShardSetError, shard_opener};
pub use traits::{
    AssignmentStore, BoxError, LeaseBackend, OwnerResolver, ReaderShardLifecycle, ResolvedOwner,
    ShardLifecycle,
};
