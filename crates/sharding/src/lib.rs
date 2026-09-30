//! Shard planning and runtime ownership shared by Telemetry products.
//!
//! The crate is product-neutral: Meter is the first consumer, while Line and
//! Track can reuse the same assignment and lifecycle contracts later.

mod backend;
mod hash_range;
mod model;
mod planner;
mod runtime;
pub mod server;
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
pub use planner::{PlanError, balanced_contiguous};
pub use runtime::{ManagerError, OwnershipManager, OwnershipManagerConfig, OwnershipState};
pub use traits::{
    AssignmentStore, BoxError, LeaseBackend, OwnerResolver, ReaderShardLifecycle, ResolvedOwner,
    ShardLifecycle,
};
