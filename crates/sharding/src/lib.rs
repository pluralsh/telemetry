//! Virtual-shard planning and runtime ownership shared by Telemetry products.
//!
//! The crate is product-neutral: Meter is the first consumer, while Line and
//! Track can reuse the same assignment and lifecycle contracts later.

mod backend;
mod model;
mod planner;
mod runtime;
mod traits;

#[cfg(feature = "kubernetes")]
pub mod kubernetes;

pub use backend::{
    FakeAssignmentStore, FakeLeaseBackend, LeaseEvent, StandaloneAssignmentStore,
    StandaloneLeaseBackend, StaticOwnerResolver,
};
pub use model::{
    Assignment, AssignmentGeneration, AssignmentState, DEFAULT_VIRTUAL_SHARDS, ModelError, Owner,
    ShardId, ShardMap, ShardRange, ShardingConfig,
};
pub use planner::{PlanError, balanced_contiguous};
pub use runtime::{ManagerError, OwnershipManager, OwnershipManagerConfig, OwnershipState};
pub use traits::{
    AssignmentStore, BoxError, LeaseBackend, OwnerResolver, ResolvedOwner, ShardLifecycle,
};
