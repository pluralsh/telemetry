use std::{error::Error, net::SocketAddr};

use async_trait::async_trait;
use tokio::sync::watch;

use crate::{AssignmentGeneration, Owner, ShardId, ShardMap, ShardSplit};

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum MigrationExecutionError {
    #[error("fatal migration execution error: {0}")]
    Fatal(String),
    #[error("retryable migration execution error: {0}")]
    Retryable(#[source] BoxError),
}

impl MigrationExecutionError {
    pub fn fatal(error: impl Into<String>) -> Self {
        Self::Fatal(error.into())
    }

    pub fn retryable(error: impl Into<BoxError>) -> Self {
        Self::Retryable(error.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedOwner {
    pub owner: Owner,
    pub endpoint: String,
    pub socket_addr: Option<SocketAddr>,
}

#[async_trait]
pub trait OwnerResolver: Send + Sync {
    async fn resolve(&self, owner: &Owner) -> Result<ResolvedOwner, BoxError>;
}

#[async_trait]
pub trait AssignmentStore: Send + Sync {
    async fn load(&self) -> Result<Option<ShardMap>, BoxError>;
    async fn publish(&self, assignment: ShardMap) -> Result<(), BoxError>;
    fn watch(&self) -> watch::Receiver<Option<ShardMap>>;
}

#[async_trait]
pub trait LeaseBackend: Send + Sync {
    fn watch_releases(&self) -> watch::Receiver<u64>;

    async fn acquire(
        &self,
        owner_id: &str,
        shard: ShardId,
        generation: AssignmentGeneration,
    ) -> Result<bool, BoxError>;

    async fn renew(
        &self,
        owner_id: &str,
        shard: ShardId,
        generation: AssignmentGeneration,
    ) -> Result<bool, BoxError>;

    async fn release(
        &self,
        owner_id: &str,
        shard: ShardId,
        generation: AssignmentGeneration,
    ) -> Result<(), BoxError>;
}

#[async_trait]
pub trait ShardLifecycle: Send + Sync {
    async fn open(&self, shard: ShardId, generation: AssignmentGeneration) -> Result<(), BoxError>;
    async fn drain(&self, shard: ShardId) -> Result<(), BoxError>;
    async fn flush(&self, shard: ShardId) -> Result<(), BoxError>;
    async fn close(&self, shard: ShardId) -> Result<(), BoxError>;
}

/// Read-only storage hook that makes locally opened shard readers match the
/// authoritative routing snapshot before that snapshot becomes query-visible.
#[async_trait]
pub trait ReaderShardLifecycle: Send + Sync {
    async fn reconcile_readers(&self, assignment: &ShardMap) -> Result<(), BoxError>;
}

/// Product storage hook used by the source writer to materialize a split.
///
/// Implementations must be idempotent because a writer can retry after a
/// process restart or a Kubernetes resource-version conflict.
#[async_trait]
pub trait ShardMigrationExecutor: Send + Sync {
    /// Validates that the target can be created without taking a checkpoint.
    async fn preflight_split(&self, split: &ShardSplit) -> Result<(), MigrationExecutionError>;

    /// Creates and verifies the projected clone after the source is closed.
    async fn clone_split(&self, split: &ShardSplit) -> Result<(), MigrationExecutionError>;
}
