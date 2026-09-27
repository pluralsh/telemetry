use std::{error::Error, net::SocketAddr};

use async_trait::async_trait;
use tokio::sync::watch;

use crate::{AssignmentGeneration, Owner, ShardId, ShardMap};

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

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
