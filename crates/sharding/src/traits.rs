use std::{error::Error, net::SocketAddr};

use async_trait::async_trait;
use tokio::sync::watch;

use crate::{AssignmentGeneration, Owner, ShardMap, ShardRange};

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
    async fn acquire(
        &self,
        owner_id: &str,
        range: ShardRange,
        generation: AssignmentGeneration,
    ) -> Result<bool, BoxError>;

    async fn renew(
        &self,
        owner_id: &str,
        range: ShardRange,
        generation: AssignmentGeneration,
    ) -> Result<bool, BoxError>;

    async fn release(
        &self,
        owner_id: &str,
        range: ShardRange,
        generation: AssignmentGeneration,
    ) -> Result<(), BoxError>;
}

#[async_trait]
pub trait ShardLifecycle: Send + Sync {
    async fn open(
        &self,
        range: ShardRange,
        generation: AssignmentGeneration,
    ) -> Result<(), BoxError>;
    async fn drain(&self, range: ShardRange) -> Result<(), BoxError>;
    async fn flush(&self, range: ShardRange) -> Result<(), BoxError>;
    async fn close(&self, range: ShardRange) -> Result<(), BoxError>;
}
