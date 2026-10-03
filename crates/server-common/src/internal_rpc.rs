//! Writer-to-writer shard forwarding over the internal gRPC services.
//!
//! The request and response messages are product-specific; the credential,
//! ownership, and retry semantics around them are shared.

// `tonic::Status` is the gRPC error boundary these helpers exist to produce.
#![allow(clippy::result_large_err)]

use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
};

use sharding::{ForwardError, RouteError, ShardId, ShardMap};
use subtle::ConstantTimeEq;
use tonic::{
    Code, Request, Status,
    metadata::{MetadataMap, MetadataValue},
    transport::{Channel, Endpoint},
};

const AUTHORIZATION: &str = "authorization";

/// Message size cap for the internal writer services, in both directions.
///
/// A forwarded write was already admitted under the sender's public request
/// limits, but its internal encoding can be larger than the decoded public
/// body (remote write v2 symbol references and Loki label strings are
/// expanded per series or stream), so the owner must accept whatever a peer
/// was willing to forward. The internal port should not be publicly exposed.
pub const MAX_INTERNAL_MESSAGE_BYTES: usize = usize::MAX;

/// Constant-time string comparison for shared secrets.
pub fn secure_eq(left: &str, right: &str) -> bool {
    let left = blake3::hash(left.as_bytes());
    let right = blake3::hash(right.as_bytes());
    bool::from(left.as_bytes().ct_eq(right.as_bytes()))
}

/// Lazily connected channels to peer writers, keyed by `host:port`.
///
/// Channels reconnect on their own, so one per endpoint is reused for every
/// forwarded write instead of dialing per request.
#[derive(Default)]
pub struct ChannelPool {
    channels: Mutex<HashMap<String, Channel>>,
}

impl ChannelPool {
    pub fn channel(&self, endpoint: &str) -> Result<Channel, Status> {
        let mut channels = self.channels.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(channel) = channels.get(endpoint) {
            return Ok(channel.clone());
        }
        let channel = Endpoint::from_shared(format!("http://{endpoint}"))
            .map_err(|error| Status::unavailable(format!("invalid owner endpoint: {error}")))?
            .connect_lazy();
        channels.insert(endpoint.to_owned(), channel.clone());
        Ok(channel)
    }
}

/// Attaches the internal cluster credential, when one is configured.
pub fn authorize<T>(request: &mut Request<T>, token: Option<&str>) -> Result<(), Status> {
    let Some(token) = token else {
        return Ok(());
    };
    let value = MetadataValue::try_from(format!("Bearer {token}"))
        .map_err(|_| Status::internal("internal credential is not a valid header value"))?;
    request.metadata_mut().insert(AUTHORIZATION, value);
    Ok(())
}

/// Verifies the internal cluster credential, when one is configured.
pub fn verify(metadata: &MetadataMap, expected: Option<&str>) -> Result<(), Status> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let provided = metadata
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if secure_eq(provided, expected) {
        Ok(())
    } else {
        Err(Status::unauthenticated(
            "invalid internal cluster credential",
        ))
    }
}

pub fn shard_id(raw: u64) -> Result<ShardId, Status> {
    u32::try_from(raw)
        .map(ShardId::new)
        .map_err(|_| Status::invalid_argument("shard id is out of range"))
}

/// Rejects a forwarded write unless it was routed with our current
/// generation to a shard this server owns. Both rejections are
/// `FailedPrecondition`, which senders treat as retryable.
pub fn check_ownership(
    assignment: &ShardMap,
    requested: u64,
    shard: ShardId,
    local_owner: &str,
) -> Result<(), Status> {
    let current = assignment.generation.get();
    if requested != current {
        return Err(Status::failed_precondition(format!(
            "stale_ownership: requested generation {requested}, current generation {current}"
        )));
    }
    if assignment
        .owner_of(shard)
        .is_none_or(|owner| owner.id != local_owner)
    {
        return Err(Status::failed_precondition(
            "non_owner: shard is not owned by this server",
        ));
    }
    Ok(())
}

/// Classifies a peer's response for [`sharding::WriteRouter::forward`].
pub fn forward_error<E>(status: &Status, error: impl FnOnce(&Status) -> E) -> ForwardError<E> {
    if status.code() == Code::FailedPrecondition {
        ForwardError::Stale(error(status))
    } else {
        ForwardError::Failed(error(status))
    }
}

pub fn route_status(error: &RouteError) -> Status {
    Status::unavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use sharding::{Assignment, AssignmentGeneration, AssignmentState, Owner, ShardRange};

    use super::*;

    fn owned_by(owner: &str) -> ShardMap {
        ShardMap::new(
            AssignmentGeneration::new(3),
            1,
            vec![Assignment::new(
                Owner::new(owner, 0),
                ShardRange::within(0, 1, 1).unwrap(),
                AssignmentState::Active,
            )],
        )
        .unwrap()
    }

    #[test]
    fn credentials_round_trip() {
        let mut request = Request::new(());
        authorize(&mut request, Some("secret")).unwrap();
        verify(request.metadata(), Some("secret")).unwrap();
        assert_eq!(
            verify(request.metadata(), Some("other"))
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
        verify(&MetadataMap::new(), None).unwrap();
    }

    #[test]
    fn ownership_rejections_are_retryable() {
        let local = owned_by("a");
        check_ownership(&local, 3, ShardId::new(0), "a").unwrap();
        let stale = check_ownership(&local, 2, ShardId::new(0), "a").unwrap_err();
        assert!(stale.message().starts_with("stale_ownership"));
        let foreign = check_ownership(&local, 3, ShardId::new(0), "b").unwrap_err();
        assert!(foreign.message().starts_with("non_owner"));
        for status in [stale, foreign] {
            assert!(matches!(
                forward_error(&status, |_| ()),
                ForwardError::Stale(())
            ));
        }
    }

    #[tokio::test]
    async fn channel_pool_reuses_channels_per_endpoint() {
        let pool = ChannelPool::default();
        pool.channel("127.0.0.1:1").unwrap();
        pool.channel("127.0.0.1:1").unwrap();
        assert_eq!(pool.channels.lock().unwrap().len(), 1);
    }
}
