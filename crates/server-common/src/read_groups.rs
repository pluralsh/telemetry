//! Reads from other shards' reader groups over the internal gRPC services.
//!
//! A reader group is the set of replicas serving one storage shard. A reader
//! coordinating a query reads its own shard in process; only the other
//! shards go through [`ReadGroups`], which resolves each group's replicas,
//! spreads requests across them, fails over between them and hedges slow
//! unary calls. Request and response messages are product-specific.

// `tonic::Status` is the gRPC error boundary these helpers exist to produce.
#![allow(clippy::result_large_err)]

use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{
    FutureExt, StreamExt,
    stream::{self, BoxStream, FuturesUnordered},
};
use sharding::{
    ShardId,
    server::{Product, ReadGroupsConfig, ServerMode, ShardingConfig},
};
use tonic::{
    Code, Request, Status, Streaming,
    transport::{Channel, Endpoint},
};

use crate::{auth::AuthConfig, internal_rpc};

/// One replica of a remote shard's reader group.
#[derive(Clone, Debug)]
pub struct Replica {
    pub address: String,
    pub channel: Channel,
}

struct Group {
    endpoint: String,
    replicas: Vec<Replica>,
    resolved_at: Option<Instant>,
}

/// The reader groups of every shard but this reader's own.
pub struct ReadGroups {
    config: ReadGroupsConfig,
    local: ShardId,
    token: Option<String>,
    groups: Mutex<HashMap<ShardId, Group>>,
    channels: Mutex<HashMap<String, Channel>>,
    next: AtomicUsize,
}

impl ReadGroups {
    pub fn new(config: ReadGroupsConfig, local: ShardId, token: Option<String>) -> Self {
        Self {
            config,
            local,
            token,
            groups: Mutex::default(),
            channels: Mutex::default(),
            next: AtomicUsize::new(0),
        }
    }

    /// The read groups of a reader-group member, which reads every shard
    /// but its own through them, or `None` when this server is not one.
    pub fn configured<P: Product>(
        mode: ServerMode,
        sharding: &ShardingConfig<P>,
        auth: &AuthConfig,
    ) -> anyhow::Result<Option<Self>> {
        let (ServerMode::Reader, Some(config)) = (mode, &sharding.read_groups) else {
            return Ok(None);
        };
        let local = config.local_shard().map_err(anyhow::Error::msg)?;
        Ok(Some(Self::new(
            config.clone(),
            local,
            auth.internal_token()?,
        )))
    }

    /// The shard this reader serves in process.
    pub const fn local_shard(&self) -> ShardId {
        self.local
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.config.request_timeout_seconds)
    }

    fn hedge_after(&self) -> Option<Duration> {
        (self.config.hedge_after_ms > 0).then(|| Duration::from_millis(self.config.hedge_after_ms))
    }

    /// A request carrying the internal credential and the read deadline.
    pub fn request<T>(&self, message: T) -> Result<Request<T>, Status> {
        let mut request = Request::new(message);
        request.set_timeout(self.request_timeout());
        internal_rpc::authorize(&mut request, self.token.as_deref())?;
        Ok(request)
    }

    /// `shard`'s replicas in the order this request should try them. The
    /// local shard is never read remotely, so asking for it is an error.
    pub async fn replicas(&self, shard: ShardId) -> Result<Vec<Replica>, Status> {
        if shard == self.local {
            return Err(Status::internal(format!(
                "shard {shard} is served in process and must not be read over gRPC"
            )));
        }
        let interval = Duration::from_secs(self.config.resolve_interval_seconds);
        let (endpoint, cached) = {
            let mut groups = self.groups.lock().unwrap_or_else(PoisonError::into_inner);
            let group = groups.entry(shard).or_insert_with(|| Group {
                endpoint: self.config.endpoint_for(shard),
                replicas: Vec::new(),
                resolved_at: None,
            });
            let fresh = group
                .resolved_at
                .is_some_and(|at| at.elapsed() < interval && !group.replicas.is_empty());
            (
                group.endpoint.clone(),
                fresh.then(|| group.replicas.clone()),
            )
        };
        let mut replicas = match cached {
            Some(replicas) => replicas,
            None => {
                let replicas = self.resolve(&endpoint).await?;
                let mut groups = self.groups.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(group) = groups.get_mut(&shard) {
                    group.replicas = replicas.clone();
                    group.resolved_at = Some(Instant::now());
                }
                replicas
            }
        };
        if !replicas.is_empty() {
            let start = self.next.fetch_add(1, Ordering::Relaxed) % replicas.len();
            replicas.rotate_left(start);
        }
        Ok(replicas)
    }

    /// Every address `endpoint` resolves to, or the endpoint itself when it
    /// does not resolve here, leaving balancing to whatever does.
    async fn resolve(&self, endpoint: &str) -> Result<Vec<Replica>, Status> {
        let mut addresses = match tokio::net::lookup_host(endpoint).await {
            Ok(addresses) => addresses.map(|address| address.to_string()).collect(),
            Err(error) => {
                tracing::debug!(%error, endpoint, "reader group did not resolve; dialing it directly");
                vec![endpoint.to_owned()]
            }
        };
        addresses.sort_unstable();
        addresses.dedup();
        addresses
            .into_iter()
            .map(|address| {
                Ok(Replica {
                    channel: self.channel(&address)?,
                    address,
                })
            })
            .collect()
    }

    fn channel(&self, address: &str) -> Result<Channel, Status> {
        let mut channels = self.channels.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(channel) = channels.get(address) {
            return Ok(channel.clone());
        }
        let uri = match address.parse::<SocketAddr>() {
            Ok(SocketAddr::V6(address)) => format!("http://[{}]:{}", address.ip(), address.port()),
            _ => format!("http://{address}"),
        };
        let channel = Endpoint::from_shared(uri)
            .map_err(|error| {
                Status::unavailable(format!("invalid reader group endpoint: {error}"))
            })?
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .http2_adaptive_window(true)
            .connect_lazy();
        channels.insert(address.to_owned(), channel.clone());
        Ok(channel)
    }

    /// Runs `call` against `shard`'s group: on one replica, failing over to
    /// the next while a replica is unavailable, and racing a second replica
    /// when the first is slower than the hedge delay. `call` should resolve
    /// once the replica has answered, so a hedge only starts when the answer
    /// itself is slow. Returns the
    /// replica that answered, so follow-up calls can stay on it.
    pub async fn call<T, F, Fut>(&self, shard: ShardId, call: F) -> Result<(Replica, T), Status>
    where
        T: Send,
        F: Fn(Replica) -> Fut,
        Fut: Future<Output = Result<T, Status>> + Send,
    {
        let replicas = self.replicas(shard).await?;
        race(replicas, self.hedge_after(), call).await
    }

    /// [`Self::call`] preferring `pinned`, the replica an earlier call of
    /// the same query used, and never hedging away from it.
    pub async fn call_pinned<T, F, Fut>(
        &self,
        shard: ShardId,
        pinned: &Replica,
        call: F,
    ) -> Result<(Replica, T), Status>
    where
        T: Send,
        F: Fn(Replica) -> Fut,
        Fut: Future<Output = Result<T, Status>> + Send,
    {
        let mut replicas = self.replicas(shard).await?;
        replicas.retain(|replica| replica.address != pinned.address);
        replicas.insert(0, pinned.clone());
        race(replicas, None, call).await
    }
}

impl ReadGroups {
    /// Opens a server stream from `shard`'s group, preferring `pinned`,
    /// failing over while a replica is unavailable, and counting a replica
    /// as answered once its first message arrives. Streams are never hedged:
    /// their first message waits on the read itself, so a slow one is a cold
    /// read, and a hedge would only start the same cold read on a second
    /// replica.
    pub async fn open_stream<T, F, Fut>(
        &self,
        shard: ShardId,
        pinned: Option<&Replica>,
        open: F,
    ) -> Result<(Replica, BoxStream<'static, Result<T, Status>>), Status>
    where
        T: Send + 'static,
        F: Fn(Replica) -> Fut,
        Fut: Future<Output = Result<tonic::Response<Streaming<T>>, Status>> + Send,
    {
        let first_message = |replica: Replica| {
            let opened = open(replica);
            async move {
                let mut stream = opened.await?.into_inner();
                let first = stream.message().await?;
                Ok((first, stream))
            }
        };
        let (replica, (first, rest)) = match pinned {
            Some(pinned) => self.call_pinned(shard, pinned, first_message).await?,
            None => race(self.replicas(shard).await?, None, first_message).await?,
        };
        let stream = match first {
            Some(first) => stream::once(async move { Ok(first) }).chain(rest).boxed(),
            None => stream::empty().boxed(),
        };
        Ok((replica, stream))
    }
}

/// Whether another replica may succeed where this one failed.
pub fn is_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::Unknown | Code::Aborted | Code::ResourceExhausted
    )
}

async fn race<T, F, Fut>(
    replicas: Vec<Replica>,
    hedge_after: Option<Duration>,
    call: F,
) -> Result<(Replica, T), Status>
where
    T: Send,
    F: Fn(Replica) -> Fut,
    Fut: Future<Output = Result<T, Status>> + Send,
{
    let mut pending = replicas.into_iter();
    let mut in_flight = FuturesUnordered::new();
    let start = |replica: Replica| {
        let attempt = call(replica.clone());
        async move { (replica, attempt.await) }.boxed()
    };
    let Some(first) = pending.next() else {
        return Err(Status::unavailable("reader group has no replicas"));
    };
    in_flight.push(start(first));
    let mut last_error = None;
    loop {
        let hedge = async {
            match hedge_after {
                Some(delay) if pending.len() > 0 => tokio::time::sleep(delay).await,
                _ => futures::future::pending().await,
            }
        };
        tokio::select! {
            finished = in_flight.next() => {
                let Some((replica, result)) = finished else {
                    return Err(last_error
                        .unwrap_or_else(|| Status::unavailable("reader group has no replicas")));
                };
                match result {
                    Ok(value) => return Ok((replica, value)),
                    Err(status) if is_retryable(&status) => {
                        tracing::debug!(address = replica.address, %status, "reader group replica failed; trying another");
                        last_error = Some(status);
                        if in_flight.is_empty() {
                            match pending.next() {
                                Some(next) => in_flight.push(start(next)),
                                None => return Err(last_error.expect("set above")),
                            }
                        }
                    }
                    Err(status) => return Err(status),
                }
            }
            () = hedge => {
                if let Some(next) = pending.next() {
                    ::metrics::counter!("telemetry_read_group_hedges_total").increment(1);
                    in_flight.push(start(next));
                }
            }
        }
    }
}

/// Shared [`ReadGroups`] handle.
pub type SharedReadGroups = Arc<ReadGroups>;

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use super::*;

    fn groups(hedge_after_ms: u64) -> ReadGroups {
        ReadGroups::new(
            ReadGroupsConfig {
                shard: Some(0),
                endpoint: "127.0.0.1:{shard}".to_owned(),
                hedge_after_ms,
                ..ReadGroupsConfig::default()
            },
            ShardId::new(0),
            None,
        )
    }

    fn replica(address: &str) -> Replica {
        Replica {
            address: address.to_owned(),
            channel: Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        }
    }

    #[tokio::test]
    async fn the_local_shard_is_never_read_remotely() {
        let groups = groups(0);
        let status = groups.replicas(ShardId::new(0)).await.err().unwrap();
        assert_eq!(status.code(), Code::Internal);
        let replicas = groups.replicas(ShardId::new(7)).await.unwrap();
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0].address, "127.0.0.1:7");
    }

    #[tokio::test]
    async fn fails_over_from_unavailable_replicas() {
        let replicas = vec![replica("a"), replica("b"), replica("c")];
        let (winner, value) = race(replicas, None, |replica| async move {
            if replica.address == "c" {
                Ok(3)
            } else {
                Err(Status::unavailable("down"))
            }
        })
        .await
        .unwrap();
        assert_eq!((winner.address.as_str(), value), ("c", 3));
        let error = race(vec![replica("a")], None, |_| async {
            Err::<(), _>(Status::invalid_argument("bad"))
        })
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
    }

    #[tokio::test(start_paused = true)]
    async fn hedges_a_slow_replica() {
        let calls = AtomicU32::new(0);
        let (winner, ()) = race(
            vec![replica("slow"), replica("fast")],
            Some(Duration::from_millis(10)),
            |replica| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if replica.address == "slow" {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }
                    Ok(())
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(winner.address, "fast");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
