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
    /// This pod itself, which runs fragments in process rather than over
    /// `channel`. Only set on replicas of the local group handed to
    /// [`ReadGroups::fan_out`].
    pub is_self: bool,
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
    own: tokio::sync::OnceCell<Vec<String>>,
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
            own: tokio::sync::OnceCell::new(),
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
        self.group_replicas(shard).await
    }

    /// This pod's replicas of its own group: every resolved replica, with
    /// this pod's own entries replaced by one marked [`Replica::is_self`].
    /// Empty when this pod's address is unknown, since the group could then
    /// not be split without running a fragment twice.
    async fn local_group(&self) -> Result<Vec<Replica>, Status> {
        let own = self.own_addresses().await;
        let Some(first) = own.first() else {
            return Ok(Vec::new());
        };
        let mut group = self.group_replicas(self.local).await?;
        group.retain(|replica| !own.contains(&replica.address));
        group.push(Replica {
            channel: self.channel(first)?,
            address: first.clone(),
            is_self: true,
        });
        Ok(group)
    }

    /// How many fragments a read of `shard` costing `estimated_cost` should
    /// become, or `None` to read it whole. See [`plan_fragments`].
    pub async fn split_ways(
        &self,
        shard: ShardId,
        estimated_cost: u64,
        min_fragment_cost: u64,
        local_inflight: usize,
    ) -> Option<usize> {
        if !self.config.split.enabled {
            return None;
        }
        let replicas = if shard == self.local {
            self.local_group().await.ok()?.len()
        } else {
            self.group_replicas(shard).await.ok()?.len()
        };
        let ways = plan_fragments(
            &self.config.split,
            min_fragment_cost,
            estimated_cost,
            replicas,
            local_inflight,
        );
        let reason = match ways {
            Some(_) => "split",
            None if local_inflight >= self.config.split.max_local_inflight => "busy",
            None if replicas < 2 => "single_replica",
            None => "small",
        };
        ::metrics::counter!("telemetry_read_group_split_decisions_total", "reason" => reason)
            .increment(1);
        ways
    }

    /// The `ip:port` addresses this pod is listed under in its own group:
    /// `split.self_address`, else `$POD_IP`, else this host's name, each
    /// resolved and paired with the port of the group's endpoint. Resolved
    /// once; empty when none of them resolves.
    async fn own_addresses(&self) -> &[String] {
        self.own
            .get_or_init(|| async {
                let endpoint = self.config.endpoint_for(self.local);
                let port = endpoint.rsplit_once(':').map(|(_, port)| port);
                let candidate = self.config.split.self_address.clone().or_else(|| {
                    let host = std::env::var("POD_IP")
                        .ok()
                        .or_else(|| std::env::var("HOSTNAME").ok())
                        .or_else(|| {
                            std::fs::read_to_string("/proc/sys/kernel/hostname")
                                .ok()
                                .map(|name| name.trim().to_owned())
                        })
                        .filter(|host| !host.is_empty())?;
                    let host = if host.contains(':') && !host.starts_with('[') {
                        format!("[{host}]")
                    } else {
                        host
                    };
                    Some(format!("{host}:{}", port?))
                });
                let Some(candidate) = candidate else {
                    return Vec::new();
                };
                let mut addresses: Vec<String> = match tokio::net::lookup_host(&candidate).await {
                    Ok(addresses) => addresses.map(|address| address.to_string()).collect(),
                    Err(error) => {
                        tracing::warn!(%error, candidate, "could not resolve this reader's own address; its shard reads will not be split");
                        Vec::new()
                    }
                };
                addresses.sort_unstable();
                addresses.dedup();
                addresses
            })
            .await
    }

    /// Settings for splitting shard reads.
    pub const fn split_config(&self) -> &sharding::server::SplitConfig {
        &self.config.split
    }

    async fn group_replicas(&self, shard: ShardId) -> Result<Vec<Replica>, Status> {
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
                    is_self: false,
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
        self.open_stream_keyed(shard, pinned, None, open).await
    }

    /// [`Self::open_stream`], trying replicas in rendezvous order of `key`
    /// when nothing is pinned, so a fragment keeps its preferred replica
    /// and that replica's caches stay warm for it.
    pub async fn open_stream_keyed<T, F, Fut>(
        &self,
        shard: ShardId,
        pinned: Option<&Replica>,
        key: Option<u64>,
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
            None => {
                let mut replicas = self.replicas(shard).await?;
                if let Some(key) = key {
                    rendezvous_order(&mut replicas, key);
                }
                race(replicas, None, first_message).await?
            }
        };
        let stream = match first {
            Some(first) => stream::once(async move { Ok(first) }).chain(rest).boxed(),
            None => stream::empty().boxed(),
        };
        Ok((replica, stream))
    }
}

/// How many fragments a shard read of `estimated_cost` should become, or
/// `None` when it should not be split. `replicas` counts every replica that
/// can take a fragment, `local_inflight` the queries this pod is running.
pub fn plan_fragments(
    config: &sharding::server::SplitConfig,
    min_fragment_cost: u64,
    estimated_cost: u64,
    replicas: usize,
    local_inflight: usize,
) -> Option<usize> {
    if !config.enabled || local_inflight >= config.max_local_inflight {
        return None;
    }
    let by_cost = if min_fragment_cost == 0 {
        usize::MAX
    } else {
        usize::try_from(estimated_cost.div_ceil(min_fragment_cost)).unwrap_or(usize::MAX)
    };
    let k = replicas.min(config.max_fragments).min(by_cost);
    (k >= 2).then_some(k)
}

/// Orders `replicas` by rendezvous hashing on `key`, best first. A key keeps
/// its preferred replica while the group is stable, and losing a replica
/// only moves the keys that preferred it, so each replica's caches keep a
/// stable slice of the key space.
pub fn rendezvous_order(replicas: &mut [Replica], key: u64) {
    use std::hash::{Hash, Hasher};
    let score = |replica: &Replica| {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        replica.address.hash(&mut hasher);
        hasher.finish()
    };
    replicas.sort_by_cached_key(|replica| std::cmp::Reverse(score(replica)));
}

impl ReadGroups {
    /// Runs one fragment per entry of `keys` on `shard`'s group, each
    /// start to finish on one replica chosen by rendezvous hashing on its
    /// key, and returns the results in `keys` order. For the local shard
    /// the candidates are this pod, marked [`Replica::is_self`], and its
    /// peers. A failed fragment is retried once on the next replica in its
    /// order, and on this pod as a last resort when it holds the shard; a
    /// failure after that fails the whole read.
    /// A lone fragment is hedged like [`Self::call`]; several are not.
    pub async fn fan_out<T, F, Fut>(
        &self,
        shard: ShardId,
        keys: &[u64],
        run: F,
    ) -> Result<Vec<T>, Status>
    where
        T: Send,
        F: Fn(usize, Replica) -> Fut,
        Fut: Future<Output = Result<T, Status>> + Send,
    {
        let replicas = if shard == self.local {
            self.local_group().await?
        } else {
            self.group_replicas(shard).await?
        };
        if replicas.is_empty() {
            return Err(Status::unavailable("reader group has no replicas"));
        }
        let hedge = if keys.len() == 1 {
            self.hedge_after()
        } else {
            None
        };
        let fragments = keys.iter().enumerate().map(|(index, key)| {
            let mut order = replicas.clone();
            rendezvous_order(&mut order, *key);
            let run = &run;
            async move {
                let attempts = if hedge.is_some() {
                    order.len()
                } else {
                    order.len().min(2)
                };
                // This pod holds the local shard itself, so it is always
                // the last resort for a fragment of it.
                let own = order
                    .iter()
                    .skip(attempts)
                    .find(|replica| replica.is_self)
                    .cloned();
                order.truncate(attempts);
                order.extend(own);
                let started = Instant::now();
                let result = race(order, hedge, |replica| run(index, replica)).await;
                ::metrics::histogram!("telemetry_read_group_fragment_seconds")
                    .record(started.elapsed().as_secs_f64());
                result.map(|(_, value)| value)
            }
        });
        ::metrics::histogram!("telemetry_read_group_fragments").record(keys.len() as f64);
        futures::future::try_join_all(fragments).await
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

    #[test]
    fn plans_fragments() {
        let on = sharding::server::SplitConfig {
            enabled: true,
            ..Default::default()
        };
        assert_eq!(plan_fragments(&on, 100, 1000, 3, 0), Some(3));
        assert_eq!(
            plan_fragments(&on, 100, 250, 8, 0),
            Some(3.min(on.max_fragments))
        );
        assert_eq!(plan_fragments(&on, 100, 150, 3, 0), Some(2));
        assert_eq!(plan_fragments(&on, 100, 100, 3, 0), None);
        assert_eq!(plan_fragments(&on, 0, 0, 3, 0), Some(3));
        assert_eq!(plan_fragments(&on, 0, 0, 1, 0), None);
        assert_eq!(plan_fragments(&on, 0, 0, 3, on.max_local_inflight), None);
        assert_eq!(plan_fragments(&Default::default(), 0, 0, 3, 0), None);
    }

    #[tokio::test]
    async fn rendezvous_is_stable_and_minimally_disruptive() {
        let all = ["a", "b", "c", "d"].map(replica);
        let first = |set: &[Replica], key| {
            let mut set = set.to_vec();
            rendezvous_order(&mut set, key);
            set[0].address.clone()
        };
        let without_b: Vec<_> = all.iter().filter(|r| r.address != "b").cloned().collect();
        for key in 0..200u64 {
            let before = first(&all, key);
            assert_eq!(before, first(&all, key));
            if before != "b" {
                assert_eq!(before, first(&without_b, key));
            }
        }
    }

    #[tokio::test]
    async fn fan_out_runs_fragments_in_order_and_retries_once() {
        let mut config = ReadGroupsConfig {
            shard: Some(0),
            endpoint: "127.0.0.1:{shard}".to_owned(),
            ..ReadGroupsConfig::default()
        };
        config.split.self_address = Some("0.0.0.0:0".to_owned());
        let groups = ReadGroups::new(config, ShardId::new(0), None);
        // Remote shard 7 resolves to one replica; failing it twice fails the read.
        let ok = groups
            .fan_out(
                ShardId::new(7),
                &[1, 2, 3],
                |i, _| async move { Ok(i * 10) },
            )
            .await
            .unwrap();
        assert_eq!(ok, vec![0, 10, 20]);
        let calls = AtomicU32::new(0);
        let err = groups
            .fan_out(ShardId::new(7), &[1], |_, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(Status::unavailable("down")) }
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
        assert_eq!(calls.load(Ordering::SeqCst), 1); // one replica: nothing to retry on
    }

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
            is_self: false,
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
