//! The `ShardReader` gRPC service, and the client a reader group member uses
//! to read every shard but its own through the other groups.

// `tonic::Status` is the gRPC error boundary of this module.
#![allow(clippy::result_large_err)]

use std::sync::{Arc, Mutex, PoisonError};

use futures::{StreamExt, TryStreamExt, stream};
use plural_metrics::{
    MetricsShardClient, MetricsShardQuery, QueryError, ShardedMetrics, WireStream,
};
use proto::metrics::internal::v1::{
    self as pb,
    shard_reader_client::ShardReaderClient,
    shard_reader_server::{ShardReader, ShardReaderServer},
};
use server_common::internal_rpc::{self, MAX_INTERNAL_MESSAGE_BYTES};
use server_common::read_groups::{ReadGroups, Replica};
use sharding::ShardId;
use tonic::{Request, Response, Status, transport::Channel};

fn query_status(error: QueryError) -> Status {
    match error {
        QueryError::InvalidQuery(message) => Status::invalid_argument(message),
        QueryError::Timeout => Status::deadline_exceeded("query timed out"),
        QueryError::Storage(message) => Status::unavailable(message),
        QueryError::Execution(message) => Status::internal(message),
    }
}

fn status_query(status: Status) -> QueryError {
    match status.code() {
        tonic::Code::InvalidArgument => QueryError::InvalidQuery(status.message().to_owned()),
        tonic::Code::DeadlineExceeded => QueryError::Timeout,
        tonic::Code::Internal => QueryError::Execution(status.message().to_owned()),
        _ => QueryError::Storage(format!("shard reader: {}", status.message())),
    }
}

/// Serves this reader's own shard to coordinating readers.
pub struct ShardReaderService {
    readers: Arc<ShardedMetrics>,
    token: Option<String>,
}

impl ShardReaderService {
    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        internal_rpc::verify(request.metadata(), self.token.as_deref())
    }
}

pub fn shard_reader_service(
    readers: Arc<ShardedMetrics>,
    token: Option<String>,
) -> ShardReaderServer<ShardReaderService> {
    ShardReaderServer::new(ShardReaderService { readers, token })
        .max_decoding_message_size(MAX_INTERNAL_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_INTERNAL_MESSAGE_BYTES)
}

type StatusStream<T> = futures::stream::BoxStream<'static, Result<T, Status>>;

#[tonic::async_trait]
impl ShardReader for ShardReaderService {
    type ResolveStream = StatusStream<pb::ResolvedChunk>;
    type SamplesStream = StatusStream<pb::SampleBatch>;

    async fn resolve(
        &self,
        request: Request<pb::ResolveRequest>,
    ) -> Result<Response<Self::ResolveStream>, Status> {
        self.authorize(&request)?;
        let stream = self
            .readers
            .serve_resolve(request.into_inner())
            .await
            .map_err(query_status)?;
        Ok(Response::new(stream.map_err(query_status).boxed()))
    }

    async fn samples(
        &self,
        request: Request<pb::ReadSamplesRequest>,
    ) -> Result<Response<Self::SamplesStream>, Status> {
        self.authorize(&request)?;
        let stream = self
            .readers
            .serve_samples(request.into_inner())
            .await
            .map_err(query_status)?;
        Ok(Response::new(stream.map_err(query_status).boxed()))
    }

    async fn series(
        &self,
        request: Request<pb::SeriesRequest>,
    ) -> Result<Response<pb::SeriesResponse>, Status> {
        self.authorize(&request)?;
        self.readers
            .serve_series(request.into_inner())
            .await
            .map(Response::new)
            .map_err(query_status)
    }

    async fn label_names(
        &self,
        request: Request<pb::LabelNamesRequest>,
    ) -> Result<Response<pb::StringsResponse>, Status> {
        self.authorize(&request)?;
        self.readers
            .serve_label_names(request.into_inner())
            .await
            .map(Response::new)
            .map_err(query_status)
    }

    async fn label_values(
        &self,
        request: Request<pb::LabelValuesRequest>,
    ) -> Result<Response<pb::StringsResponse>, Status> {
        self.authorize(&request)?;
        self.readers
            .serve_label_values(request.into_inner())
            .await
            .map(Response::new)
            .map_err(query_status)
    }

    async fn metadata(
        &self,
        request: Request<pb::MetadataRequest>,
    ) -> Result<Response<pb::MetadataResponse>, Status> {
        self.authorize(&request)?;
        self.readers
            .serve_metadata(request.into_inner())
            .await
            .map(Response::new)
            .map_err(query_status)
    }

    async fn bucket_generations(
        &self,
        request: Request<pb::BucketGenerationsRequest>,
    ) -> Result<Response<pb::BucketGenerationsResponse>, Status> {
        self.authorize(&request)?;
        self.readers
            .serve_bucket_generations(request.into_inner())
            .await
            .map(Response::new)
            .map_err(query_status)
    }
}

fn client(replica: &Replica) -> ShardReaderClient<Channel> {
    ShardReaderClient::new(replica.channel.clone())
        .max_decoding_message_size(MAX_INTERNAL_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_INTERNAL_MESSAGE_BYTES)
}

fn shard_of(read: Option<&pb::ShardRead>) -> ShardId {
    ShardId::new(read.map_or(0, |read| read.shard))
}

/// Reads other shards through their reader groups over gRPC.
pub struct GrpcMetricsShards {
    groups: Arc<ReadGroups>,
}

impl GrpcMetricsShards {
    pub fn new(groups: Arc<ReadGroups>) -> Self {
        Self { groups }
    }
}

macro_rules! unary {
    ($self:ident, $request:ident, $method:ident) => {{
        let shard = shard_of($request.read.as_ref());
        let groups = &$self.groups;
        groups
            .call(shard, |replica| {
                let request = groups.request($request.clone());
                async move { client(&replica).$method(request?).await }
            })
            .await
            .map(|(_, response)| response.into_inner())
            .map_err(status_query)
    }};
}

#[tonic::async_trait]
impl MetricsShardClient for GrpcMetricsShards {
    fn query(&self, shard: ShardId) -> Arc<dyn MetricsShardQuery> {
        Arc::new(GrpcShardQuery {
            groups: Arc::clone(&self.groups),
            shard,
            pinned: Arc::default(),
        })
    }

    async fn series(&self, request: pb::SeriesRequest) -> Result<pb::SeriesResponse, QueryError> {
        unary!(self, request, series)
    }

    async fn label_names(
        &self,
        request: pb::LabelNamesRequest,
    ) -> Result<pb::StringsResponse, QueryError> {
        unary!(self, request, label_names)
    }

    async fn label_values(
        &self,
        request: pb::LabelValuesRequest,
    ) -> Result<pb::StringsResponse, QueryError> {
        unary!(self, request, label_values)
    }

    async fn metadata(
        &self,
        request: pb::MetadataRequest,
    ) -> Result<pb::MetadataResponse, QueryError> {
        unary!(self, request, metadata)
    }

    async fn bucket_generations(
        &self,
        request: pb::BucketGenerationsRequest,
    ) -> Result<pb::BucketGenerationsResponse, QueryError> {
        unary!(self, request, bucket_generations)
    }
}

/// One query's reads of one shard, kept on the replica that answered the
/// first of them.
struct GrpcShardQuery {
    groups: Arc<ReadGroups>,
    shard: ShardId,
    pinned: Arc<Mutex<Option<Replica>>>,
}

macro_rules! streaming {
    ($self:ident, $request:ident, $method:ident) => {{
        let groups = Arc::clone(&$self.groups);
        let shard = $self.shard;
        let pinned = Arc::clone(&$self.pinned);
        stream::once(async move {
            let current = pinned
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            let (replica, stream) = groups
                .open_stream(shard, current.as_ref(), |replica| {
                    let request = groups.request($request.clone());
                    async move { client(&replica).$method(request?).await }
                })
                .await?;
            *pinned.lock().unwrap_or_else(PoisonError::into_inner) = Some(replica);
            Ok::<_, Status>(stream)
        })
        .try_flatten()
        .map_err(status_query)
        .boxed()
    }};
}

impl MetricsShardQuery for GrpcShardQuery {
    fn resolve(&self, request: pb::ResolveRequest) -> WireStream<pb::ResolvedChunk> {
        streaming!(self, request, resolve)
    }

    fn samples(&self, request: pb::ReadSamplesRequest) -> WireStream<pb::SampleBatch> {
        streaming!(self, request, samples)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
    };
    use plural_metrics::{Config, Label, Namespace, Sample, Series, ShardingOptions, Visibility};
    use sharding::{
        Assignment, AssignmentGeneration, AssignmentState, Owner, ShardMap, ShardRange,
        server::ReadGroupsConfig,
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn at(ms: i64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(ms as u64)
    }

    fn assignment() -> ShardMap {
        ShardMap::new(
            AssignmentGeneration::new(1),
            2,
            vec![Assignment::new(
                Owner::new("writer-0", 0),
                ShardRange::within(0, 2, 2).unwrap(),
                AssignmentState::Active,
            )],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn coordinators_read_other_shards_over_grpc() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            storage: SlateDbStorageConfig {
                path: "grpc-reader-groups".to_owned(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: directory.path().to_string_lossy().into_owned(),
                }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
                disk: Default::default(),
            },
            ..Default::default()
        };
        // Recent enough that retention has not expired the samples.
        let now = common::time::now_ms() / 60_000 * 60_000;
        let options = ShardingOptions::new(2, 16).unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let writers = ShardedMetrics::open_writers(
            config.clone(),
            options,
            [ShardId::new(0), ShardId::new(1)],
        )
        .await
        .unwrap();
        let series = (0..40)
            .map(|instance| {
                Series::new(
                    "requests_total",
                    vec![
                        Label::new("instance", format!("i-{instance}")),
                        Label::new("job", if instance % 2 == 0 { "api" } else { "web" }),
                    ],
                    (0..30)
                        .map(|minute| {
                            Sample::new(now - minute * 60_000, f64::from(instance) + minute as f64)
                        })
                        .collect(),
                )
            })
            .collect();
        writers
            .write(&assignment(), &namespace, series, Visibility::Durable)
            .await
            .unwrap();
        writers.close().await.unwrap();

        let open = |shards: Vec<ShardId>| {
            ShardedMetrics::open_readers(
                config.clone(),
                options,
                shards,
                slatedb::config::DbReaderOptions {
                    skip_wal_replay: false,
                    ..slatedb::config::DbReaderOptions::default()
                },
                64 * 1024 * 1024,
            )
        };
        let everything = open(vec![ShardId::new(0), ShardId::new(1)]).await.unwrap();

        // Each group listens on a port derived from its shard, which the
        // endpoint template reproduces.
        let base = 2_000 + std::process::id() % 4_000;
        let endpoint = format!("127.0.0.1:{base}{{shard}}");
        let cancel = CancellationToken::new();
        let token = Some("internal-secret".to_owned());
        let mut groups = Vec::new();
        for shard in [ShardId::new(0), ShardId::new(1)] {
            let readers = Arc::new(open(vec![shard]).await.unwrap());
            readers.shards().scope_readers_to(shard);
            let read_groups = ReadGroupsConfig {
                shard: Some(shard.get()),
                endpoint: endpoint.clone(),
                ..ReadGroupsConfig::default()
            };
            readers.connect_remote_shards(Arc::new(GrpcMetricsShards::new(Arc::new(
                ReadGroups::new(read_groups.clone(), shard, token.clone()),
            ))));
            let listener = TcpListener::bind(read_groups.endpoint_for(shard))
                .await
                .unwrap();
            let service = shard_reader_service(Arc::clone(&readers), token.clone());
            let shutdown = cancel.clone();
            tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(service)
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        shutdown.cancelled_owned(),
                    )
                    .await
                    .unwrap();
            });
            groups.push(readers);
        }

        let time = Some(at(now + 1_000));
        for coordinator in &groups {
            for query in [
                "sum(requests_total)",
                "sum by (job) (rate(requests_total[10m]))",
                "topk(3, requests_total)",
            ] {
                let expected = everything
                    .query(&namespace, query, time)
                    .await
                    .unwrap()
                    .into_matrix();
                let actual = coordinator
                    .query(&namespace, query, time)
                    .await
                    .unwrap()
                    .into_matrix();
                assert!(!expected.is_empty(), "query: {query}");
                assert_eq!(
                    format!("{actual:?}"),
                    format!("{expected:?}"),
                    "query: {query}"
                );
            }
            let window = at(now - 20 * 60_000)..=at(now);
            let query = "sum by (job) (requests_total)";
            let step = Duration::from_secs(60);
            let actual = coordinator
                .query_range(&namespace, query, window.clone(), step)
                .await
                .unwrap();
            let expected = everything
                .query_range(&namespace, query, window, step)
                .await
                .unwrap();
            assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
        }

        let unauthorized = ReadGroups::new(
            ReadGroupsConfig {
                shard: Some(0),
                endpoint,
                hedge_after_ms: 0,
                ..ReadGroupsConfig::default()
            },
            ShardId::new(0),
            Some("wrong".to_owned()),
        );
        let error = GrpcMetricsShards::new(Arc::new(unauthorized))
            .metadata(pb::MetadataRequest {
                read: Some(pb::ShardRead {
                    shard: 1,
                    namespace: Some(pb::Namespace {
                        name: "tenant".into(),
                    }),
                    ranges: vec![],
                }),
                metric: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, QueryError::Storage(_)), "{error:?}");

        cancel.cancel();
        everything.close().await.unwrap();
        for readers in groups {
            readers.close().await.unwrap();
        }
    }
}
