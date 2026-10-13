//! The `ShardReader` gRPC service, and the client a reader group member uses
//! to read every shard but its own through the other groups.

// `tonic::Status` is the gRPC error boundary of this module.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};
use plural_logs::{Error, LogsShardClient, ShardedLogs};
use proto::logs::internal::v1::{
    self as pb,
    shard_reader_client::ShardReaderClient,
    shard_reader_server::{ShardReader, ShardReaderServer},
};
use server_common::internal_rpc::{self, MAX_INTERNAL_MESSAGE_BYTES};
use server_common::read_groups::{ReadGroups, Replica};
use sharding::ShardId;
use tonic::{Request, Response, Status, transport::Channel};

fn error_status(error: Error) -> Status {
    match error {
        Error::Query(_) | Error::Invalid(_) | Error::Regex(_) => {
            Status::invalid_argument(error.to_string())
        }
        Error::Corrupt(_) | Error::Json(_) => Status::internal(error.to_string()),
        Error::Backpressure | Error::Unavailable(_) | Error::Storage(_) | Error::Shard(_) => {
            Status::unavailable(error.to_string())
        }
    }
}

fn status_error(status: Status) -> Error {
    match status.code() {
        tonic::Code::InvalidArgument => Error::Query(status.message().to_owned()),
        tonic::Code::Internal => Error::Corrupt(status.message().to_owned()),
        _ => Error::Unavailable(format!("shard reader: {}", status.message())),
    }
}

/// Serves this reader's own shard to coordinating readers.
pub struct ShardReaderService {
    readers: Arc<ShardedLogs>,
    token: Option<String>,
}

impl ShardReaderService {
    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        internal_rpc::verify(request.metadata(), self.token.as_deref())
    }
}

pub fn shard_reader_service(
    readers: Arc<ShardedLogs>,
    token: Option<String>,
) -> ShardReaderServer<ShardReaderService> {
    ShardReaderServer::new(ShardReaderService { readers, token })
        .max_decoding_message_size(MAX_INTERNAL_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_INTERNAL_MESSAGE_BYTES)
}

#[tonic::async_trait]
impl ShardReader for ShardReaderService {
    type QueryStream = futures::stream::BoxStream<'static, Result<pb::ShardOutput, Status>>;

    async fn query(
        &self,
        request: Request<pb::QueryShardRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        self.authorize(&request)?;
        let outputs = self
            .readers
            .serve_query(request.get_ref())
            .await
            .map_err(error_status)?;
        Ok(Response::new(
            stream::iter(outputs.into_iter().map(Ok)).boxed(),
        ))
    }

    async fn label_names(
        &self,
        request: Request<pb::LabelNamesRequest>,
    ) -> Result<Response<pb::StringsResponse>, Status> {
        self.authorize(&request)?;
        self.readers
            .serve_label_names(request.get_ref())
            .await
            .map(Response::new)
            .map_err(error_status)
    }

    async fn label_values(
        &self,
        request: Request<pb::LabelValuesRequest>,
    ) -> Result<Response<pb::StringsResponse>, Status> {
        self.authorize(&request)?;
        self.readers
            .serve_label_values(request.get_ref())
            .await
            .map(Response::new)
            .map_err(error_status)
    }

    async fn series(
        &self,
        request: Request<pb::SeriesRequest>,
    ) -> Result<Response<pb::SeriesResponse>, Status> {
        self.authorize(&request)?;
        self.readers
            .serve_series(request.get_ref())
            .await
            .map(Response::new)
            .map_err(error_status)
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
pub struct GrpcLogsShards {
    groups: Arc<ReadGroups>,
}

impl GrpcLogsShards {
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
            .map_err(status_error)
    }};
}

#[tonic::async_trait]
impl LogsShardClient for GrpcLogsShards {
    async fn query(
        &self,
        request: pb::QueryShardRequest,
    ) -> plural_logs::Result<Vec<pb::ShardOutput>> {
        let shard = shard_of(request.read.as_ref());
        let groups = &self.groups;
        let (_, outputs) = groups
            .open_stream(shard, None, |replica| {
                let request = groups.request(request.clone());
                async move { client(&replica).query(request?).await }
            })
            .await
            .map_err(status_error)?;
        outputs.try_collect().await.map_err(status_error)
    }

    async fn label_names(
        &self,
        request: pb::LabelNamesRequest,
    ) -> plural_logs::Result<pb::StringsResponse> {
        unary!(self, request, label_names)
    }

    async fn label_values(
        &self,
        request: pb::LabelValuesRequest,
    ) -> plural_logs::Result<pb::StringsResponse> {
        unary!(self, request, label_values)
    }

    async fn series(&self, request: pb::SeriesRequest) -> plural_logs::Result<pb::SeriesResponse> {
        unary!(self, request, series)
    }
}

#[cfg(test)]
mod tests {
    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
    };
    use plural_logs::{
        Config, Durability, Label, Labels, LogBatch, LogEntry, Namespace, QueryOptions,
        QueryRequest, QueryResult, ShardingOptions,
    };
    use sharding::{
        Assignment, AssignmentGeneration, AssignmentState, Owner, ShardMap, ShardRange,
        server::ReadGroupsConfig,
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tokio_util::sync::CancellationToken;

    use super::*;

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
            storage: StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "grpc-reader-groups".to_owned(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: directory.path().to_string_lossy().into_owned(),
                }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
                disk: Default::default(),
            }),
            ..Config::default()
        };
        // Recent enough that retention has not expired the entries.
        let start = common::time::now_ms() * 1_000_000 - 3_600_000_000_000;
        let end = start + 10_000;
        let options = ShardingOptions::new(2, 16).unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let shards = [ShardId::new(0), ShardId::new(1)];
        let writers = ShardedLogs::open(config.clone(), options, shards)
            .await
            .unwrap();
        let batches = (0..40)
            .map(|instance| {
                LogBatch::new(
                    Labels::new(vec![
                        Label::new("instance", format!("i-{instance}")),
                        Label::new("job", if instance % 2 == 0 { "api" } else { "web" }),
                    ])
                    .unwrap(),
                    (0..20)
                        .map(|i| {
                            LogEntry::new(
                                start + i * 100 + instance,
                                format!("request {i} status={}", 200 + i % 3),
                            )
                        })
                        .collect(),
                )
            })
            .collect();
        writers
            .write(&assignment(), &namespace, batches, Durability::Written)
            .await
            .unwrap();
        writers.flush().await.unwrap();
        writers.close().await.unwrap();

        let open = |shards: Vec<ShardId>| {
            ShardedLogs::open_readers(
                config.clone(),
                options,
                shards,
                slatedb::config::DbReaderOptions {
                    skip_wal_replay: false,
                    ..slatedb::config::DbReaderOptions::default()
                },
            )
        };
        let everything = open(shards.to_vec()).await.unwrap();

        // Each group listens on a port derived from its shard, which the
        // endpoint template reproduces.
        let base = 1_000 + std::process::id() % 999;
        let endpoint = format!("127.0.0.1:{base}{{shard}}");
        let cancel = CancellationToken::new();
        let token = Some("internal-secret".to_owned());
        let mut groups = Vec::new();
        for shard in shards {
            let readers = Arc::new(open(vec![shard]).await.unwrap());
            readers.shards().scope_readers_to(shard);
            let read_groups = ReadGroupsConfig {
                shard: Some(shard.get()),
                endpoint: endpoint.clone(),
                ..ReadGroupsConfig::default()
            };
            readers.connect_remote_shards(Arc::new(GrpcLogsShards::new(Arc::new(
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

        let queries = [
            QueryRequest::range(r#"{job=~".+"} |= "status=201""#, start, end, 1),
            QueryRequest::range(
                r#"sum by (job) (count_over_time({job=~".+"}[1us]))"#,
                start,
                end,
                1_000,
            ),
            QueryRequest::range(
                r#"topk(3, sum by (instance) (count_over_time({job="api"} | logfmt | status="202" [5us])))"#,
                start,
                end,
                2_500,
            ),
        ];
        for coordinator in &groups {
            for request in &queries {
                let expected = everything
                    .query(&namespace, request, QueryOptions::default())
                    .await
                    .unwrap();
                let actual = coordinator
                    .query(&namespace, request, QueryOptions::default())
                    .await
                    .unwrap();
                let empty = match &expected {
                    QueryResult::Streams(streams) => streams.is_empty(),
                    QueryResult::Matrix(series) => series.is_empty(),
                    QueryResult::Vector(samples) => samples.is_empty(),
                    QueryResult::Scalar(_) => false,
                };
                assert!(!empty, "{}", request.query);
                assert_eq!(actual, expected, "{}", request.query);
            }
            assert_eq!(
                coordinator
                    .label_values(&namespace, "instance", start, end)
                    .await
                    .unwrap()
                    .len(),
                40
            );
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
        let error = GrpcLogsShards::new(Arc::new(unauthorized))
            .label_names(pb::LabelNamesRequest {
                read: Some(pb::ShardRead {
                    shard: 1,
                    namespace: Some(pb::Namespace {
                        name: "tenant".into(),
                    }),
                }),
                start_ns: start,
                end_ns: end,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Unavailable(_)), "{error:?}");

        cancel.cancel();
        everything.close().await.unwrap();
        for readers in groups {
            readers.close().await.unwrap();
        }
    }
}
