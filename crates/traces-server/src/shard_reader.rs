//! The `ShardReader` gRPC service, and the client a reader group member uses
//! to read every shard but its own through the other groups.

// `tonic::Status` is the gRPC error boundary of this module.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};
use plural_traces::{Error, ShardedTraces, TracesShardClient, traceql::QueryError};
use proto::traces::internal::v1::{
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
        Error::Invalid(_) => Status::invalid_argument(error.to_string()),
        Error::TraceQl(_) => Status::failed_precondition(error.to_string()),
        Error::Corrupt(_) | Error::Json(_) | Error::Protobuf(_) | Error::Compression(_) => {
            Status::internal(error.to_string())
        }
        Error::Backpressure | Error::Unavailable(_) | Error::Storage(_) | Error::Shard(_) => {
            Status::unavailable(error.to_string())
        }
    }
}

fn status_error(status: Status) -> Error {
    let message = status.message().to_owned();
    match status.code() {
        tonic::Code::InvalidArgument => Error::Invalid(message),
        tonic::Code::FailedPrecondition => Error::TraceQl(QueryError::Remote(message)),
        tonic::Code::Internal => Error::Corrupt(message),
        _ => Error::Unavailable(format!("shard reader: {message}")),
    }
}

/// Serves this reader's own shard to coordinating readers.
pub struct ShardReaderService {
    readers: Arc<ShardedTraces>,
    token: Option<String>,
}

impl ShardReaderService {
    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        internal_rpc::verify(request.metadata(), self.token.as_deref())
    }
}

pub fn shard_reader_service(
    readers: Arc<ShardedTraces>,
    token: Option<String>,
) -> ShardReaderServer<ShardReaderService> {
    ShardReaderServer::new(ShardReaderService { readers, token })
        .max_decoding_message_size(MAX_INTERNAL_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_INTERNAL_MESSAGE_BYTES)
}

macro_rules! serve {
    ($self:ident, $request:ident, $method:ident) => {{
        $self.authorize(&$request)?;
        $self
            .readers
            .$method($request.get_ref())
            .await
            .map(Response::new)
            .map_err(error_status)
    }};
}

#[tonic::async_trait]
impl ShardReader for ShardReaderService {
    type SearchStream = futures::stream::BoxStream<'static, Result<pb::TracesResponse, Status>>;

    async fn get_trace(
        &self,
        request: Request<pb::GetTraceRequest>,
    ) -> Result<Response<pb::TracesResponse>, Status> {
        serve!(self, request, serve_get_trace)
    }

    async fn search(
        &self,
        request: Request<pb::SearchRequest>,
    ) -> Result<Response<Self::SearchStream>, Status> {
        self.authorize(&request)?;
        let responses = self
            .readers
            .serve_search(request.get_ref())
            .await
            .map_err(error_status)?;
        Ok(Response::new(
            stream::iter(responses.into_iter().map(Ok)).boxed(),
        ))
    }

    async fn scan(
        &self,
        request: Request<pb::ScanRequest>,
    ) -> Result<Response<pb::TracesResponse>, Status> {
        serve!(self, request, serve_scan)
    }

    async fn catalog_names(
        &self,
        request: Request<pb::CatalogNamesRequest>,
    ) -> Result<Response<pb::StringsResponse>, Status> {
        serve!(self, request, serve_catalog_names)
    }

    async fn catalog_values(
        &self,
        request: Request<pb::CatalogValuesRequest>,
    ) -> Result<Response<pb::CatalogValuesResponse>, Status> {
        serve!(self, request, serve_catalog_values)
    }

    async fn trace_ql(
        &self,
        request: Request<pb::TraceQlRequest>,
    ) -> Result<Response<pb::TraceQlResponse>, Status> {
        serve!(self, request, serve_traceql)
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
pub struct GrpcTracesShards {
    groups: Arc<ReadGroups>,
}

impl GrpcTracesShards {
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
impl TracesShardClient for GrpcTracesShards {
    async fn get_trace(
        &self,
        request: pb::GetTraceRequest,
    ) -> plural_traces::Result<pb::TracesResponse> {
        unary!(self, request, get_trace)
    }

    async fn search(
        &self,
        request: pb::SearchRequest,
    ) -> plural_traces::Result<Vec<pb::TracesResponse>> {
        let shard = shard_of(request.read.as_ref());
        let groups = &self.groups;
        let (_, responses) = groups
            .open_stream(shard, None, |replica| {
                let request = groups.request(request.clone());
                async move { client(&replica).search(request?).await }
            })
            .await
            .map_err(status_error)?;
        responses.try_collect().await.map_err(status_error)
    }

    async fn scan(&self, request: pb::ScanRequest) -> plural_traces::Result<pb::TracesResponse> {
        unary!(self, request, scan)
    }

    async fn catalog_names(
        &self,
        request: pb::CatalogNamesRequest,
    ) -> plural_traces::Result<pb::StringsResponse> {
        unary!(self, request, catalog_names)
    }

    async fn catalog_values(
        &self,
        request: pb::CatalogValuesRequest,
    ) -> plural_traces::Result<pb::CatalogValuesResponse> {
        unary!(self, request, catalog_values)
    }

    async fn traceql(
        &self,
        request: pb::TraceQlRequest,
    ) -> plural_traces::Result<pb::TraceQlResponse> {
        unary!(self, request, trace_ql)
    }
}

#[cfg(test)]
mod tests {
    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
    };
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use plural_traces::{
        AttributeScope, Config, Durability, Namespace, QueryOptions, ShardingOptions, Trace,
        TraceBatch, TraceId,
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

    fn trace(id: u8, timestamp: u64) -> Trace {
        let trace_id = TraceId::new([id; 16]).unwrap();
        Trace::new(
            trace_id,
            vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: trace_id.as_bytes().to_vec(),
                        span_id: vec![id; 8],
                        name: format!("span-{id}"),
                        start_time_unix_nano: timestamp,
                        end_time_unix_nano: timestamp + 5,
                        attributes: vec![KeyValue {
                            key: "n".to_owned(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::IntValue(i64::from(id))),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
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
        let options = ShardingOptions::new(2, 16).unwrap();
        let namespace = Namespace::new("tenant").unwrap();
        let shards = [ShardId::new(0), ShardId::new(1)];
        let routing = assignment();
        let traces = (1..=24)
            .map(|id| trace(id, 10_000 - u64::from(id) * 100))
            .collect::<Vec<_>>();
        let writers = ShardedTraces::open(config.clone(), options, shards)
            .await
            .unwrap();
        writers
            .write(
                &routing,
                &namespace,
                vec![TraceBatch::new(traces.clone())],
                Durability::Written,
            )
            .await
            .unwrap();
        writers.flush().await.unwrap();
        writers.close().await.unwrap();

        let open = |shards: Vec<ShardId>| {
            ShardedTraces::open_readers(
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
        let base = 6_000 + std::process::id() % 500;
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
            readers.connect_remote_shards(Arc::new(GrpcTracesShards::new(Arc::new(
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

        let query = QueryOptions {
            limit: 7,
            ..QueryOptions::default()
        };
        for coordinator in &groups {
            for source in ["{}", "{ .n > 10 }", "{ name = \"span-3\" }"] {
                let expected = everything
                    .query_traceql(&routing, &namespace, 0, 20_000, source, query)
                    .await
                    .unwrap();
                assert!(!expected.is_empty(), "{source}");
                assert_eq!(
                    coordinator
                        .query_traceql(&routing, &namespace, 0, 20_000, source, query)
                        .await
                        .unwrap(),
                    expected,
                    "{source}"
                );
                assert_eq!(
                    coordinator
                        .search_traceql(&routing, &namespace, 0, 20_000, source, query)
                        .await
                        .unwrap(),
                    everything
                        .search_traceql(&routing, &namespace, 0, 20_000, source, query)
                        .await
                        .unwrap(),
                    "{source}"
                );
            }
            assert_eq!(
                coordinator
                    .search(&namespace, 0, 20_000, &[])
                    .await
                    .unwrap(),
                everything.search(&namespace, 0, 20_000, &[]).await.unwrap(),
            );
            assert_eq!(
                coordinator.scan_traces(&namespace, 5).await.unwrap(),
                everything.scan_traces(&namespace, 5).await.unwrap(),
            );
            assert_eq!(
                coordinator
                    .catalog_values(&namespace, 0, 20_000, Some(AttributeScope::Span), "n")
                    .await
                    .unwrap()
                    .len(),
                24
            );
            for trace in &traces {
                assert_eq!(
                    coordinator
                        .get_trace(&routing, &namespace, trace.trace_id)
                        .await
                        .unwrap()
                        .as_ref(),
                    Some(trace)
                );
            }
        }
        let invalid = groups[0]
            .query_traceql(&routing, &namespace, 0, 20_000, "{ .n = }", query)
            .await
            .unwrap_err();
        assert!(matches!(invalid, Error::TraceQl(_)), "{invalid:?}");

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
        let error = GrpcTracesShards::new(Arc::new(unauthorized))
            .catalog_names(pb::CatalogNamesRequest {
                read: Some(pb::ShardRead {
                    shard: 1,
                    namespace: Some(pb::Namespace {
                        name: "tenant".into(),
                    }),
                }),
                start_ns: 0,
                end_ns: 20_000,
                scope: 0,
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
