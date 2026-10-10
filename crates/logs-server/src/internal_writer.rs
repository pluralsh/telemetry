use plural_logs::{Field, Fields, Label, Labels, LogBatch, LogEntry, Namespace, routing::route};
use proto::logs::internal::v1::{
    Durability as ProtoDurability, Field as ProtoField, Label as ProtoLabel,
    LogBatch as ProtoLogBatch, LogEntry as ProtoLogEntry, Namespace as ProtoNamespace,
    WriteBatchRequest, WriteBatchResponse,
    internal_writer_server::{InternalWriter, InternalWriterServer},
};
use server_common::internal_rpc;
use sharding::ShardId;
use tonic::{Request, Response, Status};

use crate::{config::Durability, state::AppState};

pub(crate) fn to_proto_request(
    namespace: &Namespace,
    shard: ShardId,
    generation: u64,
    request_id: &str,
    durability: Durability,
    batches: Vec<LogBatch>,
) -> WriteBatchRequest {
    WriteBatchRequest {
        namespace: Some(ProtoNamespace {
            name: namespace.as_str().to_owned(),
        }),
        assignment_generation: generation,
        shard_id: u64::from(shard.get()),
        batches: batches
            .into_iter()
            .map(|batch| ProtoLogBatch {
                labels: batch
                    .labels
                    .iter()
                    .map(|label| ProtoLabel {
                        name: label.name.clone(),
                        value: label.value.clone(),
                    })
                    .collect(),
                entries: batch
                    .entries
                    .into_iter()
                    .map(|entry| ProtoLogEntry {
                        timestamp_ns: entry.timestamp_ns,
                        line: entry.line,
                        structured_metadata: entry
                            .structured_metadata
                            .iter()
                            .map(|field| ProtoField {
                                name: field.name.clone(),
                                value: field.value.clone(),
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
        durability: match durability {
            Durability::Applied => ProtoDurability::Applied,
            Durability::Written => ProtoDurability::Written,
            Durability::Durable => ProtoDurability::Durable,
        } as i32,
        request_id: request_id.to_owned(),
    }
}

fn from_proto_batch(batch: ProtoLogBatch) -> Result<LogBatch, String> {
    let labels = Labels::new(
        batch
            .labels
            .into_iter()
            .map(|label| Label::new(label.name, label.value))
            .collect(),
    )
    .map_err(|error| error.to_string())?;
    let entries = batch
        .entries
        .into_iter()
        .map(|entry| {
            Fields::new(
                entry
                    .structured_metadata
                    .into_iter()
                    .map(|field| Field::new(field.name, field.value))
                    .collect(),
            )
            .map(|fields| {
                LogEntry::with_structured_metadata(entry.timestamp_ns, entry.line, fields)
            })
            .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(LogBatch::new(labels, entries))
}

#[tonic::async_trait]
impl InternalWriter for AppState {
    async fn write(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> Result<Response<WriteBatchResponse>, Status> {
        let token = self
            .config
            .auth
            .internal
            .as_ref()
            .map(|secret| secret.expose())
            .transpose()
            .map_err(|error| Status::internal(error.to_string()))?;
        internal_rpc::verify(request.metadata(), token.as_deref())?;
        let request = request.into_inner();
        let namespace = request
            .namespace
            .as_ref()
            .map(|namespace| namespace.name.as_str())
            .ok_or_else(|| Status::invalid_argument("namespace is required"))?;
        if !self.live().namespaces.contains_key(namespace) {
            return Err(Status::not_found("unknown namespace"));
        }
        let namespace = Namespace::new(namespace)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let shard = internal_rpc::shard_id(request.shard_id)?;
        let assignment = self.router.assignment().read().await;
        internal_rpc::check_ownership(
            &assignment,
            request.assignment_generation,
            shard,
            self.router.local_owner(),
        )?;
        let batches = request
            .batches
            .into_iter()
            .map(from_proto_batch)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Status::invalid_argument)?;
        if batches.iter().any(|batch| {
            batch.entries.iter().any(|entry| {
                route(&assignment, &namespace, &batch.labels, entry.timestamp_ns) != shard
            })
        }) {
            return Err(Status::invalid_argument(
                "misrouted_batch: stream does not route to requested shard",
            ));
        }
        let accepted_streams = batches.len() as u64;
        let accepted_entries = batches.iter().map(|batch| batch.entries.len() as u64).sum();
        let mut idempotency = if request.request_id.is_empty() {
            None
        } else {
            Some(self.completed_requests.lock().await)
        };
        if let Some(requests) = idempotency.as_ref()
            && requests.contains(&request.request_id)
        {
            return Ok(Response::new(WriteBatchResponse {
                assignment_generation: assignment.generation.get(),
                accepted_streams,
                accepted_entries,
            }));
        }
        drop(assignment);
        // The wire durability is authoritative for forwarded requests.
        let durability = match ProtoDurability::try_from(request.durability)
            .unwrap_or(ProtoDurability::Applied)
        {
            ProtoDurability::Applied => plural_logs::Durability::Applied,
            ProtoDurability::Written => plural_logs::Durability::Written,
            ProtoDurability::Durable => plural_logs::Durability::Durable,
        };
        let admitted = self
            .router
            .admit(shard)
            .await
            .map_err(|error| internal_rpc::route_status(&error))?;
        self.write_local(&namespace, shard, batches, durability)
            .await
            .map_err(logs_status)?;
        drop(admitted);
        if let Some(requests) = idempotency.as_mut() {
            requests.insert(request.request_id);
        }
        Ok(Response::new(WriteBatchResponse {
            assignment_generation: self.router.assignment().read().await.generation.get(),
            accepted_streams,
            accepted_entries,
        }))
    }
}

fn logs_status(error: plural_logs::Error) -> Status {
    match error {
        plural_logs::Error::Invalid(_) => Status::invalid_argument(error.to_string()),
        plural_logs::Error::Backpressure
        | plural_logs::Error::Unavailable(_)
        | plural_logs::Error::Storage(_)
        | plural_logs::Error::Shard(_) => Status::unavailable(error.to_string()),
        plural_logs::Error::Corrupt(_)
        | plural_logs::Error::Json(_)
        | plural_logs::Error::Query(_)
        | plural_logs::Error::Regex(_) => Status::internal(error.to_string()),
    }
}

pub fn grpc_service(state: AppState) -> InternalWriterServer<AppState> {
    InternalWriterServer::new(state)
        .max_decoding_message_size(server_common::internal_rpc::MAX_INTERNAL_MESSAGE_BYTES)
        .max_encoding_message_size(server_common::internal_rpc::MAX_INTERNAL_MESSAGE_BYTES)
}

#[cfg(test)]
mod tests {
    use common::storage::config::StorageConfig;
    use plural_logs::{Direction, QueryOptions, QueryRequest, QueryResult};

    use super::*;
    use crate::config::{Config, NamespaceConfig};

    async fn state() -> AppState {
        AppState::open(Config {
            storage: StorageConfig::InMemory,
            sharding: crate::config::ShardingConfig {
                shards: 2,
                ..Default::default()
            },
            namespaces: vec![NamespaceConfig {
                name: "tenant".into(),
                auth: Default::default(),
                usage_reporting_endpoint: None,
            }],
            ..Config::default()
        })
        .await
        .unwrap()
    }

    fn request(generation: u64, request_id: &str) -> WriteBatchRequest {
        let namespace = Namespace::new("tenant").unwrap();
        let routing = sharding::ShardMap::new(
            sharding::AssignmentGeneration::new(1),
            2,
            vec![sharding::Assignment::new(
                sharding::Owner::new("test", 0),
                sharding::ShardRange::within(0, 2, 2).unwrap(),
                sharding::AssignmentState::Active,
            )],
        )
        .unwrap();
        let labels = (0..10_000)
            .map(|candidate| {
                Labels::new(vec![Label::new("app", format!("api-{candidate}"))]).unwrap()
            })
            .find(|labels| route(&routing, &namespace, labels, 1).get() == 1)
            .unwrap();
        to_proto_request(
            &namespace,
            ShardId::new(1),
            generation,
            request_id,
            Durability::Durable,
            vec![LogBatch::new(labels, vec![LogEntry::new(1, "once")])],
        )
    }

    #[tokio::test]
    async fn rejects_stale_generation_and_deduplicates_retries() {
        let state = state().await;
        let stale = InternalWriter::write(&state, Request::new(request(0, "stale")))
            .await
            .unwrap_err();
        assert_eq!(stale.code(), tonic::Code::FailedPrecondition);

        for _ in 0..2 {
            InternalWriter::write(&state, Request::new(request(1, "same-request")))
                .await
                .unwrap();
        }
        let namespace = Namespace::new("tenant").unwrap();
        let result = state
            .db
            .query(
                &namespace,
                &QueryRequest::range(r#"{app=~".+"}"#, 0, 2, 1),
                QueryOptions {
                    direction: Direction::Forward,
                    ..QueryOptions::default()
                },
            )
            .await
            .unwrap();
        let QueryResult::Streams(streams) = result else {
            panic!("expected streams");
        };
        assert_eq!(
            streams
                .iter()
                .map(|stream| stream.entries.len())
                .sum::<usize>(),
            1
        );
        state.shutdown().await.unwrap();
    }
}
