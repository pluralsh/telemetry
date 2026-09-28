use line::{Field, Fields, Label, Labels, LogBatch, LogEntry, Namespace, ShardingOptions};
use meter_server::auth::secure_eq;
use proto::line::internal::v1::{
    Durability as ProtoDurability, Field as ProtoField, Label as ProtoLabel,
    LogBatch as ProtoLogBatch, LogEntry as ProtoLogEntry, Namespace as ProtoNamespace,
    WriteBatchRequest, WriteBatchResponse,
    internal_writer_server::{InternalWriter, InternalWriterServer},
};
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
        if let Some(secret) = &self.config.auth.internal {
            let expected = secret
                .expose()
                .map_err(|error| Status::internal(error.to_string()))?;
            let provided = request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
                .unwrap_or_default();
            if !secure_eq(provided, &expected) {
                return Err(Status::unauthenticated(
                    "invalid internal cluster credential",
                ));
            }
        }
        let request = request.into_inner();
        let namespace = request
            .namespace
            .as_ref()
            .map(|namespace| namespace.name.as_str())
            .ok_or_else(|| Status::invalid_argument("namespace is required"))?;
        if !self.namespaces.contains_key(namespace) {
            return Err(Status::not_found("unknown namespace"));
        }
        let namespace = Namespace::new(namespace)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let shard = ShardId::new(
            request
                .shard_id
                .try_into()
                .map_err(|_| Status::invalid_argument("shard id is out of range"))?,
        );
        let assignment = self.assignment.read().await;
        if request.assignment_generation != assignment.generation.get() {
            return Err(Status::failed_precondition(format!(
                "stale_ownership: requested generation {}, current generation {}",
                request.assignment_generation,
                assignment.generation.get()
            )));
        }
        if assignment
            .owner_of(shard)
            .is_none_or(|owner| owner.id != self.local_owner)
        {
            return Err(Status::failed_precondition(
                "non_owner: shard is not owned by this server",
            ));
        }
        let options = ShardingOptions::new(
            assignment.virtual_shards,
            self.config.sharding.io_concurrency_multiplier,
        )
        .map_err(|error| Status::internal(error.to_string()))?;
        let mut batches = request
            .batches
            .into_iter()
            .map(from_proto_batch)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Status::invalid_argument)?;
        if batches
            .iter()
            .any(|batch| options.route(&assignment.routing, &namespace, &batch.labels) != shard)
        {
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
        if self.draining_shards.read().await.contains(&shard) {
            return Err(Status::unavailable("local shard is draining"));
        }
        // The wire durability is authoritative for forwarded requests.
        let durability = match ProtoDurability::try_from(request.durability)
            .unwrap_or(ProtoDurability::Applied)
        {
            ProtoDurability::Applied => line::Durability::Applied,
            ProtoDurability::Written => line::Durability::Written,
            ProtoDurability::Durable => line::Durability::Durable,
        };
        let database = self
            .db
            .shard(shard)
            .await
            .ok_or_else(|| Status::unavailable("local shard is not open"))?;
        if let Err(error) = database
            .write_with_durability(&namespace, std::mem::take(&mut batches), durability)
            .await
        {
            return Err(Status::unavailable(error.to_string()));
        }
        if let Some(requests) = idempotency.as_mut() {
            requests.insert(request.request_id);
        }
        self.mark_dirty();
        self.invalidate_queries().await;
        Ok(Response::new(WriteBatchResponse {
            assignment_generation: self.assignment.read().await.generation.get(),
            accepted_streams,
            accepted_entries,
        }))
    }
}

pub fn grpc_service(state: AppState) -> InternalWriterServer<AppState> {
    InternalWriterServer::new(state)
}

#[cfg(test)]
mod tests {
    use common::storage::config::StorageConfig;
    use line::{Direction, QueryOptions, QueryRequest, QueryResult};

    use super::*;
    use crate::config::{Config, NamespaceConfig};

    async fn state() -> AppState {
        AppState::open(Config {
            storage: StorageConfig::InMemory,
            sharding: crate::config::ShardingConfig {
                virtual_shards: 2,
                ..Default::default()
            },
            namespaces: vec![NamespaceConfig {
                name: "tenant".into(),
                auth: Default::default(),
            }],
            ..Config::default()
        })
        .await
        .unwrap()
    }

    fn request(generation: u64, request_id: &str) -> WriteBatchRequest {
        let namespace = Namespace::new("tenant").unwrap();
        let options = ShardingOptions::new(2, 4).unwrap();
        let routing = sharding::HashRangeMap::bootstrap(2).unwrap();
        let labels = (0..10_000)
            .map(|candidate| {
                Labels::new(vec![Label::new("app", format!("api-{candidate}"))]).unwrap()
            })
            .find(|labels| options.route(&routing, &namespace, labels).get() == 1)
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
