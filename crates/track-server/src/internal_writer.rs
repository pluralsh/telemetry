use meter_server::auth::secure_eq;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use prost::Message;
use proto::track::internal::v1::{
    Durability as ProtoDurability, Namespace as ProtoNamespace, Trace as ProtoTrace, TracePayload,
    WriteBatchRequest, WriteBatchResponse,
    internal_writer_server::{InternalWriter, InternalWriterServer},
    trace_payload,
};
use sharding::ShardId;
use tonic::{Request, Response, Status};
use track::{Namespace, ShardingOptions, TraceBatch, trace_batches_from_resource_spans};

use crate::{config::Durability, state::AppState};

pub(crate) fn to_proto_request(
    namespace: &Namespace,
    shard: ShardId,
    generation: u64,
    request_id: &str,
    durability: Durability,
    batches: Vec<TraceBatch>,
) -> anyhow::Result<WriteBatchRequest> {
    let mut traces = Vec::new();
    for trace in batches.into_iter().flat_map(|batch| batch.traces) {
        let mut encoded_resources = Vec::new();
        for resource in trace.resource_spans {
            encoded_resources.push(resource.encode_to_vec());
        }
        traces.push(TracePayload {
            payload: Some(trace_payload::Payload::Trace(
                ProtoTrace {
                    resource_spans: encoded_resources,
                }
                .encode_to_vec(),
            )),
        });
    }
    Ok(WriteBatchRequest {
        namespace: Some(ProtoNamespace {
            name: namespace.as_str().to_owned(),
        }),
        assignment_generation: generation,
        shard_id: u64::from(shard.get()),
        traces,
        durability: match durability {
            Durability::Applied => ProtoDurability::Applied,
            Durability::Written => ProtoDurability::Written,
            Durability::Durable => ProtoDurability::Durable,
        } as i32,
        request_id: request_id.to_owned(),
    })
}

fn decode_payload(payload: TracePayload) -> Result<Vec<TraceBatch>, Box<Status>> {
    let resources = match payload.payload {
        Some(trace_payload::Payload::ResourceSpans(bytes)) => {
            vec![
                ResourceSpans::decode(bytes.as_slice())
                    .map_err(|error| Box::new(Status::invalid_argument(error.to_string())))?,
            ]
        }
        Some(trace_payload::Payload::Trace(bytes)) => {
            let trace = ProtoTrace::decode(bytes.as_slice())
                .map_err(|error| Box::new(Status::invalid_argument(error.to_string())))?;
            if trace.resource_spans.is_empty() {
                return Err(Box::new(Status::invalid_argument(
                    "encoded Trace must contain ResourceSpans",
                )));
            }
            trace
                .resource_spans
                .into_iter()
                .map(|bytes| {
                    ResourceSpans::decode(bytes.as_slice())
                        .map_err(|error| Box::new(Status::invalid_argument(error.to_string())))
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        None => {
            return Err(Box::new(Status::invalid_argument(
                "trace payload is required",
            )));
        }
    };
    trace_batches_from_resource_spans(resources)
        .map_err(|error| Box::new(Status::invalid_argument(error.to_string())))
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
        let namespace_name = request
            .namespace
            .as_ref()
            .map(|namespace| namespace.name.as_str())
            .ok_or_else(|| Status::invalid_argument("namespace is required"))?;
        if !self.namespaces.contains_key(namespace_name) {
            return Err(Status::not_found("unknown namespace"));
        }
        let namespace = Namespace::new(namespace_name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let shard = ShardId::new(
            request
                .shard_id
                .try_into()
                .map_err(|_| Status::invalid_argument("shard id is out of range"))?,
        );
        let assignment = self.assignment.read().await;
        if request.assignment_generation != assignment.generation.get() {
            return Err(Status::failed_precondition("stale_ownership"));
        }
        if assignment
            .owner_of(shard)
            .is_none_or(|owner| owner.id != self.local_owner)
        {
            return Err(Status::failed_precondition("non_owner"));
        }
        let mut batches = request
            .traces
            .into_iter()
            .map(decode_payload)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|status| *status)?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let options = ShardingOptions::new(
            self.config.sharding.virtual_shards,
            self.config.sharding.io_concurrency_multiplier,
        )
        .map_err(|error| Status::internal(error.to_string()))?;
        if batches
            .iter()
            .flat_map(|batch| &batch.traces)
            .any(|trace| options.route(&namespace, trace.trace_id) != shard)
        {
            return Err(Status::invalid_argument("misrouted_trace"));
        }
        let accepted_traces = batches.iter().map(|batch| batch.traces.len() as u64).sum();
        let accepted_spans = batches
            .iter()
            .flat_map(|batch| &batch.traces)
            .map(|trace| trace.spans().count() as u64)
            .sum();
        let mut completed = self.completed_requests.lock().await;
        if !request.request_id.is_empty() && completed.contains(&request.request_id) {
            return Ok(Response::new(WriteBatchResponse {
                assignment_generation: assignment.generation.get(),
                accepted_traces,
                accepted_spans,
            }));
        }
        drop(assignment);
        if self.draining_shards.read().await.contains(&shard) {
            return Err(Status::unavailable("local shard is draining"));
        }
        let durability = match ProtoDurability::try_from(request.durability)
            .unwrap_or(ProtoDurability::Applied)
        {
            ProtoDurability::Applied => track::Durability::Applied,
            ProtoDurability::Written => track::Durability::Written,
            ProtoDurability::Durable => track::Durability::Durable,
        };
        let database = self
            .db
            .shard(shard)
            .await
            .ok_or_else(|| Status::unavailable("local shard is not open"))?;
        database
            .write_with_durability(&namespace, std::mem::take(&mut batches), durability)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if !request.request_id.is_empty() {
            completed.insert(request.request_id);
        }
        Ok(Response::new(WriteBatchResponse {
            assignment_generation: self.assignment.read().await.generation.get(),
            accepted_traces,
            accepted_spans,
        }))
    }
}

pub fn grpc_service(state: AppState) -> InternalWriterServer<AppState> {
    InternalWriterServer::new(state)
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    use super::*;

    #[test]
    fn accepts_both_internal_payload_encodings_and_rejects_empty() {
        let resource = ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![1; 8],
                    start_time_unix_nano: 1,
                    end_time_unix_nano: 2,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let direct = TracePayload {
            payload: Some(trace_payload::Payload::ResourceSpans(
                resource.encode_to_vec(),
            )),
        };
        assert_eq!(decode_payload(direct).unwrap().len(), 1);
        let envelope = TracePayload {
            payload: Some(trace_payload::Payload::Trace(
                ProtoTrace {
                    resource_spans: vec![resource.encode_to_vec()],
                }
                .encode_to_vec(),
            )),
        };
        assert_eq!(decode_payload(envelope).unwrap().len(), 1);
        assert!(decode_payload(TracePayload { payload: None }).is_err());
    }
}
