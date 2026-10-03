use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use plural_traces::{
    Namespace, TraceBatch, routing::route_trace, trace_batches_from_resource_spans,
};
use prost::Message;
use proto::traces::internal::v1::{
    Durability as ProtoDurability, Namespace as ProtoNamespace, Trace as ProtoTrace, TracePayload,
    WriteBatchRequest, WriteBatchResponse,
    internal_writer_server::{InternalWriter, InternalWriterServer},
    trace_payload,
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
        let shard = internal_rpc::shard_id(request.shard_id)?;
        let assignment = self.router.assignment().read().await;
        internal_rpc::check_ownership(
            &assignment,
            request.assignment_generation,
            shard,
            self.router.local_owner(),
        )?;
        let batches = request
            .traces
            .into_iter()
            .map(decode_payload)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|status| *status)?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        if batches
            .iter()
            .flat_map(|batch| &batch.traces)
            .any(|trace| route_trace(&assignment, &namespace, trace) != shard)
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
        let durability = match ProtoDurability::try_from(request.durability)
            .unwrap_or(ProtoDurability::Applied)
        {
            ProtoDurability::Applied => plural_traces::Durability::Applied,
            ProtoDurability::Written => plural_traces::Durability::Written,
            ProtoDurability::Durable => plural_traces::Durability::Durable,
        };
        let admitted = self
            .router
            .admit(shard)
            .await
            .map_err(|error| internal_rpc::route_status(&error))?;
        self.write_local(&namespace, shard, batches, durability)
            .await
            .map_err(traces_status)?;
        drop(admitted);
        if !request.request_id.is_empty() {
            completed.insert(request.request_id);
        }
        Ok(Response::new(WriteBatchResponse {
            assignment_generation: self.router.assignment().read().await.generation.get(),
            accepted_traces,
            accepted_spans,
        }))
    }
}

fn traces_status(error: plural_traces::Error) -> Status {
    match error {
        plural_traces::Error::Invalid(_) => Status::invalid_argument(error.to_string()),
        plural_traces::Error::Backpressure
        | plural_traces::Error::Unavailable(_)
        | plural_traces::Error::Storage(_) => Status::unavailable(error.to_string()),
        plural_traces::Error::Corrupt(_)
        | plural_traces::Error::Json(_)
        | plural_traces::Error::Protobuf(_)
        | plural_traces::Error::Compression(_)
        | plural_traces::Error::TraceQl(_) => Status::internal(error.to_string()),
        plural_traces::Error::Shard(_) => Status::unavailable(error.to_string()),
    }
}

pub fn grpc_service(state: AppState) -> InternalWriterServer<AppState> {
    InternalWriterServer::new(state)
        .max_decoding_message_size(server_common::internal_rpc::MAX_INTERNAL_MESSAGE_BYTES)
        .max_encoding_message_size(server_common::internal_rpc::MAX_INTERNAL_MESSAGE_BYTES)
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
