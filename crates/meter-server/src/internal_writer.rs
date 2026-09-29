use meter::{Label, Namespace, Sample, Series, ShardingOptions};
use proto::meter::internal::v1::{
    Durability as ProtoDurability, Label as ProtoLabel, Namespace as ProtoNamespace,
    Sample as ProtoSample, Series as ProtoSeries, WriteBatchRequest, WriteBatchResponse,
    internal_writer_server::{InternalWriter, InternalWriterServer},
};
use sharding::ShardId;
use tonic::{Request, Response as TonicResponse, Status};

use crate::{auth::secure_eq, config::Durability, state::AppState};

fn proto_durability(durability: Durability) -> ProtoDurability {
    match durability {
        Durability::Applied => ProtoDurability::Applied,
        Durability::Written => ProtoDurability::Written,
        Durability::Durable => ProtoDurability::Durable,
    }
}

pub(crate) fn to_proto_request(
    namespace: &str,
    shard: ShardId,
    generation: u64,
    request_id: &str,
    durability: Durability,
    series: Vec<Series>,
) -> WriteBatchRequest {
    WriteBatchRequest {
        namespace: Some(ProtoNamespace {
            name: namespace.to_owned(),
        }),
        assignment_generation: generation,
        shard_id: u64::from(shard.get()),
        series: series
            .into_iter()
            .map(|series| ProtoSeries {
                labels: series
                    .labels
                    .into_iter()
                    .map(|label| ProtoLabel {
                        name: label.name,
                        value: label.value,
                    })
                    .collect(),
                samples: series
                    .samples
                    .into_iter()
                    .map(|sample| ProtoSample {
                        timestamp_ms: sample.timestamp_ms,
                        value: sample.value,
                    })
                    .collect(),
            })
            .collect(),
        metadata: vec![],
        durability: proto_durability(durability) as i32,
        request_id: request_id.to_owned(),
    }
}

fn from_proto_series(series: ProtoSeries) -> Series {
    Series {
        labels: series
            .labels
            .into_iter()
            .map(|label| Label::new(label.name, label.value))
            .collect(),
        metric_type: None,
        unit: None,
        description: None,
        samples: series
            .samples
            .into_iter()
            .map(|sample| Sample::new(sample.timestamp_ms, sample.value))
            .collect(),
    }
}

#[tonic::async_trait]
impl InternalWriter for AppState {
    async fn write(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> Result<TonicResponse<WriteBatchResponse>, Status> {
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
        if self.namespace(namespace).is_none() {
            return Err(Status::not_found("unknown namespace"));
        }
        let shard_u32 = u32::try_from(request.shard_id)
            .map_err(|_| Status::invalid_argument("shard id is out of range"))?;
        let shard = ShardId::new(shard_u32);
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
        let meter_namespace = Namespace::new(namespace)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let options = ShardingOptions::new(
            assignment.virtual_shards,
            self.config.sharding.io_concurrency_limit,
        )
        .map_err(|error| Status::internal(error.to_string()))?;
        let mut series = Vec::with_capacity(request.series.len());
        let mut samples = 0;
        for item in request.series {
            let item = from_proto_series(item);
            if options.route(&assignment.routing, &meter_namespace, &item.labels) != shard {
                return Err(Status::invalid_argument(
                    "misrouted_series: series does not route to requested shard",
                ));
            }
            samples += item.samples.len();
            series.push(item);
        }
        let accepted_series = series.len() as u64;
        if !request.request_id.is_empty()
            && self
                .completed_requests
                .lock()
                .map_err(|_| Status::internal("idempotency lock poisoned"))?
                .contains(&(namespace.to_owned(), request.request_id.clone()))
        {
            return Ok(TonicResponse::new(WriteBatchResponse {
                assignment_generation: assignment.generation.get(),
                accepted_series,
                accepted_samples: samples as u64,
            }));
        }
        drop(assignment);
        let durability = match ProtoDurability::try_from(request.durability)
            .unwrap_or(ProtoDurability::Applied)
        {
            ProtoDurability::Applied => Durability::Applied,
            ProtoDurability::Written => Durability::Written,
            ProtoDurability::Durable => Durability::Durable,
        };
        self.write_local(namespace, shard, series, durability)
            .await
            .map_err(|error| Status::unavailable(error.message))?;
        if !request.request_id.is_empty() {
            self.completed_requests
                .lock()
                .map_err(|_| Status::internal("idempotency lock poisoned"))?
                .insert((namespace.to_owned(), request.request_id));
        }
        Ok(TonicResponse::new(WriteBatchResponse {
            assignment_generation: self.assignment.read().await.generation.get(),
            accepted_series,
            accepted_samples: samples as u64,
        }))
    }
}

pub fn grpc_service(state: AppState) -> InternalWriterServer<AppState> {
    InternalWriterServer::new(state)
}
