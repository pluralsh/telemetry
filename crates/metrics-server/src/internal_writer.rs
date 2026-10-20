use plural_metrics::{
    HistogramSample, Label, Namespace, Sample, Series, histogram_from_wire, histogram_to_wire,
    routing::route,
};
use proto::metrics::internal::v1::{
    Durability as ProtoDurability, HistogramSample as ProtoHistogram, Label as ProtoLabel,
    Namespace as ProtoNamespace, Sample as ProtoSample, Series as ProtoSeries, WriteBatchRequest,
    WriteBatchResponse,
    internal_writer_server::{InternalWriter, InternalWriterServer},
};
use server_common::internal_rpc;
use sharding::ShardId;
use tonic::{Request, Response as TonicResponse, Status};

use crate::{config::Durability, state::AppState};

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
                        value: Some(sample.value),
                    })
                    .collect(),
                histograms: series
                    .histograms
                    .into_iter()
                    .map(to_proto_histogram)
                    .collect(),
            })
            .collect(),
        metadata: vec![],
        durability: proto_durability(durability) as i32,
        request_id: request_id.to_owned(),
    }
}

fn to_proto_histogram(sample: HistogramSample) -> ProtoHistogram {
    histogram_to_wire(sample.timestamp_ms, &sample.histogram)
}

fn from_proto_histogram(sample: ProtoHistogram) -> Result<HistogramSample, String> {
    histogram_from_wire(sample)
}

fn from_proto_series(series: ProtoSeries) -> Result<Series, String> {
    Ok(Series {
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
            .map(|sample| Sample::new(sample.timestamp_ms, sample.value()))
            .collect(),
        histograms: series
            .histograms
            .into_iter()
            .map(from_proto_histogram)
            .collect::<Result<_, _>>()?,
    })
}

#[tonic::async_trait]
impl InternalWriter for AppState {
    async fn write(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> Result<TonicResponse<WriteBatchResponse>, Status> {
        let token = self
            .config
            .auth
            .internal_token()
            .map_err(|error| Status::internal(error.to_string()))?;
        internal_rpc::verify(request.metadata(), token.as_deref())?;
        let request = request.into_inner();
        let namespace = request
            .namespace
            .as_ref()
            .map(|namespace| namespace.name.as_str())
            .ok_or_else(|| Status::invalid_argument("namespace is required"))?;
        if !self.has_namespace(namespace) {
            return Err(Status::not_found("unknown namespace"));
        }
        let shard = internal_rpc::shard_id(request.shard_id)?;
        let assignment = self.router.assignment().read().await;
        internal_rpc::check_ownership(
            &assignment,
            request.assignment_generation,
            shard,
            self.router.local_owner(),
        )?;
        let metrics_namespace = Namespace::new(namespace)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let mut series = Vec::with_capacity(request.series.len());
        let mut samples = 0;
        for item in request.series {
            let item = from_proto_series(item).map_err(Status::invalid_argument)?;
            let mut timestamps = item
                .samples
                .iter()
                .map(|sample| sample.timestamp_ms)
                .chain(item.histograms.iter().map(|sample| sample.timestamp_ms));
            if timestamps.any(|timestamp_ms| {
                route(&assignment, &metrics_namespace, &item.labels, timestamp_ms) != shard
            }) {
                return Err(Status::invalid_argument(
                    "misrouted_series: series does not route to requested shard",
                ));
            }
            samples += item.samples.len() + item.histograms.len();
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
        let admitted = self
            .router
            .admit(shard)
            .await
            .map_err(|error| internal_rpc::route_status(&error))?;
        self.write_local(&metrics_namespace, shard, series, durability)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        drop(admitted);
        if !request.request_id.is_empty() {
            self.completed_requests
                .lock()
                .map_err(|_| Status::internal("idempotency lock poisoned"))?
                .insert((namespace.to_owned(), request.request_id));
        }
        Ok(TonicResponse::new(WriteBatchResponse {
            assignment_generation: self.router.assignment().read().await.generation.get(),
            accepted_series,
            accepted_samples: samples as u64,
        }))
    }
}

pub fn grpc_service(state: AppState) -> InternalWriterServer<AppState> {
    InternalWriterServer::new(state)
        .max_decoding_message_size(server_common::internal_rpc::MAX_INTERNAL_MESSAGE_BYTES)
        .max_encoding_message_size(server_common::internal_rpc::MAX_INTERNAL_MESSAGE_BYTES)
}

#[cfg(test)]
mod tests {
    use plural_metrics::FloatHistogram;
    use prost::Message;

    use super::*;

    #[test]
    fn should_round_trip_signed_zeros_bit_exactly() {
        // given
        let histogram = FloatHistogram {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 0.0,
            sum: -0.0,
            ..FloatHistogram::default()
        };
        let series = Series {
            labels: vec![Label::new("__name__", "m")],
            metric_type: None,
            unit: None,
            description: None,
            samples: vec![Sample::new(1, -0.0), Sample::new(2, 0.0)],
            histograms: vec![HistogramSample::new(3, histogram)],
        };
        let request = to_proto_request(
            "alpha",
            ShardId::new(0),
            1,
            "r",
            Durability::Applied,
            vec![series],
        );

        // when
        let decoded = WriteBatchRequest::decode(request.encode_to_vec().as_slice()).unwrap();
        let series = from_proto_series(decoded.series.into_iter().next().unwrap()).unwrap();

        // then
        let bits: Vec<u64> = series.samples.iter().map(|s| s.value.to_bits()).collect();
        assert_eq!(bits, [(-0.0f64).to_bits(), 0.0f64.to_bits()]);
        assert_eq!(
            series.histograms[0].histogram.sum.to_bits(),
            (-0.0f64).to_bits()
        );
    }
}
