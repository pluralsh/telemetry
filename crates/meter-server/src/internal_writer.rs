use meter::{
    Bucket, CounterResetHint, FloatHistogram, HistogramSample, Label, Namespace, Sample, Series,
    routing::route,
};
use proto::meter::internal::v1::{
    Durability as ProtoDurability, HistogramSample as ProtoHistogram, Label as ProtoLabel,
    Namespace as ProtoNamespace, Sample as ProtoSample, Series as ProtoSeries, WriteBatchRequest,
    WriteBatchResponse,
    histogram_sample::CounterResetHint as ProtoResetHint,
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
                        value: sample.value,
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
    let h = sample.histogram;
    let hint = match h.counter_reset_hint {
        CounterResetHint::Unknown => ProtoResetHint::Unknown,
        CounterResetHint::CounterReset => ProtoResetHint::Reset,
        CounterResetHint::NotCounterReset => ProtoResetHint::NotReset,
        CounterResetHint::Gauge => ProtoResetHint::Gauge,
    };
    ProtoHistogram {
        timestamp_ms: sample.timestamp_ms,
        counter_reset_hint: hint as i32,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count: h.zero_count,
        count: h.count,
        sum: h.sum,
        positive_indexes: h.positive.iter().map(|b| b.index).collect(),
        positive_counts: h.positive.iter().map(|b| b.count).collect(),
        negative_indexes: h.negative.iter().map(|b| b.index).collect(),
        negative_counts: h.negative.iter().map(|b| b.count).collect(),
        custom_values: h.custom_values.to_vec(),
    }
}

fn from_proto_histogram(sample: ProtoHistogram) -> Result<HistogramSample, String> {
    let buckets = |indexes: Vec<i32>, counts: Vec<f64>| {
        if indexes.len() != counts.len() {
            return Err("histogram bucket indexes and counts differ in length".to_string());
        }
        Ok(indexes
            .into_iter()
            .zip(counts)
            .map(|(index, count)| Bucket { index, count })
            .collect())
    };
    let counter_reset_hint = match ProtoResetHint::try_from(sample.counter_reset_hint)
        .unwrap_or(ProtoResetHint::Unknown)
    {
        ProtoResetHint::Unknown => CounterResetHint::Unknown,
        ProtoResetHint::Reset => CounterResetHint::CounterReset,
        ProtoResetHint::NotReset => CounterResetHint::NotCounterReset,
        ProtoResetHint::Gauge => CounterResetHint::Gauge,
    };
    let histogram = FloatHistogram {
        counter_reset_hint,
        schema: sample.schema,
        zero_threshold: sample.zero_threshold,
        zero_count: sample.zero_count,
        count: sample.count,
        sum: sample.sum,
        positive: buckets(sample.positive_indexes, sample.positive_counts)?,
        negative: buckets(sample.negative_indexes, sample.negative_counts)?,
        custom_values: sample.custom_values.into(),
    };
    histogram.validate()?;
    Ok(HistogramSample::new(sample.timestamp_ms, histogram))
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
            .map(|sample| Sample::new(sample.timestamp_ms, sample.value))
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
        if self.namespace(namespace).is_none() {
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
        let meter_namespace = Namespace::new(namespace)
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
                route(&assignment, &meter_namespace, &item.labels, timestamp_ms) != shard
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
        self.write_local(&meter_namespace, shard, series, durability)
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
}
