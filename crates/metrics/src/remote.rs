//! Reading storage shards that other reader groups serve.
//!
//! A reader serving one shard coordinates a query over all of them: its own
//! shard is read in process and every other shard through a
//! [`MetricsShardClient`], the transport the server provides. This module
//! owns the wire format on both ends: it encodes what the coordinator asks
//! for, and `ShardedMetrics::serve_*` answer those requests from the local
//! shard on the receiving side. Remote shards plug in below the PromQL
//! engine, at the [`SeriesSource`] boundary, so evaluation stays exact.

use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::stream::BoxStream;
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use promql_parser::label::{MatchOp, Matcher, Matchers};
use promql_parser::parser::VectorSelector;
use promql_parser::parser::token::{T_EQL_REGEX, T_NEQ_REGEX};
use proto::metrics::internal::v1 as pb;
use sharding::ShardId;

use crate::histogram::{Bucket, CounterResetHint, FloatHistogram};
use crate::model::{Label, Labels, MetricMetadata, MetricType, Temporality};
use crate::promql::memory::QueryError as SourceQueryError;
use crate::promql::source::{
    ResolvedSeriesChunk, ResolvedSeriesRef, SampleBatch, SampleBlock, SamplesRequest, SeriesSource,
    TimeRange,
};
use crate::promql::source_adapter::{decode_bucket, encode_bucket};
use crate::sharded::ShardedMetrics;
use crate::{Error, HistogramSample, Namespace, QueryError};

/// Float samples per response message before a sample batch is split.
const SAMPLES_PER_MESSAGE: usize = 256 * 1024;

pub type WireStream<T> = BoxStream<'static, Result<T, QueryError>>;

/// Transport to the reader groups serving other shards.
#[async_trait]
pub trait MetricsShardClient: Send + Sync + 'static {
    /// One query's reads of `shard`. The resolve and sample calls of a query
    /// should stay on the replica that answered first, whose caches the
    /// resolve warmed.
    fn query(&self, shard: ShardId) -> Arc<dyn MetricsShardQuery>;

    async fn series(&self, request: pb::SeriesRequest) -> Result<pb::SeriesResponse, QueryError>;

    async fn label_names(
        &self,
        request: pb::LabelNamesRequest,
    ) -> Result<pb::StringsResponse, QueryError>;

    async fn label_values(
        &self,
        request: pb::LabelValuesRequest,
    ) -> Result<pb::StringsResponse, QueryError>;

    async fn metadata(
        &self,
        request: pb::MetadataRequest,
    ) -> Result<pb::MetadataResponse, QueryError>;

    async fn bucket_generations(
        &self,
        request: pb::BucketGenerationsRequest,
    ) -> Result<pb::BucketGenerationsResponse, QueryError>;
}

/// The streaming reads of one query against one remote shard.
pub trait MetricsShardQuery: Send + Sync {
    fn resolve(&self, request: pb::ResolveRequest) -> WireStream<pb::ResolvedChunk>;
    fn samples(&self, request: pb::ReadSamplesRequest) -> WireStream<pb::SampleBatch>;
}

pub(crate) fn shard_read(
    shard: ShardId,
    namespace: &Namespace,
    ranges: &[(i64, i64)],
) -> pb::ShardRead {
    pb::ShardRead {
        shard: shard.get(),
        namespace: Some(pb::Namespace {
            name: namespace.to_string(),
        }),
        ranges: ranges
            .iter()
            .map(|&(start, end)| pb::SecondsRange { start, end })
            .collect(),
    }
}

/// A [`SeriesSource`] over one remote shard for one query.
pub(crate) struct RemoteShardSource {
    read: pb::ShardRead,
    query: Arc<dyn MetricsShardQuery>,
}

impl RemoteShardSource {
    pub(crate) fn new(read: pb::ShardRead, query: Arc<dyn MetricsShardQuery>) -> Self {
        Self { read, query }
    }
}

impl SeriesSource for RemoteShardSource {
    fn resolve(
        &self,
        selector: &VectorSelector,
        time_range: TimeRange,
    ) -> impl Stream<Item = Result<ResolvedSeriesChunk, SourceQueryError>> + Send {
        let request = pb::ResolveRequest {
            read: Some(self.read.clone()),
            selector: Some(selector_to_wire(selector)),
            start_ms: time_range.start_ms,
            end_ms_exclusive: time_range.end_ms_exclusive,
        };
        self.query
            .resolve(request)
            .map_err(to_source_error)
            .and_then(|chunk| async move { chunk_from_wire(chunk) })
    }

    fn samples(
        &self,
        request: SamplesRequest,
    ) -> impl Stream<Item = Result<SampleBatch, SourceQueryError>> + Send {
        let series_count = request.series.len();
        let wire = pb::ReadSamplesRequest {
            read: Some(self.read.clone()),
            series: request
                .series
                .iter()
                .map(|series| pb::SeriesRef {
                    bucket_id: series.bucket_id,
                    series_id: series.series_id,
                    metric_name: series.metric_name.to_string(),
                })
                .collect(),
            start_ms: request.time_range.start_ms,
            end_ms_exclusive: request.time_range.end_ms_exclusive,
        };
        self.query
            .samples(wire)
            .map_err(to_source_error)
            .and_then(move |batch| async move { batch_from_wire(batch, series_count) })
    }
}

fn to_source_error(error: QueryError) -> SourceQueryError {
    match error {
        QueryError::Storage(message) => SourceQueryError::Storage(message),
        other => SourceQueryError::Internal(other.to_string()),
    }
}

fn from_source_error(error: SourceQueryError) -> QueryError {
    error.into()
}

fn invalid(message: impl Into<String>) -> QueryError {
    QueryError::InvalidQuery(message.into())
}

fn corrupt(message: impl std::fmt::Display) -> SourceQueryError {
    SourceQueryError::Storage(format!("invalid shard reader response: {message}"))
}

fn selector_to_wire(selector: &VectorSelector) -> pb::Selector {
    let matchers = |matchers: &[Matcher]| matchers.iter().map(matcher_to_wire).collect();
    pb::Selector {
        name: selector.name.clone(),
        matchers: matchers(&selector.matchers.matchers),
        or_matchers: selector
            .matchers
            .or_matchers
            .iter()
            .map(|group| pb::MatcherGroup {
                matchers: matchers(group),
            })
            .collect(),
    }
}

fn matcher_to_wire(matcher: &Matcher) -> pb::Matcher {
    let op = match matcher.op {
        MatchOp::Equal => pb::matcher::Op::Equal,
        MatchOp::NotEqual => pb::matcher::Op::NotEqual,
        MatchOp::Re(_) => pb::matcher::Op::Regex,
        MatchOp::NotRe(_) => pb::matcher::Op::NotRegex,
    };
    pb::Matcher {
        op: op as i32,
        name: matcher.name.clone(),
        value: matcher.value.clone(),
    }
}

fn selector_from_wire(selector: pb::Selector) -> Result<VectorSelector, QueryError> {
    let matchers = |matchers: Vec<pb::Matcher>| {
        matchers
            .into_iter()
            .map(matcher_from_wire)
            .collect::<Result<Vec<_>, _>>()
    };
    let or_matchers = selector
        .or_matchers
        .into_iter()
        .map(|group| matchers(group.matchers))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(VectorSelector::new(
        selector.name,
        Matchers::new(matchers(selector.matchers)?).with_or_matchers(or_matchers),
    ))
}

fn matcher_from_wire(matcher: pb::Matcher) -> Result<Matcher, QueryError> {
    let op = pb::matcher::Op::try_from(matcher.op)
        .map_err(|_| invalid(format!("unknown matcher op {}", matcher.op)))?;
    match op {
        pb::matcher::Op::Equal => Ok(Matcher::new(MatchOp::Equal, &matcher.name, &matcher.value)),
        pb::matcher::Op::NotEqual => Ok(Matcher::new(
            MatchOp::NotEqual,
            &matcher.name,
            &matcher.value,
        )),
        pb::matcher::Op::Regex => {
            Matcher::new_matcher(T_EQL_REGEX, matcher.name, matcher.value).map_err(invalid)
        }
        pb::matcher::Op::NotRegex => {
            Matcher::new_matcher(T_NEQ_REGEX, matcher.name, matcher.value).map_err(invalid)
        }
    }
}

fn labels_to_wire(labels: &Labels) -> pb::LabelSet {
    pb::LabelSet {
        labels: labels
            .iter()
            .map(|label| pb::Label {
                name: label.name.clone(),
                value: label.value.clone(),
            })
            .collect(),
    }
}

fn labels_from_wire(labels: pb::LabelSet) -> Labels {
    Labels::new(
        labels
            .labels
            .into_iter()
            .map(|label| Label::new(label.name, label.value))
            .collect(),
    )
}

fn chunk_to_wire(chunk: ResolvedSeriesChunk) -> pb::ResolvedChunk {
    pb::ResolvedChunk {
        bucket_id: chunk.bucket_id,
        labels: chunk.labels.iter().map(labels_to_wire).collect(),
        series_ids: chunk.series.iter().map(|series| series.series_id).collect(),
        metric_names: chunk
            .series
            .iter()
            .map(|series| series.metric_name.to_string())
            .collect(),
    }
}

fn chunk_from_wire(chunk: pb::ResolvedChunk) -> Result<ResolvedSeriesChunk, SourceQueryError> {
    let count = chunk.series_ids.len();
    if chunk.labels.len() != count || chunk.metric_names.len() != count {
        return Err(corrupt("resolved chunk columns differ in length"));
    }
    let bucket_id = chunk.bucket_id;
    Ok(ResolvedSeriesChunk {
        bucket_id,
        labels: chunk.labels.into_iter().map(labels_from_wire).collect(),
        series: chunk
            .series_ids
            .into_iter()
            .zip(chunk.metric_names)
            .map(|(series_id, metric_name)| {
                ResolvedSeriesRef::new(bucket_id, series_id, Arc::from(metric_name))
            })
            .collect(),
    })
}

/// Splits `batch` into messages of roughly [`SAMPLES_PER_MESSAGE`] samples,
/// each covering whole series.
fn batch_to_wire(batch: SampleBatch) -> Vec<pb::SampleBatch> {
    let SampleBlock {
        timestamps,
        values,
        histogram_timestamps,
        histograms,
    } = batch.samples;
    let mut messages = Vec::new();
    let mut current = pb::SampleBatch {
        series_start: batch.series_range.start as u32,
        series: Vec::new(),
    };
    let mut current_samples = 0;
    let columns = timestamps
        .into_iter()
        .zip(values)
        .zip(histogram_timestamps.into_iter().zip(histograms));
    for (index, ((timestamps, values), (histogram_timestamps, histograms))) in columns.enumerate() {
        if current_samples >= SAMPLES_PER_MESSAGE {
            let next = pb::SampleBatch {
                series_start: (batch.series_range.start + index) as u32,
                series: Vec::new(),
            };
            messages.push(std::mem::replace(&mut current, next));
            current_samples = 0;
        }
        current_samples += timestamps.len() + histograms.len();
        let mut chunks = Vec::new();
        crate::serde::chunk::encode_section(&timestamps, &values, &mut chunks);
        current.series.push(pb::SeriesSamples {
            chunks,
            histograms: histogram_timestamps
                .into_iter()
                .zip(histograms)
                .map(|(timestamp_ms, histogram)| histogram_to_wire(timestamp_ms, &histogram))
                .collect(),
        });
    }
    messages.push(current);
    messages
}

fn batch_from_wire(
    batch: pb::SampleBatch,
    series_count: usize,
) -> Result<SampleBatch, SourceQueryError> {
    let start = batch.series_start as usize;
    let end = start + batch.series.len();
    if end > series_count {
        return Err(corrupt("sample batch beyond the requested series"));
    }
    let mut samples = SampleBlock::with_series_count(batch.series.len());
    for (index, series) in batch.series.into_iter().enumerate() {
        crate::serde::chunk::decode(
            &series.chunks,
            &mut samples.timestamps[index],
            &mut samples.values[index],
        )
        .map_err(corrupt)?;
        for histogram in series.histograms {
            let sample = histogram_from_wire(histogram).map_err(corrupt)?;
            samples.histogram_timestamps[index].push(sample.timestamp_ms);
            samples.histograms[index].push(Arc::new(sample.histogram));
        }
    }
    Ok(SampleBatch {
        series_range: start..end,
        samples,
    })
}

/// A native histogram sample in its internal wire form.
pub fn histogram_to_wire(timestamp_ms: i64, histogram: &FloatHistogram) -> pb::HistogramSample {
    use pb::histogram_sample::CounterResetHint as WireHint;
    let hint = match histogram.counter_reset_hint {
        CounterResetHint::Unknown => WireHint::Unknown,
        CounterResetHint::CounterReset => WireHint::Reset,
        CounterResetHint::NotCounterReset => WireHint::NotReset,
        CounterResetHint::Gauge => WireHint::Gauge,
    };
    pb::HistogramSample {
        timestamp_ms,
        counter_reset_hint: hint as i32,
        schema: histogram.schema,
        zero_threshold: Some(histogram.zero_threshold),
        zero_count: Some(histogram.zero_count),
        count: Some(histogram.count),
        sum: Some(histogram.sum),
        positive_indexes: histogram.positive.iter().map(|b| b.index).collect(),
        positive_counts: histogram.positive.iter().map(|b| b.count).collect(),
        negative_indexes: histogram.negative.iter().map(|b| b.index).collect(),
        negative_counts: histogram.negative.iter().map(|b| b.count).collect(),
        custom_values: histogram.custom_values.to_vec(),
    }
}

/// Inverse of [`histogram_to_wire`]; rejects malformed histograms.
pub fn histogram_from_wire(sample: pb::HistogramSample) -> Result<HistogramSample, String> {
    use pb::histogram_sample::CounterResetHint as WireHint;
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
    let counter_reset_hint =
        match WireHint::try_from(sample.counter_reset_hint).unwrap_or(WireHint::Unknown) {
            WireHint::Unknown => CounterResetHint::Unknown,
            WireHint::Reset => CounterResetHint::CounterReset,
            WireHint::NotReset => CounterResetHint::NotCounterReset,
            WireHint::Gauge => CounterResetHint::Gauge,
        };
    let histogram = FloatHistogram {
        counter_reset_hint,
        schema: sample.schema,
        zero_threshold: sample.zero_threshold(),
        zero_count: sample.zero_count(),
        count: sample.count(),
        sum: sample.sum(),
        positive: buckets(sample.positive_indexes, sample.positive_counts)?,
        negative: buckets(sample.negative_indexes, sample.negative_counts)?,
        custom_values: sample.custom_values.into(),
    };
    histogram.validate()?;
    Ok(HistogramSample::new(sample.timestamp_ms, histogram))
}

fn temporality_to_wire(temporality: Temporality) -> pb::read_metric_metadata::Temporality {
    use pb::read_metric_metadata::Temporality as Wire;
    match temporality {
        Temporality::Cumulative => Wire::Cumulative,
        Temporality::Delta => Wire::Delta,
        Temporality::Unspecified => Wire::Unspecified,
    }
}

fn temporality_from_wire(temporality: i32) -> Temporality {
    use pb::read_metric_metadata::Temporality as Wire;
    match Wire::try_from(temporality).unwrap_or(Wire::Unspecified) {
        Wire::Cumulative => Temporality::Cumulative,
        Wire::Delta => Temporality::Delta,
        Wire::Unspecified => Temporality::Unspecified,
    }
}

fn metadata_to_wire(metadata: MetricMetadata) -> pb::ReadMetricMetadata {
    use pb::read_metric_metadata::Kind;
    let (kind, monotonic, temporality) = match metadata.metric_type {
        None => (Kind::Unspecified, false, Temporality::Unspecified),
        Some(MetricType::Gauge) => (Kind::Gauge, false, Temporality::Unspecified),
        Some(MetricType::Sum {
            monotonic,
            temporality,
        }) => (Kind::Sum, monotonic, temporality),
        Some(MetricType::Histogram { temporality }) => (Kind::Histogram, false, temporality),
        Some(MetricType::ExponentialHistogram { temporality }) => {
            (Kind::ExponentialHistogram, false, temporality)
        }
        Some(MetricType::Summary) => (Kind::Summary, false, Temporality::Unspecified),
    };
    pb::ReadMetricMetadata {
        metric_name: metadata.metric_name,
        kind: kind as i32,
        monotonic,
        temporality: temporality_to_wire(temporality) as i32,
        description: metadata.description,
        unit: metadata.unit,
    }
}

fn metadata_from_wire(metadata: pb::ReadMetricMetadata) -> MetricMetadata {
    use pb::read_metric_metadata::Kind;
    let temporality = temporality_from_wire(metadata.temporality);
    let metric_type = match Kind::try_from(metadata.kind).unwrap_or(Kind::Unspecified) {
        Kind::Unspecified => None,
        Kind::Gauge => Some(MetricType::Gauge),
        Kind::Sum => Some(MetricType::Sum {
            monotonic: metadata.monotonic,
            temporality,
        }),
        Kind::Histogram => Some(MetricType::Histogram { temporality }),
        Kind::ExponentialHistogram => Some(MetricType::ExponentialHistogram { temporality }),
        Kind::Summary => Some(MetricType::Summary),
    };
    MetricMetadata {
        metric_name: metadata.metric_name,
        metric_type,
        description: metadata.description,
        unit: metadata.unit,
    }
}

fn secs_range(start: i64, end: i64) -> std::ops::RangeInclusive<SystemTime> {
    let at = |secs: i64| UNIX_EPOCH + Duration::from_secs(secs.max(0).cast_unsigned());
    at(start)..=at(end)
}

/// Remote reads of one shard, issued by [`ShardedMetrics`]'s fan-outs.
pub(crate) struct RemoteShard {
    pub(crate) shard: ShardId,
    pub(crate) client: Arc<dyn MetricsShardClient>,
}

impl RemoteShard {
    pub(crate) fn source(&self, namespace: &Namespace, ranges: &[(i64, i64)]) -> RemoteShardSource {
        RemoteShardSource::new(
            shard_read(self.shard, namespace, ranges),
            self.client.query(self.shard),
        )
    }

    pub(crate) async fn series(
        &self,
        namespace: &Namespace,
        matchers: &[&str],
        range: (i64, i64),
    ) -> Result<Vec<Labels>, QueryError> {
        let response = self
            .client
            .series(pb::SeriesRequest {
                read: Some(shard_read(self.shard, namespace, &[range])),
                matchers: matchers.iter().map(|&m| m.to_owned()).collect(),
            })
            .await?;
        Ok(response.series.into_iter().map(labels_from_wire).collect())
    }

    pub(crate) async fn labels(
        &self,
        namespace: &Namespace,
        matchers: Option<&[&str]>,
        range: (i64, i64),
    ) -> Result<Vec<String>, QueryError> {
        let response = self
            .client
            .label_names(pb::LabelNamesRequest {
                read: Some(shard_read(self.shard, namespace, &[range])),
                matchers: matchers
                    .unwrap_or_default()
                    .iter()
                    .map(|&m| m.to_owned())
                    .collect(),
            })
            .await?;
        Ok(response.values)
    }

    pub(crate) async fn label_values(
        &self,
        namespace: &Namespace,
        label_name: &str,
        matchers: Option<&[&str]>,
        range: (i64, i64),
    ) -> Result<Vec<String>, QueryError> {
        let response = self
            .client
            .label_values(pb::LabelValuesRequest {
                read: Some(shard_read(self.shard, namespace, &[range])),
                label_name: label_name.to_owned(),
                matchers: matchers
                    .unwrap_or_default()
                    .iter()
                    .map(|&m| m.to_owned())
                    .collect(),
            })
            .await?;
        Ok(response.values)
    }

    pub(crate) async fn metadata(
        &self,
        namespace: &Namespace,
        metric: Option<&str>,
    ) -> Result<Vec<MetricMetadata>, QueryError> {
        let response = self
            .client
            .metadata(pb::MetadataRequest {
                read: Some(shard_read(self.shard, namespace, &[])),
                metric: metric.map(str::to_owned),
            })
            .await?;
        Ok(response
            .metadata
            .into_iter()
            .map(metadata_from_wire)
            .collect())
    }

    pub(crate) async fn bucket_generations(
        &self,
        namespace: &Namespace,
        buckets: &[crate::model::TimeBucket],
    ) -> crate::Result<Vec<Option<u64>>> {
        let response = self
            .client
            .bucket_generations(pb::BucketGenerationsRequest {
                read: Some(shard_read(self.shard, namespace, &[])),
                bucket_ids: buckets.iter().copied().map(encode_bucket).collect(),
            })
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        if response.generations.len() != buckets.len() {
            return Err(Error::Storage(
                "shard reader returned the wrong number of bucket generations".into(),
            ));
        }
        Ok(response
            .generations
            .into_iter()
            .map(|generation| generation.value)
            .collect())
    }
}

/// The receiving side: requests from coordinating readers, answered from
/// this reader's own shard only.
impl ShardedMetrics {
    async fn served_read(
        &self,
        read: Option<pb::ShardRead>,
    ) -> Result<(ShardId, Namespace, Vec<(i64, i64)>), QueryError> {
        let read = read.ok_or_else(|| invalid("missing shard read"))?;
        let shard = ShardId::new(read.shard);
        if self
            .shards()
            .reader_scope()
            .is_some_and(|scope| scope != shard)
        {
            return Err(QueryError::Storage(format!(
                "shard {shard} is not served by this reader group"
            )));
        }
        let namespace = read
            .namespace
            .map(|namespace| namespace.name)
            .unwrap_or_default();
        let namespace = Namespace::new(namespace).map_err(|error| invalid(error.to_string()))?;
        let ranges = read
            .ranges
            .into_iter()
            .map(|range| (range.start, range.end))
            .collect();
        Ok((shard, namespace, ranges))
    }

    pub async fn serve_resolve(
        &self,
        request: pb::ResolveRequest,
    ) -> Result<WireStream<pb::ResolvedChunk>, QueryError> {
        let (shard, namespace, ranges) = self.served_read(request.read).await?;
        let selector = selector_from_wire(request.selector.unwrap_or_default())?;
        let time_range = TimeRange::new(request.start_ms, request.end_ms_exclusive);
        let permit = self.admit_shard_read().await?;
        let source = self.local_source(shard, &namespace, &ranges).await?;
        let (mut sender, receiver) = mpsc::channel(4);
        tokio::spawn(async move {
            let _permit = permit;
            let mut stream = pin!(source.resolve(&selector, time_range));
            while let Some(chunk) = stream.next().await {
                let message = chunk.map(chunk_to_wire).map_err(from_source_error);
                let failed = message.is_err();
                if sender.send(message).await.is_err() || failed {
                    return;
                }
            }
        });
        Ok(receiver.boxed())
    }

    pub async fn serve_samples(
        &self,
        request: pb::ReadSamplesRequest,
    ) -> Result<WireStream<pb::SampleBatch>, QueryError> {
        let (shard, namespace, ranges) = self.served_read(request.read).await?;
        let series: Arc<[ResolvedSeriesRef]> = request
            .series
            .into_iter()
            .map(|series| {
                ResolvedSeriesRef::new(
                    series.bucket_id,
                    series.series_id,
                    series.metric_name.into(),
                )
            })
            .collect();
        if let Some(series) = series
            .iter()
            .find(|series| decode_bucket(series.bucket_id).is_none())
        {
            return Err(invalid(format!("invalid bucket id {}", series.bucket_id)));
        }
        let samples = SamplesRequest::new(
            series,
            TimeRange::new(request.start_ms, request.end_ms_exclusive),
        );
        let permit = self.admit_shard_read().await?;
        let source = self.local_source(shard, &namespace, &ranges).await?;
        let (mut sender, receiver) = mpsc::channel(4);
        tokio::spawn(async move {
            let _permit = permit;
            let mut stream = pin!(source.samples(samples));
            while let Some(batch) = stream.next().await {
                let messages = match batch {
                    Ok(batch) => batch_to_wire(batch).into_iter().map(Ok).collect(),
                    Err(error) => vec![Err(from_source_error(error))],
                };
                for message in messages {
                    let failed = message.is_err();
                    if sender.send(message).await.is_err() || failed {
                        return;
                    }
                }
            }
        });
        Ok(receiver.boxed())
    }

    pub async fn serve_series(
        &self,
        request: pb::SeriesRequest,
    ) -> Result<pb::SeriesResponse, QueryError> {
        let (shard, namespace, ranges) = self.served_read(request.read).await?;
        let (start, end) = single_range(&ranges)?;
        let matchers: Vec<&str> = request.matchers.iter().map(String::as_str).collect();
        let series = self
            .local_series(shard, &namespace, &matchers, secs_range(start, end))
            .await?;
        Ok(pb::SeriesResponse {
            series: series.iter().map(labels_to_wire).collect(),
        })
    }

    pub async fn serve_label_names(
        &self,
        request: pb::LabelNamesRequest,
    ) -> Result<pb::StringsResponse, QueryError> {
        let (shard, namespace, ranges) = self.served_read(request.read).await?;
        let (start, end) = single_range(&ranges)?;
        let matchers: Vec<&str> = request.matchers.iter().map(String::as_str).collect();
        let values = self
            .local_labels(shard, &namespace, Some(&matchers), secs_range(start, end))
            .await?;
        Ok(pb::StringsResponse { values })
    }

    pub async fn serve_label_values(
        &self,
        request: pb::LabelValuesRequest,
    ) -> Result<pb::StringsResponse, QueryError> {
        let (shard, namespace, ranges) = self.served_read(request.read).await?;
        let (start, end) = single_range(&ranges)?;
        let matchers: Vec<&str> = request.matchers.iter().map(String::as_str).collect();
        let values = self
            .local_label_values(
                shard,
                &namespace,
                &request.label_name,
                Some(&matchers),
                secs_range(start, end),
            )
            .await?;
        Ok(pb::StringsResponse { values })
    }

    pub async fn serve_metadata(
        &self,
        request: pb::MetadataRequest,
    ) -> Result<pb::MetadataResponse, QueryError> {
        let (shard, namespace, _) = self.served_read(request.read).await?;
        let metadata = self
            .local_metadata(shard, &namespace, request.metric.as_deref())
            .await?;
        Ok(pb::MetadataResponse {
            metadata: metadata.into_iter().map(metadata_to_wire).collect(),
        })
    }

    pub async fn serve_bucket_generations(
        &self,
        request: pb::BucketGenerationsRequest,
    ) -> Result<pb::BucketGenerationsResponse, QueryError> {
        let (shard, namespace, _) = self.served_read(request.read).await?;
        let buckets = request
            .bucket_ids
            .iter()
            .map(|&id| decode_bucket(id).ok_or_else(|| invalid(format!("invalid bucket id {id}"))))
            .collect::<Result<Vec<_>, _>>()?;
        let generations = self
            .local_bucket_generations(shard, &namespace, &buckets)
            .await?;
        Ok(pb::BucketGenerationsResponse {
            generations: generations
                .into_iter()
                .map(|value| pb::Generation { value })
                .collect(),
        })
    }
}

fn single_range(ranges: &[(i64, i64)]) -> Result<(i64, i64), QueryError> {
    match ranges {
        [range] => Ok(*range),
        _ => Err(invalid("expected exactly one time range")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selectors_survive_the_wire() {
        let expr = promql_parser::parser::parse(r#"up{job=~"api|web", env!="dev", path!~"/a{2}"}"#)
            .unwrap();
        let promql_parser::parser::Expr::VectorSelector(selector) = expr else {
            panic!("not a selector");
        };
        let decoded = selector_from_wire(selector_to_wire(&selector)).unwrap();
        assert_eq!(decoded.name, selector.name);
        assert_eq!(decoded.matchers, selector.matchers);
        assert!(decoded.matchers.matchers.iter().any(|m| m.is_match("api")));
    }

    #[test]
    fn sample_batches_survive_the_wire_including_stale_markers() {
        let mut samples = SampleBlock::with_series_count(2);
        samples.timestamps[0] = (0..3000).map(|i| i * 15_000).collect();
        samples.values[0] = (0..3000).map(|i| f64::from(i) * 0.5).collect();
        samples.values[0][7] = f64::from_bits(crate::STALE_NAN);
        let histogram = FloatHistogram {
            count: 3.0,
            sum: 4.5,
            ..FloatHistogram::default()
        };
        samples.histogram_timestamps[1] = vec![42];
        samples.histograms[1] = vec![Arc::new(histogram.clone())];
        let messages = batch_to_wire(SampleBatch {
            series_range: 5..7,
            samples,
        });
        assert_eq!(messages.len(), 1);
        let decoded = batch_from_wire(messages.into_iter().next().unwrap(), 7).unwrap();
        assert_eq!(decoded.series_range, 5..7);
        assert_eq!(decoded.samples.timestamps[0].len(), 3000);
        assert_eq!(decoded.samples.values[0][1999], 999.5);
        assert!(crate::is_stale_nan(decoded.samples.values[0][7]));
        assert_eq!(decoded.samples.histogram_timestamps[1], vec![42]);
        assert_eq!(*decoded.samples.histograms[1][0], histogram);
    }

    #[test]
    fn large_sample_batches_split_on_series_boundaries() {
        let mut samples = SampleBlock::with_series_count(3);
        for series in 0..3 {
            samples.timestamps[series] = (0..SAMPLES_PER_MESSAGE as i64).collect();
            samples.values[series] = vec![1.0; SAMPLES_PER_MESSAGE];
        }
        let messages = batch_to_wire(SampleBatch {
            series_range: 10..13,
            samples,
        });
        let starts: Vec<_> = messages.iter().map(|m| m.series_start).collect();
        assert_eq!(starts, vec![10, 11, 12]);
        assert!(batch_from_wire(messages[2].clone(), 12).is_err());
    }

    #[test]
    fn metadata_survives_the_wire() {
        let metadata = MetricMetadata {
            metric_name: "requests_total".into(),
            metric_type: Some(MetricType::Sum {
                monotonic: true,
                temporality: Temporality::Delta,
            }),
            description: Some("Requests.".into()),
            unit: None,
        };
        assert_eq!(
            metadata_from_wire(metadata_to_wire(metadata.clone())),
            metadata
        );
    }
}
