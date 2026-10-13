//! Reading storage shards that other reader groups serve.
//!
//! A reader serving one shard coordinates a query over all of them: its own
//! shard is read in process and every other shard through a
//! [`TracesShardClient`], the transport the server provides. A remote shard
//! answers a TraceQL query over its own traces, and the coordinator merges
//! every shard's first hits into global `(start, trace ID)` order.

use std::sync::Arc;

use async_trait::async_trait;
use common::discovery::DiscoveryValue;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use prost::Message;
use proto::traces::internal::v1 as pb;
use sharding::ShardId;

use crate::traceql::{KindValue, StaticValue, StatusValue};
use crate::{
    AttributeMatcher, AttributeScope, AttributeValue, Error, MatchedSpan, Namespace, QueryOptions,
    Result, Trace, TraceId, TraceQlResult, TraceSummary,
};

/// Transport to the reader groups serving other shards.
#[async_trait]
pub trait TracesShardClient: Send + Sync + 'static {
    async fn get_trace(&self, request: pb::GetTraceRequest) -> Result<pb::TracesResponse>;
    /// Every message of the shard's streamed search response.
    async fn search(&self, request: pb::SearchRequest) -> Result<Vec<pb::TracesResponse>>;
    async fn scan(&self, request: pb::ScanRequest) -> Result<pb::TracesResponse>;
    async fn catalog_names(&self, request: pb::CatalogNamesRequest) -> Result<pb::StringsResponse>;
    async fn catalog_values(
        &self,
        request: pb::CatalogValuesRequest,
    ) -> Result<pb::CatalogValuesResponse>;
    async fn traceql(&self, request: pb::TraceQlRequest) -> Result<pb::TraceQlResponse>;
}

/// Traces per streamed search message.
pub(crate) const SEARCH_CHUNK_TRACES: usize = 256;

pub(crate) fn shard_read(shard: ShardId, namespace: &Namespace) -> pb::ShardRead {
    pb::ShardRead {
        shard: shard.get(),
        namespace: Some(pb::Namespace {
            name: namespace.to_string(),
        }),
    }
}

pub(crate) fn read_from_wire(read: Option<&pb::ShardRead>) -> Result<(ShardId, Namespace)> {
    let read = read.ok_or_else(|| Error::Invalid("missing shard read".into()))?;
    let name = read
        .namespace
        .as_ref()
        .map(|namespace| namespace.name.clone())
        .unwrap_or_default();
    let namespace = Namespace::new(name).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok((ShardId::new(read.shard), namespace))
}

fn size(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

pub(crate) fn trace_to_wire(trace: &Trace) -> pb::TracePart {
    pb::TracePart {
        trace_id: trace.trace_id.as_bytes().to_vec(),
        resource_spans: trace
            .resource_spans
            .iter()
            .map(Message::encode_to_vec)
            .collect(),
    }
}

pub(crate) fn trace_from_wire(part: pb::TracePart) -> Result<Trace> {
    Trace::new(
        TraceId::from_slice(&part.trace_id)?,
        part.resource_spans
            .iter()
            .map(|bytes| ResourceSpans::decode(bytes.as_slice()))
            .collect::<std::result::Result<_, _>>()?,
    )
}

pub(crate) fn traces_from_wire(response: pb::TracesResponse) -> Result<Vec<Trace>> {
    response.traces.into_iter().map(trace_from_wire).collect()
}

pub(crate) fn scope_to_wire(scope: Option<AttributeScope>) -> i32 {
    (match scope {
        None => pb::AttributeScope::Unspecified,
        Some(AttributeScope::Resource) => pb::AttributeScope::Resource,
        Some(AttributeScope::Span) => pb::AttributeScope::Span,
    }) as i32
}

pub(crate) fn scope_from_wire(scope: i32) -> Result<Option<AttributeScope>> {
    match pb::AttributeScope::try_from(scope) {
        Ok(pb::AttributeScope::Unspecified) => Ok(None),
        Ok(pb::AttributeScope::Resource) => Ok(Some(AttributeScope::Resource)),
        Ok(pb::AttributeScope::Span) => Ok(Some(AttributeScope::Span)),
        Err(_) => Err(Error::Invalid(format!("unknown attribute scope {scope}"))),
    }
}

fn attribute_to_wire(value: pb::attribute_value::Value) -> pb::AttributeValue {
    pb::AttributeValue { value: Some(value) }
}

pub(crate) fn matcher_to_wire(matcher: &AttributeMatcher) -> pb::AttributeMatcher {
    use pb::attribute_value::Value;
    pb::AttributeMatcher {
        scope: scope_to_wire(Some(matcher.scope)),
        name: matcher.name.clone(),
        value: Some(attribute_to_wire(match &matcher.value {
            AttributeValue::String(value) => Value::String(value.clone()),
            AttributeValue::Bool(value) => Value::Bool(*value),
            AttributeValue::Int(value) => Value::Int(*value),
            AttributeValue::Double(value) => Value::Double(*value),
        })),
    }
}

pub(crate) fn matcher_from_wire(matcher: pb::AttributeMatcher) -> Result<AttributeMatcher> {
    use pb::attribute_value::Value;
    let scope = scope_from_wire(matcher.scope)?
        .ok_or_else(|| Error::Invalid("attribute matcher needs a scope".into()))?;
    let value = match matcher.value.and_then(|value| value.value) {
        Some(Value::String(value)) => AttributeValue::String(value),
        Some(Value::Bool(value)) => AttributeValue::Bool(value),
        Some(Value::Int(value)) => AttributeValue::Int(value),
        Some(Value::Double(value)) => AttributeValue::Double(value),
        None => return Err(Error::Invalid("attribute matcher needs a value".into())),
    };
    AttributeMatcher::new(scope, matcher.name, value)
}

pub(crate) fn discovery_to_wire(value: &DiscoveryValue) -> pb::AttributeValue {
    use pb::attribute_value::Value;
    attribute_to_wire(match value {
        DiscoveryValue::String(value) => Value::String(value.clone()),
        DiscoveryValue::Bool(value) => Value::Bool(*value),
        DiscoveryValue::Int(value) => Value::Int(*value),
        DiscoveryValue::Double(value) => Value::Double(*value),
    })
}

fn discovery_from_wire(value: pb::AttributeValue) -> Result<DiscoveryValue> {
    use pb::attribute_value::Value;
    Ok(match value.value {
        Some(Value::String(value)) => DiscoveryValue::String(value),
        Some(Value::Bool(value)) => DiscoveryValue::Bool(value),
        Some(Value::Int(value)) => DiscoveryValue::Int(value),
        Some(Value::Double(value)) => DiscoveryValue::Double(value),
        None => return Err(Error::Corrupt("catalog value without a value".into())),
    })
}

pub(crate) fn traceql_request(
    shard: ShardId,
    namespace: &Namespace,
    (start_ns, end_ns): (u64, u64),
    source: &str,
    options: &QueryOptions,
    summaries: bool,
) -> pb::TraceQlRequest {
    pb::TraceQlRequest {
        read: Some(shard_read(shard, namespace)),
        start_ns,
        end_ns,
        query: source.to_owned(),
        limit: options.limit as u64,
        max_candidate_traces: options.max_candidate_traces as u64,
        max_spans_per_trace: options.max_spans_per_trace as u64,
        max_concurrency: options.max_concurrency as u64,
        summaries,
    }
}

pub(crate) fn options_from_wire(request: &pb::TraceQlRequest) -> QueryOptions {
    QueryOptions {
        limit: size(request.limit),
        max_candidate_traces: size(request.max_candidate_traces),
        max_spans_per_trace: size(request.max_spans_per_trace),
        max_concurrency: size(request.max_concurrency),
    }
}

fn value_to_wire(value: &StaticValue) -> pb::TraceQlValue {
    use pb::trace_ql_value::Value;
    pb::TraceQlValue {
        value: Some(match value {
            StaticValue::Nil => Value::Nil(true),
            StaticValue::String(value) => Value::String(value.clone()),
            StaticValue::Bool(value) => Value::Bool(*value),
            StaticValue::Int(value) => Value::Int(*value),
            StaticValue::Float(value) => Value::Float(*value),
            StaticValue::Duration(value) => Value::DurationNs(*value),
            StaticValue::Status(status) => Value::Status(
                (match status {
                    StatusValue::Unset => pb::SpanStatus::Unset,
                    StatusValue::Ok => pb::SpanStatus::Ok,
                    StatusValue::Error => pb::SpanStatus::Error,
                }) as i32,
            ),
            StaticValue::Kind(kind) => Value::Kind(
                (match kind {
                    KindValue::Unspecified => pb::SpanKind::Unspecified,
                    KindValue::Internal => pb::SpanKind::Internal,
                    KindValue::Server => pb::SpanKind::Server,
                    KindValue::Client => pb::SpanKind::Client,
                    KindValue::Producer => pb::SpanKind::Producer,
                    KindValue::Consumer => pb::SpanKind::Consumer,
                }) as i32,
            ),
        }),
    }
}

fn value_from_wire(value: Option<pb::TraceQlValue>) -> Result<StaticValue> {
    use pb::trace_ql_value::Value;
    let corrupt = || Error::Corrupt("invalid TraceQL value from a shard reader".into());
    Ok(
        match value.and_then(|value| value.value).ok_or_else(corrupt)? {
            Value::Nil(_) => StaticValue::Nil,
            Value::String(value) => StaticValue::String(value),
            Value::Bool(value) => StaticValue::Bool(value),
            Value::Int(value) => StaticValue::Int(value),
            Value::Float(value) => StaticValue::Float(value),
            Value::DurationNs(value) => StaticValue::Duration(value),
            Value::Status(status) => StaticValue::Status(
                match pb::SpanStatus::try_from(status).map_err(|_| corrupt())? {
                    pb::SpanStatus::Unset => StatusValue::Unset,
                    pb::SpanStatus::Ok => StatusValue::Ok,
                    pb::SpanStatus::Error => StatusValue::Error,
                },
            ),
            Value::Kind(kind) => {
                StaticValue::Kind(match pb::SpanKind::try_from(kind).map_err(|_| corrupt())? {
                    pb::SpanKind::Unspecified => KindValue::Unspecified,
                    pb::SpanKind::Internal => KindValue::Internal,
                    pb::SpanKind::Server => KindValue::Server,
                    pb::SpanKind::Client => KindValue::Client,
                    pb::SpanKind::Producer => KindValue::Producer,
                    pb::SpanKind::Consumer => KindValue::Consumer,
                })
            }
        },
    )
}

pub(crate) fn result_to_wire(result: TraceQlResult) -> pb::TraceQlHit {
    pb::TraceQlHit {
        trace_id: result.trace_id.as_bytes().to_vec(),
        start_ns: result.start_ns,
        end_ns: result.end_ns,
        root_service_name: result.root_service_name,
        root_span_name: result.root_span_name,
        count: result.spanset_count as u64,
        matched_spans: result
            .matched_spans
            .into_iter()
            .map(|span| pb::MatchedSpan {
                span_id: span.span_id,
                parent_span_id: span.parent_span_id,
                name: span.name,
                start_ns: span.start_ns,
                end_ns: span.end_ns,
                selected: span
                    .selected
                    .iter()
                    .map(|(name, value)| pb::SelectedValue {
                        name: name.clone(),
                        value: Some(value_to_wire(value)),
                    })
                    .collect(),
            })
            .collect(),
    }
}

pub(crate) fn result_from_wire(hit: pb::TraceQlHit) -> Result<TraceQlResult> {
    Ok(TraceQlResult {
        trace_id: TraceId::from_slice(&hit.trace_id)?,
        start_ns: hit.start_ns,
        end_ns: hit.end_ns,
        root_service_name: hit.root_service_name,
        root_span_name: hit.root_span_name,
        spanset_count: size(hit.count),
        matched_spans: hit
            .matched_spans
            .into_iter()
            .map(|span| {
                Ok(MatchedSpan {
                    span_id: span.span_id,
                    parent_span_id: span.parent_span_id,
                    name: span.name,
                    start_ns: span.start_ns,
                    end_ns: span.end_ns,
                    selected: span
                        .selected
                        .into_iter()
                        .map(|selected| Ok((selected.name, value_from_wire(selected.value)?)))
                        .collect::<Result<_>>()?,
                })
            })
            .collect::<Result<_>>()?,
    })
}

pub(crate) fn summary_to_wire(summary: TraceSummary) -> pb::TraceQlHit {
    pb::TraceQlHit {
        trace_id: summary.trace_id.as_bytes().to_vec(),
        start_ns: summary.start_ns,
        end_ns: summary.end_ns,
        root_service_name: summary.root_service_name,
        root_span_name: summary.root_span_name,
        count: summary.matched_spans as u64,
        matched_spans: Vec::new(),
    }
}

pub(crate) fn summary_from_wire(hit: pb::TraceQlHit) -> Result<TraceSummary> {
    Ok(TraceSummary {
        trace_id: TraceId::from_slice(&hit.trace_id)?,
        start_ns: hit.start_ns,
        end_ns: hit.end_ns,
        root_service_name: hit.root_service_name,
        root_span_name: hit.root_span_name,
        matched_spans: size(hit.count),
    })
}

/// One shard read through the reader group serving it.
#[derive(Clone)]
pub(crate) struct RemoteShard {
    pub(crate) shard: ShardId,
    pub(crate) client: Arc<dyn TracesShardClient>,
}

impl RemoteShard {
    pub(crate) async fn get_trace(
        &self,
        namespace: &Namespace,
        trace_id: TraceId,
    ) -> Result<Vec<Trace>> {
        traces_from_wire(
            self.client
                .get_trace(pb::GetTraceRequest {
                    read: Some(shard_read(self.shard, namespace)),
                    trace_id: trace_id.as_bytes().to_vec(),
                })
                .await?,
        )
    }

    pub(crate) async fn search(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        matchers: &[AttributeMatcher],
    ) -> Result<Vec<Trace>> {
        let mut traces = Vec::new();
        for response in self
            .client
            .search(pb::SearchRequest {
                read: Some(shard_read(self.shard, namespace)),
                start_ns,
                end_ns,
                matchers: matchers.iter().map(matcher_to_wire).collect(),
            })
            .await?
        {
            traces.extend(traces_from_wire(response)?);
        }
        Ok(traces)
    }

    pub(crate) async fn scan(&self, namespace: &Namespace, limit: usize) -> Result<Vec<Trace>> {
        traces_from_wire(
            self.client
                .scan(pb::ScanRequest {
                    read: Some(shard_read(self.shard, namespace)),
                    limit: limit as u64,
                })
                .await?,
        )
    }

    pub(crate) async fn catalog_names(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        scope: Option<AttributeScope>,
    ) -> Result<Vec<String>> {
        Ok(self
            .client
            .catalog_names(pb::CatalogNamesRequest {
                read: Some(shard_read(self.shard, namespace)),
                start_ns,
                end_ns,
                scope: scope_to_wire(scope),
            })
            .await?
            .values)
    }

    pub(crate) async fn catalog_values(
        &self,
        namespace: &Namespace,
        start_ns: u64,
        end_ns: u64,
        scope: Option<AttributeScope>,
        name: &str,
    ) -> Result<Vec<DiscoveryValue>> {
        self.client
            .catalog_values(pb::CatalogValuesRequest {
                read: Some(shard_read(self.shard, namespace)),
                start_ns,
                end_ns,
                scope: scope_to_wire(scope),
                name: name.to_owned(),
            })
            .await?
            .values
            .into_iter()
            .map(discovery_from_wire)
            .collect()
    }

    pub(crate) async fn traceql<T: crate::db::Hit>(
        &self,
        namespace: &Namespace,
        range: (u64, u64),
        source: &str,
        options: &QueryOptions,
    ) -> Result<Vec<T>> {
        self.client
            .traceql(traceql_request(
                self.shard,
                namespace,
                range,
                source,
                options,
                T::SUMMARIES,
            ))
            .await?
            .hits
            .into_iter()
            .map(T::from_wire)
            .collect()
    }
}
