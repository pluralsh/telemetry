use std::collections::BTreeMap;

use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    trace::v1::{ResourceSpans, ScopeSpans},
};

use crate::{Error, Result, Trace, TraceBatch, TraceId};

/// Splits an OTLP export into independently routable, single-trace batches.
///
/// Resource and instrumentation-scope context is cloned only where needed,
/// while spans (including attributes, events, links, kind, and status) are
/// moved without transformation.
pub fn trace_batches(request: ExportTraceServiceRequest) -> Result<Vec<TraceBatch>> {
    trace_batches_from_resource_spans(request.resource_spans)
}

pub fn trace_batches_from_resource_spans(
    resource_spans: Vec<ResourceSpans>,
) -> Result<Vec<TraceBatch>> {
    let mut grouped = BTreeMap::<TraceId, Vec<ResourceSpans>>::new();
    let mut span_count = 0usize;

    for resource in resource_spans {
        let ResourceSpans {
            resource: resource_value,
            scope_spans,
            schema_url: resource_schema_url,
        } = resource;
        let mut by_trace = BTreeMap::<TraceId, Vec<ScopeSpans>>::new();
        for scope in scope_spans {
            let ScopeSpans {
                scope: scope_value,
                spans,
                schema_url: scope_schema_url,
            } = scope;
            let mut scope_by_trace = BTreeMap::new();
            for span in spans {
                let trace_id = TraceId::from_slice(&span.trace_id)?;
                scope_by_trace
                    .entry(trace_id)
                    .or_insert_with(Vec::new)
                    .push(span);
                span_count = span_count.saturating_add(1);
            }
            for (trace_id, spans) in scope_by_trace {
                by_trace.entry(trace_id).or_default().push(ScopeSpans {
                    scope: scope_value.clone(),
                    spans,
                    schema_url: scope_schema_url.clone(),
                });
            }
        }
        for (trace_id, scope_spans) in by_trace {
            grouped.entry(trace_id).or_default().push(ResourceSpans {
                resource: resource_value.clone(),
                scope_spans,
                schema_url: resource_schema_url.clone(),
            });
        }
    }

    if span_count == 0 {
        return Err(Error::Invalid(
            "OTLP trace export must contain at least one span".to_owned(),
        ));
    }

    grouped
        .into_iter()
        .map(|(trace_id, resource_spans)| {
            Trace::new(trace_id, resource_spans).map(|trace| TraceBatch::new(vec![trace]))
        })
        .collect()
}

impl TryFrom<ExportTraceServiceRequest> for TraceBatch {
    type Error = Error;

    fn try_from(request: ExportTraceServiceRequest) -> Result<Self> {
        let batches = trace_batches(request)?;
        Ok(Self::new(
            batches.into_iter().flat_map(|batch| batch.traces).collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span},
    };

    use super::*;

    fn span(id: u8) -> Span {
        Span {
            trace_id: vec![id; 16],
            span_id: vec![id; 8],
            parent_span_id: vec![9; 8],
            name: format!("span-{id}"),
            kind: span::SpanKind::Server as i32,
            start_time_unix_nano: 10,
            end_time_unix_nano: 20,
            attributes: vec![KeyValue {
                key: "typed".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(42)),
                }),
            }],
            events: vec![span::Event {
                time_unix_nano: 12,
                name: "event".into(),
                attributes: Vec::new(),
                dropped_attributes_count: 0,
            }],
            links: vec![span::Link {
                trace_id: vec![7; 16],
                span_id: vec![7; 8],
                trace_state: String::new(),
                attributes: Vec::new(),
                dropped_attributes_count: 0,
                flags: 0,
            }],
            status: Some(Status {
                message: "ok".into(),
                code: 1,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn splits_mixed_trace_ids_and_preserves_otlp_context() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("api".into())),
                        }),
                    }],
                    dropped_attributes_count: 3,
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "scope".into(),
                        version: "1".into(),
                        attributes: Vec::new(),
                        dropped_attributes_count: 2,
                    }),
                    spans: vec![span(2), span(1)],
                    schema_url: "scope-schema".into(),
                }],
                schema_url: "resource-schema".into(),
            }],
        };
        let batches = trace_batches(request).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches[0].traces[0].trace_id,
            TraceId::new([1; 16]).unwrap()
        );
        let resource = &batches[0].traces[0].resource_spans[0];
        assert_eq!(resource.schema_url, "resource-schema");
        assert_eq!(
            resource.resource.as_ref().unwrap().dropped_attributes_count,
            3
        );
        let scope = &resource.scope_spans[0];
        assert_eq!(scope.schema_url, "scope-schema");
        assert_eq!(scope.scope.as_ref().unwrap().name, "scope");
        assert_eq!(scope.spans[0], span(1));
    }

    #[test]
    fn rejects_empty_and_malformed_trace_ids() {
        assert!(trace_batches(ExportTraceServiceRequest::default()).is_err());
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![1; 15],
                        ..span(1)
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        assert!(trace_batches(request).is_err());
    }
}
