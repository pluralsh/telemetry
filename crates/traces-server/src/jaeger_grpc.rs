use axum::http::{HeaderMap, HeaderValue, header};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span},
};
use prost::Message;
use server_common::auth::{Permission, authorize};
use tonic::{Request, Response, Status as GrpcStatus};

use crate::{
    AppState,
    config::ServerMode,
    http::ApiError,
    jaeger::{
        Batch, KeyValue as JaegerKeyValue, PostSpansRequest, PostSpansResponse, Process,
        Span as JaegerSpan, ValueType,
        collector_service_server::{CollectorService, CollectorServiceServer},
    },
};

#[tonic::async_trait]
impl CollectorService for AppState {
    async fn post_spans(
        &self,
        request: Request<PostSpansRequest>,
    ) -> Result<Response<PostSpansResponse>, GrpcStatus> {
        if self.config.mode == ServerMode::Reader {
            return Err(GrpcStatus::not_found("write routes are disabled"));
        }
        let namespace = request
            .metadata()
            .get("x-scope-orgid")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| GrpcStatus::invalid_argument("x-scope-orgid metadata is required"))?;
        let namespace_config = self
            .namespaces
            .get(namespace)
            .ok_or_else(|| GrpcStatus::not_found("unknown namespace"))?;
        let mut headers = HeaderMap::new();
        if let Some(value) = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
        {
            headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(value)
                    .map_err(|error| GrpcStatus::invalid_argument(error.to_string()))?,
            );
        }
        if !authorize(
            &headers,
            self.config.auth.unauthenticated,
            &self.config.auth.global,
            &namespace_config.auth,
            self.jwt.as_ref(),
            namespace,
            Permission::Write,
        )
        .await
        {
            return Err(GrpcStatus::unauthenticated("authentication required"));
        }
        if request.get_ref().encoded_len() > self.config.request.max_request_bytes {
            return Err(GrpcStatus::resource_exhausted(
                "request exceeds configured byte limit",
            ));
        }
        let namespace = plural_traces::Namespace::new(namespace)
            .map_err(|error| GrpcStatus::invalid_argument(error.to_string()))?;
        let _permit = self
            .request_limit
            .acquire()
            .await
            .map_err(|_| GrpcStatus::unavailable("server is shutting down"))?;
        let batch = request
            .into_inner()
            .batch
            .ok_or_else(|| GrpcStatus::invalid_argument("Jaeger batch is required"))?;
        let resource_spans = convert_batch(batch).map_err(GrpcStatus::invalid_argument)?;
        let batches = plural_traces::trace_batches_from_resource_spans(resource_spans)
            .map_err(|error| GrpcStatus::invalid_argument(error.to_string()))?;
        self.route_write(&namespace, batches, ulid::Ulid::new().to_string())
            .await
            .map_err(ApiError::into_grpc_status)?;
        Ok(Response::new(PostSpansResponse {}))
    }
}

pub fn jaeger_grpc_service(state: AppState) -> CollectorServiceServer<AppState> {
    CollectorServiceServer::new(state)
}

fn convert_batch(batch: Batch) -> Result<Vec<ResourceSpans>, &'static str> {
    if batch.spans.is_empty() {
        return Err("Jaeger batch must contain at least one span");
    }
    batch
        .spans
        .into_iter()
        .map(|value| convert_span(value, batch.process.as_ref()))
        .collect()
}

fn convert_span(
    value: JaegerSpan,
    batch_process: Option<&Process>,
) -> Result<ResourceSpans, &'static str> {
    if value.trace_id.len() != 16 || value.span_id.len() != 8 {
        return Err("Jaeger trace IDs must be 16 bytes and span IDs 8 bytes");
    }
    let process = value.process.as_ref().or(batch_process);
    let mut resource_attributes = process
        .map(|process| process.tags.iter().map(convert_tag).collect::<Vec<_>>())
        .unwrap_or_default();
    if let Some(process) = process
        && !process.service_name.is_empty()
    {
        resource_attributes.push(string_value("service.name", process.service_name.clone()));
    }
    let parent_span_id = value
        .references
        .iter()
        .find(|reference| reference.ref_type == 0 && reference.trace_id == value.trace_id)
        .map(|reference| reference.span_id.clone())
        .unwrap_or_default();
    let links = value
        .references
        .iter()
        .filter(|reference| !(reference.ref_type == 0 && reference.trace_id == value.trace_id))
        .map(|reference| span::Link {
            trace_id: reference.trace_id.clone(),
            span_id: reference.span_id.clone(),
            ..Default::default()
        })
        .collect();
    let status = value
        .tags
        .iter()
        .find(|tag| tag.key == "error" && tag.v_bool)
        .map(|_| Status {
            code: 2,
            ..Default::default()
        });
    let start = timestamp_ns(value.start_time.as_ref())?;
    let duration = duration_ns(value.duration.as_ref())?;
    let events = value
        .logs
        .into_iter()
        .map(|log| {
            let name = log
                .fields
                .iter()
                .find(|field| field.key == "event")
                .map(tag_string)
                .unwrap_or_else(|| "log".to_owned());
            Ok(span::Event {
                time_unix_nano: timestamp_ns(log.timestamp.as_ref())?,
                name,
                attributes: log
                    .fields
                    .iter()
                    .filter(|field| field.key != "event")
                    .map(convert_tag)
                    .collect(),
                dropped_attributes_count: 0,
            })
        })
        .collect::<Result<Vec<_>, &'static str>>()?;
    let attributes = value.tags.iter().map(convert_tag).collect();
    Ok(ResourceSpans {
        resource: Some(Resource {
            attributes: resource_attributes,
            dropped_attributes_count: 0,
        }),
        scope_spans: vec![ScopeSpans {
            spans: vec![Span {
                trace_id: value.trace_id,
                span_id: value.span_id,
                parent_span_id,
                flags: value.flags,
                name: value.operation_name,
                start_time_unix_nano: start,
                end_time_unix_nano: start.saturating_add(duration),
                attributes,
                events,
                links,
                status,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    })
}

fn convert_tag(value: &JaegerKeyValue) -> KeyValue {
    let key = value.key.clone();
    let converted = match ValueType::try_from(value.v_type).unwrap_or(ValueType::String) {
        ValueType::String => any_value::Value::StringValue(value.v_str.clone()),
        ValueType::Bool => any_value::Value::BoolValue(value.v_bool),
        ValueType::Int64 => any_value::Value::IntValue(value.v_int64),
        ValueType::Float64 => any_value::Value::DoubleValue(value.v_float64),
        ValueType::Binary => any_value::Value::BytesValue(value.v_binary.clone()),
    };
    KeyValue {
        key,
        value: Some(AnyValue {
            value: Some(converted),
        }),
    }
}

fn string_value(key: impl Into<String>, value: impl Into<String>) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
    }
}

fn tag_string(value: &JaegerKeyValue) -> String {
    match ValueType::try_from(value.v_type).unwrap_or(ValueType::String) {
        ValueType::String => value.v_str.clone(),
        ValueType::Bool => value.v_bool.to_string(),
        ValueType::Int64 => value.v_int64.to_string(),
        ValueType::Float64 => value.v_float64.to_string(),
        ValueType::Binary => {
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &value.v_binary)
        }
    }
}

fn timestamp_ns(value: Option<&prost_types::Timestamp>) -> Result<u64, &'static str> {
    let value = value.ok_or("Jaeger timestamp is required")?;
    if value.seconds < 0 || !(0..1_000_000_000).contains(&value.nanos) {
        return Err("invalid Jaeger timestamp");
    }
    Ok((value.seconds as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(value.nanos as u64))
}

fn duration_ns(value: Option<&prost_types::Duration>) -> Result<u64, &'static str> {
    let value = value.ok_or("Jaeger duration is required")?;
    if value.seconds < 0 || value.nanos < 0 || value.nanos >= 1_000_000_000 {
        return Err("invalid Jaeger duration");
    }
    Ok((value.seconds as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(value.nanos as u64))
}
