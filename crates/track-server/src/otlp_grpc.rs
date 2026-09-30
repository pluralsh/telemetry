use axum::http::{HeaderMap, HeaderValue, header};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
    trace_service_server::{TraceService, TraceServiceServer},
};
use prost::Message;
use server_common::auth::{Permission, authorize};
use tonic_otlp::{Request, Response, Status};
use track::{Namespace, trace_batches};

use crate::{AppState, config::ServerMode};

#[tonic::async_trait]
impl TraceService for AppState {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        if self.config.mode == ServerMode::Reader {
            return Err(Status::not_found("write routes are disabled"));
        }
        let namespace = request
            .metadata()
            .get("x-scope-orgid")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| Status::invalid_argument("x-scope-orgid metadata is required"))?;
        let namespace_config = self
            .namespaces
            .get(namespace)
            .ok_or_else(|| Status::not_found("unknown namespace"))?;
        let mut headers = HeaderMap::new();
        if let Some(value) = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
        {
            headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(value)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
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
            return Err(Status::unauthenticated("authentication required"));
        }
        let namespace = Namespace::new(namespace)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if request.get_ref().encoded_len() > self.config.request.max_request_bytes {
            return Err(Status::resource_exhausted(
                "request exceeds configured byte limit",
            ));
        }
        let _permit = self
            .request_limit
            .acquire()
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))?;
        let batches = trace_batches(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.route_write(&namespace, batches, ulid::Ulid::new().to_string())
            .await
            .map_err(crate::http::otlp_grpc_status)?;
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

pub fn otlp_grpc_service(state: AppState) -> TraceServiceServer<AppState> {
    TraceServiceServer::new(state)
}
