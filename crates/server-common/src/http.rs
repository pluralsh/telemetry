//! HTTP error responses and request helpers shared by the product servers.

use std::fmt::Display;

use axum::{
    Json,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use prost::Message;
use serde_json::json;

/// An HTTP API failure. Renders as a Prometheus/Loki-style JSON error, or as
/// a `google.rpc.Status` for OTLP/HTTP endpoints.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
    error_type: Option<&'static str>,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Display) -> Self {
        Self {
            status,
            message: message.to_string(),
            error_type: None,
        }
    }

    /// Overrides the Prometheus `errorType`, which otherwise follows the status.
    pub fn with_error_type(mut self, error_type: &'static str) -> Self {
        self.error_type = Some(error_type);
        self
    }

    pub fn bad_request(error: impl Display) -> Self {
        Self::new(StatusCode::BAD_REQUEST, error)
    }

    pub fn not_found(error: impl Display) -> Self {
        Self::new(StatusCode::NOT_FOUND, error)
    }

    pub fn internal(error: impl Display) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, error)
    }

    pub fn unavailable(error: impl Display) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, error)
    }

    /// Write backpressure; clients should retry after a short delay.
    pub fn too_many_requests(error: impl Display) -> Self {
        Self::new(StatusCode::TOO_MANY_REQUESTS, error)
    }

    pub fn unauthorized() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "authentication required")
    }

    pub fn unsupported_media(error: impl Display) -> Self {
        Self::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, error)
    }

    pub fn too_large() -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body exceeds configured limit",
        )
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn into_parts(self) -> (StatusCode, String) {
        (self.status, self.message)
    }

    /// Renders an OTLP/HTTP failure as a `google.rpc.Status`, JSON-encoded
    /// when the request was JSON and protobuf-encoded otherwise.
    pub fn into_otlp_response(self, json_response: bool) -> Response {
        let status = self.status;
        let code = grpc_code_for_http(status);
        let response = if json_response {
            (
                status,
                [(header::CONTENT_TYPE, "application/json")],
                Json(json!({"code": code, "message": self.message})),
            )
                .into_response()
        } else {
            let encoded = GoogleRpcStatus {
                code,
                message: self.message,
            }
            .encode_to_vec();
            (
                status,
                [(header::CONTENT_TYPE, "application/x-protobuf")],
                encoded,
            )
                .into_response()
        };
        with_retry_after(status, response)
    }

    /// Backpressure maps to `Unavailable` because OTLP gRPC exporters only
    /// retry `ResourceExhausted` when it carries RetryInfo.
    pub fn into_grpc_status(self) -> tonic::Status {
        let code = match self.status {
            StatusCode::BAD_REQUEST => tonic::Code::InvalidArgument,
            StatusCode::UNAUTHORIZED => tonic::Code::Unauthenticated,
            StatusCode::NOT_FOUND => tonic::Code::NotFound,
            StatusCode::PAYLOAD_TOO_LARGE => tonic::Code::ResourceExhausted,
            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => {
                tonic::Code::Unavailable
            }
            _ => tonic::Code::Internal,
        };
        tonic::Status::new(code, self.message)
    }
}

impl From<sharding::RouteError> for ApiError {
    fn from(error: sharding::RouteError) -> Self {
        Self::unavailable(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let error_type = self.error_type.unwrap_or(match self.status {
            StatusCode::NOT_FOUND => "not_found",
            StatusCode::SERVICE_UNAVAILABLE => "unavailable",
            status if status.is_server_error() => "internal",
            _ => "bad_data",
        });
        let response = (
            self.status,
            Json(json!({"status": "error", "errorType": error_type, "error": self.message})),
        )
            .into_response();
        with_retry_after(self.status, response)
    }
}

fn with_retry_after(status: StatusCode, mut response: Response) -> Response {
    if status == StatusCode::TOO_MANY_REQUESTS {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    response
}

/// `google.rpc.Status`, the OTLP/HTTP error payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct GoogleRpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
}

/// The canonical gRPC code for an HTTP status, per the OTLP/HTTP mapping.
pub fn grpc_code_for_http(status: StatusCode) -> i32 {
    let code = match status {
        StatusCode::BAD_REQUEST | StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            tonic::Code::InvalidArgument
        }
        StatusCode::NOT_FOUND => tonic::Code::NotFound,
        StatusCode::PAYLOAD_TOO_LARGE | StatusCode::TOO_MANY_REQUESTS => {
            tonic::Code::ResourceExhausted
        }
        StatusCode::INTERNAL_SERVER_ERROR => tonic::Code::Internal,
        StatusCode::SERVICE_UNAVAILABLE => tonic::Code::Unavailable,
        StatusCode::UNAUTHORIZED => tonic::Code::Unauthenticated,
        _ => tonic::Code::Unknown,
    };
    code as i32
}

/// The media type of a request, without parameters such as `charset`.
pub fn content_type(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// Rejects request bodies larger than `limit` bytes with 413.
pub fn check_size(size: usize, limit: usize) -> Result<(), ApiError> {
    if size > limit {
        Err(ApiError::too_large())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

    use super::*;

    #[tokio::test]
    async fn json_errors_classify_client_and_server_failures() {
        let body = |response: Response| async {
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()
        };
        let client = body(ApiError::bad_request("nope").into_response()).await;
        assert_eq!(client["errorType"], "bad_data");
        assert_eq!(client["error"], "nope");
        let server = body(ApiError::internal("boom").into_response()).await;
        assert_eq!(server["errorType"], "internal");
        let unavailable = body(ApiError::unavailable("moving").into_response()).await;
        assert_eq!(unavailable["errorType"], "unavailable");
        let execution = ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "bad match")
            .with_error_type("execution")
            .into_response();
        assert_eq!(execution.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body(execution).await["errorType"], "execution");
    }

    #[tokio::test]
    async fn backpressure_is_retryable_in_both_encodings() {
        let json = ApiError::too_many_requests("full").into_response();
        assert_eq!(json.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(json.headers()[header::RETRY_AFTER], "1");

        let otlp = ApiError::too_many_requests("full").into_otlp_response(false);
        assert_eq!(otlp.headers()[header::RETRY_AFTER], "1");
        let bytes = to_bytes(otlp.into_body(), usize::MAX).await.unwrap();
        let status = GoogleRpcStatus::decode(bytes).unwrap();
        assert_eq!(status.code, tonic::Code::ResourceExhausted as i32);
        assert_eq!(status.message, "full");
    }

    #[test]
    fn grpc_backpressure_is_retryable_unavailable() {
        let status = ApiError::too_many_requests("full").into_grpc_status();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "full");
    }

    #[test]
    fn content_type_strips_parameters() {
        let mut headers = HeaderMap::new();
        assert_eq!(content_type(&headers), None);
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );
        assert_eq!(content_type(&headers), Some("application/json"));
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(" ; x"));
        assert_eq!(content_type(&headers), None);
    }

    #[test]
    fn check_size_rejects_oversized_bodies() {
        assert!(check_size(10, 10).is_ok());
        assert_eq!(
            check_size(11, 10).unwrap_err().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }
}
