pub mod config;
pub mod openapi;

mod http;
mod internal_writer;
mod jaeger_grpc;
mod otlp_grpc;
mod state;

pub mod jaeger {
    #![allow(clippy::result_large_err)]
    tonic::include_proto!("jaeger.api_v2");
}

pub use http::router;
pub use internal_writer::grpc_service;
pub use jaeger_grpc::jaeger_grpc_service;
pub use otlp_grpc::otlp_grpc_service;
pub use state::AppState;

#[cfg(test)]
mod tests;
