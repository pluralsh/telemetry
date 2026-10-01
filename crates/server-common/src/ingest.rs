//! Pluggable hooks around public ingestion, shared by the product servers.
//!
//! [`IngestLayer`] is a tower layer for the public write routes (axum) and
//! write gRPC servers (tonic). It counts request body bytes as the handler
//! reads them and, once the handler has responded, reports successful writes
//! to every [`IngestMiddleware`] in the [`IngestPipeline`]. Forwarded shard
//! writes between peers do not pass through it, so each request is seen once.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use axum::{
    body::Bytes,
    extract::OriginalUri,
    http::{Request, Response, StatusCode},
};
use http_body::{Body, Frame, SizeHint};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower::{Layer, Service};

use crate::usage::{UsageReporter, UsageReportingConfig};

const WRITE_PREFIX: &str = "/write/ns/";
const SCOPE_HEADER: &str = "x-scope-orgid";
const GRPC_STATUS: &str = "grpc-status";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Signal {
    Metrics,
    Logs,
    Traces,
}

impl Signal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Metrics => "metrics",
            Self::Logs => "logs",
            Self::Traces => "traces",
        }
    }
}

/// A public write that the server accepted.
#[derive(Debug, Clone, Copy)]
pub struct IngestRequest<'a> {
    pub signal: Signal,
    pub namespace: &'a str,
    /// Request body size as received, before any content decoding.
    pub bytes: u64,
}

pub trait IngestMiddleware: Send + Sync {
    fn record(&self, request: &IngestRequest<'_>);
}

#[derive(Clone)]
pub struct IngestPipeline {
    signal: Signal,
    middleware: Arc<[Arc<dyn IngestMiddleware>]>,
}

impl IngestPipeline {
    pub fn new(signal: Signal, middleware: Vec<Arc<dyn IngestMiddleware>>) -> Self {
        Self {
            signal,
            middleware: middleware.into(),
        }
    }

    /// Builds the pipeline every product server runs, returning the
    /// background tasks the caller must await on shutdown after cancelling
    /// `cancellation`.
    pub fn standard<'a>(
        signal: Signal,
        namespaces: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
        usage: &UsageReportingConfig,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<(Self, Vec<JoinHandle<()>>)> {
        let mut middleware: Vec<Arc<dyn IngestMiddleware>> = Vec::new();
        let mut tasks = Vec::new();
        if let Some(reporter) = UsageReporter::new(signal, namespaces, usage)? {
            let reporter = Arc::new(reporter);
            tasks.push(Arc::clone(&reporter).spawn(cancellation.clone()));
            middleware.push(reporter);
        }
        Ok((Self::new(signal, middleware), tasks))
    }

    pub fn is_empty(&self) -> bool {
        self.middleware.is_empty()
    }

    pub fn record(&self, namespace: &str, bytes: u64) {
        let request = IngestRequest {
            signal: self.signal,
            namespace,
            bytes,
        };
        for middleware in self.middleware.iter() {
            middleware.record(&request);
        }
    }

    /// Layer for axum write routes, resolving the namespace from
    /// `/write/ns/{namespace}/...` and counting any 2xx response.
    pub fn http_layer(&self) -> IngestLayer<axum::body::Body> {
        IngestLayer {
            pipeline: self.clone(),
            protocol: Protocol::Http,
            rebody: axum::body::Body::new,
        }
    }

    /// Layer for unary gRPC write servers, resolving the namespace from the
    /// `x-scope-orgid` metadata. `rebody` converts the counting body back
    /// into the request body type of the wrapped tonic server.
    pub fn grpc_layer<B>(&self, rebody: fn(CountingBody<B>) -> B) -> IngestLayer<B> {
        IngestLayer {
            pipeline: self.clone(),
            protocol: Protocol::Grpc,
            rebody,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Protocol {
    Http,
    Grpc,
}

impl Protocol {
    fn namespace<B>(self, request: &Request<B>) -> Option<String> {
        match self {
            Self::Http => {
                // Nested routers strip their prefix from `uri()`.
                let uri = request
                    .extensions()
                    .get::<OriginalUri>()
                    .map_or(request.uri(), |original| &original.0);
                let (_, rest) = uri.path().split_once(WRITE_PREFIX)?;
                rest.split('/')
                    .next()
                    .filter(|namespace| !namespace.is_empty())
                    .map(str::to_owned)
            }
            Self::Grpc => request
                .headers()
                .get(SCOPE_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        }
    }

    /// Unary tonic handlers that fail reply trailers-only, with
    /// `grpc-status` in the headers; successes carry it in the trailers.
    fn succeeded<B>(self, response: &Response<B>) -> bool {
        match self {
            Self::Http => response.status().is_success(),
            Self::Grpc => {
                response.status() == StatusCode::OK
                    && response
                        .headers()
                        .get(GRPC_STATUS)
                        .is_none_or(|status| status == "0")
            }
        }
    }
}

pub struct IngestLayer<B> {
    pipeline: IngestPipeline,
    protocol: Protocol,
    rebody: fn(CountingBody<B>) -> B,
}

impl<B> Clone for IngestLayer<B> {
    fn clone(&self) -> Self {
        Self {
            pipeline: self.pipeline.clone(),
            protocol: self.protocol,
            rebody: self.rebody,
        }
    }
}

impl<S, B> Layer<S> for IngestLayer<B> {
    type Service = IngestService<S, B>;

    fn layer(&self, inner: S) -> Self::Service {
        IngestService {
            inner,
            layer: self.clone(),
        }
    }
}

pub struct IngestService<S, B> {
    inner: S,
    layer: IngestLayer<B>,
}

impl<S: Clone, B> Clone for IngestService<S, B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            layer: self.layer.clone(),
        }
    }
}

impl<S, B, ResBody> Service<Request<B>> for IngestService<S, B>
where
    S: Service<Request<B>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Body<Data = Bytes> + Unpin + Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        // Call the instance that `poll_ready` readied, leaving a fresh clone.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let namespace = if self.layer.pipeline.is_empty() {
            None
        } else {
            self.layer.protocol.namespace(&request)
        };
        let Some(namespace) = namespace else {
            return Box::pin(inner.call(request));
        };
        let bytes = Arc::new(AtomicU64::new(0));
        let rebody = self.layer.rebody;
        let counted = Arc::clone(&bytes);
        let request = request.map(|inner| {
            rebody(CountingBody {
                inner,
                bytes: counted,
            })
        });
        let pipeline = self.layer.pipeline.clone();
        let protocol = self.layer.protocol;
        let response = inner.call(request);
        Box::pin(async move {
            let response = response.await?;
            if protocol.succeeded(&response) {
                pipeline.record(&namespace, bytes.load(Ordering::Acquire));
            }
            Ok(response)
        })
    }
}

/// A request body that tallies the data bytes read through it.
pub struct CountingBody<B> {
    inner: B,
    bytes: Arc<AtomicU64>,
}

impl<B> Body for CountingBody<B>
where
    B: Body<Data = Bytes> + Unpin,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let polled = Pin::new(&mut self.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &polled
            && let Some(data) = frame.data_ref()
        {
            self.bytes.fetch_add(data.len() as u64, Ordering::AcqRel);
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use axum::{
        Router,
        body::Body as AxumBody,
        http::{HeaderMap, StatusCode},
        routing::post,
    };
    use tower::ServiceExt;

    use super::*;

    #[derive(Default)]
    struct Recorder {
        recorded: Mutex<Vec<(Signal, String, u64)>>,
    }

    impl IngestMiddleware for Recorder {
        fn record(&self, request: &IngestRequest<'_>) {
            self.recorded.lock().unwrap().push((
                request.signal,
                request.namespace.to_owned(),
                request.bytes,
            ));
        }
    }

    fn pipeline() -> (IngestPipeline, Arc<Recorder>) {
        let recorder = Arc::new(Recorder::default());
        (
            IngestPipeline::new(Signal::Logs, vec![recorder.clone()]),
            recorder,
        )
    }

    async fn send(app: &Router, path: &str, headers: &[(&str, &str)], body: &'static str) {
        let mut request = Request::post(path);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        app.clone()
            .oneshot(request.body(AxumBody::from(body)).unwrap())
            .await
            .unwrap();
    }

    async fn write(body: axum::body::Bytes) -> StatusCode {
        if body.as_ref() == b"bad" {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::NO_CONTENT
        }
    }

    #[tokio::test]
    async fn http_layer_records_successful_writes_under_a_nested_prefix() {
        let (pipeline, recorder) = pipeline();
        let writes = Router::new()
            .route("/write/ns/{namespace}/push", post(write))
            .route_layer(pipeline.http_layer());
        let app = Router::new().nest("/logs", writes);

        send(&app, "/logs/write/ns/tenant/push", &[], "hello").await;
        send(&app, "/logs/write/ns/tenant/push", &[], "bad").await;
        send(&app, "/logs/write/ns/other/missing", &[], "unrouted").await;

        assert_eq!(
            *recorder.recorded.lock().unwrap(),
            vec![(Signal::Logs, "tenant".to_owned(), 5)]
        );
    }

    async fn grpc(body: axum::body::Bytes) -> (StatusCode, HeaderMap) {
        let mut response = HeaderMap::new();
        if body.as_ref() == b"bad" {
            response.insert(GRPC_STATUS, "3".parse().unwrap());
        }
        (StatusCode::OK, response)
    }

    #[tokio::test]
    async fn grpc_layer_uses_scope_header_and_trailers_only_failures() {
        let (pipeline, recorder) = pipeline();
        let app = Router::new()
            .route("/svc/Export", post(grpc))
            .layer(pipeline.grpc_layer(AxumBody::new));

        send(&app, "/svc/Export", &[(SCOPE_HEADER, "tenant")], "spans").await;
        send(&app, "/svc/Export", &[(SCOPE_HEADER, "tenant")], "bad").await;
        send(&app, "/svc/Export", &[], "anonymous").await;

        assert_eq!(
            *recorder.recorded.lock().unwrap(),
            vec![(Signal::Logs, "tenant".to_owned(), 5)]
        );
    }

    #[tokio::test]
    async fn standard_pipeline_is_empty_without_endpoints() {
        let cancellation = CancellationToken::new();
        let (pipeline, tasks) = IngestPipeline::standard(
            Signal::Metrics,
            [("tenant", None)],
            &UsageReportingConfig::default(),
            &cancellation,
        )
        .unwrap();
        assert!(pipeline.is_empty());
        assert!(tasks.is_empty());
    }
}
