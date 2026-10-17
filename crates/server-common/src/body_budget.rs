//! A process-wide bound on the request bodies a server holds: as received,
//! after content decoding, and while handlers decode and parse them.
//!
//! Every request that carries a body takes a [`BodyTicket`] from the
//! process's [`BodyBudget`] and reserves bytes on it before allocating them:
//! its declared `Content-Length` up front, each buffer growth while the body
//! streams in (chunked bodies, gzip output), and whatever its handler decodes
//! from it. A request that could never fit is rejected with 413; one that
//! would fit once others finish gets 429 with `Retry-After`, or gRPC
//! `UNAVAILABLE`, rather than allocating past the budget. The ticket releases
//! everything once the response is ready.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use axum::{
    body::{Body, Bytes},
    extract::{Request, State},
    http::{self, HeaderMap, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use http_body::Body as HttpBody;
use tokio::sync::Semaphore;
use tower::{Layer, Service};

use crate::http::{ApiError, content_type};

/// Why a body was not admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyRejection {
    /// Larger than the request limit or the whole budget: 413.
    TooLarge,
    /// Would fit once other requests release theirs: 429.
    Busy,
    /// The body stream failed, e.g. invalid gzip: 400.
    Invalid,
}

impl BodyRejection {
    pub fn api_error(self) -> ApiError {
        match self {
            Self::TooLarge => ApiError::too_large(),
            Self::Busy => ApiError::too_many_requests("request body memory budget is full"),
            Self::Invalid => ApiError::bad_request("invalid request body"),
        }
    }

    pub fn grpc_status(self) -> tonic::Status {
        match self {
            Self::TooLarge => {
                tonic::Status::resource_exhausted("request exceeds the request body memory budget")
            }
            Self::Busy => tonic::Status::unavailable("request body memory budget is full"),
            Self::Invalid => tonic::Status::invalid_argument("invalid request body"),
        }
    }

    fn reason(self) -> &'static str {
        match self {
            Self::TooLarge => "too_large",
            Self::Busy => "busy",
            Self::Invalid => "invalid",
        }
    }
}

impl From<BodyRejection> for ApiError {
    fn from(rejection: BodyRejection) -> Self {
        rejection.api_error()
    }
}

/// The bytes every in-flight request body may hold at once, shared by all
/// of a process's listeners.
#[derive(Clone, Debug)]
pub struct BodyBudget {
    inner: Arc<BudgetInner>,
}

#[derive(Debug)]
struct BudgetInner {
    semaphore: Semaphore,
    capacity: usize,
}

impl BodyBudget {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.clamp(1, Semaphore::MAX_PERMITS);
        metrics::gauge!("telemetry_request_body_budget_bytes").set(capacity as f64);
        Self {
            inner: Arc::new(BudgetInner {
                semaphore: Semaphore::new(capacity),
                capacity,
            }),
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn available(&self) -> usize {
        self.inner.semaphore.available_permits()
    }

    pub fn ticket(&self) -> BodyTicket {
        BodyTicket(Arc::new(TicketInner {
            budget: self.clone(),
            held: AtomicUsize::new(0),
        }))
    }

    fn held(&self) -> usize {
        self.capacity() - self.available()
    }
}

/// One request's share of a [`BodyBudget`], released when every clone
/// drops.
#[derive(Clone, Debug)]
pub struct BodyTicket(Arc<TicketInner>);

#[derive(Debug)]
struct TicketInner {
    budget: BodyBudget,
    held: AtomicUsize,
}

impl BodyTicket {
    /// Holds `bytes` more for this request, failing rather than waiting.
    pub fn reserve(&self, bytes: usize) -> Result<(), BodyRejection> {
        if bytes == 0 {
            return Ok(());
        }
        let budget = &self.0.budget;
        let held = self.0.held.load(Ordering::Relaxed);
        let fits = held
            .checked_add(bytes)
            .is_some_and(|total| total <= budget.capacity());
        let rejection = match u32::try_from(bytes) {
            Ok(permits) if fits => match budget.inner.semaphore.try_acquire_many(permits) {
                Ok(permit) => {
                    permit.forget();
                    self.0.held.fetch_add(bytes, Ordering::Relaxed);
                    metrics::gauge!("telemetry_request_body_bytes_in_flight")
                        .set(budget.held() as f64);
                    return Ok(());
                }
                Err(_) => BodyRejection::Busy,
            },
            _ => BodyRejection::TooLarge,
        };
        metrics::counter!("telemetry_request_body_rejections_total", "reason" => rejection.reason())
            .increment(1);
        Err(rejection)
    }

    /// Bytes this request holds.
    pub fn held(&self) -> usize {
        self.0.held.load(Ordering::Relaxed)
    }
}

impl Drop for TicketInner {
    fn drop(&mut self) {
        let held = *self.held.get_mut();
        if held > 0 {
            self.budget.inner.semaphore.add_permits(held);
            metrics::gauge!("telemetry_request_body_bytes_in_flight")
                .set(self.budget.held() as f64);
        }
    }
}

/// Reads `body` into one buffer of at most `limit` bytes, reserving each
/// growth of that buffer on `ticket` before making it. The first `reserved`
/// bytes of capacity are already held, as for a declared `Content-Length`.
pub async fn collect<B>(
    body: B,
    ticket: &BodyTicket,
    limit: usize,
    reserved: usize,
) -> Result<Bytes, BodyRejection>
where
    B: HttpBody<Data = Bytes>,
{
    let mut body = std::pin::pin!(body);
    let mut buffer: Vec<u8> = Vec::new();
    let mut held = 0usize;
    if reserved > 0 {
        buffer
            .try_reserve_exact(reserved)
            .map_err(|_| BodyRejection::TooLarge)?;
        held = buffer.capacity().max(reserved);
        ticket.reserve(held.saturating_sub(reserved))?;
    }
    while let Some(frame) = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        let frame = frame.map_err(|_| BodyRejection::Invalid)?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        let needed = buffer.len() + data.len();
        if needed > limit {
            return Err(BodyRejection::TooLarge);
        }
        if needed > buffer.capacity() {
            let target = needed.max(buffer.capacity().saturating_mul(2)).min(limit);
            ticket.reserve(target - held)?;
            buffer
                .try_reserve_exact(target - buffer.len())
                .map_err(|_| BodyRejection::TooLarge)?;
            held = target;
            if buffer.capacity() > held {
                ticket.reserve(buffer.capacity() - held)?;
                held = buffer.capacity();
            }
        }
        buffer.extend_from_slice(&data);
    }
    Ok(Bytes::from(buffer))
}

/// Per-request limits and the shared budget for the HTTP body middleware.
#[derive(Clone, Debug)]
pub struct BodyLimits {
    pub budget: BodyBudget,
    /// Largest body as received, before content decoding.
    pub max_request: usize,
    /// Largest body after gzip decoding.
    pub max_decoded: usize,
}

/// Marks a request whose body the HTTP layers will gunzip, so
/// [`hold_decoded_bodies`] counts what decoding produces.
#[derive(Clone, Copy, Debug)]
struct Compressed;

fn declared_length(headers: &HeaderMap) -> Option<usize> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn is_gzip(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("gzip"))
}

/// Renders a rejection the way the route's protocol reports errors.
fn reject(path: &str, headers: &HeaderMap, rejection: BodyRejection) -> Response {
    let error = rejection.api_error();
    if path.contains("/otlp/") {
        error.into_otlp_response(content_type(headers) == Some("application/json"))
    } else {
        error.into_response()
    }
}

/// Outermost body middleware: admits the body by its declared length,
/// buffers it as received under the budget, and hands the handler the
/// request's [`BodyTicket`] as an extension. Run it outside gzip decoding,
/// with [`hold_decoded_bodies`] inside it.
pub async fn hold_request_bodies(
    State(limits): State<BodyLimits>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_owned();
    let (mut parts, body) = request.into_parts();
    let ticket = limits.budget.ticket();
    let declared = declared_length(&parts.headers);
    if let Some(length) = declared {
        if length > limits.max_request {
            return reject(&path, &parts.headers, BodyRejection::TooLarge);
        }
        if let Err(rejection) = ticket.reserve(length) {
            return reject(&path, &parts.headers, rejection);
        }
    }
    let bytes = match collect(
        body,
        &ticket,
        limits.max_request,
        declared.unwrap_or_default(),
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(rejection) => return reject(&path, &parts.headers, rejection),
    };
    if is_gzip(&parts.headers) {
        parts.extensions.insert(Compressed);
    }
    parts.extensions.insert(ticket.clone());
    let response = next
        .run(Request::from_parts(parts, Body::from(bytes)))
        .await;
    drop(ticket);
    response
}

/// Inner body middleware, after gzip decoding: buffers the decoded body of
/// a compressed request under the request's ticket, cutting a decompression
/// bomb off at `max_decoded` bytes. Other bodies pass through untouched,
/// already held as received.
pub async fn hold_decoded_bodies(
    State(limits): State<BodyLimits>,
    request: Request,
    next: Next,
) -> Response {
    let compressed = request.extensions().get::<Compressed>().is_some();
    let Some(ticket) = request.extensions().get::<BodyTicket>().cloned() else {
        return next.run(request).await;
    };
    if !compressed || is_gzip(request.headers()) {
        return next.run(request).await;
    }
    let path = request.uri().path().to_owned();
    let (parts, body) = request.into_parts();
    match collect(body, &ticket, limits.max_decoded, 0).await {
        Ok(bytes) => {
            next.run(Request::from_parts(parts, Body::from(bytes)))
                .await
        }
        Err(rejection) => reject(&path, &parts.headers, rejection),
    }
}

/// [`crate::http::limit_request_bodies`] with every body held under the
/// budget: as received, and again as gunzipped.
pub fn budget_request_bodies<S>(router: axum::Router<S>, limits: &BodyLimits) -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router
        .layer(axum::extract::DefaultBodyLimit::max(limits.max_decoded))
        .layer(axum::middleware::from_fn_with_state(
            limits.clone(),
            hold_decoded_bodies,
        ))
        .layer(
            tower_http::decompression::RequestDecompressionLayer::new()
                .pass_through_unaccepted(true),
        )
        .layer(axum::middleware::from_fn_with_state(
            limits.clone(),
            hold_request_bodies,
        ))
}

/// Holds `bytes` on the request's ticket, as a handler does before decoding
/// or parsing its body into a new allocation of about that size. Requests
/// outside the body middleware are not budgeted.
pub fn reserve(ticket: Option<&BodyTicket>, bytes: usize) -> Result<(), BodyRejection> {
    ticket.map_or(Ok(()), |ticket| ticket.reserve(bytes))
}

/// How many bytes a handler reserves per decoded body byte for its parsed
/// form and, when it forwards rows to peers, their re-encoding: each about
/// the size of the decoded body.
pub const PARSED_BYTES_PER_DECODED_BYTE: usize = 2;

/// Wraps a gRPC service so each request buffers its body under `budget`
/// and also holds as much again for the message decoded from it, failing
/// with `UNAVAILABLE` while the budget is full and `RESOURCE_EXHAUSTED` when
/// it could never fit.
#[derive(Clone, Debug)]
pub struct GrpcBodyBudget<S> {
    inner: S,
    budget: BodyBudget,
}

impl<S> GrpcBodyBudget<S> {
    pub fn new(inner: S, budget: BodyBudget) -> Self {
        Self { inner, budget }
    }
}

/// [`GrpcBodyBudget`] as a tower layer.
#[derive(Clone, Debug)]
pub struct GrpcBodyBudgetLayer(pub BodyBudget);

impl<S> Layer<S> for GrpcBodyBudgetLayer {
    type Service = GrpcBodyBudget<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcBodyBudget::new(inner, self.0.clone())
    }
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for GrpcBodyBudget<S> {
    const NAME: &'static str = S::NAME;
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl<S> Service<http::Request<tonic::body::Body>> for GrpcBodyBudget<S>
where
    S: Service<http::Request<tonic::body::Body>, Response = http::Response<tonic::body::Body>>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        // The clone that was polled ready serves this call.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let ticket = self.budget.ticket();
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let bytes = match collect(body, &ticket, usize::MAX, 0).await {
                Ok(bytes) => bytes,
                Err(rejection) => return Ok(rejection.grpc_status().into_http()),
            };
            if let Err(rejection) = ticket.reserve(bytes.len()) {
                return Ok(rejection.grpc_status().into_http());
            }
            let body = tonic::body::Body::new(Body::from(bytes));
            let response = inner.call(http::Request::from_parts(parts, body)).await;
            drop(ticket);
            response
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use axum::{Router, body::to_bytes, http::StatusCode, routing::post};
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn tickets_reserve_within_the_budget_and_release_on_drop() {
        let budget = BodyBudget::new(100);
        let first = budget.ticket();
        first.reserve(60).unwrap();
        let second = budget.ticket();
        assert_eq!(second.reserve(50), Err(BodyRejection::Busy));
        assert_eq!(second.reserve(101), Err(BodyRejection::TooLarge));
        second.reserve(40).unwrap();
        // One request holding more than the whole budget never fits.
        assert_eq!(first.reserve(41), Err(BodyRejection::TooLarge));
        assert_eq!(budget.available(), 0);
        drop(first);
        assert_eq!(budget.available(), 60);
        drop(second);
        assert_eq!(budget.available(), 100);
    }

    const WIRE: usize = 64 << 10;
    const DECODED: usize = 256 << 10;

    fn router(budget: &BodyBudget, held: Arc<AtomicUsize>) -> Router {
        let limits = BodyLimits {
            budget: budget.clone(),
            max_request: WIRE,
            max_decoded: DECODED,
        };
        let budget = budget.clone();
        let router = Router::new().route(
            "/",
            post(move |body: Bytes| {
                let (budget, held) = (budget.clone(), held.clone());
                async move {
                    held.fetch_max(budget.capacity() - budget.available(), Ordering::Relaxed);
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    body.len().to_string()
                }
            }),
        );
        budget_request_bodies(router, &limits)
    }

    fn request(body: Vec<u8>, gzip: bool, chunked: bool) -> Request {
        let mut request = http::Request::post("/");
        if gzip {
            request = request.header(header::CONTENT_ENCODING, "gzip");
        }
        let body = if chunked {
            Body::from_stream(futures::stream::iter(
                body.chunks(1000)
                    .map(|chunk| Ok::<_, std::io::Error>(Bytes::copy_from_slice(chunk)))
                    .collect::<Vec<_>>(),
            ))
        } else {
            request = request.header(header::CONTENT_LENGTH, body.len());
            Body::from(body)
        };
        request.body(body).unwrap()
    }

    fn gzip(size: usize) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        let chunk = vec![0; 64 << 10];
        let mut left = size;
        while left > 0 {
            let len = left.min(chunk.len());
            encoder.write_all(&chunk[..len]).unwrap();
            left -= len;
        }
        encoder.finish().unwrap()
    }

    async fn status(app: &Router, request: Request) -> (StatusCode, Option<String>, Bytes) {
        let response = app.clone().oneshot(request).await.unwrap();
        let retry = response
            .headers()
            .get(header::RETRY_AFTER)
            .map(|value| value.to_str().unwrap().to_owned());
        let status = response.status();
        (
            status,
            retry,
            to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
    }

    #[tokio::test]
    async fn concurrent_bodies_beyond_the_budget_get_429_and_never_exceed_it() {
        // Room for three declared bodies of WIRE bytes, not four.
        let budget = BodyBudget::new(3 * WIRE + WIRE / 2);
        let held = Arc::new(AtomicUsize::new(0));
        let app = router(&budget, held.clone());
        let responses = futures::future::join_all((0..8).map(|index| {
            let app = app.clone();
            async move { status(&app, request(vec![1; WIRE], false, index % 2 == 1)).await }
        }))
        .await;
        let ok = responses
            .iter()
            .filter(|(status, ..)| *status == StatusCode::OK)
            .count();
        let busy = responses
            .iter()
            .filter(|(status, retry, _)| {
                *status == StatusCode::TOO_MANY_REQUESTS && retry.as_deref() == Some("1")
            })
            .count();
        assert!(ok >= 3, "{responses:?}");
        assert_eq!(ok + busy, 8, "{responses:?}");
        assert!(busy > 0);
        assert!(held.load(Ordering::Relaxed) <= budget.capacity());
        assert_eq!(budget.available(), budget.capacity());
        // Once they finish, the next one fits again.
        let (status, ..) = status(&app, request(vec![1; WIRE], false, false)).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn bodies_larger_than_the_request_limit_or_budget_get_413() {
        let budget = BodyBudget::new(WIRE / 2);
        let app = router(&budget, Arc::default());
        for chunked in [false, true] {
            // Over the budget, though within the request limit.
            let (status, ..) = status(&app, request(vec![1; WIRE], false, chunked)).await;
            assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "chunked={chunked}");
        }
        let budget = BodyBudget::new(8 * WIRE);
        let app = router(&budget, Arc::default());
        for chunked in [false, true] {
            let (status, ..) = status(&app, request(vec![1; WIRE + 1], false, chunked)).await;
            assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "chunked={chunked}");
        }
        assert_eq!(budget.available(), budget.capacity());
    }

    #[tokio::test]
    async fn decompression_bombs_stop_at_the_decoded_cap() {
        let budget = BodyBudget::new(8 * DECODED);
        let held = Arc::new(AtomicUsize::new(0));
        let app = router(&budget, held.clone());
        let (status_ok, _, body) = status(&app, request(gzip(DECODED), true, false)).await;
        assert_eq!(
            (status_ok, &body[..]),
            (StatusCode::OK, DECODED.to_string().as_bytes())
        );
        // Held while handled: the compressed body and the decoded one.
        assert!(held.load(Ordering::Relaxed) >= DECODED);
        assert!(held.load(Ordering::Relaxed) <= DECODED + WIRE);
        // 256 MiB of zeros gzips to about 256 KiB; only one request limit of
        // it is read and at most the decoded cap is ever inflated.
        let bomb = gzip(256 << 20);
        assert!(bomb.len() > WIRE);
        let (status_bomb, ..) = status(&app, request(bomb[..WIRE].to_vec(), true, true)).await;
        assert_eq!(status_bomb, StatusCode::PAYLOAD_TOO_LARGE);
        let (status_bomb, ..) = status(&app, request(gzip(4 * DECODED), true, false)).await;
        assert_eq!(status_bomb, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(budget.available(), budget.capacity());
    }

    #[tokio::test]
    async fn grpc_bodies_hold_the_budget_and_fail_unavailable_when_full() {
        let budget = BodyBudget::new(1000);
        let seen = Arc::new(AtomicUsize::new(0));
        let service = {
            let (budget, seen) = (budget.clone(), seen.clone());
            tower::service_fn(move |_request: http::Request<tonic::body::Body>| {
                let (budget, seen) = (budget.clone(), seen.clone());
                async move {
                    seen.store(budget.capacity() - budget.available(), Ordering::Relaxed);
                    Ok::<_, std::convert::Infallible>(http::Response::new(
                        tonic::body::Body::default(),
                    ))
                }
            })
        };
        let mut wrapped = GrpcBodyBudgetLayer(budget.clone()).layer(service);
        let call =
            |size: usize| http::Request::new(tonic::body::Body::new(Body::from(vec![0u8; size])));
        let response = wrapped
            .ready()
            .await
            .unwrap()
            .call(call(400))
            .await
            .unwrap();
        assert!(response.headers().get("grpc-status").is_none());
        // The body and the message decoded from it.
        assert_eq!(seen.load(Ordering::Relaxed), 800);
        assert_eq!(budget.available(), 1000);

        let held = budget.ticket();
        held.reserve(700).unwrap();
        let response = wrapped
            .ready()
            .await
            .unwrap()
            .call(call(200))
            .await
            .unwrap();
        assert_eq!(response.headers()["grpc-status"], "14");
        drop(held);
        let response = wrapped
            .ready()
            .await
            .unwrap()
            .call(call(600))
            .await
            .unwrap();
        assert_eq!(response.headers()["grpc-status"], "8");
        assert_eq!(budget.available(), 1000);
    }
}
