//! HTTP API of a [`VectorCluster`]: namespaces, writes, queries, shards.
//!
//! | Method   | Path                                       |                                   |
//! |----------|--------------------------------------------|-----------------------------------|
//! | `GET`    | `/api/v1/namespaces`                       | list namespaces                   |
//! | `PUT`    | `/api/v1/namespaces/{namespace}`           | create a namespace                |
//! | `GET`    | `/api/v1/namespaces/{namespace}`           | namespace metadata                |
//! | `DELETE` | `/api/v1/namespaces/{namespace}`           | delete a namespace and its rows   |
//! | `GET`    | `/api/v1/namespaces/{namespace}/schema`    | attribute schema                  |
//! | `PATCH`  | `/api/v1/namespaces/{namespace}/schema`    | add or update schema entries      |
//! | `POST`   | `/api/v1/namespaces/{namespace}/write`     | upserts, patches, deletes, docs   |
//! | `POST`   | `/api/v1/namespaces/{namespace}/query`     | query, or multi-query (`queries`) |
//! | `GET`    | `/api/v1/namespaces/{namespace}/rows/{id}` | one row, with its vector          |
//! | `GET`    | `/api/v1/shards`                           | list shards                       |
//!
//! Bodies are JSON; errors are `{"status": "error", "message": ".."}`. The
//! OpenAPI description is in [`openapi`](super::openapi).
//!
//! # Routing
//!
//! Any replica accepts any request. One this replica can't serve (see
//! [`VectorCluster::place`]) is forwarded: writes, row and metadata reads
//! to the writer owning the namespace's shard, queries (always
//! eventually consistent outside standalone deployments) to the readers of
//! the shard, falling back to its writer. Namespace and shard listings gather each shard's namespaces
//! from its writer. Forwarded requests carry
//! [`FORWARDED_HEADER`] and are never forwarded again; a replica that can't
//! serve one answers 503, and the sender retries against the latest
//! topology.
//!
//! # Authentication
//!
//! Every API request is checked by the replica that receives it (see
//! [`Guard`]); forwarded requests carry the internal token instead of the
//! caller's credentials.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use server_common::auth::Permission;

use super::auth::{Guard, INTERNAL_TOKEN_HEADER};
use super::config::VectorServerConfig;
use super::error::ApiError;
use super::handlers::{handle_healthy, handle_ready};
use super::metrics::{MetricsState, handle_metrics};
use super::middleware::{MetricsLayer, TracingLayer};
use super::vector::shutdown_signal;
use crate::Error;
use crate::cluster::{
    AttributeSchema, CreateNamespaceRequest, KubernetesSettings, MultiQueryRequest, NamespaceMeta,
    Need, Placement, QueryRequest, Role, ShardInfo, ShardMeta, ShardState, VectorCluster,
    WriteRequest,
};

/// Marks a request one replica forwarded to another.
pub const FORWARDED_HEADER: &str = "x-vector-forwarded";

const FORWARD_ATTEMPTS: usize = 10;
const FORWARD_BACKOFF: Duration = Duration::from_millis(200);
const FORWARD_TIMEOUT: Duration = Duration::from_secs(60);

/// Network addresses (`host:port`) of the other replicas.
pub trait Peers: Send + Sync {
    /// The writer replica named `owner` in the topology.
    fn writer(&self, owner: &str) -> Option<String>;
    /// The readers serving `shard`, if there are any.
    fn readers(&self, shard: u32) -> Option<String>;
}

/// A single process has no peers.
pub struct NoPeers;

impl Peers for NoPeers {
    fn writer(&self, _owner: &str) -> Option<String> {
        None
    }

    fn readers(&self, _shard: u32) -> Option<String> {
        None
    }
}

impl Peers for KubernetesSettings {
    fn writer(&self, owner: &str) -> Option<String> {
        Some(self.writer_address(owner))
    }

    fn readers(&self, shard: u32) -> Option<String> {
        Some(self.reader_address(shard))
    }
}

/// Fixed addresses, e.g. for processes on one host.
#[derive(Default)]
pub struct StaticPeers {
    writers: RwLock<HashMap<String, String>>,
    readers: RwLock<HashMap<u32, String>>,
}

impl StaticPeers {
    pub fn add_writer(&self, owner: &str, address: String) {
        self.writers
            .write()
            .expect("lock poisoned")
            .insert(owner.into(), address);
    }

    pub fn add_readers(&self, shard: u32, address: String) {
        self.readers
            .write()
            .expect("lock poisoned")
            .insert(shard, address);
    }
}

impl Peers for StaticPeers {
    fn writer(&self, owner: &str) -> Option<String> {
        self.writers
            .read()
            .expect("lock poisoned")
            .get(owner)
            .cloned()
    }

    fn readers(&self, shard: u32) -> Option<String> {
        self.readers
            .read()
            .expect("lock poisoned")
            .get(&shard)
            .cloned()
    }
}

/// A replica's cluster and how to reach the others.
pub struct ClusterApp {
    cluster: Arc<VectorCluster>,
    peers: Arc<dyn Peers>,
    guard: Arc<Guard>,
    client: reqwest::Client,
}

impl ClusterApp {
    pub fn new(cluster: Arc<VectorCluster>, peers: Arc<dyn Peers>, guard: Arc<Guard>) -> Arc<Self> {
        Arc::new(Self {
            cluster,
            peers,
            guard,
            client: reqwest::Client::builder()
                .timeout(FORWARD_TIMEOUT)
                .build()
                .expect("build HTTP client"),
        })
    }

    /// Where to send an operation needing `need` on a shard this replica
    /// doesn't serve, best first.
    fn targets(&self, shard: u32, owner: Option<&str>, need: Need) -> Vec<String> {
        let readers = (need == Need::EventualRead)
            .then(|| self.peers.readers(shard))
            .flatten();
        readers
            .into_iter()
            .chain(owner.and_then(|o| self.peers.writer(o)))
            .collect()
    }

    /// Runs `request` on a replica that can serve `place` for it: `None`
    /// when that is this one, otherwise the response of the replica it was
    /// forwarded to.
    async fn route(
        &self,
        subject: Subject<'_>,
        need: Need,
        request: &Forward<'_>,
    ) -> Result<Option<Response>, ApiError> {
        let mut last = String::from("no replica serves it");
        for attempt in 0..FORWARD_ATTEMPTS {
            let placement = match subject {
                Subject::Namespace(ns) => self.cluster.place(ns, need).await?,
                Subject::Shard(shard) => self.cluster.place_shard(shard, need).await,
            };
            let Placement::Remote { shard, owner } = placement else {
                return Ok(None);
            };
            if request.forwarded {
                return Err(ApiError(Error::Unavailable(format!(
                    "shard {shard} is not served by this replica"
                ))));
            }
            for target in self.targets(shard, owner.as_deref(), need) {
                match self.send(&target, request).await {
                    Ok(response) if response.status() != StatusCode::SERVICE_UNAVAILABLE => {
                        return Ok(Some(response));
                    }
                    Ok(_) => last = format!("{target} answered 503"),
                    Err(e) => last = format!("{target}: {e}"),
                }
            }
            if attempt + 1 < FORWARD_ATTEMPTS {
                tokio::time::sleep(FORWARD_BACKOFF).await;
            }
        }
        Err(ApiError(Error::Unavailable(format!(
            "could not reach a replica for the request: {last}"
        ))))
    }

    async fn send(&self, target: &str, request: &Forward<'_>) -> reqwest::Result<Response> {
        let url = format!("http://{target}{}", request.path);
        let mut builder = self
            .client
            .request(request.method.clone(), url)
            .header(FORWARDED_HEADER, "1");
        if let Some(token) = self.guard.internal_token() {
            builder = builder.header(INTERNAL_TOKEN_HEADER, token);
        }
        if let Some(body) = request.body {
            builder = builder
                .header(header::CONTENT_TYPE, "application/json")
                .body(body.clone());
        }
        let response = builder.send().await?;
        let status = response.status();
        let content_type = response.headers().get(header::CONTENT_TYPE).cloned();
        let body = response.bytes().await?;
        let mut out = (status, body).into_response();
        if let Some(content_type) = content_type {
            out.headers_mut().insert(header::CONTENT_TYPE, content_type);
        }
        Ok(out)
    }

    /// Routes an operation on namespace `ns`.
    async fn route_namespace(
        &self,
        ns: &str,
        need: Need,
        request: &Forward<'_>,
    ) -> Result<Option<Response>, ApiError> {
        self.route(Subject::Namespace(ns), need, request).await
    }

    /// The namespaces of active shard `shard`, from its writer, so a
    /// namespace is listed as soon as it is created.
    async fn shard_namespaces(
        &self,
        shard: &ShardMeta,
        forwarded: bool,
    ) -> Result<Vec<NamespaceMeta>, ApiError> {
        let path = format!("/internal/v1/shards/{}/namespaces", shard.id);
        let request = Forward {
            method: Method::GET,
            path: &path,
            body: None,
            forwarded,
        };
        let need = Need::StrongRead;
        let response = self.route(Subject::Shard(shard), need, &request).await?;
        let Some(response) = response else {
            return Ok(self.cluster.shard_namespaces(shard.id).await?);
        };
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .map_err(|e| ApiError(Error::Internal(format!("read forwarded response: {e}"))))?;
        if !status.is_success() {
            return Err(ApiError(Error::Unavailable(format!(
                "listing shard {} answered {status}: {}",
                shard.id,
                String::from_utf8_lossy(&body)
            ))));
        }
        let listed: Listed = parse(&body)?;
        Ok(listed.namespaces)
    }

    /// Every active shard's namespaces: from this replica alone when the
    /// request was forwarded.
    async fn gather(&self, forwarded: bool) -> Result<BTreeMap<u32, Vec<NamespaceMeta>>, ApiError> {
        let topology = self
            .cluster
            .topology()
            .ok_or_else(|| ApiError(Error::Unavailable("no topology yet".into())))?;
        let mut out = BTreeMap::new();
        for shard in topology.active() {
            let namespaces = if forwarded {
                match self.cluster.shard_namespaces(shard.id).await {
                    Ok(namespaces) => namespaces,
                    Err(Error::Unavailable(_)) => continue,
                    Err(e) => return Err(ApiError(e)),
                }
            } else {
                self.shard_namespaces(shard, false).await?
            };
            out.insert(shard.id, namespaces);
        }
        Ok(out)
    }
}

/// What a routed operation acts on.
#[derive(Clone, Copy)]
enum Subject<'a> {
    Namespace(&'a str),
    Shard(&'a ShardMeta),
}

#[derive(serde::Deserialize)]
struct Listed {
    namespaces: Vec<NamespaceMeta>,
}

/// A request as it would be forwarded.
struct Forward<'a> {
    method: Method,
    path: &'a str,
    body: Option<&'a Bytes>,
    forwarded: bool,
}

impl<'a> Forward<'a> {
    fn new(method: Method, uri: &'a Uri, headers: &HeaderMap, body: Option<&'a Bytes>) -> Self {
        Self {
            method,
            path: uri.path_and_query().map_or(uri.path(), |p| p.as_str()),
            body,
            forwarded: is_forwarded(headers),
        }
    }
}

fn is_forwarded(headers: &HeaderMap) -> bool {
    headers.contains_key(FORWARDED_HEADER)
}

type App = Arc<ClusterApp>;
type ApiResult = Result<Response, ApiError>;

/// HTTP server for a [`VectorCluster`].
pub struct ClusterServer {
    cluster: Arc<VectorCluster>,
    peers: Arc<dyn Peers>,
    guard: Arc<Guard>,
    config: VectorServerConfig,
    metrics: MetricsState,
}

impl ClusterServer {
    pub fn new(
        cluster: Arc<VectorCluster>,
        config: VectorServerConfig,
        metrics: MetricsState,
    ) -> Self {
        Self {
            cluster,
            peers: Arc::new(NoPeers),
            guard: Arc::new(Guard::open()),
            config,
            metrics,
        }
    }

    /// Forwards requests this replica can't serve to `peers`.
    pub fn with_peers(mut self, peers: Arc<dyn Peers>) -> Self {
        self.peers = peers;
        self
    }

    /// Authenticates requests with `guard`.
    pub fn with_guard(mut self, guard: Arc<Guard>) -> Self {
        self.guard = guard;
        self
    }

    /// Serves until SIGINT/SIGTERM, then closes the cluster.
    pub async fn run(self) {
        let metrics_router = Router::new()
            .route("/metrics", get(handle_metrics))
            .with_state(self.metrics);
        let app = cluster_router_prefixed(
            self.cluster.clone(),
            self.peers,
            self.guard,
            &self.config.path_prefix,
        )
        .merge(metrics_router)
        .layer(MetricsLayer::new())
        .layer(TracingLayer::new());

        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        tracing::info!("Starting vector cluster HTTP server on {}", addr);
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .unwrap();

        if let Err(e) = self.cluster.close().await {
            tracing::error!("Failed to close vector cluster: {e}");
        }
        tracing::info!("Server shut down gracefully");
    }
}

/// The cluster API routes of a single process, without metrics or
/// middleware.
pub fn cluster_router(cluster: Arc<VectorCluster>) -> Router {
    cluster_router_with(cluster, Arc::new(NoPeers))
}

/// The cluster API routes of a replica that forwards to `peers`.
pub fn cluster_router_with(cluster: Arc<VectorCluster>, peers: Arc<dyn Peers>) -> Router {
    cluster_router_guarded(cluster, peers, Arc::new(Guard::open()))
}

/// The cluster API routes of a replica that forwards to `peers` and
/// authenticates requests with `guard`.
pub fn cluster_router_guarded(
    cluster: Arc<VectorCluster>,
    peers: Arc<dyn Peers>,
    guard: Arc<Guard>,
) -> Router {
    cluster_router_prefixed(cluster, peers, guard, "")
}

/// [`cluster_router_guarded`] with the `/api/v1` routes under `prefix`.
/// Every replica of a cluster must use the same prefix, since forwarded
/// requests keep their original path.
pub fn cluster_router_prefixed(
    cluster: Arc<VectorCluster>,
    peers: Arc<dyn Peers>,
    guard: Arc<Guard>,
    prefix: &str,
) -> Router {
    let api = Router::new()
        .route("/api/v1/namespaces", get(list_namespaces))
        .route(
            "/api/v1/namespaces/{namespace}",
            put(create_namespace)
                .get(get_namespace)
                .delete(delete_namespace),
        )
        .route(
            "/api/v1/namespaces/{namespace}/schema",
            get(get_schema).patch(update_schema),
        )
        .route("/api/v1/namespaces/{namespace}/write", post(write))
        .route("/api/v1/namespaces/{namespace}/query", post(query))
        .route("/api/v1/namespaces/{namespace}/rows/{id}", get(get_row))
        .route("/api/v1/shards", get(list_shards));
    let root = Router::new()
        .route(
            "/internal/v1/shards/{shard}/namespaces",
            get(local_shard_namespaces),
        )
        .route("/-/healthy", get(handle_healthy))
        .route("/-/ready", get(handle_ready));
    let routes = if prefix.is_empty() {
        root.merge(api)
    } else {
        root.nest(prefix, api)
    };
    routes.with_state(ClusterApp::new(cluster, peers, guard))
}

fn parse<T: DeserializeOwned>(body: &Bytes) -> Result<T, ApiError> {
    serde_json::from_slice(body)
        .map_err(|e| ApiError(Error::InvalidInput(format!("invalid request body: {e}"))))
}

fn ok(value: impl serde::Serialize) -> ApiResult {
    Ok(Json(value).into_response())
}

/// Forwards the request when this replica can't serve it; otherwise
/// evaluates to nothing and the handler continues locally.
macro_rules! route {
    ($app:expr, $ns:expr, $need:expr, $method:expr, $uri:expr, $headers:expr, $body:expr) => {
        let permission = match $need {
            Need::Write => Permission::Write,
            Need::StrongRead | Need::EventualRead => Permission::Read,
        };
        $app.guard.namespace(&$headers, &$ns, permission).await?;
        if let Some(response) = $app
            .route_namespace(&$ns, $need, &Forward::new($method, &$uri, &$headers, $body))
            .await?
        {
            return Ok(response);
        }
    };
}

/// Lists the namespaces the caller may read.
async fn list_namespaces(State(app): State<App>, headers: HeaderMap) -> ApiResult {
    let mut namespaces = Vec::new();
    for namespace in app
        .gather(is_forwarded(&headers))
        .await?
        .into_values()
        .flatten()
    {
        if app.guard.visible(&headers, &namespace.name).await {
            namespaces.push(namespace);
        }
    }
    namespaces.sort_by(|a: &NamespaceMeta, b| a.name.cmp(&b.name));
    ok(json!({ "namespaces": namespaces }))
}

async fn local_shard_namespaces(
    State(app): State<App>,
    Path(shard): Path<u32>,
    headers: HeaderMap,
) -> ApiResult {
    app.guard.internal(&headers)?;
    ok(json!({ "namespaces": app.cluster.shard_namespaces(shard).await? }))
}

async fn create_namespace(
    State(app): State<App>,
    Path(ns): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult {
    route!(app, ns, Need::Write, Method::PUT, uri, headers, Some(&body));
    let request: CreateNamespaceRequest = parse(&body)?;
    let meta = app.cluster.create_namespace(&ns, request).await?;
    Ok((StatusCode::CREATED, Json(meta)).into_response())
}

async fn get_namespace(
    State(app): State<App>,
    Path(ns): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> ApiResult {
    route!(app, ns, Need::StrongRead, Method::GET, uri, headers, None);
    ok(app.cluster.namespace(&ns).await?)
}

async fn delete_namespace(
    State(app): State<App>,
    Path(ns): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> ApiResult {
    route!(app, ns, Need::Write, Method::DELETE, uri, headers, None);
    app.cluster.delete_namespace(&ns).await?;
    ok(json!({ "status": "ok" }))
}

async fn get_schema(
    State(app): State<App>,
    Path(ns): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> ApiResult {
    route!(app, ns, Need::StrongRead, Method::GET, uri, headers, None);
    ok(app.cluster.namespace(&ns).await?.schema)
}

async fn update_schema(
    State(app): State<App>,
    Path(ns): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult {
    route!(
        app,
        ns,
        Need::Write,
        Method::PATCH,
        uri,
        headers,
        Some(&body)
    );
    let updates: BTreeMap<String, AttributeSchema> = parse(&body)?;
    ok(app.cluster.update_schema(&ns, &updates).await?.schema)
}

async fn write(
    State(app): State<App>,
    Path(ns): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult {
    route!(
        app,
        ns,
        Need::Write,
        Method::POST,
        uri,
        headers,
        Some(&body)
    );
    let request: WriteRequest = parse(&body)?;
    ok(app.cluster.write(&ns, request).await?)
}

/// Queries are eventually consistent outside standalone deployments, where
/// the cluster rejects strong ones.
async fn query(
    State(app): State<App>,
    Path(ns): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult {
    let value: Value = parse(&body)?;
    route!(
        app,
        ns,
        Need::EventualRead,
        Method::POST,
        uri,
        headers,
        Some(&body)
    );
    let invalid =
        |e: serde_json::Error| ApiError(Error::InvalidInput(format!("invalid request body: {e}")));
    let result = if value.get("queries").is_some() {
        let request: MultiQueryRequest = serde_json::from_value(value).map_err(invalid)?;
        app.cluster
            .multi_query(&ns, request)
            .await
            .map(|r| Json(r).into_response())
    } else {
        let request: QueryRequest = serde_json::from_value(value).map_err(invalid)?;
        app.cluster
            .query(&ns, request)
            .await
            .map(|r| Json(r).into_response())
    };
    match result {
        // A reader that hasn't caught up with a new namespace or index
        // defers to the writer: itself, or by answering 503 so the replica
        // that forwarded the query tries the writer next.
        Err(Error::Unavailable(_) | Error::NotFound(_))
            if app.cluster.role() == Role::Reader && is_forwarded(&headers) =>
        {
            Err(ApiError(Error::Unavailable(format!(
                "this reader has not caught up with namespace '{ns}'"
            ))))
        }
        Err(Error::Unavailable(_) | Error::NotFound(_)) if app.cluster.role() == Role::Reader => {
            let request = Forward::new(Method::POST, &uri, &headers, Some(&body));
            match app.route_namespace(&ns, Need::StrongRead, &request).await? {
                Some(response) => Ok(response),
                None => Err(ApiError(Error::Internal("a reader can't write".into()))),
            }
        }
        result => Ok(result?),
    }
}

async fn get_row(
    State(app): State<App>,
    Path((ns, id)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> ApiResult {
    route!(app, ns, Need::StrongRead, Method::GET, uri, headers, None);
    match app.cluster.get_row(&ns, &id).await? {
        Some(row) => ok(row),
        None => Err(ApiError(Error::NotFound(format!(
            "row '{id}' not found in namespace '{ns}'"
        )))),
    }
}

async fn list_shards(State(app): State<App>, headers: HeaderMap) -> ApiResult {
    app.guard.admin(&headers, Permission::Read).await?;
    let topology = app
        .cluster
        .topology()
        .ok_or_else(|| ApiError(Error::Unavailable("no topology yet".into())))?;
    let mut namespaces = app.gather(is_forwarded(&headers)).await?;
    let shards: Vec<ShardInfo> = topology
        .shards
        .into_iter()
        .map(|meta| {
            let names = match meta.state {
                ShardState::Active => namespaces.remove(&meta.id).unwrap_or_default(),
                ShardState::Retired => Vec::new(),
            };
            ShardInfo {
                meta,
                namespaces: names.into_iter().map(|n| n.name).collect(),
            }
        })
        .collect();
    ok(json!({ "shards": shards }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterConfig;

    async fn serve() -> String {
        let cluster = Arc::new(VectorCluster::open(ClusterConfig::default()).await.unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, cluster_router(cluster))
                .await
                .unwrap();
        });
        format!("http://{addr}/api/v1")
    }

    async fn send(
        client: &reqwest::Client,
        method: reqwest::Method,
        url: String,
        body: Option<Value>,
    ) -> (u16, Value) {
        respond(client.request(method, url), body).await
    }

    async fn send_as(
        client: &reqwest::Client,
        (user, password): (&str, &str),
        method: reqwest::Method,
        url: String,
        body: Option<Value>,
    ) -> (u16, Value) {
        let request = client.request(method, url).basic_auth(user, Some(password));
        respond(request, body).await
    }

    async fn respond(mut request: reqwest::RequestBuilder, body: Option<Value>) -> (u16, Value) {
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        (status, response.json().await.unwrap())
    }

    #[tokio::test]
    async fn should_serve_the_namespace_lifecycle() {
        use reqwest::Method;
        let base = serve().await;
        let client = reqwest::Client::new();

        // create
        let (status, meta) = send(
            &client,
            Method::PUT,
            format!("{base}/namespaces/docs"),
            Some(json!({
                "dimensions": 2,
                "distance_metric": "cosine_distance",
                "schema": {"body": {"type": "string", "full_text_search": true}},
            })),
        )
        .await;
        assert_eq!(status, 201);
        assert_eq!(meta["dimensions"], 2);
        let (status, _) = send(
            &client,
            Method::PUT,
            format!("{base}/namespaces/docs"),
            Some(json!({"dimensions": 2})),
        )
        .await;
        assert_eq!(status, 409);

        // write rows and documents
        let (status, written) = send(
            &client,
            Method::POST,
            format!("{base}/namespaces/docs/write"),
            Some(json!({
                "upsert_rows": [{"id": "r1", "vector": [1, 0], "body": "hello world", "n": 1}],
                "upsert_documents": [{"id": "d1", "attributes": {"n": 2}, "passages": [
                    {"vector": [0, 1], "body": "first passage"},
                    {"vector": [0.1, 1], "body": "second passage"},
                ]}],
            })),
        )
        .await;
        assert_eq!(status, 200, "{written}");
        assert_eq!(written["rows_upserted"], 3);

        // query, multi-query, get row
        let (status, result) = send(
            &client,
            Method::POST,
            format!("{base}/namespaces/docs/query"),
            Some(json!({
                "rank_by": {"op": "ANN", "vector": [0, 1]},
                "filters": {"op": "Gte", "attribute": "n", "value": 2},
                "limit": {"total": 5, "per": {"attributes": ["$doc_id"], "limit": 1}},
                "include_attributes": ["$doc_id"],
            })),
        )
        .await;
        assert_eq!(status, 200, "{result}");
        assert_eq!(
            result["rows"],
            json!([{"id": "d1#0", "$dist": 0.0, "$doc_id": "d1"}])
        );

        let (status, fused) = send(
            &client,
            Method::POST,
            format!("{base}/namespaces/docs/query"),
            Some(json!({
                "queries": [
                    {"rank_by": {"op": "BM25", "attribute": "body", "query": "passage"}},
                    {"rank_by": {"op": "kNN", "vector": [1, 0]}},
                ],
                "rrf": {},
            })),
        )
        .await;
        assert_eq!(status, 200, "{fused}");
        assert_eq!(fused["rows"].as_array().unwrap().len(), 3);

        let (status, row) = send(
            &client,
            Method::GET,
            format!("{base}/namespaces/docs/rows/r1"),
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(row["vector"], json!([1.0, 0.0]));
        let (status, _) = send(
            &client,
            Method::GET,
            format!("{base}/namespaces/docs/rows/nope"),
            None,
        )
        .await;
        assert_eq!(status, 404);

        // schema
        let (status, schema) = send(
            &client,
            Method::PATCH,
            format!("{base}/namespaces/docs/schema"),
            Some(json!({"label": {"type": "string"}})),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(schema["n"]["type"], "int");
        assert_eq!(schema["label"]["type"], "string");

        // errors
        let (status, error) = send(
            &client,
            Method::POST,
            format!("{base}/namespaces/docs/query"),
            Some(json!({"filters": {"op": "Between", "attribute": "n", "value": 1}})),
        )
        .await;
        assert_eq!(status, 400);
        assert_eq!(error["status"], "error");
        let message = error["message"].as_str().unwrap();
        assert!(
            message.contains("Between") && message.contains("ContainsAny"),
            "{message}"
        );
        for bad in [
            json!({"bogus": 1}),
            json!({"filters": ["n", "Gte", 2]}),
            json!({"filters": ["Not", ["n", "Gte", 2]]}),
            json!({"rank_by": ["vector", "kNN", [1, 0]]}),
        ] {
            let (status, _) = send(
                &client,
                Method::POST,
                format!("{base}/namespaces/docs/query"),
                Some(bad.clone()),
            )
            .await;
            assert_eq!(status, 400, "{bad}");
        }

        // shards
        send(
            &client,
            Method::POST,
            format!("{base}/namespaces/other/write"),
            Some(json!({"upsert_rows": [{"id": "x", "vector": [1, 2, 3]}]})),
        )
        .await;
        let (status, shards) = send(&client, Method::GET, format!("{base}/shards"), None).await;
        assert_eq!(status, 200, "{shards}");
        assert_eq!(shards["shards"].as_array().unwrap().len(), 1);
        assert_eq!(shards["shards"][0]["state"], "active");
        assert_eq!(shards["shards"][0]["owner"], "standalone");
        assert_eq!(
            shards["shards"][0]["namespaces"].as_array().unwrap().len(),
            2
        );
        let split = client
            .post(format!("{base}/shards/0/split"))
            .send()
            .await
            .unwrap();
        assert_eq!(split.status(), 404, "shards split only when writers join");

        // delete
        let (status, _) = send(
            &client,
            Method::DELETE,
            format!("{base}/namespaces/docs"),
            None,
        )
        .await;
        assert_eq!(status, 200);
        let (_, list) = send(&client, Method::GET, format!("{base}/namespaces"), None).await;
        assert_eq!(list["namespaces"].as_array().unwrap().len(), 1);
        let (status, _) = send(
            &client,
            Method::GET,
            format!("{base}/namespaces/docs"),
            None,
        )
        .await;
        assert_eq!(status, 404);
    }

    mod replicas {
        use super::*;
        use crate::cluster::{
            ClusterRuntime, MemoryLeases, MemoryTopologyStore, Role, Topology, TopologyStore,
        };
        use crate::server::auth::AccessConfig;
        use common::StorageConfig;
        use common::storage::config::{
            LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
        };
        use reqwest::Method;
        use tempfile::TempDir;

        struct Cluster {
            dir: TempDir,
            store: Arc<MemoryTopologyStore>,
            leases: Arc<MemoryLeases>,
            peers: Arc<StaticPeers>,
            guard: Arc<Guard>,
            prefix: String,
            replicas: Vec<Arc<VectorCluster>>,
        }

        impl Cluster {
            /// A cluster of `writers`, each owning one shard.
            async fn new(writers: &[&str]) -> Self {
                Self::guarded(writers, Guard::open()).await
            }

            async fn guarded(writers: &[&str], guard: Guard) -> Self {
                Self::with(writers, guard, "").await
            }

            async fn with(writers: &[&str], guard: Guard, prefix: &str) -> Self {
                let store = MemoryTopologyStore::new();
                let members: Vec<String> = writers.iter().map(|w| w.to_string()).collect();
                store
                    .publish(None, &Topology::initial(&members, 0))
                    .await
                    .unwrap();
                let mut cluster = Self {
                    dir: TempDir::new().unwrap(),
                    store,
                    leases: MemoryLeases::new(),
                    peers: Arc::new(StaticPeers::default()),
                    guard: Arc::new(guard),
                    prefix: prefix.into(),
                    replicas: Vec::new(),
                };
                for writer in writers {
                    let address = cluster.start(writer, Role::Writer, None).await;
                    cluster.peers.add_writer(writer, address);
                }
                cluster
            }

            /// Starts a replica, returning its address.
            async fn start(&mut self, identity: &str, role: Role, shard: Option<u32>) -> String {
                let config = ClusterConfig {
                    role,
                    reader_shard: shard,
                    reconcile_interval: Duration::from_millis(200),
                    storage: StorageConfig::SlateDb(SlateDbStorageConfig {
                        path: "vectors".into(),
                        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                            path: self.dir.path().to_string_lossy().to_string(),
                        }),
                        settings_path: None,
                        block_cache: None,
                        meta_cache: None,
                        disk: Default::default(),
                    }),
                    ..ClusterConfig::default()
                };
                let runtime = ClusterRuntime {
                    identity: identity.into(),
                    topology: self.store.clone(),
                    leases: self.leases.clone(),
                };
                let replica = Arc::new(VectorCluster::open_with(config, runtime).await.unwrap());
                replica.spawn_reconciler();
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap().to_string();
                let router = cluster_router_prefixed(
                    replica.clone(),
                    self.peers.clone(),
                    self.guard.clone(),
                    &self.prefix,
                );
                tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
                self.replicas.push(replica);
                address
            }

            async fn start_reader(&mut self, shard: u32) -> String {
                let address = self
                    .start(&format!("reader-{shard}"), Role::Reader, Some(shard))
                    .await;
                self.peers.add_readers(shard, address.clone());
                address
            }

            /// Waits until a split gives `writer` a shard, returning the
            /// topology.
            async fn await_owner(&self, writer: &str) -> Topology {
                for _ in 0..100 {
                    let topology = self.store.load().await.unwrap().unwrap();
                    if topology.owned_by(writer).is_some() {
                        return topology;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                panic!("{writer} never got a shard");
            }

            async fn close(self) {
                for replica in &self.replicas {
                    replica.close().await.unwrap();
                }
            }
        }

        async fn query(
            client: &reqwest::Client,
            at: &str,
            ns: &str,
            level: Option<&str>,
        ) -> (u16, Value) {
            let mut body = json!({"rank_by": {"op": "kNN", "vector": [1.0, 2.0]}, "top_k": 10});
            if let Some(level) = level {
                body["consistency"] = json!({ "level": level });
            }
            let url = format!("http://{at}/api/v1/namespaces/{ns}/query");
            send(client, Method::POST, url, Some(body)).await
        }

        async fn rows(client: &reqwest::Client, at: &str, ns: &str) -> Value {
            let (status, result) = query(client, at, ns, None).await;
            assert_eq!(status, 200, "{ns}: {result}");
            result["rows"].clone()
        }

        /// Waits for queries at `at` to satisfy `done`, returning the last
        /// results.
        async fn await_rows(
            client: &reqwest::Client,
            at: &str,
            names: &[String],
            done: impl Fn(&[Value]) -> bool,
        ) -> Vec<Value> {
            let mut last = Vec::new();
            for _ in 0..60 {
                last.clear();
                for name in names {
                    last.push(rows(client, at, name).await);
                }
                if done(&last) {
                    return last;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            panic!("queries at {at} still return {last:?}");
        }

        async fn await_eventual(
            client: &reqwest::Client,
            at: &str,
            names: &[String],
            expected: &[Value],
        ) {
            await_rows(client, at, names, |rows| rows == expected).await;
        }

        #[tokio::test]
        async fn should_route_every_request_through_any_reader() {
            // given - two writers, and one reader per shard
            let mut cluster = Cluster::new(&["w0", "w1"]).await;
            let entry = cluster.start_reader(0).await;
            cluster.start_reader(1).await;
            let client = reqwest::Client::new();
            let names: Vec<String> = (0..6).map(|i| format!("tenant-{i}")).collect();

            // when - every request goes to the reader of shard 0
            for (i, name) in names.iter().enumerate() {
                let rows: Vec<Value> = (0..4)
                    .map(|j| json!({"id": format!("r{j}"), "vector": [1.0, j as f32], "n": i}))
                    .collect();
                let (status, written) = send(
                    &client,
                    Method::POST,
                    format!("http://{entry}/api/v1/namespaces/{name}/write"),
                    Some(json!({"upsert_rows": rows})),
                )
                .await;
                assert_eq!(status, 200, "{written}");
            }

            // then - writes reached the owners, listings gather every shard,
            // the shard readers serve every row, and strong reads are refused
            let (_, listed) = send(
                &client,
                Method::GET,
                format!("http://{entry}/api/v1/namespaces"),
                None,
            )
            .await;
            assert_eq!(listed["namespaces"].as_array().unwrap().len(), names.len());
            let expected = await_rows(&client, &entry, &names, |rows| {
                rows.iter()
                    .all(|r| r.as_array().is_some_and(|r| r.len() == 4))
            })
            .await;
            let (status, refused) = query(&client, &entry, &names[0], Some("strong")).await;
            assert_eq!(status, 400, "{refused}");

            // when - a third writer joins, and the coordinator plans a split
            // for it
            let address = cluster.start("w2", Role::Writer, None).await;
            cluster.peers.add_writer("w2", address);
            let current = cluster.store.load().await.unwrap().unwrap();
            let planned = current
                .plan(&["w0".into(), "w1".into(), "w2".into()])
                .unwrap();
            assert!(
                cluster
                    .store
                    .publish(Some(current.generation), &planned)
                    .await
                    .unwrap()
            );
            let split = cluster.await_owner("w2").await;

            // then - each writer owns one shard, and the answers are the same
            // before and after the children get readers
            let children = split.owned_by("w2").unwrap().parent;
            await_eventual(&client, &entry, &names, &expected).await;
            for child in split.active().filter(|s| s.parent == children) {
                cluster.start_reader(child.id).await;
            }
            await_eventual(&client, &entry, &names, &expected).await;
            let owners: Vec<_> = split.active().filter_map(|s| s.owner.clone()).collect();
            assert_eq!(owners.len(), 3);
            for writer in ["w0", "w1", "w2"] {
                assert!(owners.iter().any(|o| o == writer), "{writer} owns no shard");
            }
            let (_, shards) = send(
                &client,
                Method::GET,
                format!("http://{entry}/api/v1/shards"),
                None,
            )
            .await;
            let shards = shards["shards"].as_array().unwrap();
            assert_eq!(shards.len(), 4, "three active shards and the split parent");
            let listed: usize = shards
                .iter()
                .map(|s| s["namespaces"].as_array().unwrap().len())
                .sum();
            assert_eq!(listed, names.len());
            cluster.close().await;
        }

        #[tokio::test]
        async fn should_serve_and_forward_the_api_under_a_path_prefix() {
            // given - two writers serving the API under /vector
            let cluster = Cluster::with(&["w0", "w1"], Guard::open(), "/vector").await;
            let entry = cluster.peers.writer("w0").unwrap();
            let client = reqwest::Client::new();
            let names: Vec<String> = (0..6).map(|i| format!("tenant-{i}")).collect();
            let row = json!({"upsert_rows": [{"id": "r", "vector": [1.0, 2.0]}]});

            // when - every write enters through w0, which forwards the ones
            // w1 owns
            for name in &names {
                let url = format!("http://{entry}/vector/api/v1/namespaces/{name}/write");
                let (status, written) = send(&client, Method::POST, url, Some(row.clone())).await;
                assert_eq!(status, 200, "{name}: {written}");
            }

            // then - both shards hold namespaces, listings gather them, and
            // the unprefixed API and prefixed health routes don't exist
            let topology = cluster.store.load().await.unwrap().unwrap();
            let owner = |name: &str| {
                let id = crate::storage::namespaced::NamespaceId::for_name(name);
                topology.shard_for(id).unwrap().owner.clone()
            };
            assert!(names.iter().any(|n| owner(n).as_deref() == Some("w1")));
            let (status, listed) = send(
                &client,
                Method::GET,
                format!("http://{entry}/vector/api/v1/namespaces"),
                None,
            )
            .await;
            assert_eq!(status, 200, "{listed}");
            assert_eq!(listed["namespaces"].as_array().unwrap().len(), names.len());
            let unprefixed = client
                .get(format!("http://{entry}/api/v1/namespaces"))
                .send()
                .await
                .unwrap();
            assert_eq!(unprefixed.status().as_u16(), 404);
            let healthy = client
                .get(format!("http://{entry}/-/healthy"))
                .send()
                .await
                .unwrap();
            assert_eq!(healthy.status().as_u16(), 200);
            cluster.close().await;
        }

        #[tokio::test]
        async fn should_authenticate_at_the_entry_replica_and_forward_with_the_internal_token() {
            // given - tenants with their own write credentials, behind readers
            let names: Vec<String> = (0..4).map(|i| format!("tenant-{i}")).collect();
            let declared: Vec<Value> = names
                .iter()
                .map(|name| {
                    json!({"name": name, "auth": {"write": [{"type": "basic", "username": name,
                        "password": {"source": "literal", "value": format!("{name}-secret")}}]}})
                })
                .collect();
            let config = json!({
                "auth": {
                    "global": {"write": [{"type": "basic", "username": "admin",
                        "password": {"source": "literal", "value": "root"}}]},
                    "internal": {"source": "literal", "value": "peer-token"},
                },
                "namespaces": declared,
            });
            let access = AccessConfig::from_yaml(&config.to_string()).unwrap();
            let guard = Guard::new(&access).await.unwrap();
            let mut cluster = Cluster::guarded(&["w0", "w1"], guard).await;
            let entry = cluster.start_reader(0).await;
            cluster.start_reader(1).await;
            let client = reqwest::Client::new();
            let secrets: Vec<String> = names.iter().map(|n| format!("{n}-secret")).collect();
            let tenant = |i: usize| (names[i].as_str(), secrets[i].as_str());
            let api = |path: &str| format!("http://{entry}/api/v1/{path}");
            let row = json!({"upsert_rows": [{"id": "r", "vector": [1.0, 2.0]}]});

            // when - each tenant writes through the reader
            for (i, name) in names.iter().enumerate() {
                let url = api(&format!("namespaces/{name}/write"));
                let (status, written) =
                    send_as(&client, tenant(i), Method::POST, url, Some(row.clone())).await;
                assert_eq!(status, 200, "{written}");
            }

            // then - other credentials, no credentials and undeclared
            // namespaces are refused
            let url = api("namespaces/tenant-1/write");
            let (status, _) = send_as(
                &client,
                tenant(0),
                Method::POST,
                url.clone(),
                Some(row.clone()),
            )
            .await;
            assert_eq!(status, 401);
            let (status, _) = send(&client, Method::POST, url, Some(row.clone())).await;
            assert_eq!(status, 401);
            let url = api("namespaces/undeclared/write");
            let (status, _) = send_as(
                &client,
                ("admin", "root"),
                Method::POST,
                url,
                Some(row.clone()),
            )
            .await;
            assert_eq!(status, 404);

            // and listings show what the caller can read
            let (_, listed) =
                send_as(&client, tenant(2), Method::GET, api("namespaces"), None).await;
            assert_eq!(
                listed["namespaces"].as_array().unwrap().len(),
                1,
                "{listed}"
            );
            assert_eq!(listed["namespaces"][0]["name"], "tenant-2");
            let (_, listed) = send_as(
                &client,
                ("admin", "root"),
                Method::GET,
                api("namespaces"),
                None,
            )
            .await;
            assert_eq!(listed["namespaces"].as_array().unwrap().len(), names.len());

            // and shard operations need global credentials
            let (status, _) = send_as(&client, tenant(0), Method::GET, api("shards"), None).await;
            assert_eq!(status, 401);
            let (status, _) =
                send_as(&client, ("admin", "root"), Method::GET, api("shards"), None).await;
            assert_eq!(status, 200);

            // and a request posing as forwarded needs the internal token
            let writer = cluster.peers.writer("w0").unwrap();
            let forged = client
                .post(format!("http://{writer}/api/v1/namespaces/tenant-0/write"))
                .header(FORWARDED_HEADER, "1")
                .json(&row)
                .send()
                .await
                .unwrap();
            assert_eq!(forged.status().as_u16(), 401);
            let shard = client
                .get(format!("http://{writer}/internal/v1/shards/0/namespaces"))
                .send()
                .await
                .unwrap();
            assert_eq!(shard.status().as_u16(), 401);

            // and tenants read their rows through the reader
            let body = json!({"rank_by": {"op": "kNN", "vector": [1.0, 2.0]}, "top_k": 10});
            for (i, name) in names.iter().enumerate() {
                let url = api(&format!("namespaces/{name}/query"));
                let mut rows = 0;
                for _ in 0..60 {
                    let (status, result) = send_as(
                        &client,
                        tenant(i),
                        Method::POST,
                        url.clone(),
                        Some(body.clone()),
                    )
                    .await;
                    assert_eq!(status, 200, "{result}");
                    rows = result["rows"].as_array().unwrap().len();
                    if rows == 1 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                assert_eq!(rows, 1, "{}", names[i]);
            }
            cluster.close().await;
        }
    }
}
