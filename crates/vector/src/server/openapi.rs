//! OpenAPI description of the cluster HTTP API (`plural-vector cluster`).
//!
//! The types here document the wire format only; requests are parsed by
//! the [`cluster`](crate::cluster) types they mirror.

#![allow(dead_code)]

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};
use utoipa::{OpenApi, ToSchema};

/// Error body of every 4xx and 5xx response.
#[derive(ToSchema)]
struct ErrorEnvelope {
    /// Always `error`.
    status: String,
    message: String,
}

#[derive(ToSchema)]
struct StatusOk {
    /// Always `ok`.
    status: String,
}

/// How vector distance is measured. Fixed when the namespace is created.
#[derive(ToSchema)]
#[schema(rename_all = "snake_case")]
enum DistanceMetric {
    /// `1 - cos(a, b)`, in `[0, 2]`. Vectors are normalized on write.
    CosineDistance,
    /// `|a - b|²`.
    EuclideanSquared,
    /// `-(a · b)`, so lower is still closer.
    DotProduct,
}

#[derive(ToSchema)]
enum AttributeType {
    #[schema(rename = "string")]
    String,
    #[schema(rename = "int")]
    Int,
    #[schema(rename = "float")]
    Float,
    #[schema(rename = "bool")]
    Bool,
    #[schema(rename = "[]string")]
    StringArray,
    #[schema(rename = "[]int")]
    IntArray,
}

/// Type and indexing of one attribute.
#[derive(ToSchema)]
struct AttributeSchema {
    /// Attribute type. Fixed once set.
    #[schema(rename = "type")]
    attribute_type: AttributeType,
    /// Index a `string` attribute for `BM25` ranking. Applies to rows
    /// written after it is enabled. Default `false`.
    full_text_search: Option<bool>,
    /// Embed a `string` attribute into the vector of rows written without
    /// one, with the server's embedding model. One attribute per
    /// namespace. Default `false`.
    embed: Option<bool>,
}

#[derive(ToSchema)]
struct CreateNamespace {
    /// Vector length, 1 to 65535. Every row's vector must have it.
    /// Optional when an attribute embeds: the embedding model's length.
    #[schema(minimum = 1)]
    dimensions: Option<u16>,
    /// Default `cosine_distance`.
    distance_metric: Option<DistanceMetric>,
    /// Attributes to declare up front. Undeclared attributes are typed from
    /// the first value written.
    schema: Option<BTreeMap<String, AttributeSchema>>,
}

/// A namespace's metadata.
#[derive(ToSchema)]
struct Namespace {
    name: String,
    /// Hex key prefix of the namespace inside its shard.
    id: String,
    /// Shard holding the namespace.
    shard: u32,
    dimensions: u16,
    distance_metric: DistanceMetric,
    /// Every known attribute, declared or inferred.
    schema: BTreeMap<String, AttributeSchema>,
    /// The model embedding the namespace, e.g.
    /// `openai:text-embedding-3-small`, fixed when an attribute first
    /// embeds.
    embedding_model: Option<String>,
    /// Creation time, in Unix milliseconds.
    created_at_ms: u64,
}

#[derive(ToSchema)]
struct NamespaceList {
    namespaces: Vec<Namespace>,
}

/// Schema entries to add or update, by attribute name.
#[derive(ToSchema)]
struct SchemaUpdate(BTreeMap<String, AttributeSchema>);

/// A document stored as one row per passage.
#[derive(ToSchema)]
struct Document {
    /// Document id: a string or non-negative integer. Passage `i` is
    /// stored as row `"{id}#{i}"`.
    #[schema(value_type = Value)]
    id: serde_json::Value,
    /// Attributes copied onto every passage row.
    #[schema(value_type = Object)]
    attributes: Option<serde_json::Value>,
    /// Passages in order, each `{"vector": [..], attribute: value, ..}`.
    /// Passage attributes override document attributes.
    #[schema(value_type = Vec<Object>)]
    passages: Vec<serde_json::Value>,
}

/// Changes to one namespace. Writing to a missing namespace creates it,
/// taking its dimensions from the first vector in the request, or from the
/// embedding model when `schema` declares an `embed` attribute. Rows of a
/// namespace with an `embed` attribute may omit `vector`.
///
/// Operations apply in the order `delete_by_filter`, `delete_documents`,
/// `upsert_documents`, `upsert_rows`, `patch_rows`, `deletes`. The request
/// is validated before anything is applied; each operation is atomic, the
/// request as a whole is not.
#[derive(ToSchema)]
struct Write {
    /// Rows to insert or replace: `{"id": .., "vector": [..], attribute:
    /// value, ..}`. `null` attributes are omitted.
    #[schema(value_type = Option<Vec<Object>>)]
    upsert_rows: Option<Vec<serde_json::Value>>,
    /// Partial rows merged into existing rows by `id`. `null` removes an
    /// attribute; rows that don't exist are skipped.
    #[schema(value_type = Option<Vec<Object>>)]
    patch_rows: Option<Vec<serde_json::Value>>,
    /// Ids of rows to delete.
    #[schema(value_type = Option<Vec<Value>>)]
    deletes: Option<Vec<serde_json::Value>>,
    /// Deletes every row matching this filter.
    delete_by_filter: Option<Filter>,
    /// Documents to insert or replace. A document's previous passages are
    /// replaced, including any beyond the new passage count.
    upsert_documents: Option<Vec<Document>>,
    /// Ids of documents whose passages to delete.
    #[schema(value_type = Option<Vec<Value>>)]
    delete_documents: Option<Vec<serde_json::Value>>,
    /// Metric of a namespace this write creates. Must match an existing
    /// namespace's metric.
    distance_metric: Option<DistanceMetric>,
    /// Schema entries applied before the write, as in `PATCH .../schema`.
    schema: Option<BTreeMap<String, AttributeSchema>>,
}

#[derive(ToSchema)]
struct WriteResult {
    rows_upserted: u64,
    rows_patched: u64,
    /// Rows removed by `deletes`, `delete_by_filter`, `delete_documents`,
    /// and stale passages of upserted documents.
    rows_deleted: u64,
    documents_upserted: u64,
    documents_deleted: u64,
}

/// At most `limit` rows per distinct combination of `attributes`.
#[derive(ToSchema)]
struct PerLimit {
    /// Attributes to group by, e.g. `["$doc_id"]`. `id` is allowed.
    attributes: Vec<String>,
    #[schema(minimum = 1)]
    limit: u64,
}

#[derive(ToSchema)]
struct LimitSpec {
    /// Rows to return, 1 to 10000.
    #[schema(minimum = 1)]
    total: u64,
    per: Option<PerLimit>,
}

#[derive(ToSchema)]
#[schema(rename_all = "lowercase")]
enum ConsistencyLevel {
    /// See every write acknowledged before the query. Only standalone
    /// deployments serve strong queries, and default to them; sharded
    /// clusters answer 400.
    Strong,
    /// Read the last published snapshot, which lags writes by the flush
    /// interval (plus the reader refresh interval on readers). The default,
    /// and the only level, in sharded clusters.
    Eventual,
}

#[derive(ToSchema)]
struct Consistency {
    level: ConsistencyLevel,
}

#[derive(ToSchema)]
struct Rrf {
    /// Rank offset: a row at rank `r` (from 1) adds `1 / (k + r)` to its
    /// fused score. Default `60`.
    k: Option<f64>,
    /// Fused rows to return. Defaults to the largest sub-query limit.
    limit: Option<u64>,
}

/// A filter operand: one value of the attribute's type, or of its element
/// type for array attributes.
#[derive(ToSchema)]
#[serde(untagged)]
enum FilterValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

#[derive(ToSchema)]
struct FilterList {
    #[schema(no_recursion)]
    filters: Vec<Filter>,
}

#[derive(ToSchema)]
struct FilterNegation {
    #[schema(no_recursion)]
    filter: Box<Filter>,
}

#[derive(ToSchema)]
struct AttributeRef {
    attribute: String,
}

/// An attribute compared with one value.
#[derive(ToSchema)]
struct Comparison {
    /// Any attribute but `id` and `vector`.
    attribute: String,
    value: FilterValue,
}

/// An attribute compared with a set of values.
#[derive(ToSchema)]
struct SetComparison {
    attribute: String,
    values: Vec<FilterValue>,
}

/// A `string` attribute matched against a glob pattern: `*`, `?`,
/// classes like `[a-z]` and `[!0-9]`, and `\` to escape.
#[derive(ToSchema)]
struct GlobPattern {
    attribute: String,
    pattern: String,
}

/// A boolean filter over row attributes, tagged by `op`. On array
/// attributes, operators match when any element does; negated operators
/// also match rows without the attribute. See
/// [filters](/vector/queries/#filters).
#[derive(ToSchema)]
#[serde(tag = "op")]
enum Filter {
    /// Every filter matches.
    And(FilterList),
    /// Any filter matches.
    Or(FilterList),
    /// The filter doesn't match.
    Not(FilterNegation),
    /// The row has the attribute.
    Exists(AttributeRef),
    /// The row doesn't have the attribute.
    NotExists(AttributeRef),
    Eq(Comparison),
    NotEq(Comparison),
    Lt(Comparison),
    Lte(Comparison),
    Gt(Comparison),
    Gte(Comparison),
    /// An array attribute has the value as an element.
    Contains(Comparison),
    NotContains(Comparison),
    /// Equals one of the values.
    In(SetComparison),
    NotIn(SetComparison),
    /// An array attribute has any of the values as an element.
    ContainsAny(SetComparison),
    NotContainsAny(SetComparison),
    /// Matches the pattern, case-sensitively.
    Glob(GlobPattern),
    NotGlob(GlobPattern),
    /// Matches the pattern, ignoring case.
    IGlob(GlobPattern),
    NotIGlob(GlobPattern),
}

#[derive(ToSchema)]
struct VectorRank {
    /// The query vector, with the namespace's dimensions. Exactly one of
    /// `vector` and `text`.
    vector: Option<Vec<f32>>,
    /// Text to embed into the query vector, in a namespace with an
    /// `embed` attribute.
    text: Option<String>,
}

#[derive(ToSchema)]
struct TextRank {
    /// A `full_text_search` attribute.
    attribute: String,
    query: String,
}

#[derive(ToSchema)]
#[schema(rename_all = "lowercase")]
enum Direction {
    Asc,
    Desc,
}

#[derive(ToSchema)]
struct AttributeOrder {
    /// An attribute, or `id` (ascending only). Rows without the attribute
    /// come last.
    attribute: String,
    /// Default `asc`.
    direction: Option<Direction>,
}

/// How to order rows, tagged by `op`. See
/// [ranking](/vector/queries/#ranking).
#[derive(ToSchema)]
#[serde(tag = "op")]
enum RankBy {
    /// Approximate nearest neighbours, closest first. Adds `$dist`.
    #[serde(rename = "ANN")]
    Ann(VectorRank),
    /// Exact nearest neighbours of the rows matching the filter. Adds
    /// `$dist`.
    #[serde(rename = "kNN")]
    Knn(VectorRank),
    /// BM25 relevance, best first. Adds `$score`.
    #[serde(rename = "BM25")]
    Bm25(TextRank),
    /// Attribute order.
    Order(AttributeOrder),
}

/// An aggregate over the rows matching `filters`, tagged by `op`.
#[derive(ToSchema)]
#[serde(tag = "op")]
enum Aggregate {
    /// The number of matching rows.
    Count,
}

/// Declares a query schema with the fields of a single query, plus `$extra`.
macro_rules! query_schema {
    ($(#[$meta:meta])* $name:ident { $($extra:tt)* }) => {
        $(#[$meta])*
        #[derive(ToSchema)]
        struct $name {
            /// How to order rows. Default: ascending `id`.
            rank_by: Option<RankBy>,
            /// Rows must match this filter.
            filters: Option<Filter>,
            /// A row count (default `10`), or `{"total": n, "per": {..}}` to
            /// cap rows per group, e.g. passages per document.
            #[schema(value_type = Option<Value>)]
            limit: Option<serde_json::Value>,
            /// Alias of a numeric `limit`.
            top_k: Option<u64>,
            /// `true` (every attribute except `vector`, the default), `false`
            /// (only `id` and the score), or a list of names; list `vector`
            /// to return it.
            #[schema(value_type = Option<Value>)]
            include_attributes: Option<serde_json::Value>,
            /// Attributes to leave out.
            exclude_attributes: Option<Vec<String>>,
            /// Named aggregates over every row matching `filters`, e.g.
            /// `{"total": {"op": "Count"}}`. Without `rank_by`, the response
            /// has no rows.
            aggregate_by: Option<BTreeMap<String, Aggregate>>,
            consistency: Option<Consistency>,
            /// ANN centroids to probe. Default `limit` clamped to 10–100;
            /// filtered queries that come up short are retried with more.
            nprobe: Option<u64>,
            $($extra)*
        }
    };
}

query_schema!(
    /// One query of a multi-query batch.
    SubQuery {}
);

query_schema!(
    /// A query, or a batch of queries when `queries` is set.
    Query {
        /// Multi-query: run these queries against one snapshot. Only
        /// `consistency` and `rrf` may accompany it.
        queries: Option<Vec<SubQuery>>,
        /// Multi-query: fuse the results into one ranking by reciprocal rank
        /// fusion. Without it, each query's result is returned in `results`.
        rrf: Option<Rrf>,
    }
);

#[derive(ToSchema)]
struct Performance {
    query_execution_ms: f64,
}

/// The result of one query of a batch.
#[derive(ToSchema)]
struct SubQueryResult {
    #[schema(value_type = Option<Vec<Object>>)]
    rows: Option<Vec<serde_json::Value>>,
    #[schema(value_type = Option<Object>)]
    aggregations: Option<serde_json::Value>,
    performance: Performance,
}

#[derive(ToSchema)]
struct QueryResult {
    /// Matching rows, best first: `id`, then `$dist` (vector ranking) or
    /// `$score` (BM25 and RRF), then the selected attributes. Absent for
    /// aggregation-only queries.
    #[schema(value_type = Option<Vec<Object>>)]
    rows: Option<Vec<serde_json::Value>>,
    /// Aggregate values by name.
    #[schema(value_type = Option<Object>)]
    aggregations: Option<serde_json::Value>,
    /// Multi-query without `rrf`: one result per query, in order.
    results: Option<Vec<SubQueryResult>>,
    /// Absent for multi-query without `rrf`.
    performance: Option<Performance>,
}

#[derive(ToSchema)]
#[schema(rename_all = "snake_case")]
enum ShardState {
    /// Holds and serves namespaces.
    Active,
    /// Split into `children`; kept because their clones reference its files.
    Retired,
}

/// Namespace ids `[start, end)`, as 16 hex digits.
#[derive(ToSchema)]
struct NamespaceRange {
    start: String,
    /// Absent when the range extends to the end of the id space.
    end: Option<String>,
}

#[derive(ToSchema)]
struct Shard {
    id: u32,
    state: ShardState,
    /// Namespace ids the shard holds; every id belongs to exactly one
    /// active shard.
    range: NamespaceRange,
    /// Writer replica that owns the shard: each writer owns one active
    /// shard.
    owner: Option<String>,
    /// A writer that joined without a shard: the owner is splitting this
    /// shard and hands it the upper half.
    split_to: Option<String>,
    /// Shard this one was split from.
    parent: Option<u32>,
    /// Shards this one was split into.
    children: Option<Vec<u32>>,
    /// Creation time, in Unix milliseconds.
    created_at_ms: u64,
    /// Namespaces the shard holds.
    namespaces: Vec<String>,
}

#[derive(ToSchema)]
struct ShardList {
    shards: Vec<Shard>,
}

/// A row with every attribute, including `vector`.
#[derive(ToSchema)]
struct Row {
    id: String,
    vector: Vec<f32>,
    /// Further attributes, by name.
    #[schema(value_type = Object)]
    attributes: serde_json::Value,
}

#[utoipa::path(
    get,
    path = "/api/v1/namespaces",
    tag = "namespaces",
    responses((status = 200, description = "Every namespace, by name", body = NamespaceList))
)]
fn list_namespaces() {}

#[utoipa::path(
    put,
    path = "/api/v1/namespaces/{namespace}",
    tag = "namespaces",
    params(("namespace" = String, Path, description = "1–128 characters of `A-Z a-z 0-9 - _ .`")),
    request_body(content = CreateNamespace, content_type = "application/json"),
    responses(
        (status = 201, description = "Namespace created on the shard whose range holds its id", body = Namespace),
        (status = 400, description = "Invalid name, dimensions, or schema, or an embed attribute without a matching embedding model", body = ErrorEnvelope),
        (status = 409, description = "The namespace exists", body = ErrorEnvelope),
        (status = 502, description = "The embedding provider failed while learning its dimensions", body = ErrorEnvelope)
    )
)]
fn create_namespace() {}

#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace}",
    tag = "namespaces",
    params(("namespace" = String, Path)),
    responses(
        (status = 200, description = "Namespace metadata", body = Namespace),
        (status = 404, description = "No such namespace", body = ErrorEnvelope)
    )
)]
fn get_namespace() {}

#[utoipa::path(
    delete,
    path = "/api/v1/namespaces/{namespace}",
    tag = "namespaces",
    params(("namespace" = String, Path)),
    responses(
        (status = 200, description = "Namespace and all its rows deleted", body = StatusOk),
        (status = 404, description = "No such namespace", body = ErrorEnvelope)
    )
)]
fn delete_namespace() {}

#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace}/schema",
    tag = "namespaces",
    params(("namespace" = String, Path)),
    responses(
        (status = 200, description = "Attribute schemas by name", body = SchemaUpdate),
        (status = 404, description = "No such namespace", body = ErrorEnvelope)
    )
)]
fn get_schema() {}

#[utoipa::path(
    patch,
    path = "/api/v1/namespaces/{namespace}/schema",
    tag = "namespaces",
    params(("namespace" = String, Path)),
    request_body(content = SchemaUpdate, content_type = "application/json"),
    responses(
        (status = 200, description = "The updated schema", body = SchemaUpdate),
        (status = 400, description = "A type change, or full-text search on a non-string", body = ErrorEnvelope),
        (status = 404, description = "No such namespace", body = ErrorEnvelope)
    )
)]
fn update_schema() {}

#[utoipa::path(
    post,
    path = "/api/v1/namespaces/{namespace}/write",
    tag = "rows",
    params(("namespace" = String, Path)),
    request_body(content = Write, content_type = "application/json"),
    responses(
        (status = 200, description = "Applied; visible to row reads and strong queries at once, and to eventual queries after the next flush", body = WriteResult),
        (status = 400, description = "Invalid rows, types, or filter; nothing applied", body = ErrorEnvelope),
        (status = 404, description = "No such namespace and no vector to create it from", body = ErrorEnvelope),
        (status = 502, description = "The embedding provider failed", body = ErrorEnvelope),
        (status = 503, description = "The replica serving the namespace is unavailable, for example while its shard splits for a new writer; retry", body = ErrorEnvelope)
    )
)]
fn write() {}

#[utoipa::path(
    post,
    path = "/api/v1/namespaces/{namespace}/query",
    tag = "rows",
    params(("namespace" = String, Path)),
    request_body(content = Query, content_type = "application/json"),
    responses(
        (status = 200, description = "Matching rows and aggregates", body = QueryResult),
        (status = 400, description = "Invalid ranking, filter, or limit, or a strong query to a sharded cluster", body = ErrorEnvelope),
        (status = 404, description = "No such namespace", body = ErrorEnvelope),
        (status = 502, description = "The embedding provider failed", body = ErrorEnvelope),
        (status = 503, description = "The replica serving the namespace is unavailable, for example while its shard splits for a new writer; retry", body = ErrorEnvelope)
    )
)]
fn query() {}

#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace}/rows/{id}",
    tag = "rows",
    params(
        ("namespace" = String, Path),
        ("id" = String, Path, description = "Row id; passage rows are `{document}#{index}`")
    ),
    responses(
        (status = 200, description = "The row", body = Row),
        (status = 404, description = "No such namespace or row", body = ErrorEnvelope)
    )
)]
fn get_row() {}

#[utoipa::path(
    get,
    path = "/api/v1/shards",
    tag = "shards",
    responses((status = 200, description = "Every shard, active and retired", body = ShardList))
)]
fn list_shards() {}

#[utoipa::path(
    get,
    path = "/-/healthy",
    tag = "operations",
    responses((status = 200, description = "Process is running"))
)]
fn healthy() {}

#[utoipa::path(
    get,
    path = "/-/ready",
    tag = "operations",
    responses((status = 200, description = "Server is ready"))
)]
fn ready() {}

#[utoipa::path(
    get,
    path = "/metrics",
    tag = "operations",
    responses((status = 200, description = "Prometheus exposition of server metrics", content_type = "text/plain"))
)]
fn metrics() {}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Vector HTTP API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Namespaced vector search with attribute filters, full-text ranking, multi-passage documents, and shard inspection.\n\nWhen authentication is configured, requests carry Basic credentials or a JWT bearer token. Namespace credentials grant read (queries, rows, metadata) or write (also creating, configuring, deleting and writing to the namespace) access to one namespace; global credentials cover every namespace and the shard listing. Namespaces must be declared, for example by a NamespaceAuthentication resource, and others answer 404. Listings show only the namespaces the caller can read."
    ),
    modifiers(&Authentication),
    paths(
        list_namespaces,
        create_namespace,
        get_namespace,
        delete_namespace,
        get_schema,
        update_schema,
        write,
        query,
        get_row,
        list_shards,
        healthy,
        ready,
        metrics,
    ),
    components(schemas(
        ErrorEnvelope,
        StatusOk,
        DistanceMetric,
        AttributeType,
        AttributeSchema,
        CreateNamespace,
        Namespace,
        NamespaceList,
        SchemaUpdate,
        Document,
        Write,
        WriteResult,
        PerLimit,
        LimitSpec,
        ConsistencyLevel,
        Consistency,
        Rrf,
        FilterValue,
        FilterList,
        FilterNegation,
        AttributeRef,
        Comparison,
        SetComparison,
        GlobPattern,
        Filter,
        VectorRank,
        TextRank,
        Direction,
        AttributeOrder,
        RankBy,
        Aggregate,
        SubQuery,
        Query,
        Performance,
        SubQueryResult,
        QueryResult,
        ShardState,
        NamespaceRange,
        Shard,
        ShardList,
        Row,
    )),
    tags(
        (name = "namespaces", description = "Create, inspect, and delete namespaces"),
        (name = "rows", description = "Write and query rows and documents"),
        (name = "shards", description = "Inspect shards: one per writer, split as writers join"),
        (name = "operations", description = "Probes and metrics")
    )
)]
struct ApiDoc;

/// Documents the credentials every `/api/` operation accepts.
struct Authentication;

impl utoipa::Modify for Authentication {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{
            HttpAuthScheme, HttpBuilder, SecurityRequirement, SecurityScheme,
        };
        use utoipa::openapi::{ContentBuilder, Ref, ResponseBuilder};

        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "basic",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Basic).build()),
        );
        components.add_security_scheme(
            "bearer",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
        let unauthorized = ResponseBuilder::new()
            .description("Missing or insufficient credentials, when authentication is configured")
            .content(
                "application/json",
                ContentBuilder::new()
                    .schema(Some(Ref::from_schema_name("ErrorEnvelope")))
                    .build(),
            )
            .build();
        for (path, item) in openapi.paths.paths.iter_mut() {
            if !path.starts_with("/api/") {
                continue;
            }
            for operation in [
                &mut item.get,
                &mut item.put,
                &mut item.post,
                &mut item.delete,
                &mut item.patch,
            ]
            .into_iter()
            .flatten()
            {
                operation.security = Some(vec![
                    SecurityRequirement::new("basic", Vec::<String>::new()),
                    SecurityRequirement::new("bearer", Vec::<String>::new()),
                ]);
                operation
                    .responses
                    .responses
                    .insert("401".into(), unauthorized.clone().into());
            }
        }
    }
}

/// The `op`-tagged unions of the query language.
const TAGGED_UNIONS: [&str; 3] = ["Filter", "RankBy", "Aggregate"];

/// Gives each `op` of a tagged union its own flat schema (`FilterEq`,
/// `RankByANN`, ..) and the union a discriminator mapping to them, so
/// generated clients get one type per operation. Variant payload schemas
/// left unreferenced are dropped.
fn discriminate(document: &mut Value) {
    let schemas = document["components"]["schemas"]
        .as_object_mut()
        .expect("components have schemas");
    let mut payloads = Vec::new();
    for union in TAGGED_UNIONS {
        let variants = schemas[union]["oneOf"]
            .as_array()
            .expect("tagged unions are oneOf")
            .clone();
        let mut refs = Vec::new();
        let mut mapping = Map::new();
        for variant in variants {
            let (payload, tag) = match variant["allOf"].as_array() {
                Some(parts) => {
                    let name = parts[0]["$ref"]
                        .as_str()
                        .and_then(|r| r.rsplit('/').next())
                        .expect("variant payloads are references");
                    payloads.push(name.to_string());
                    (schemas[name].clone(), parts[1].clone())
                }
                None => (json!({}), variant.clone()),
            };
            let op = tag["properties"]["op"]["enum"][0]
                .as_str()
                .expect("variants are tagged by op")
                .to_string();
            let mut properties =
                Map::from_iter([("op".to_string(), tag["properties"]["op"].clone())]);
            properties.extend(
                payload["properties"]
                    .as_object()
                    .cloned()
                    .unwrap_or_default(),
            );
            let mut required = vec![Value::from("op")];
            required.extend(payload["required"].as_array().cloned().unwrap_or_default());
            let mut schema =
                json!({"type": "object", "required": required, "properties": properties});
            if let Some(description) = variant.get("description").or(payload.get("description")) {
                schema["description"] = description.clone();
            }
            let mut chars = op.chars();
            let name = format!(
                "{union}{}{}",
                chars
                    .next()
                    .map(|c| c.to_ascii_uppercase())
                    .unwrap_or_default(),
                chars.as_str()
            );
            let reference = format!("#/components/schemas/{name}");
            schemas.insert(name, schema);
            refs.push(json!({ "$ref": reference }));
            mapping.insert(op, Value::from(reference));
        }
        schemas[union]["oneOf"] = Value::Array(refs);
        schemas[union]["discriminator"] = json!({"propertyName": "op", "mapping": mapping});
    }
    for payload in payloads {
        let Some(schema) = document["components"]["schemas"]
            .as_object_mut()
            .and_then(|schemas| schemas.remove(&payload))
        else {
            continue;
        };
        let reference = Value::from(format!("#/components/schemas/{payload}"));
        if references(document, &reference) {
            document["components"]["schemas"][payload] = schema;
        }
    }
}

fn references(value: &Value, reference: &Value) -> bool {
    match value {
        Value::Object(map) => {
            map.get("$ref") == Some(reference) || map.values().any(|v| references(v, reference))
        }
        Value::Array(items) => items.iter().any(|v| references(v, reference)),
        _ => false,
    }
}

/// The OpenAPI document of the cluster HTTP API.
pub fn document() -> Value {
    let mut document = serde_json::to_value(ApiDoc::openapi()).expect("OpenAPI serializes");
    discriminate(&mut document);
    document
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_document_every_cluster_route() {
        let document = document();
        let paths = document["paths"].as_object().unwrap();
        for path in [
            "/api/v1/namespaces",
            "/api/v1/namespaces/{namespace}",
            "/api/v1/namespaces/{namespace}/schema",
            "/api/v1/namespaces/{namespace}/write",
            "/api/v1/namespaces/{namespace}/query",
            "/api/v1/namespaces/{namespace}/rows/{id}",
            "/api/v1/shards",
        ] {
            assert!(paths.contains_key(path), "{path} is undocumented");
        }
    }

    #[test]
    fn should_give_each_query_operation_its_own_schema() {
        let document = document();
        let schemas = &document["components"]["schemas"];
        let filter = &schemas["Filter"];
        assert_eq!(filter["discriminator"]["propertyName"], "op");
        assert_eq!(
            filter["discriminator"]["mapping"]["NotContainsAny"],
            "#/components/schemas/FilterNotContainsAny"
        );
        assert_eq!(filter["oneOf"].as_array().unwrap().len(), 21);
        let eq = &schemas["FilterEq"];
        assert_eq!(eq["properties"]["op"]["enum"], json!(["Eq"]));
        assert_eq!(
            eq["properties"]["value"]["$ref"],
            "#/components/schemas/FilterValue"
        );
        assert_eq!(eq["required"], json!(["op", "attribute", "value"]));
        assert_eq!(
            schemas["FilterAnd"]["properties"]["filters"]["items"]["$ref"],
            "#/components/schemas/Filter"
        );
        assert_eq!(
            schemas["RankBy"]["discriminator"]["mapping"]["kNN"],
            "#/components/schemas/RankByKNN"
        );
        assert!(schemas["AggregateCount"].is_object());
        assert!(schemas.get("Comparison").is_none(), "payloads are inlined");
        assert!(schemas.get("FilterValue").is_some());
    }

    #[test]
    fn should_document_credentials_on_api_routes_only() {
        let document = document();
        let write = &document["paths"]["/api/v1/namespaces/{namespace}/write"]["post"];
        assert!(write["responses"].get("401").is_some());
        assert_eq!(write["security"].as_array().unwrap().len(), 2);
        assert!(
            document["paths"]["/-/healthy"]["get"]
                .get("security")
                .is_none()
        );
    }
}
