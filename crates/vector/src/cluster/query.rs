//! Namespace queries: ranking, filtering, collapsing, and projection.
//!
//! A query ranks a namespace's rows by one of
//!
//! - `{"op": "ANN", "vector": [..]}`: approximate nearest neighbours. Small
//!   filtered candidate sets are scored exactly instead;
//! - `{"op": "kNN", "vector": [..]}`: exact nearest neighbours of the rows
//!   matching the filter. Either vector ranking takes a `text` instead of
//!   the `vector` in namespaces with an embed attribute;
//! - `{"op": "BM25", "attribute": .., "query": ..}`: full-text relevance
//!   over a `full_text_search` attribute;
//! - `{"op": "Order", "attribute": .., "direction": "asc" | "desc"}`:
//!   attribute order (`id` sorts ascending only);
//! - nothing: ascending id order,
//!
//! keeps the rows matching `filters`, optionally collapses them to at most
//! `limit.per.limit` rows per distinct value of `limit.per.attributes`
//! (e.g. the best passages per document), and renders the selected
//! attributes.
//!
//! Filters, rankings, and aggregates are objects tagged by `op`, so each
//! maps onto one type of a tagged union in client SDKs.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::embedding::{self, Embedder, InputKind};
use super::namespace::Namespace;
use super::schema::{AttributeType, ID_KEY, NamespaceMeta, attribute_to_json};
use crate::error::{Error, Result};
use crate::model::{
    AttributeValue, FieldSelection, Filter, Query, SearchOptions, VECTOR_FIELD_NAME, Vector,
};
use crate::query_engine::QueryEngine;
use crate::query_engine::filter::PreparedFilter;

/// Rows returned when a query sets no limit.
pub const DEFAULT_LIMIT: usize = 10;
/// Largest `limit.total`, and the most candidates a collapsing query ranks.
pub const MAX_LIMIT: usize = 10_000;
/// Filtered ANN queries with at most this many matching rows are answered
/// by exact kNN over those rows.
const EXACT_KNN_THRESHOLD: u64 = 2_048;
/// Times a filtered ANN query that came up short is retried with 4x the
/// probes.
const ANN_RETRIES: usize = 3;
/// Default `k` of reciprocal rank fusion.
const DEFAULT_RRF_K: f64 = 60.0;

/// Attribute carrying a row's vector distance in query results.
pub const DIST_KEY: &str = "$dist";
/// Attribute carrying a row's relevance score in query results.
pub const SCORE_KEY: &str = "$score";

/// A query against one namespace.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    /// How to rank rows; see the module docs.
    #[serde(default)]
    pub rank_by: Option<RankByExpr>,
    #[serde(default)]
    pub filters: Option<FilterExpr>,
    #[serde(default)]
    pub limit: Option<Limit>,
    /// Alias of `limit` as a plain row count.
    #[serde(default)]
    pub top_k: Option<usize>,
    /// `true` (every attribute but the vector, the default), `false`, or a
    /// list of attribute names (`"vector"` included).
    #[serde(default)]
    pub include_attributes: Option<IncludeAttributes>,
    #[serde(default)]
    pub exclude_attributes: Option<Vec<String>>,
    /// Named aggregates over the rows matching `filters`, e.g.
    /// `{"total": {"op": "Count"}}`.
    #[serde(default)]
    pub aggregate_by: Option<BTreeMap<String, Aggregate>>,
    #[serde(default)]
    pub consistency: Option<Consistency>,
    /// ANN centroids to probe (defaults to `limit` clamped to 10..=100).
    #[serde(default)]
    pub nprobe: Option<usize>,
}

/// Row limit: a count, or a total with a per-group cap.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Limit {
    Total(usize),
    Spec(LimitSpec),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitSpec {
    pub total: usize,
    #[serde(default)]
    pub per: Option<PerLimit>,
}

/// At most `limit` rows per distinct combination of `attributes`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerLimit {
    pub attributes: Vec<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum IncludeAttributes {
    All(bool),
    Only(Vec<String>),
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Consistency {
    pub level: ConsistencyLevel,
}

/// Standalone deployments default to strong; sharded clusters only serve
/// eventual queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConsistencyLevel {
    /// See every write acknowledged before the query.
    Strong,
    /// Read the last published snapshot; skips the pre-query sync.
    Eventual,
}

/// A boolean filter over row attributes, tagged by `op`.
///
/// Array attributes match when any element does. Negated operators also
/// match rows without the attribute.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(remote = "Self", tag = "op", deny_unknown_fields)]
pub enum FilterExpr {
    And {
        filters: Vec<FilterExpr>,
    },
    Or {
        filters: Vec<FilterExpr>,
    },
    Not {
        filter: Box<FilterExpr>,
    },
    Exists(AttributeRef),
    NotExists(AttributeRef),
    Eq(Comparison),
    NotEq(Comparison),
    Lt(Comparison),
    Lte(Comparison),
    Gt(Comparison),
    Gte(Comparison),
    /// An array attribute has `value` as an element.
    Contains(Comparison),
    NotContains(Comparison),
    In(SetComparison),
    NotIn(SetComparison),
    /// An array attribute has any of `values` as an element.
    ContainsAny(SetComparison),
    NotContainsAny(SetComparison),
    Glob(GlobPattern),
    NotGlob(GlobPattern),
    /// `Glob`, ignoring case.
    IGlob(GlobPattern),
    NotIGlob(GlobPattern),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttributeRef {
    pub attribute: String,
}

/// An attribute compared with one value of its (element) type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Comparison {
    pub attribute: String,
    pub value: Value,
}

/// An attribute compared with a set of values of its (element) type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetComparison {
    pub attribute: String,
    pub values: Vec<Value>,
}

/// A string attribute matched against a glob pattern.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobPattern {
    pub attribute: String,
    pub pattern: String,
}

/// How to rank rows, tagged by `op`; see the module docs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(remote = "Self", tag = "op", deny_unknown_fields)]
pub enum RankByExpr {
    #[serde(rename = "ANN")]
    Ann {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        vector: Option<Vec<f32>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        text: Option<String>,
    },
    #[serde(rename = "kNN")]
    Knn {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        vector: Option<Vec<f32>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        text: Option<String>,
    },
    #[serde(rename = "BM25")]
    Bm25 { attribute: String, query: String },
    Order {
        attribute: String,
        #[serde(default)]
        direction: Direction,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    #[default]
    Asc,
    Desc,
}

/// An aggregate over the rows matching a query's filters, tagged by `op`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(remote = "Self", tag = "op", deny_unknown_fields)]
pub enum Aggregate {
    /// The number of matching rows.
    Count,
}

/// Implements serde for an `op`-tagged union declared with
/// `#[serde(remote = "Self")]`, accepting only JSON objects: serde's
/// internally tagged enums also take `[op, field, ..]` arrays.
macro_rules! op_tagged {
    ($($ty:ty),*) => {$(
        impl Serialize for $ty {
            fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
                <$ty>::serialize(self, s)
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
                let object = Map::<String, Value>::deserialize(d)?;
                <$ty>::deserialize(Value::Object(object)).map_err(serde::de::Error::custom)
            }
        }
    )*};
}

op_tagged!(FilterExpr, RankByExpr, Aggregate);

/// Several queries over one snapshot of a namespace.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiQueryRequest {
    pub queries: Vec<QueryRequest>,
    /// Fuse the results into one ranking by reciprocal rank fusion.
    #[serde(default)]
    pub rrf: Option<RrfSpec>,
    #[serde(default)]
    pub consistency: Option<Consistency>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RrfSpec {
    /// Rank offset: a row ranked `r` (from 1) contributes `1 / (k + r)`.
    #[serde(default)]
    pub k: Option<f64>,
    /// Fused rows to return (defaults to the largest sub-query limit).
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResponse {
    /// Absent for aggregation-only queries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Map<String, Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<Map<String, Value>>,
    pub performance: Performance,
}

#[derive(Debug, Clone, Serialize)]
pub struct Performance {
    pub query_execution_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum MultiQueryResponse {
    /// One response per query.
    Separate { results: Vec<QueryResponse> },
    /// The fused ranking.
    Fused(QueryResponse),
}

/// A parsed `rank_by`.
#[derive(Debug, Clone)]
enum RankBy {
    Ann(Vec<f32>),
    Knn(Vec<f32>),
    Bm25 { field: String, query: String },
    Attribute { field: String, descending: bool },
    Id,
}

#[derive(Debug, Clone, Copy)]
enum ScoreKind {
    Dist,
    Score,
}

struct Ranked {
    row: Vector,
    score: Option<(ScoreKind, f32)>,
}

/// Runs `request` against `namespace`, after syncing it when `strong`.
pub(crate) async fn query(
    namespace: &Namespace,
    mut request: QueryRequest,
    strong: bool,
    embedder: Option<&Embedder>,
) -> Result<QueryResponse> {
    let start = Instant::now();
    embed_texts(std::iter::once(&mut request), &namespace.meta(), embedder).await?;
    let engine = namespace.engine(strong).await?;
    let mut response = run(namespace, &engine, &request).await?;
    response.performance.query_execution_ms = elapsed_ms(start);
    Ok(response)
}

/// Runs every query of `request` against one snapshot of `namespace`.
pub(crate) async fn multi_query(
    namespace: &Namespace,
    mut request: MultiQueryRequest,
    strong: bool,
    embedder: Option<&Embedder>,
) -> Result<MultiQueryResponse> {
    let start = Instant::now();
    if request.queries.is_empty() {
        return Err(Error::InvalidInput("queries must not be empty".into()));
    }
    embed_texts(request.queries.iter_mut(), &namespace.meta(), embedder).await?;
    let engine = namespace.engine(strong).await?;
    let mut results = Vec::with_capacity(request.queries.len());
    for query in &request.queries {
        results.push(run(namespace, &engine, query).await?);
    }
    let Some(rrf) = request.rrf else {
        return Ok(MultiQueryResponse::Separate { results });
    };
    let k = rrf.k.unwrap_or(DEFAULT_RRF_K);
    let limit = rrf.limit.unwrap_or_else(|| {
        request
            .queries
            .iter()
            .map(|q| total_limit(q).unwrap_or(DEFAULT_LIMIT))
            .max()
            .unwrap_or(DEFAULT_LIMIT)
    });
    let mut fused: HashMap<String, (f64, Map<String, Value>)> = HashMap::new();
    for result in results {
        let Some(rows) = result.rows else {
            return Err(Error::InvalidInput(
                "rrf needs every query to return rows".into(),
            ));
        };
        for (rank, mut row) in rows.into_iter().enumerate() {
            let id = row.get(ID_KEY).map(Value::to_string).unwrap_or_default();
            row.remove(DIST_KEY);
            row.remove(SCORE_KEY);
            let contribution = 1.0 / (k + rank as f64 + 1.0);
            fused
                .entry(id)
                .and_modify(|(score, _)| *score += contribution)
                .or_insert((contribution, row));
        }
    }
    let mut fused: Vec<(f64, Map<String, Value>)> = fused.into_values().collect();
    fused.sort_by(|a, b| b.0.total_cmp(&a.0));
    fused.truncate(limit);
    let rows = fused
        .into_iter()
        .map(|(score, mut row)| {
            row.insert(SCORE_KEY.into(), Value::from(score));
            row
        })
        .collect();
    Ok(MultiQueryResponse::Fused(QueryResponse {
        rows: Some(rows),
        aggregations: None,
        performance: Performance {
            query_execution_ms: elapsed_ms(start),
        },
    }))
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn total_limit(request: &QueryRequest) -> Option<usize> {
    match (&request.limit, request.top_k) {
        (Some(Limit::Total(total)), _) => Some(*total),
        (Some(Limit::Spec(spec)), _) => Some(spec.total),
        (None, top_k) => top_k,
    }
}

async fn run(
    namespace: &Namespace,
    engine: &QueryEngine,
    request: &QueryRequest,
) -> Result<QueryResponse> {
    let meta = namespace.meta();
    let filter = request
        .filters
        .as_ref()
        .map(|f| parse_filter(f, &meta))
        .transpose()?;
    let prepared = engine.prepare_filter(filter.as_ref()).await?;

    let aggregations = match &request.aggregate_by {
        Some(aggregates) => Some(aggregate(engine, &prepared, aggregates).await?),
        None => None,
    };
    if aggregations.is_some() && request.rank_by.is_none() {
        return Ok(QueryResponse {
            rows: None,
            aggregations,
            performance: Performance {
                query_execution_ms: 0.0,
            },
        });
    }

    let rank_by = parse_rank_by(request.rank_by.as_ref(), &meta)?;
    let total = total_limit(request).unwrap_or(DEFAULT_LIMIT);
    if total == 0 || total > MAX_LIMIT {
        return Err(Error::InvalidInput(format!(
            "limit must be between 1 and {MAX_LIMIT}"
        )));
    }
    let per = match &request.limit {
        Some(Limit::Spec(LimitSpec { per: Some(per), .. })) => {
            if per.attributes.is_empty() || per.limit == 0 {
                return Err(Error::InvalidInput(
                    "limit.per needs attributes and a positive limit".into(),
                ));
            }
            Some(per)
        }
        _ => None,
    };

    let ctx = RankContext {
        namespace,
        engine,
        filter: filter.as_ref(),
        prepared: &prepared,
        nprobe: request.nprobe,
    };
    let ranked = match per {
        None => ctx.rank(&rank_by, total).await?,
        Some(per) => {
            let mut k = (total * 4).clamp(64, MAX_LIMIT);
            loop {
                let ranked = ctx.rank(&rank_by, k).await?;
                let exhausted = ranked.len() < k || k >= MAX_LIMIT;
                let collapsed = collapse(ranked, per, total);
                if collapsed.len() >= total || exhausted {
                    break collapsed;
                }
                k = (k * 4).min(MAX_LIMIT);
            }
        }
    };

    let projection = Projection::new(
        request.include_attributes.as_ref(),
        request.exclude_attributes.as_deref(),
    );
    let rows = ranked
        .iter()
        .map(|r| projection.render(&r.row, r.score, meta.distance_metric))
        .collect();
    Ok(QueryResponse {
        rows: Some(rows),
        aggregations,
        performance: Performance {
            query_execution_ms: 0.0,
        },
    })
}

async fn aggregate(
    engine: &QueryEngine,
    prepared: &PreparedFilter,
    aggregates: &BTreeMap<String, Aggregate>,
) -> Result<Map<String, Value>> {
    let mut out = Map::new();
    let mut count = None;
    for (name, aggregate) in aggregates {
        match aggregate {
            Aggregate::Count => {
                if count.is_none() {
                    count = Some(engine.matching_ids(prepared).await?.len());
                }
                out.insert(name.clone(), Value::from(count.unwrap_or_default()));
            }
        }
    }
    Ok(out)
}

struct RankContext<'a> {
    namespace: &'a Namespace,
    engine: &'a QueryEngine,
    filter: Option<&'a Filter>,
    prepared: &'a PreparedFilter,
    nprobe: Option<usize>,
}

impl RankContext<'_> {
    async fn rank(&self, rank_by: &RankBy, k: usize) -> Result<Vec<Ranked>> {
        match rank_by {
            RankBy::Ann(vector) => {
                if let Some(candidates) = self.prepared.candidates()
                    && candidates.len() <= EXACT_KNN_THRESHOLD
                {
                    return self.exact(vector, &candidates, k).await;
                }
                self.ann(vector, k).await
            }
            RankBy::Knn(vector) => {
                let candidates = self.engine.matching_ids(self.prepared).await?;
                self.exact(vector, &candidates, k).await
            }
            RankBy::Bm25 { field, query } => {
                let mut query = Query::bm25(field.clone(), query.clone()).with_limit(k);
                query.filter = self.filter.cloned();
                query.include_fields = FieldSelection::All;
                let results = self
                    .engine
                    .search_with_options(&query, SearchOptions::default())
                    .await?;
                Ok(results
                    .into_iter()
                    .map(|r| Ranked {
                        row: r.vector,
                        score: Some((ScoreKind::Score, r.score)),
                    })
                    .collect())
            }
            RankBy::Attribute { field, descending } => {
                let ids = self
                    .engine
                    .ids_by_attribute(field, *descending, self.prepared, k)
                    .await?;
                self.unscored(&ids).await
            }
            RankBy::Id => {
                let ids = self.engine.ids_by_external_id(self.prepared, k).await?;
                self.unscored(&ids).await
            }
        }
    }

    async fn exact(
        &self,
        vector: &[f32],
        candidates: &roaring::RoaringTreemap,
        k: usize,
    ) -> Result<Vec<Ranked>> {
        let results = self.engine.exact_knn(vector, candidates, k).await?;
        Ok(results
            .into_iter()
            .map(|r| Ranked {
                row: r.vector,
                score: Some((ScoreKind::Dist, r.score)),
            })
            .collect())
    }

    async fn ann(&self, vector: &[f32], k: usize) -> Result<Vec<Ranked>> {
        let mut query = Query::ann(vector.to_vec()).with_limit(k);
        query.filter = self.filter.cloned();
        query.include_fields = FieldSelection::All;
        let centroids = self.namespace.num_centroids();
        let mut nprobe = self.nprobe.unwrap_or_else(|| k.clamp(10, 100));
        let mut attempts = 0;
        loop {
            let options = SearchOptions {
                nprobe: Some(nprobe),
                ..Default::default()
            };
            let results = self.engine.search_with_options(&query, options).await?;
            attempts += 1;
            let short = results.len() < k && self.filter.is_some();
            if !short || attempts > ANN_RETRIES || nprobe >= centroids || self.nprobe.is_some() {
                return Ok(results
                    .into_iter()
                    .map(|r| Ranked {
                        row: r.vector,
                        score: Some((ScoreKind::Dist, r.score)),
                    })
                    .collect());
            }
            nprobe = nprobe.saturating_mul(4).min(centroids.max(1));
        }
    }

    async fn unscored(&self, ids: &[u64]) -> Result<Vec<Ranked>> {
        Ok(self
            .engine
            .load_rows(ids)
            .await?
            .into_iter()
            .map(|row| Ranked { row, score: None })
            .collect())
    }
}

/// Keeps at most `per.limit` rows per distinct value of `per.attributes`,
/// in rank order, up to `total` rows.
fn collapse(ranked: Vec<Ranked>, per: &PerLimit, total: usize) -> Vec<Ranked> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut out = Vec::with_capacity(total);
    for ranked in ranked {
        if out.len() >= total {
            break;
        }
        let key: Vec<Value> = per
            .attributes
            .iter()
            .map(|name| group_value(&ranked.row, name))
            .collect();
        let count = counts.entry(Value::Array(key).to_string()).or_default();
        if *count < per.limit {
            *count += 1;
            out.push(ranked);
        }
    }
    out
}

fn group_value(row: &Vector, name: &str) -> Value {
    if name == ID_KEY {
        return Value::String(row.id.clone());
    }
    row.attributes
        .iter()
        .find(|a| a.name == name)
        .map(|a| attribute_to_json(&a.value))
        .unwrap_or(Value::Null)
}

/// Which attributes a result row carries.
struct Projection {
    /// `None`: every attribute but the vector.
    include: Option<HashSet<String>>,
    exclude: HashSet<String>,
}

impl Projection {
    fn new(include: Option<&IncludeAttributes>, exclude: Option<&[String]>) -> Self {
        let include = match include {
            None | Some(IncludeAttributes::All(true)) => None,
            Some(IncludeAttributes::All(false)) => Some(HashSet::new()),
            Some(IncludeAttributes::Only(names)) => Some(names.iter().cloned().collect()),
        };
        Self {
            include,
            exclude: exclude.unwrap_or_default().iter().cloned().collect(),
        }
    }

    fn wants(&self, name: &str) -> bool {
        if self.exclude.contains(name) {
            return false;
        }
        match &self.include {
            None => name != VECTOR_FIELD_NAME,
            Some(names) => names.contains(name),
        }
    }

    fn render(
        &self,
        row: &Vector,
        score: Option<(ScoreKind, f32)>,
        metric: super::schema::Metric,
    ) -> Map<String, Value> {
        let mut out = Map::new();
        out.insert(ID_KEY.into(), Value::String(row.id.clone()));
        match score {
            Some((ScoreKind::Dist, score)) => {
                out.insert(DIST_KEY.into(), Value::from(metric.distance(score)));
            }
            Some((ScoreKind::Score, score)) => {
                out.insert(SCORE_KEY.into(), Value::from(score));
            }
            None => {}
        }
        for attribute in &row.attributes {
            if self.wants(&attribute.name) {
                out.insert(attribute.name.clone(), attribute_to_json(&attribute.value));
            }
        }
        out
    }
}

/// Replaces the `text` of vector rankings with its embedding, embedding the
/// texts of all `requests` together.
async fn embed_texts<'a>(
    requests: impl Iterator<Item = &'a mut QueryRequest>,
    meta: &NamespaceMeta,
    embedder: Option<&Embedder>,
) -> Result<()> {
    let mut pending = Vec::new();
    for request in requests {
        if let Some(RankByExpr::Ann { vector, text } | RankByExpr::Knn { vector, text }) =
            &mut request.rank_by
            && vector.is_none()
            && let Some(query) = text.take()
        {
            pending.push((query, vector));
        }
    }
    if pending.is_empty() {
        return Ok(());
    }
    let embedder = embedding::for_namespace(meta, embedder)?;
    let texts: Vec<&str> = pending.iter().map(|(text, _)| text.as_str()).collect();
    let vectors = embedder.embed(&texts, InputKind::Query).await?;
    for ((_, vector), embedded) in pending.into_iter().zip(vectors) {
        *vector = Some(embedded.to_vec());
    }
    Ok(())
}

fn parse_rank_by(expr: Option<&RankByExpr>, meta: &NamespaceMeta) -> Result<RankBy> {
    let dimensions = |vector: &Option<Vec<f32>>, text: &Option<String>| match (vector, text) {
        (Some(_), Some(_)) => Err(Error::InvalidInput(
            "rank by a vector or a text, not both".into(),
        )),
        (None, _) => Err(Error::InvalidInput(
            "vector ranking needs a vector or a text to embed".into(),
        )),
        (Some(vector), None) if vector.len() == meta.dimensions as usize => Ok(vector.clone()),
        (Some(vector), None) => Err(Error::InvalidInput(format!(
            "query vector has {} dimensions; namespace has {}",
            vector.len(),
            meta.dimensions
        ))),
    };
    Ok(match expr {
        None => RankBy::Id,
        Some(RankByExpr::Ann { vector, text }) => RankBy::Ann(dimensions(vector, text)?),
        Some(RankByExpr::Knn { vector, text }) => RankBy::Knn(dimensions(vector, text)?),
        Some(RankByExpr::Bm25 { attribute, query }) => {
            if !meta
                .schema
                .get(attribute)
                .is_some_and(|s| s.full_text_search)
            {
                return Err(Error::InvalidInput(format!(
                    "attribute '{attribute}' is not full-text indexed; set \
                     full_text_search in its schema"
                )));
            }
            RankBy::Bm25 {
                field: attribute.clone(),
                query: query.clone(),
            }
        }
        Some(RankByExpr::Order {
            attribute,
            direction,
        }) => {
            let descending = *direction == Direction::Desc;
            if attribute == ID_KEY {
                if descending {
                    return Err(Error::InvalidInput(
                        "ordering by id descending is not supported".into(),
                    ));
                }
                return Ok(RankBy::Id);
            }
            if attribute == VECTOR_FIELD_NAME {
                return Err(Error::InvalidInput(
                    "order by vector distance with ANN or kNN".into(),
                ));
            }
            if meta
                .schema
                .get(attribute)
                .is_some_and(|s| s.full_text_search)
            {
                return Err(Error::InvalidInput(format!(
                    "cannot order by full-text attribute '{attribute}'"
                )));
            }
            RankBy::Attribute {
                field: attribute.clone(),
                descending,
            }
        }
    })
}

/// Resolves a filter expression against `meta`'s schema, coercing operands
/// to their attribute's type.
pub(crate) fn parse_filter(expr: &FilterExpr, meta: &NamespaceMeta) -> Result<Filter> {
    use FilterExpr as F;
    let all = |filters: &[FilterExpr]| -> Result<Vec<Filter>> {
        filters.iter().map(|f| parse_filter(f, meta)).collect()
    };
    let not = |filter: Filter| Filter::Not(Box::new(filter));
    Ok(match expr {
        F::And { filters } => Filter::And(all(filters)?),
        F::Or { filters } => Filter::Or(all(filters)?),
        F::Not { filter } => not(parse_filter(filter, meta)?),
        F::Exists(a) => Filter::Exists(filterable(&a.attribute)?),
        F::NotExists(a) => not(Filter::Exists(filterable(&a.attribute)?)),
        F::Eq(c) | F::Contains(c) => {
            let (a, v) = c.resolve(meta)?;
            Filter::Eq(a, v)
        }
        F::NotEq(c) | F::NotContains(c) => {
            let (a, v) = c.resolve(meta)?;
            Filter::Neq(a, v)
        }
        F::Lt(c) => {
            let (a, v) = c.resolve(meta)?;
            Filter::Lt(a, v)
        }
        F::Lte(c) => {
            let (a, v) = c.resolve(meta)?;
            Filter::Lte(a, v)
        }
        F::Gt(c) => {
            let (a, v) = c.resolve(meta)?;
            Filter::Gt(a, v)
        }
        F::Gte(c) => {
            let (a, v) = c.resolve(meta)?;
            Filter::Gte(a, v)
        }
        F::In(s) | F::ContainsAny(s) => {
            let (a, v) = s.resolve(meta)?;
            Filter::In(a, v)
        }
        F::NotIn(s) | F::NotContainsAny(s) => {
            let (a, v) = s.resolve(meta)?;
            Filter::NotIn(a, v)
        }
        F::Glob(g) => {
            let (a, p) = g.resolve(meta)?;
            Filter::Glob(a, p)
        }
        F::NotGlob(g) => {
            let (a, p) = g.resolve(meta)?;
            not(Filter::Glob(a, p))
        }
        F::IGlob(g) => {
            let (a, p) = g.resolve(meta)?;
            Filter::IGlob(a, p)
        }
        F::NotIGlob(g) => {
            let (a, p) = g.resolve(meta)?;
            not(Filter::IGlob(a, p))
        }
    })
}

fn filterable(attribute: &str) -> Result<String> {
    if attribute == ID_KEY || attribute == VECTOR_FIELD_NAME {
        return Err(Error::InvalidInput(format!(
            "invalid filter on '{attribute}': filtering on id or vector is not supported"
        )));
    }
    Ok(attribute.to_string())
}

impl Comparison {
    fn resolve(&self, meta: &NamespaceMeta) -> Result<(String, AttributeValue)> {
        let attribute = filterable(&self.attribute)?;
        let element = meta.attribute_type(&attribute).map(element_type);
        let value = scalar_value(&attribute, &self.value, element)?;
        Ok((attribute, value))
    }
}

impl SetComparison {
    fn resolve(&self, meta: &NamespaceMeta) -> Result<(String, Vec<AttributeValue>)> {
        let attribute = filterable(&self.attribute)?;
        let element = meta.attribute_type(&attribute).map(element_type);
        let values = self
            .values
            .iter()
            .map(|v| scalar_value(&attribute, v, element))
            .collect::<Result<_>>()?;
        Ok((attribute, values))
    }
}

impl GlobPattern {
    fn resolve(&self, meta: &NamespaceMeta) -> Result<(String, String)> {
        let attribute = filterable(&self.attribute)?;
        if meta
            .attribute_type(&attribute)
            .is_some_and(|t| element_type(t) != AttributeType::String)
        {
            return Err(Error::InvalidInput(format!(
                "invalid filter on '{attribute}': glob operators need a string attribute"
            )));
        }
        Ok((attribute, self.pattern.clone()))
    }
}

/// The type filter operands of an attribute have: an array's element type.
fn element_type(attribute_type: AttributeType) -> AttributeType {
    match attribute_type {
        AttributeType::StringArray => AttributeType::String,
        AttributeType::IntArray => AttributeType::Int,
        other => other,
    }
}

fn scalar_value(
    attribute: &str,
    value: &Value,
    element: Option<AttributeType>,
) -> Result<AttributeValue> {
    let mismatch = |want: &str| {
        Error::InvalidInput(format!("filter on '{attribute}' needs {want}, got {value}"))
    };
    if value.is_null() {
        return Err(mismatch(
            "a value (use Exists or NotExists to test for a missing attribute)",
        ));
    }
    if value.is_array() || value.is_object() {
        return Err(mismatch("a single value (use In or ContainsAny for sets)"));
    }
    Ok(match element {
        Some(AttributeType::String) => {
            AttributeValue::String(value.as_str().ok_or_else(|| mismatch("a string"))?.into())
        }
        Some(AttributeType::Int) => {
            AttributeValue::Int64(value.as_i64().ok_or_else(|| mismatch("an integer"))?)
        }
        Some(AttributeType::Float) => {
            AttributeValue::Float64(value.as_f64().ok_or_else(|| mismatch("a number"))?)
        }
        Some(AttributeType::Bool) => {
            AttributeValue::Bool(value.as_bool().ok_or_else(|| mismatch("a boolean"))?)
        }
        Some(AttributeType::StringArray | AttributeType::IntArray) => {
            unreachable!("element types are scalar")
        }
        None => match value {
            Value::String(s) => AttributeValue::String(s.clone()),
            Value::Bool(b) => AttributeValue::Bool(*b),
            Value::Number(n) => match n.as_i64() {
                Some(i) => AttributeValue::Int64(i),
                None => AttributeValue::Float64(n.as_f64().unwrap_or_default()),
            },
            _ => return Err(mismatch("a string, number, or boolean")),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::schema::{AttributeSchema, Metric};
    use serde_json::json;

    fn meta() -> NamespaceMeta {
        let schema = |t| AttributeSchema {
            attribute_type: t,
            full_text_search: false,
            embed: false,
        };
        NamespaceMeta {
            name: "ns".into(),
            id: "00".into(),
            shard: 0,
            dimensions: 2,
            distance_metric: Metric::Cosine,
            schema: BTreeMap::from([
                ("price".to_string(), schema(AttributeType::Float)),
                ("tags".to_string(), schema(AttributeType::StringArray)),
                ("name".to_string(), schema(AttributeType::String)),
            ]),
            embedding_model: None,
            created_at_ms: 0,
        }
    }

    fn filter(value: Value) -> std::result::Result<FilterExpr, serde_json::Error> {
        serde_json::from_value(value)
    }

    fn rank_by(value: Value) -> Result<RankBy> {
        parse_rank_by(Some(&serde_json::from_value(value).unwrap()), &meta())
    }

    #[test]
    fn should_parse_nested_filters_with_coercion() {
        // given
        let expr = filter(json!({
            "op": "And",
            "filters": [
                {"op": "Gte", "attribute": "price", "value": 3},
                {"op": "Or", "filters": [
                    {"op": "Contains", "attribute": "tags", "value": "a"},
                    {"op": "ContainsAny", "attribute": "tags", "values": ["b", "c"]}
                ]},
                {"op": "Not", "filter": {"op": "Glob", "attribute": "name", "pattern": "foo*"}},
                {"op": "NotExists", "attribute": "missing"}
            ]
        }))
        .unwrap();

        // when
        let parsed = parse_filter(&expr, &meta()).unwrap();

        // then
        assert_eq!(
            parsed,
            Filter::And(vec![
                Filter::Gte("price".into(), AttributeValue::Float64(3.0)),
                Filter::Or(vec![
                    Filter::Eq("tags".into(), AttributeValue::String("a".into())),
                    Filter::In(
                        "tags".into(),
                        vec![
                            AttributeValue::String("b".into()),
                            AttributeValue::String("c".into())
                        ]
                    ),
                ]),
                Filter::Not(Box::new(Filter::Glob("name".into(), "foo*".into()))),
                Filter::Not(Box::new(Filter::Exists("missing".into()))),
            ])
        );
    }

    #[test]
    fn should_round_trip_filters_through_json() {
        let expr = FilterExpr::Not {
            filter: Box::new(FilterExpr::In(SetComparison {
                attribute: "tags".into(),
                values: vec![json!("a")],
            })),
        };
        let value = serde_json::to_value(&expr).unwrap();
        assert_eq!(
            value,
            json!({"op": "Not", "filter": {"op": "In", "attribute": "tags", "values": ["a"]}})
        );
        assert_eq!(filter(value).unwrap(), expr);
    }

    #[test]
    fn should_reject_malformed_filters() {
        // Shape errors fail to deserialize.
        for bad in [
            json!(["price", "Eq", 1]),
            json!({"attribute": "price", "value": 1}),
            json!({"op": "Between", "attribute": "price", "value": 1}),
            json!({"op": "Eq", "attribute": "price"}),
            json!({"op": "Eq", "attribute": "price", "values": [1]}),
            json!({"op": "Eq", "attribute": "price", "value": 1, "extra": true}),
            json!({"op": "In", "attribute": "price", "value": 1}),
            json!({"op": "And", "filters": {"op": "Exists", "attribute": "a"}}),
        ] {
            assert!(filter(bad.clone()).is_err(), "{bad} should not deserialize");
        }
        // Type errors fail against the schema.
        let meta = meta();
        for bad in [
            json!({"op": "Eq", "attribute": "price", "value": "cheap"}),
            json!({"op": "Eq", "attribute": "price", "value": null}),
            json!({"op": "Eq", "attribute": "tags", "value": ["a"]}),
            json!({"op": "Glob", "attribute": "price", "pattern": "1*"}),
            json!({"op": "Eq", "attribute": "id", "value": "a"}),
            json!({"op": "Exists", "attribute": "vector"}),
        ] {
            let expr = filter(bad.clone()).unwrap();
            assert!(
                parse_filter(&expr, &meta).is_err(),
                "{bad} should be rejected"
            );
        }
    }

    #[test]
    fn should_parse_rank_by_forms() {
        assert!(matches!(
            rank_by(json!({"op": "ANN", "vector": [1, 0]})).unwrap(),
            RankBy::Ann(_)
        ));
        assert!(matches!(
            rank_by(json!({"op": "kNN", "vector": [1.0, 0.5]})).unwrap(),
            RankBy::Knn(_)
        ));
        assert!(matches!(
            rank_by(json!({"op": "Order", "attribute": "price", "direction": "desc"})).unwrap(),
            RankBy::Attribute {
                descending: true,
                ..
            }
        ));
        assert!(matches!(
            rank_by(json!({"op": "Order", "attribute": "price"})).unwrap(),
            RankBy::Attribute {
                descending: false,
                ..
            }
        ));
        assert!(matches!(
            rank_by(json!({"op": "Order", "attribute": "id"})).unwrap(),
            RankBy::Id
        ));
        assert!(matches!(parse_rank_by(None, &meta()).unwrap(), RankBy::Id));
        // wrong dimensions, non-text BM25 field, id descending
        assert!(rank_by(json!({"op": "ANN", "vector": [1, 0, 0]})).is_err());
        assert!(rank_by(json!({"op": "BM25", "attribute": "name", "query": "hi"})).is_err());
        assert!(rank_by(json!({"op": "Order", "attribute": "id", "direction": "desc"})).is_err());
        for bad in [
            json!(["vector", "ANN", [1, 0]]),
            json!({"op": "Order", "attribute": "price", "direction": "up"}),
            json!({"op": "ANN", "vector": "abc"}),
        ] {
            assert!(
                serde_json::from_value::<RankByExpr>(bad.clone()).is_err(),
                "{bad} should not deserialize"
            );
        }
    }

    #[test]
    fn should_parse_aggregates() {
        let parsed: BTreeMap<String, Aggregate> =
            serde_json::from_value(json!({"n": {"op": "Count"}})).unwrap();
        assert_eq!(parsed["n"], Aggregate::Count);
        assert!(serde_json::from_value::<Aggregate>(json!(["Count"])).is_err());
        assert!(serde_json::from_value::<Aggregate>(json!({"op": "Sum"})).is_err());
    }

    #[test]
    fn should_collapse_rows_per_group_in_rank_order() {
        // given
        let row = |id: &str, doc: &str| Ranked {
            row: Vector::builder(id, vec![0.0, 0.0])
                .attribute("$doc_id", doc)
                .build(),
            score: None,
        };
        let ranked = vec![
            row("a#0", "a"),
            row("a#1", "a"),
            row("b#0", "b"),
            row("a#2", "a"),
            row("c#0", "c"),
        ];
        let per = PerLimit {
            attributes: vec!["$doc_id".into()],
            limit: 2,
        };

        // when
        let collapsed = collapse(ranked, &per, 3);

        // then
        let ids: Vec<_> = collapsed.iter().map(|r| r.row.id.as_str()).collect();
        assert_eq!(ids, vec!["a#0", "a#1", "b#0"]);
    }
}
