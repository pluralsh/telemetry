//! Differential tests: every vector read path against a brute-force model.
//!
//! Each seed drives a random workload of inserts, upserts, deletes and
//! re-inserts of deleted ids through a real [`VectorDb`] (small split/merge
//! thresholds force many centroids, splits, merges and boundary replication)
//! while mirroring it in a `BTreeMap`. The read paths are then checked against
//! the model:
//!
//! - exhaustive ANN (every centroid probed) and exact kNN must equal the
//!   brute-force top-k, ids and score bits,
//! - default-nprobe ANN is approximate, so it must only be *sound*: sorted,
//!   unique, filter-respecting, with true distances,
//! - prepared filters must select exactly the model's matching ids,
//! - point reads must return the last write, or nothing after a delete.
//!
//! Set `VECTOR_DIFF_SEEDS=<n>` to run more seeds when fuzzing locally.

use std::collections::{BTreeMap, BTreeSet};

use common::StorageConfig;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use crate::db::{VectorDb, VectorDbRead};
use crate::math::distance::{VectorDistance, compute_distance};
use crate::model::{
    AttributeValue, Config, Filter, MetadataFieldSpec, Query, SearchOptions, SearchResult, Vector,
};
use crate::serde::FieldType;
use crate::serde::collection_meta::DistanceMetric;

const DIMENSIONS: usize = 8;
const BATCHES: usize = 10;
const QUERIES: usize = 25;
const CATEGORIES: [&str; 6] = ["shoes", "shirts", "sh", "socks", "boots", "hats"];
const GLOBS: [&str; 7] = ["s*", "sh*", "*s", "?oots", "[bh]*", "sh?r*", "*"];

#[derive(Clone, Debug)]
struct Doc {
    vector: Vec<f32>,
    category: Option<String>,
    price: Option<i64>,
}

impl Doc {
    fn to_vector(&self, id: &str) -> Vector {
        let mut builder = Vector::builder(id, self.vector.clone());
        if let Some(category) = &self.category {
            builder = builder.attribute("category", category.as_str());
        }
        if let Some(price) = self.price {
            builder = builder.attribute("price", price);
        }
        builder.build()
    }

    fn field(&self, name: &str) -> Option<AttributeValue> {
        match name {
            "category" => self.category.clone().map(AttributeValue::String),
            "price" => self.price.map(AttributeValue::Int64),
            _ => None,
        }
    }
}

/// Brute-force evaluation of the documented [`Filter`] semantics.
fn model_matches(filter: &Filter, doc: &Doc) -> bool {
    let eq = |field: &str, value: &AttributeValue| doc.field(field).as_ref() == Some(value);
    let cmp = |field: &str, value: &AttributeValue| match (doc.field(field), value) {
        (Some(AttributeValue::Int64(a)), AttributeValue::Int64(b)) => Some(a.cmp(b)),
        (Some(AttributeValue::String(a)), AttributeValue::String(b)) => Some(a.as_str().cmp(b)),
        _ => None,
    };
    let glob = |field: &str, pattern: &str, ci: bool| match doc.field(field) {
        Some(AttributeValue::String(s)) => crate::utils::glob_match(pattern, &s, ci),
        _ => false,
    };
    match filter {
        Filter::Eq(f, v) => eq(f, v),
        Filter::Neq(f, v) => !eq(f, v),
        Filter::In(f, vs) => vs.iter().any(|v| eq(f, v)),
        Filter::NotIn(f, vs) => !vs.iter().any(|v| eq(f, v)),
        Filter::Lt(f, v) => cmp(f, v).is_some_and(|o| o.is_lt()),
        Filter::Lte(f, v) => cmp(f, v).is_some_and(|o| o.is_le()),
        Filter::Gt(f, v) => cmp(f, v).is_some_and(|o| o.is_gt()),
        Filter::Gte(f, v) => cmp(f, v).is_some_and(|o| o.is_ge()),
        Filter::Glob(f, p) => glob(f, p, false),
        Filter::IGlob(f, p) => glob(f, p, true),
        Filter::Exists(f) => doc.field(f).is_some(),
        Filter::Not(inner) => !model_matches(inner, doc),
        Filter::And(fs) => fs.iter().all(|f| model_matches(f, doc)),
        Filter::Or(fs) => fs.iter().any(|f| model_matches(f, doc)),
    }
}

fn random_category(rng: &mut StdRng) -> AttributeValue {
    AttributeValue::String(CATEGORIES.choose(rng).unwrap().to_string())
}

fn random_filter(rng: &mut StdRng, depth: u32) -> Filter {
    if depth == 0 || rng.gen_bool(0.5) {
        let price = AttributeValue::Int64(rng.gen_range(-5..105));
        return match rng.gen_range(0..12) {
            0 => Filter::Eq("category".into(), random_category(rng)),
            1 => Filter::Neq("category".into(), random_category(rng)),
            2 => Filter::In(
                "category".into(),
                vec![random_category(rng), random_category(rng)],
            ),
            3 => Filter::NotIn("category".into(), vec![random_category(rng)]),
            4 => Filter::Lt("price".into(), price),
            5 => Filter::Lte("price".into(), price),
            6 => Filter::Gt("price".into(), price),
            7 => Filter::Gte("price".into(), price),
            8 => Filter::Glob("category".into(), GLOBS.choose(rng).unwrap().to_string()),
            9 => Filter::IGlob("category".into(), GLOBS.choose(rng).unwrap().to_uppercase()),
            10 => Filter::Eq("price".into(), price),
            _ => Filter::Exists(
                if rng.gen_bool(0.5) {
                    "category"
                } else {
                    "price"
                }
                .into(),
            ),
        };
    }
    let children = |rng: &mut StdRng| -> Vec<Filter> {
        (0..rng.gen_range(0..4))
            .map(|_| random_filter(rng, depth - 1))
            .collect()
    };
    match rng.gen_range(0..3) {
        0 => Filter::Not(Box::new(random_filter(rng, depth - 1))),
        1 => Filter::And(children(rng)),
        _ => Filter::Or(children(rng)),
    }
}

struct Workload {
    rng: StdRng,
    centers: Vec<Vec<f32>>,
    model: BTreeMap<String, Doc>,
    deleted: BTreeSet<String>,
    next_id: usize,
}

impl Workload {
    fn new(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let centers = (0..6)
            .map(|_| {
                (0..DIMENSIONS)
                    .map(|_| rng.gen_range(-10.0..10.0))
                    .collect()
            })
            .collect();
        Self {
            rng,
            centers,
            model: BTreeMap::new(),
            deleted: BTreeSet::new(),
            next_id: 0,
        }
    }

    /// A point near one of the cluster centres, so centroids are meaningful
    /// and many points sit near boundaries.
    fn point(&mut self) -> Vec<f32> {
        let center = self.centers.choose(&mut self.rng).unwrap();
        center
            .iter()
            .map(|c| c + self.rng.gen_range(-3.0..3.0))
            .collect()
    }

    fn doc(&mut self) -> Doc {
        let vector = self.point();
        let category = self
            .rng
            .gen_bool(0.9)
            .then(|| CATEGORIES.choose(&mut self.rng).unwrap().to_string());
        let price = self.rng.gen_bool(0.8).then(|| self.rng.gen_range(0..100));
        Doc {
            vector,
            category,
            price,
        }
    }

    /// One batch of new docs, upserts of live docs and re-inserts of deleted
    /// ids, then deletes of live docs, then a flush.
    async fn apply_batch(&mut self, db: &VectorDb) {
        let mut writes = BTreeMap::new();
        for _ in 0..40 {
            let id = format!("doc-{:05}", self.next_id);
            self.next_id += 1;
            let doc = self.doc();
            writes.insert(id, doc);
        }
        let live: Vec<String> = self.model.keys().cloned().collect();
        let deleted: Vec<String> = self.deleted.iter().cloned().collect();
        let revived: Vec<String> = live
            .choose_multiple(&mut self.rng, live.len() / 10)
            .chain(deleted.choose_multiple(&mut self.rng, 3))
            .cloned()
            .collect();
        for id in revived {
            let doc = self.doc();
            self.deleted.remove(&id);
            writes.insert(id, doc);
        }
        let vectors = writes.iter().map(|(id, doc)| doc.to_vector(id)).collect();
        db.write(vectors).await.unwrap();
        self.model.extend(writes);

        let live: Vec<String> = self.model.keys().cloned().collect();
        let deletes: Vec<String> = live
            .choose_multiple(&mut self.rng, live.len() / 20)
            .cloned()
            .collect();
        if !deletes.is_empty() {
            db.delete(deletes.clone()).await.unwrap();
            for id in deletes {
                self.model.remove(&id);
                self.deleted.insert(id);
            }
        }
        db.flush().await.unwrap();
    }

    fn matching_ids(&self, filter: Option<&Filter>) -> BTreeSet<String> {
        self.model
            .iter()
            .filter(|(_, doc)| filter.is_none_or(|f| model_matches(f, doc)))
            .map(|(id, _)| id.clone())
            .collect()
    }

    fn distance(&self, metric: DistanceMetric, query: &[f32], id: &str) -> VectorDistance {
        let mut stored = self.model[id].vector.clone();
        metric.normalize_if_needed(&mut stored);
        compute_distance(query, &stored, metric)
    }

    /// Every matching doc's distance, best first.
    fn brute_force(
        &self,
        metric: DistanceMetric,
        query: &[f32],
        filter: Option<&Filter>,
    ) -> Vec<(VectorDistance, String)> {
        let mut query = query.to_vec();
        metric.normalize_if_needed(&mut query);
        let mut scored: Vec<(VectorDistance, String)> = self
            .matching_ids(filter)
            .into_iter()
            .map(|id| (self.distance(metric, &query, &id), id))
            .collect();
        scored.sort();
        scored
    }
}

/// Asserts `results` is a correct top `limit` of `expected` (all matches,
/// best first). Distinct docs can tie on an f32 distance and the engine breaks
/// ties by internal id, so this checks the score sequence and each returned
/// doc's true distance rather than one particular tie order.
fn assert_top_k(
    results: &[SearchResult],
    expected: &[(VectorDistance, String)],
    limit: usize,
    context: &str,
) {
    let truth: BTreeMap<&str, u32> = expected
        .iter()
        .map(|(d, id)| (id.as_str(), d.score().to_bits()))
        .collect();
    let actual_scores: Vec<u32> = results.iter().map(|r| r.score.to_bits()).collect();
    let expected_scores: Vec<u32> = expected
        .iter()
        .take(limit)
        .map(|(d, _)| d.score().to_bits())
        .collect();
    assert_eq!(actual_scores, expected_scores, "{context}: score sequence");
    let mut seen = BTreeSet::new();
    for result in results {
        let id = result.vector.id.as_str();
        assert!(seen.insert(id), "{context}: {id} duplicated");
        assert_eq!(
            truth.get(id),
            Some(&result.score.to_bits()),
            "{context}: {id} is not a match at its reported distance"
        );
    }
}

fn config(metric: DistanceMetric) -> Config {
    Config {
        storage: StorageConfig::InMemory,
        dimensions: DIMENSIONS as u16,
        distance_metric: metric,
        split_threshold_vectors: 30,
        merge_threshold_vectors: 8,
        split_search_neighbourhood: 8,
        metadata_fields: vec![
            MetadataFieldSpec::new("category", FieldType::String, true),
            MetadataFieldSpec::new("price", FieldType::Int64, true),
        ],
        ..Default::default()
    }
}

fn seeds() -> u64 {
    std::env::var("VECTOR_DIFF_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2)
}

async fn check_against_model(metric: DistanceMetric, seed: u64) {
    // given - a random workload mirrored into the model
    let db = VectorDb::open(config(metric)).await.unwrap();
    let mut workload = Workload::new(seed);
    for _ in 0..BATCHES {
        workload.apply_batch(&db).await;
    }
    let centroids = db.num_centroids();
    assert!(
        centroids > 1,
        "seed {seed}: workload should split, got {centroids} centroid(s)"
    );
    let engine = db.query_engine();
    let context = |what: &str, filter: &Option<Filter>| {
        format!("{metric:?} seed {seed} {what} filter {filter:?}")
    };

    // then - point reads return the last write, and nothing after a delete
    for (id, doc) in &workload.model {
        let stored = db
            .get(id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("seed {seed}: {id} missing"));
        let expected = doc.to_vector(id);
        for name in ["vector", "category", "price"] {
            assert_eq!(
                stored.attribute(name),
                expected.attribute(name),
                "seed {seed}: {id}.{name}"
            );
        }
    }
    for id in &workload.deleted {
        assert!(
            db.get(id).await.unwrap().is_none(),
            "seed {seed}: deleted {id} still readable"
        );
    }

    for _ in 0..QUERIES {
        let query_vector = workload.point();
        let filter = workload
            .rng
            .gen_bool(0.7)
            .then(|| random_filter(&mut workload.rng, 3));
        let limit = *[1usize, 5, 20, 1000].choose(&mut workload.rng).unwrap();
        let expected = workload.brute_force(metric, &query_vector, filter.as_ref());
        let mut query = Query::ann(query_vector.clone()).with_limit(limit);
        if let Some(f) = &filter {
            query = query.with_filter(f.clone());
        }

        // then - prepared filters select exactly the model's matches
        let prepared = engine.prepare_filter(filter.as_ref()).await.unwrap();
        let internal: Vec<u64> = engine
            .matching_ids(&prepared)
            .await
            .unwrap()
            .iter()
            .collect();
        let selected: BTreeSet<String> = engine
            .load_rows(&internal)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.id)
            .collect();
        assert_eq!(
            selected,
            workload.matching_ids(filter.as_ref()),
            "{}",
            context("filter", &filter)
        );

        // then - probing every centroid is exact
        let options = SearchOptions {
            nprobe: Some(centroids + 1),
            ..Default::default()
        };
        let ann = db.search_with_options(&query, options).await.unwrap();
        assert_top_k(&ann, &expected, limit, &context("ann all-probe", &filter));
        let exhaustive = db.search_exact_nprobe(&query, centroids + 1).await.unwrap();
        assert_top_k(
            &exhaustive,
            &expected,
            limit,
            &context("exhaustive nprobe", &filter),
        );

        // then - exact kNN over the filter's candidates is exact
        let candidates = engine.matching_ids(&prepared).await.unwrap();
        let knn = engine
            .exact_knn(&query_vector, &candidates, limit)
            .await
            .unwrap();
        assert_top_k(&knn, &expected, limit, &context("exact knn", &filter));

        // then - default ANN is sound even though recall is approximate
        let approx = db.search(&query).await.unwrap();
        let mut normalized = query_vector.clone();
        metric.normalize_if_needed(&mut normalized);
        let allowed = workload.matching_ids(filter.as_ref());
        let mut seen = BTreeSet::new();
        let mut previous: Option<VectorDistance> = None;
        assert!(
            approx.len() <= limit,
            "{}",
            context("approx limit", &filter)
        );
        for result in &approx {
            let id = &result.vector.id;
            assert!(
                allowed.contains(id),
                "{}: {id} fails filter",
                context("approx", &filter)
            );
            assert!(
                seen.insert(id.clone()),
                "{}: {id} duplicated",
                context("approx", &filter)
            );
            let truth = workload.distance(metric, &normalized, id);
            assert_eq!(
                result.score.to_bits(),
                truth.score().to_bits(),
                "{}: {id} score",
                context("approx", &filter)
            );
            assert!(
                previous.is_none_or(|p| p <= truth),
                "{}: unsorted",
                context("approx", &filter)
            );
            previous = Some(truth);
        }
    }
}

async fn check_all_seeds(metric: DistanceMetric, base: u64) {
    for seed in base..base + seeds() {
        check_against_model(metric, seed).await;
    }
}

#[tokio::test]
async fn should_match_brute_force_model_for_l2() {
    check_all_seeds(DistanceMetric::L2, 1_000).await;
}

#[tokio::test]
async fn should_match_brute_force_model_for_cosine() {
    check_all_seeds(DistanceMetric::Cosine, 2_000).await;
}

#[tokio::test]
async fn should_match_brute_force_model_for_dot_product() {
    check_all_seeds(DistanceMetric::DotProduct, 3_000).await;
}
