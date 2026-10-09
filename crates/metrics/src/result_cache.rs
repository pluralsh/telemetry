//! Cross-query cache of range-query results.
//!
//! An entry holds one expression's per-step results over a run of steps,
//! keyed by label set, together with the write generation every shard
//! reported for each bucket those steps read. A later query on the same
//! step grid reuses the longest prefix of its steps whose buckets still
//! carry the recorded generations and evaluates only the rest.
//!
//! Reuse relies on a step's result depending only on its own time and the
//! data it reads: there is no `@` (excluded at planning), and subqueries
//! step on absolute multiples of their own step.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use moka::future::Cache;
use sharding::ShardId;

use crate::model::{Labels, RangeSample, TimeBucket};
use crate::tsdb::{duration_to_ms, step_read_offsets};
use crate::{Namespace, tsdb_metrics};

const HOUR_MS: i64 = 3_600_000;
/// Inactive expressions leave the cache, while frequently reused expressions
/// remain resident until weighted capacity eviction. Generations catch every
/// change to the stored data.
const ENTRY_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ResultKey {
    namespace: Namespace,
    expression: String,
    step_ms: i64,
    /// `start mod step`: queries share an entry only on the same step grid.
    phase_ms: i64,
    lookback_ms: i64,
}

/// A cacheable range query: its key and the windows each step reads.
pub(crate) struct CachePlan {
    pub(crate) key: Arc<ResultKey>,
    offsets: Vec<(i64, i64)>,
}

impl CachePlan {
    /// `None` for queries the cache must not serve: unparsable (the
    /// evaluator reports the error) or using `@`.
    pub(crate) fn new(
        namespace: &Namespace,
        query: &str,
        start_ms: i64,
        step_ms: i64,
        lookback: Duration,
    ) -> Option<Self> {
        let expr = promql_parser::parser::parse(query).ok()?;
        let offsets = step_read_offsets(&expr, lookback)?;
        Some(Self {
            key: Arc::new(ResultKey {
                namespace: namespace.clone(),
                expression: canonical_expression(query, &expr),
                step_ms,
                phase_ms: start_ms.rem_euclid(step_ms),
                lookback_ms: duration_to_ms(lookback),
            }),
            offsets,
        })
    }

    /// The buckets step `t` reads.
    fn step_buckets(&self, t: i64) -> impl Iterator<Item = TimeBucket> + '_ {
        self.offsets
            .iter()
            .flat_map(move |&(lo, hi)| hour_buckets(t.saturating_add(lo), t.saturating_add(hi)))
    }

    /// Every bucket read by a step in `first..=last`.
    pub(crate) fn buckets(&self, first: i64, last: i64) -> Vec<TimeBucket> {
        let mut buckets: Vec<TimeBucket> = self
            .offsets
            .iter()
            .flat_map(|&(lo, hi)| hour_buckets(first.saturating_add(lo), last.saturating_add(hi)))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        buckets.sort_unstable_by_key(|bucket| bucket.start);
        buckets
    }
}

/// The printed form of `expr` when it parses back to the same tree, so
/// spacing and redundant parentheses do not split entries; else the query.
fn canonical_expression(query: &str, expr: &promql_parser::parser::Expr) -> String {
    let printed = expr.to_string();
    match promql_parser::parser::parse(&printed) {
        Ok(reparsed) if reparsed == *expr => printed,
        _ => query.trim().to_string(),
    }
}

/// The hour buckets overlapping `[earliest_ms, latest_ms]`.
fn hour_buckets(earliest_ms: i64, latest_ms: i64) -> impl Iterator<Item = TimeBucket> {
    let first = earliest_ms.max(0).div_euclid(HOUR_MS);
    let last = latest_ms.div_euclid(HOUR_MS);
    (first..=last).filter_map(|hour| u32::try_from(hour * 60).ok().map(TimeBucket::hour))
}

/// Generations of a set of buckets on every open shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Generations {
    shards: Vec<ShardId>,
    /// Per bucket, the generation on each of `shards`, in order.
    buckets: HashMap<TimeBucket, Vec<Option<u64>>>,
}

impl Generations {
    /// From per-shard generation lists aligned with `buckets`.
    pub(crate) fn new(buckets: &[TimeBucket], per_shard: Vec<(ShardId, Vec<Option<u64>>)>) -> Self {
        let shards = per_shard.iter().map(|(id, _)| *id).collect();
        let buckets = buckets
            .iter()
            .enumerate()
            .map(|(index, bucket)| {
                let generations = per_shard.iter().map(|(_, values)| values[index]).collect();
                (*bucket, generations)
            })
            .collect();
        Self { shards, buckets }
    }

    fn retain(&mut self, buckets: &[TimeBucket]) {
        let keep: HashSet<_> = buckets.iter().collect();
        self.buckets.retain(|bucket, _| keep.contains(bucket));
    }
}

pub(crate) struct ResultEntry {
    first_ms: i64,
    last_ms: i64,
    series: Vec<RangeSample>,
    generations: Generations,
}

pub(crate) struct ResultCache {
    entries: Cache<Arc<ResultKey>, Arc<ResultEntry>>,
    reused_steps: AtomicU64,
    computed_steps: AtomicU64,
}

/// How a request splits into reused and evaluated steps.
pub(crate) struct Lookup {
    entry: Option<Arc<ResultEntry>>,
    /// Leading requested steps served from `entry`.
    pub(crate) reused_steps: usize,
}

impl ResultCache {
    pub(crate) fn new(capacity_bytes: u64) -> Self {
        Self {
            entries: Cache::builder()
                .max_capacity(capacity_bytes)
                .time_to_idle(ENTRY_IDLE_TIMEOUT)
                .weigher(|key: &Arc<ResultKey>, entry: &Arc<ResultEntry>| {
                    let bytes = key.expression.len() + entry_bytes(entry);
                    u32::try_from(bytes).unwrap_or(u32::MAX)
                })
                .build(),
            reused_steps: AtomicU64::new(0),
            computed_steps: AtomicU64::new(0),
        }
    }

    /// The longest run of steps `start, start + step, ...` (up to
    /// `last_ms`) whose cached results are still valid under `current`.
    pub(crate) async fn lookup(
        &self,
        plan: &CachePlan,
        start_ms: i64,
        last_ms: i64,
        current: &Generations,
    ) -> Lookup {
        let miss = Lookup {
            entry: None,
            reused_steps: 0,
        };
        let Some(entry) = self.entries.get(&plan.key).await else {
            return miss;
        };
        if entry.generations.shards != current.shards
            || start_ms < entry.first_ms
            || start_ms > entry.last_ms
        {
            return miss;
        }
        let step_ms = plan.key.step_ms;
        let mut valid: HashMap<TimeBucket, bool> = HashMap::new();
        let mut reused_steps = 0;
        let mut t = start_ms;
        while t <= last_ms.min(entry.last_ms) {
            let unchanged = plan.step_buckets(t).all(|bucket| {
                *valid.entry(bucket).or_insert_with(|| {
                    let cached = entry.generations.buckets.get(&bucket);
                    cached.is_some() && cached == current.buckets.get(&bucket)
                })
            });
            if !unchanged {
                break;
            }
            reused_steps += 1;
            t += step_ms;
        }
        Lookup {
            entry: Some(entry),
            reused_steps,
        }
    }

    /// The reused steps of `lookup`, from `start_ms` through `through_ms`.
    pub(crate) fn reused(lookup: &Lookup, start_ms: i64, through_ms: i64) -> Vec<RangeSample> {
        match &lookup.entry {
            Some(entry) if lookup.reused_steps > 0 => entry
                .series
                .iter()
                .filter_map(|series| slice(series, start_ms, through_ms))
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Records the steps of `series` from `first_ms` through `last_ms`.
    pub(crate) async fn insert(
        &self,
        plan: &CachePlan,
        first_ms: i64,
        last_ms: i64,
        series: &[RangeSample],
        mut generations: Generations,
    ) {
        generations.retain(&plan.buckets(first_ms, last_ms));
        let series = series
            .iter()
            .filter_map(|series| slice(series, first_ms, last_ms))
            .collect();
        let entry = ResultEntry {
            first_ms,
            last_ms,
            series,
            generations,
        };
        self.entries
            .insert(Arc::clone(&plan.key), Arc::new(entry))
            .await;
    }

    pub(crate) fn record(&self, reused: usize, computed: usize) {
        self.reused_steps
            .fetch_add(reused as u64, Ordering::Relaxed);
        self.computed_steps
            .fetch_add(computed as u64, Ordering::Relaxed);
        ::metrics::counter!(tsdb_metrics::TSDB_RESULT_CACHE_STEPS, "outcome" => "reused")
            .increment(reused as u64);
        ::metrics::counter!(tsdb_metrics::TSDB_RESULT_CACHE_STEPS, "outcome" => "computed")
            .increment(computed as u64);
    }

    /// `(reused, computed)` steps since the cache was created.
    #[cfg(test)]
    pub(crate) fn step_counts(&self) -> (u64, u64) {
        (
            self.reused_steps.load(Ordering::Relaxed),
            self.computed_steps.load(Ordering::Relaxed),
        )
    }
}

/// `series` restricted to steps in `[from_ms, through_ms]`, or `None` when
/// none remain, since range results omit series without points.
fn slice(series: &RangeSample, from_ms: i64, through_ms: i64) -> Option<RangeSample> {
    let samples = window(&series.samples, from_ms, through_ms, |(t, _)| *t);
    let histograms = window(&series.histograms, from_ms, through_ms, |(t, _)| *t);
    if samples.is_empty() && histograms.is_empty() {
        return None;
    }
    Some(RangeSample {
        labels: series.labels.clone(),
        samples: samples.to_vec(),
        histograms: histograms.to_vec(),
    })
}

fn window<T>(points: &[T], from_ms: i64, through_ms: i64, time: impl Fn(&T) -> i64) -> &[T] {
    let begin = points.partition_point(|point| time(point) < from_ms);
    let end = points.partition_point(|point| time(point) <= through_ms);
    &points[begin..end.max(begin)]
}

/// Whether two series of `series` share a label set, which makes a merge by
/// label set ambiguous.
pub(crate) fn has_duplicate_labels(series: &[RangeSample]) -> bool {
    let mut seen = HashSet::with_capacity(series.len());
    !series.iter().all(|series| seen.insert(&series.labels))
}

/// Joins results for consecutive step runs by label set. `head` covers
/// steps strictly before `tail`'s; series keep `head`'s order, followed by
/// series that only `tail` has, in its order.
pub(crate) fn merge(head: Vec<RangeSample>, tail: Vec<RangeSample>) -> Vec<RangeSample> {
    let mut merged = head;
    let mut index: HashMap<Labels, usize> = merged
        .iter()
        .enumerate()
        .map(|(position, series)| (series.labels.clone(), position))
        .collect();
    for series in tail {
        match index.get(&series.labels) {
            Some(&position) => {
                let target = &mut merged[position];
                target.samples.extend(series.samples);
                target.histograms.extend(series.histograms);
            }
            None => {
                index.insert(series.labels.clone(), merged.len());
                merged.push(series);
            }
        }
    }
    merged
}

fn entry_bytes(entry: &ResultEntry) -> usize {
    let series: usize = entry
        .series
        .iter()
        .map(|series| {
            let labels: usize = series
                .labels
                .iter()
                .map(|label| label.name.len() + label.value.len() + 48)
                .sum();
            let histograms: usize = series
                .histograms
                .iter()
                .map(|(_, histogram)| {
                    128 + (histogram.positive.len() + histogram.negative.len()) * 16
                        + histogram.custom_values.len() * 8
                })
                .sum();
            64 + labels + series.samples.len() * 16 + histograms
        })
        .sum();
    let generations = entry.generations.buckets.len() * (32 + entry.generations.shards.len() * 16);
    128 + series + generations
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Label;

    fn series(name: &str, points: &[(i64, f64)]) -> RangeSample {
        RangeSample {
            labels: Labels::new(vec![Label::metric_name(name)]),
            samples: points.to_vec(),
            histograms: Vec::new(),
        }
    }

    fn plan(query: &str, start_ms: i64, step_ms: i64) -> CachePlan {
        CachePlan::new(
            &Namespace::default(),
            query,
            start_ms,
            step_ms,
            Duration::from_secs(300),
        )
        .expect("cacheable")
    }

    #[test]
    fn should_share_keys_across_formatting_and_split_them_by_grid() {
        let a = plan("sum by (job) (rate(x[5m]))", 60_000, 15_000);
        let b = plan("sum  by(job)(rate( x[5m] ))", 75_000, 15_000);
        let shifted = plan("sum by (job) (rate(x[5m]))", 61_000, 15_000);
        assert_eq!(a.key, b.key);
        assert_ne!(a.key, shifted.key);
    }

    #[test]
    fn should_not_plan_queries_using_at_or_failing_to_parse() {
        let ns = Namespace::default();
        let lookback = Duration::from_secs(300);
        assert!(CachePlan::new(&ns, "x @ 100", 0, 1_000, lookback).is_none());
        assert!(CachePlan::new(&ns, "rate(x[5m] @ end())", 0, 1_000, lookback).is_none());
        assert!(
            CachePlan::new(&ns, "max_over_time(x[5m:1m] @ start())", 0, 1_000, lookback).is_none()
        );
        assert!(CachePlan::new(&ns, "sum(", 0, 1_000, lookback).is_none());
    }

    #[test]
    fn should_map_steps_to_the_buckets_their_windows_read() {
        // given: a 10m window offset by 1h
        let plan = plan("rate(x[10m] offset 1h)", 0, 60_000);
        let t = 2 * HOUR_MS + 5 * 60_000;

        // when
        let buckets: Vec<_> = plan.step_buckets(t).collect();

        // then: (t - 1h - 10m, t - 1h] spans hours 0 and 1
        assert_eq!(buckets, vec![TimeBucket::hour(0), TimeBucket::hour(60)]);
        assert!(plan_without_selectors().step_buckets(t).next().is_none());
    }

    fn plan_without_selectors() -> CachePlan {
        plan("vector(time())", 0, 60_000)
    }

    #[test]
    fn should_merge_by_label_set_keeping_head_order() {
        let head = vec![series("b", &[(1, 1.0)]), series("a", &[(1, 2.0)])];
        let tail = vec![
            series("c", &[(2, 3.0)]),
            series("a", &[(2, 4.0)]),
            series("b", &[(2, 5.0)]),
        ];
        let merged = merge(head, tail);
        let names: Vec<_> = merged
            .iter()
            .map(|s| s.labels.get("__name__").unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["b", "a", "c"]);
        assert_eq!(merged[1].samples, vec![(1, 2.0), (2, 4.0)]);
        assert!(!has_duplicate_labels(&merged));
        assert!(has_duplicate_labels(&[series("a", &[]), series("a", &[])]));
    }

    #[tokio::test]
    async fn should_reuse_only_steps_whose_buckets_are_unchanged() {
        // given: steps every 30m over hours 0..3, each reading its own hour
        let plan = plan("x", 0, 30 * 60_000);
        let cache = ResultCache::new(1 << 20);
        let shard = ShardId::new(0);
        let last = 5 * 30 * 60_000;
        let buckets = plan.buckets(0, last);
        let generations = |values: &[u64]| {
            Generations::new(
                &buckets,
                vec![(shard, values.iter().map(|v| Some(*v)).collect())],
            )
        };
        let points: Vec<_> = (0..=5).map(|i| (i * 30 * 60_000, i as f64)).collect();
        cache
            .insert(
                &plan,
                0,
                last,
                &[series("x", &points)],
                generations(&[1, 1, 1]),
            )
            .await;

        // when: hour 1 changes
        let lookup = cache.lookup(&plan, 0, last, &generations(&[1, 2, 1])).await;

        // then: steps at 0 and 30m read hour 0 only (5m lookback); the step
        // at 1h reads hours 0 and 1
        assert_eq!(lookup.reused_steps, 2);
        let reused = ResultCache::reused(&lookup, 0, 30 * 60_000);
        assert_eq!(reused[0].samples, points[..2].to_vec());

        // and: a different shard set reuses nothing
        let other = Generations::new(&buckets, vec![(ShardId::new(1), vec![Some(1); 3])]);
        assert_eq!(cache.lookup(&plan, 0, last, &other).await.reused_steps, 0);
    }
}
