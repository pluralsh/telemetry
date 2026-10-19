// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

//! Cross-query cache of range metric query results.
//!
//! An entry holds one query's per-step results over a run of steps, keyed
//! by label set, with the write generation every shard reported for each
//! base segment those steps read. A later query on the same step grid
//! reuses the longest prefix of its steps whose segments still carry the
//! recorded generations and evaluates only the rest, so a dashboard that
//! refreshes recomputes only its newest steps.
//!
//! Reuse relies on a step's result depending only on the rows in its own
//! window, which [`crate::query::step_lookback`] checks. Retention expiry
//! changes no generation, so a cached step whose rows have since expired
//! is served until its entry goes idle.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use moka::future::Cache;
use sharding::ShardId;

use crate::Namespace;
use crate::query::{MatrixSeries, QueryRequest};

/// Inactive queries leave the cache, while frequently reused ones remain
/// resident until weighted capacity eviction. Generations catch every
/// change to the stored data.
const ENTRY_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ResultKey {
    namespace: Namespace,
    query: String,
    step_ns: i64,
    /// `start mod step`: queries share an entry only on the same step grid.
    phase_ns: i64,
}

/// A cacheable range query: its key and the base segments each step reads.
pub(crate) struct CachePlan {
    key: Arc<ResultKey>,
    lookback_ns: i64,
    segment_ns: i64,
}

impl CachePlan {
    /// `None` for queries the cache must not serve; see
    /// [`crate::query::step_lookback`].
    pub(crate) fn new(
        namespace: &Namespace,
        request: &QueryRequest,
        segment_ns: i64,
    ) -> Option<Self> {
        let step_ns = request.step_ns.filter(|step| *step > 0)?;
        let lookback_ns = crate::query::step_lookback(request)?;
        (segment_ns > 0).then(|| Self {
            key: Arc::new(ResultKey {
                namespace: namespace.clone(),
                query: request.query.trim().to_owned(),
                step_ns,
                phase_ns: request.start_ns.rem_euclid(step_ns),
            }),
            lookback_ns,
            segment_ns,
        })
    }

    pub(crate) fn step_ns(&self) -> i64 {
        self.key.step_ns
    }

    /// Starts of the base segments overlapping `[earliest, latest]`.
    fn starts(&self, earliest: i64, latest: i64) -> impl Iterator<Item = i64> + use<> {
        let segment_ns = self.segment_ns;
        let first = earliest.div_euclid(segment_ns);
        let last = latest.div_euclid(segment_ns);
        (first..=last).map(move |index| index.saturating_mul(segment_ns))
    }

    /// The base segments step `t` reads.
    fn step_starts(&self, t: i64) -> impl Iterator<Item = i64> + use<> {
        self.starts(t.saturating_sub(self.lookback_ns), t)
    }

    /// Every base segment read by a step in `first..=last`.
    pub(crate) fn segment_starts(&self, first: i64, last: i64) -> Vec<i64> {
        self.starts(first.saturating_sub(self.lookback_ns), last)
            .collect()
    }
}

/// Generations of a set of base segments on every shard a query reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Generations {
    shards: Vec<ShardId>,
    /// Per segment start, the generation on each of `shards`, in order.
    segments: HashMap<i64, Vec<Option<u64>>>,
}

impl Generations {
    /// From per-shard generation lists aligned with `starts`.
    pub(crate) fn new(starts: &[i64], per_shard: Vec<(ShardId, Vec<Option<u64>>)>) -> Self {
        let shards = per_shard.iter().map(|(id, _)| *id).collect();
        let segments = starts
            .iter()
            .enumerate()
            .map(|(index, start)| {
                let generations = per_shard.iter().map(|(_, values)| values[index]).collect();
                (*start, generations)
            })
            .collect();
        Self { shards, segments }
    }

    fn retain(&mut self, starts: &[i64]) {
        let keep: HashSet<_> = starts.iter().collect();
        self.segments.retain(|start, _| keep.contains(start));
    }
}

struct ResultEntry {
    first_ns: i64,
    last_ns: i64,
    series: Vec<MatrixSeries>,
    generations: Generations,
}

pub(crate) struct ResultCache {
    entries: Cache<Arc<ResultKey>, Arc<ResultEntry>>,
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
                    let bytes = key.query.len() + entry_bytes(entry);
                    u32::try_from(bytes).unwrap_or(u32::MAX)
                })
                .build(),
        }
    }

    /// The longest run of steps `start, start + step, ...` (up to
    /// `last_ns`) whose cached results are still valid under `current`.
    pub(crate) async fn lookup(
        &self,
        plan: &CachePlan,
        start_ns: i64,
        last_ns: i64,
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
            || start_ns < entry.first_ns
            || start_ns > entry.last_ns
        {
            return miss;
        }
        let mut valid: HashMap<i64, bool> = HashMap::new();
        let mut reused_steps = 0;
        let mut t = start_ns;
        while t <= last_ns.min(entry.last_ns) {
            let unchanged = plan.step_starts(t).all(|start| {
                *valid.entry(start).or_insert_with(|| {
                    let cached = entry.generations.segments.get(&start);
                    cached.is_some() && cached == current.segments.get(&start)
                })
            });
            if !unchanged {
                break;
            }
            reused_steps += 1;
            t = t.saturating_add(plan.step_ns());
        }
        Lookup {
            entry: Some(entry),
            reused_steps,
        }
    }

    /// The reused steps of `lookup`, from `start_ns` through `through_ns`.
    pub(crate) fn reused(lookup: &Lookup, start_ns: i64, through_ns: i64) -> Vec<MatrixSeries> {
        match &lookup.entry {
            Some(entry) if lookup.reused_steps > 0 => entry
                .series
                .iter()
                .filter_map(|series| slice(series, start_ns, through_ns))
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Records the steps of `series` from `first_ns` through `last_ns`.
    pub(crate) async fn insert(
        &self,
        plan: &CachePlan,
        first_ns: i64,
        last_ns: i64,
        series: &[MatrixSeries],
        mut generations: Generations,
    ) {
        generations.retain(&plan.segment_starts(first_ns, last_ns));
        let series = series
            .iter()
            .filter_map(|series| slice(series, first_ns, last_ns))
            .collect();
        let entry = ResultEntry {
            first_ns,
            last_ns,
            series,
            generations,
        };
        self.entries
            .insert(Arc::clone(&plan.key), Arc::new(entry))
            .await;
    }

    pub(crate) fn record(reused: usize, computed: usize) {
        ::metrics::counter!("logs_result_cache_steps_total", "outcome" => "reused")
            .increment(reused as u64);
        ::metrics::counter!("logs_result_cache_steps_total", "outcome" => "computed")
            .increment(computed as u64);
    }
}

/// `series` restricted to steps in `[from_ns, through_ns]`, or `None` when
/// none remain, since range results omit series without points.
fn slice(series: &MatrixSeries, from_ns: i64, through_ns: i64) -> Option<MatrixSeries> {
    let begin = series
        .samples
        .partition_point(|sample| sample.timestamp_ns < from_ns);
    let end = series
        .samples
        .partition_point(|sample| sample.timestamp_ns <= through_ns);
    let samples = &series.samples[begin..end.max(begin)];
    (!samples.is_empty()).then(|| MatrixSeries {
        labels: series.labels.clone(),
        samples: samples.to_vec(),
    })
}

/// Joins results for consecutive step runs by label set, sorted by labels
/// as an evaluated matrix is. `head` covers steps strictly before `tail`'s.
pub(crate) fn merge(head: Vec<MatrixSeries>, tail: Vec<MatrixSeries>) -> Vec<MatrixSeries> {
    let mut merged = head;
    let mut index: HashMap<crate::Labels, usize> = merged
        .iter()
        .enumerate()
        .map(|(position, series)| (series.labels.clone(), position))
        .collect();
    for series in tail {
        match index.get(&series.labels) {
            Some(&position) => merged[position].samples.extend(series.samples),
            None => {
                index.insert(series.labels.clone(), merged.len());
                merged.push(series);
            }
        }
    }
    merged.sort_by(|left, right| left.labels.cmp(&right.labels));
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
            64 + labels + series.samples.len() * 16
        })
        .sum();
    let generations = entry.generations.segments.len() * (32 + entry.generations.shards.len() * 16);
    128 + series + generations
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Sample;
    use crate::{Label, Labels};

    const MINUTE: i64 = 60_000_000_000;
    const HOUR: i64 = 60 * MINUTE;

    fn series(app: &str, points: &[(i64, f64)]) -> MatrixSeries {
        MatrixSeries {
            labels: Labels::new(vec![Label::new("app", app)]).unwrap(),
            samples: points
                .iter()
                .map(|&(timestamp_ns, value)| Sample {
                    timestamp_ns,
                    value,
                })
                .collect(),
        }
    }

    fn plan(query: &str, start_ns: i64, step_ns: i64) -> Option<CachePlan> {
        CachePlan::new(
            &Namespace::new("tenant").unwrap(),
            &QueryRequest::range(query, start_ns, start_ns + HOUR, step_ns),
            HOUR,
        )
    }

    #[test]
    fn caches_only_range_metric_queries_on_one_grid() {
        let count = r#"sum(count_over_time({app="a"}[5m]))"#;
        assert!(plan(r#"{app="a"}"#, 0, MINUTE).is_none());
        assert!(
            CachePlan::new(
                &Namespace::new("tenant").unwrap(),
                &QueryRequest::instant(count, HOUR),
                HOUR
            )
            .is_none()
        );
        let a = plan(count, 0, MINUTE).unwrap();
        let shifted = plan(count, 1, MINUTE).unwrap();
        assert_eq!(a.key, plan(count, 2 * MINUTE, MINUTE).unwrap().key);
        assert_ne!(a.key, shifted.key);
        // A 5m window reaches back into the previous hour.
        assert_eq!(
            a.step_starts(HOUR + MINUTE).collect::<Vec<_>>(),
            vec![0, HOUR]
        );
        assert_eq!(
            a.step_starts(HOUR + 6 * MINUTE).collect::<Vec<_>>(),
            vec![HOUR]
        );
    }

    #[tokio::test]
    async fn reuses_only_steps_whose_segments_are_unchanged() {
        // Steps every 30m over hours 0..3, each reading its 5m window.
        let plan = plan(r#"count_over_time({app="a"}[5m])"#, 0, 30 * MINUTE).unwrap();
        let cache = ResultCache::new(1 << 20);
        let shard = ShardId::new(0);
        let last = 5 * 30 * MINUTE;
        let starts = plan.segment_starts(0, last);
        let generations = |values: &[u64]| {
            Generations::new(
                &starts,
                vec![(shard, values.iter().map(|v| Some(*v)).collect())],
            )
        };
        let points: Vec<_> = (0..=5).map(|i| (i * 30 * MINUTE, i as f64)).collect();
        cache
            .insert(
                &plan,
                0,
                last,
                &[series("a", &points)],
                generations(&[1, 1, 1, 1]),
            )
            .await;

        // Segments are hours -1..=2. Hour 1 changes: the steps at 0 and 30m
        // read no later than hour 0; the step at 1h reads hours 0 and 1.
        let lookup = cache
            .lookup(&plan, 0, last, &generations(&[1, 1, 2, 1]))
            .await;
        assert_eq!(lookup.reused_steps, 2);
        let reused = ResultCache::reused(&lookup, 0, 30 * MINUTE);
        assert_eq!(reused, vec![series("a", &points[..2])]);

        // A different shard set reuses nothing.
        let other = Generations::new(&starts, vec![(ShardId::new(1), vec![Some(1); 4])]);
        assert_eq!(cache.lookup(&plan, 0, last, &other).await.reused_steps, 0);
    }

    #[test]
    fn merges_by_label_set_in_label_order() {
        let head = vec![series("b", &[(1, 1.0)]), series("a", &[(1, 2.0)])];
        let tail = vec![series("c", &[(2, 3.0)]), series("a", &[(2, 4.0)])];
        assert_eq!(
            merge(head, tail),
            vec![
                series("a", &[(1, 2.0), (2, 4.0)]),
                series("b", &[(1, 1.0)]),
                series("c", &[(2, 3.0)]),
            ]
        );
    }
}
