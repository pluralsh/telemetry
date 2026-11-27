//! Evaluating parts of a query on the shards that hold the series, so the
//! coordinator receives partial results instead of every matched sample.
//!
//! See [`crate::pushdown`] for what is pushed down. A shard's read is
//! addressed by query text and step range alone, so any replica of its group
//! can answer it whatever its storage looks like, and a long step range is
//! cut into fragments that different replicas evaluate. Pushdown is skipped
//! whenever a routing-epoch cutover falls within the data the query reads,
//! because a series moved by a scale-up has samples on two shards, and each
//! would then aggregate only its part.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::future::try_join_all;
use sharding::ShardId;

use super::ShardedMetrics;
use crate::promql::memory::MemoryReservation;
use crate::promql::operators::precomputed::{PrecomputedRef, PrecomputedSeries};
use crate::remote::{
    histogram_from_wire, histogram_to_wire, labels_from_wire, labels_to_wire, shard_read,
};
use crate::tsdb::{
    DEFAULT_MEMORY_CAP_BYTES, duration_to_ms, execute_query_source, query_value_to_range_samples,
};
use crate::{Labels, Namespace, QueryError, RangeSample};
use proto::metrics::internal::v1 as pb;

/// A query rewritten to read shard-evaluated partials.
pub(super) struct PushedDown {
    pub query: String,
    pub partials: Vec<PrecomputedRef>,
}

impl ShardedMetrics {
    /// Turns pushdown on or off. Off by default.
    pub fn set_pushdown(&self, enabled: bool) {
        self.pushdown.store(enabled, Ordering::Relaxed);
    }

    /// Records the routing cutovers (nanoseconds) that moved series between
    /// shards. Until known, no query is pushed down.
    pub fn set_routing_cutovers(&self, cutovers_ns: Vec<i64>) {
        *self
            .cutovers_ms
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(
            cutovers_ns
                .into_iter()
                .map(|ns| ns.div_euclid(1_000_000))
                .collect(),
        );
    }

    /// Reads routing cutovers from `routing` on every query, so a scale-up
    /// is seen as soon as the router is. Returns false when already set.
    pub fn set_routing(&self, routing: Arc<tokio::sync::RwLock<sharding::ShardMap>>) -> bool {
        self.routing.set(routing).is_ok()
    }

    /// Evaluates the pushable parts of `query` on every shard, or `None` to
    /// run it whole over raw samples. `ranges` are the seconds ranges the
    /// whole query reads; `step_ms` is zero for an instant query.
    pub(super) async fn pushdown(
        &self,
        namespace: &Namespace,
        query: &str,
        (start_ms, end_ms, step_ms): (i64, i64, i64),
        ranges: &[(i64, i64)],
    ) -> Result<Option<PushedDown>, QueryError> {
        if !self.pushdown.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let Some(client) = self.remote.get() else {
            return Ok(None);
        };
        if !client.pushdown() || self.crosses_cutover(ranges).await {
            ::metrics::counter!("metrics_pushdown_total", "result" => "fallback").increment(1);
            return Ok(None);
        }
        let Some(plan) = crate::pushdown::plan(query) else {
            return Ok(None);
        };
        let queries = plan
            .queries()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let mut shards = self
            .shards
            .entries()
            .await
            .into_iter()
            .map(|(shard, _)| (shard, true))
            .collect::<Vec<_>>();
        shards.extend(self.shards.remote_shards().into_iter().map(|s| (s, false)));
        shards.sort_by_key(|(shard, _)| *shard);
        let per_shard = try_join_all(shards.iter().map(|&(shard, is_local)| {
            self.shard_partials(
                shard,
                is_local,
                namespace,
                &queries,
                (start_ms, end_ms, step_ms),
                ranges,
            )
        }))
        .await?;
        ::metrics::counter!("metrics_pushdown_total", "result" => "pushed").increment(1);
        let partials = (0..queries.len())
            .map(|index| {
                PrecomputedRef(Arc::new(PrecomputedSeries {
                    series: per_shard
                        .iter()
                        .flat_map(|shard| shard[index].iter().cloned())
                        .collect(),
                }))
            })
            .collect();
        Ok(Some(PushedDown {
            query: plan.query,
            partials,
        }))
    }

    /// Whether a cutover lies within the data `ranges` read, or cutovers
    /// are not known at all.
    async fn crosses_cutover(&self, ranges: &[(i64, i64)]) -> bool {
        let cutovers = match self.routing.get() {
            Some(routing) => Some(
                routing
                    .read()
                    .await
                    .epochs
                    .iter()
                    .skip(1)
                    .map(|epoch| epoch.effective_from_ns.div_euclid(1_000_000))
                    .collect::<Vec<_>>(),
            ),
            None => self
                .cutovers_ms
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        };
        let Some(cutovers) = cutovers else {
            return true;
        };
        let start = ranges.iter().map(|r| r.0).min().unwrap_or(i64::MIN);
        let end = ranges.iter().map(|r| r.1).max().unwrap_or(i64::MAX);
        let (start, end) = (start.saturating_mul(1000), end.saturating_mul(1000));
        // A moved series has samples on its old shard before the cutover and
        // on its new one from it, so any cutover inside the window counts.
        cutovers
            .iter()
            .any(|&cutover| cutover > start && cutover <= end)
    }

    /// One shard's partial series for each of `queries`.
    async fn shard_partials(
        &self,
        shard: ShardId,
        is_local: bool,
        namespace: &Namespace,
        queries: &[String],
        (start_ms, end_ms, step_ms): (i64, i64, i64),
        ranges: &[(i64, i64)],
    ) -> Result<Vec<Vec<RangeSample>>, QueryError> {
        let client = self.remote.get().expect("pushdown requires a shard client");
        let steps = if step_ms > 0 {
            u64::try_from((end_ms - start_ms) / step_ms + 1).unwrap_or(1)
        } else {
            1
        };
        let ways = if step_ms > 0 {
            client
                .split_ways(shard, steps, self.inflight.load(Ordering::Relaxed))
                .await
        } else {
            None
        };
        let request = |from_step: u64, to_step: u64| pb::EvaluateRequest {
            read: Some(shard_read(shard, namespace, ranges)),
            queries: queries.to_vec(),
            start_ms: start_ms + from_step as i64 * step_ms.max(0),
            end_ms: if step_ms > 0 {
                start_ms + (to_step as i64 - 1) * step_ms
            } else {
                end_ms
            },
            step_ms,
        };
        let requests = match ways {
            Some(ways) => {
                let ways = (ways as u64).min(steps);
                let per = steps.div_ceil(ways);
                (0..steps)
                    .step_by(per as usize)
                    .map(|from| request(from, (from + per).min(steps)))
                    .collect::<Vec<_>>()
            }
            None => vec![request(0, steps)],
        };
        let responses = if is_local && requests.len() == 1 {
            vec![self.serve_evaluate_local(&requests[0]).await?]
        } else {
            let run_local = |request: pb::EvaluateRequest| {
                use futures::FutureExt;
                async move { self.serve_evaluate_local(&request).await }.boxed()
            };
            let local: Option<crate::remote::LocalEvaluation<'_>> =
                is_local.then_some(&run_local as _);
            client.evaluate(requests, local).await?
        };
        let mut merged = vec![BTreeMap::<Labels, RangeSample>::new(); queries.len()];
        for response in responses {
            if response.results.len() != queries.len() {
                return Err(QueryError::Storage(
                    "shard reader returned the wrong number of results".into(),
                ));
            }
            for (slot, result) in merged.iter_mut().zip(response.results) {
                for series in result.series {
                    let labels = series.labels.map_or_else(Labels::empty, labels_from_wire);
                    let entry = slot.entry(labels.clone()).or_insert_with(|| RangeSample {
                        labels,
                        samples: Vec::new(),
                        histograms: Vec::new(),
                    });
                    entry
                        .samples
                        .extend(series.timestamps.into_iter().zip(series.values));
                    for histogram in series.histograms {
                        let sample = histogram_from_wire(histogram)
                            .map_err(|error| QueryError::Storage(error))?;
                        entry
                            .histograms
                            .push((sample.timestamp_ms, Arc::new(sample.histogram)));
                    }
                }
            }
        }
        Ok(merged
            .into_iter()
            .map(|slot| slot.into_values().collect())
            .collect())
    }

    /// Evaluates a request's queries over the local shard it names, in
    /// process, without taking a shard-read slot: the caller already holds
    /// one query slot or its own.
    async fn serve_evaluate_local(
        &self,
        request: &pb::EvaluateRequest,
    ) -> Result<pb::EvaluateResponse, QueryError> {
        let (shard, namespace, ranges) = self.served_read(request.read.clone()).await?;
        let results = self
            .evaluate_on_shard(
                shard,
                &namespace,
                &request.queries,
                (request.start_ms, request.end_ms, request.step_ms),
                &ranges,
            )
            .await?;
        Ok(pb::EvaluateResponse {
            results: results
                .into_iter()
                .map(|series| pb::EvaluatedQuery {
                    series: series
                        .into_iter()
                        .map(|sample| pb::EvaluatedSeries {
                            labels: Some(labels_to_wire(&sample.labels)),
                            timestamps: sample.samples.iter().map(|(t, _)| *t).collect(),
                            values: sample.samples.iter().map(|(_, v)| *v).collect(),
                            histograms: sample
                                .histograms
                                .iter()
                                .map(|(t, h)| histogram_to_wire(*t, h))
                                .collect(),
                        })
                        .collect(),
                })
                .collect(),
        })
    }

    /// Serves a coordinating reader's pushed-down evaluation of this
    /// reader's own shard.
    pub async fn serve_evaluate(
        &self,
        request: pb::EvaluateRequest,
    ) -> Result<pb::EvaluateResponse, QueryError> {
        let _permit = self.admit_shard_read().await?;
        self.serve_evaluate_local(&request).await
    }

    /// Each of `queries` evaluated over shard `shard`'s own series.
    async fn evaluate_on_shard(
        &self,
        shard: ShardId,
        namespace: &Namespace,
        queries: &[String],
        (start_ms, end_ms, step_ms): (i64, i64, i64),
        ranges: &[(i64, i64)],
    ) -> Result<Vec<Vec<RangeSample>>, QueryError> {
        let options = crate::QueryOptions::default();
        let lookback = duration_to_ms(options.lookback_delta);
        let source = Arc::new(self.local_source(shard, namespace, ranges).await?);
        let instant = step_ms == 0;
        let mut results = Vec::with_capacity(queries.len());
        for query in queries {
            let plan = self.tuned(if instant {
                crate::promql::plan::LoweringContext::for_instant(start_ms, lookback)
            } else {
                crate::promql::plan::LoweringContext::new(start_ms, end_ms, step_ms, lookback)
            });
            let outcome = execute_query_source(
                query,
                Arc::clone(&source),
                plan,
                instant,
                MemoryReservation::new(DEFAULT_MEMORY_CAP_BYTES),
            )
            .await?;
            results.push(query_value_to_range_samples(outcome.value)?);
        }
        Ok(results)
    }
}
