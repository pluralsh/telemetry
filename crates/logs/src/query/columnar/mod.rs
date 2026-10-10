//! The columnar query engine. Rows are read into per-stream batches whose
//! lines and metadata stay in one buffer; pipeline stages write labels,
//! lines and values into per-batch columns, and only what a query returns
//! is ever built as maps: range series and label groups are found by
//! hashing a row's labels in place, and log rows are materialized only for
//! candidates that can be among the first `limit`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{StreamExt, TryStreamExt};
use tokio::sync::oneshot;

use super::parallel::{BATCH_ROWS, panicked, pool};
use super::*;
use crate::db::SEGMENT_READ_CONCURRENCY;

mod batch;
mod sink;

pub(crate) use batch::{BatchSlice, ColumnBatch, StreamBatches, Tie};
use sink::{Outputs, Plan};

/// Reads one database, running the query's pipelines as pages decode.
pub(super) async fn load<'a>(
    database: &LogDb,
    namespace: &Namespace,
    plan: &ScanPlan<'a>,
    budget: &PageBudget,
    targets: Option<ScanTargets>,
) -> Result<ColumnarSink<'a>> {
    let (start, end) = (plan.scan_start, plan.request.end_ns);
    if plan.early_stop_log().is_some() {
        let mut sink = ColumnarSink::new(plan, BATCH_ROWS)?;
        database
            .read_segments_columns(
                namespace,
                (start, end),
                &plan.streams,
                budget,
                plan.direction == Direction::Backward,
                |slices| {
                    for slice in slices {
                        sink.push(slice)?;
                    }
                    Ok(
                        if sink.kept_logs(plan.direction, plan.limit)? >= plan.limit {
                            ControlFlow::Break(())
                        } else {
                            ControlFlow::Continue(())
                        },
                    )
                },
            )
            .await?;
        let mut sink = sink.finish().await?;
        sink.outputs.truncate_logs(plan.direction, plan.limit);
        return Ok(sink);
    }
    let targets = match targets {
        Some(targets) => targets,
        None => {
            database
                .scan_targets(namespace, start, end, &plan.streams)
                .await?
        }
    };
    if plan.lineless.is_none()
        && let Some(terms) = &plan.indexed_terms
    {
        let mut sink = ColumnarSink::new(plan, 0)?;
        let index_top_k = direct_index_top_k(plan.query, plan.limit);
        if let Some(rows) = database
            .read_match_bounded(namespace, &targets, (terms, index_top_k), budget.limit())
            .await?
        {
            let mut batches = StreamBatches::new(false);
            for (row, score) in rows {
                let score = score.to_string();
                let metadata = row
                    .entry
                    .structured_metadata
                    .iter()
                    .map(|field| (field.name.as_str(), field.value.as_str()))
                    .chain([(SCORE_METADATA_FIELD, score.as_str())]);
                batches.push(
                    &row.labels,
                    row.entry.timestamp_ns,
                    &row.entry.line,
                    metadata,
                    None,
                );
            }
            for slice in batches.take() {
                sink.push(slice)?;
            }
            return sink.finish().await;
        }
    }
    // Duplicates share a timestamp, so never span segments: each segment is
    // read into a sink of its own and the sinks merged in read order.
    let sinks = futures::stream::iter(targets.split())
        .map(|targets| async move {
            let mut sink = ColumnarSink::new(plan, 0)?;
            let consume =
                |slices: Vec<BatchSlice>| slices.into_iter().try_for_each(|slice| sink.push(slice));
            match &plan.lineless {
                Some(read) => {
                    database
                        .read_samples(namespace, targets, budget, read, consume)
                        .await?
                }
                None => {
                    database
                        .read_bounded_columns(namespace, targets, budget, consume)
                        .await?
                }
            }
            sink.finish().await
        })
        .buffered(SEGMENT_READ_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    let mut sinks = sinks.into_iter();
    let Some(mut merged) = sinks.next() else {
        return ColumnarSink::new(plan, 0);
    };
    for sink in sinks {
        merged.outputs.extend(sink.outputs);
    }
    Ok(merged)
}

/// Merges each database's outputs, in database order, and evaluates.
pub(super) fn evaluate(
    plan: &ScanPlan<'_>,
    sinks: Vec<ColumnarSink<'_>>,
    options: QueryOptions,
) -> Result<QueryResult> {
    let mut sinks = sinks.into_iter();
    let Some(mut merged) = sinks.next() else {
        return evaluate(plan, vec![ColumnarSink::new(plan, 0)?], options);
    };
    for (database, mut other) in (1..).zip(sinks) {
        other.outputs.set_database(database);
        merged.outputs.extend(other.outputs);
    }
    if matches!(plan.query.value, Expr::Log(_)) {
        let indexed = plan.indexed_terms.is_some();
        return finish_logs(merged.outputs.into_logs(), &options, indexed);
    }
    let rows = merged.outputs.finish(&merged.plan);
    super::evaluate(plan.query, rows, plan.request)
}

enum Job {
    Done(Outputs),
    Running(oneshot::Receiver<Result<Outputs>>),
}

/// Collects one database's batches and runs them through the query's
/// pipelines on the pool, merging results in read order so output matches
/// running them in sequence.
pub(super) struct ColumnarSink<'a> {
    plan: Plan<'a>,
    outputs: Outputs,
    shared: Option<Arc<(Query, QueryRequest)>>,
    preselect: Option<(usize, Direction)>,
    pending: Vec<BatchSlice>,
    pending_rows: usize,
    jobs: VecDeque<Job>,
    running: Arc<AtomicUsize>,
    /// Rows still run inline before batching starts: a limited log query
    /// that fills its limit early then stops reading exactly where it would
    /// in sequence.
    inline: usize,
}

impl<'a> ColumnarSink<'a> {
    fn new(plan: &ScanPlan<'a>, inline: usize) -> Result<Self> {
        let preselect = (plan.indexed_terms.is_none()).then_some((plan.limit, plan.direction));
        let pipelines = Plan::new(plan.query, plan.request, preselect)?;
        Ok(Self {
            outputs: pipelines.outputs(),
            plan: pipelines,
            shared: plan.shared.clone().filter(|_| pool().is_some()),
            preselect,
            pending: Vec::new(),
            pending_rows: 0,
            jobs: VecDeque::new(),
            running: Arc::default(),
            inline,
        })
    }

    fn push(&mut self, slice: BatchSlice) -> Result<()> {
        if self.shared.is_none() || self.inline > 0 {
            self.inline = self.inline.saturating_sub(slice.len());
            self.plan.run(&slice, &mut self.outputs)?;
            self.outputs.settle();
            return Ok(());
        }
        self.pending_rows += slice.len();
        self.pending.push(slice);
        if self.pending_rows >= BATCH_ROWS {
            self.dispatch()?;
        }
        Ok(())
    }

    /// Hands the pending batches to the pool, or runs them here once this
    /// sink has a job per pool thread in flight, which bounds the rows
    /// queued behind a slow pipeline.
    fn dispatch(&mut self) -> Result<()> {
        let (Some(shared), Some((pool, threads))) = (&self.shared, pool()) else {
            return Ok(());
        };
        let batches = std::mem::take(&mut self.pending);
        self.pending_rows = 0;
        if self.running.load(Ordering::Acquire) >= threads {
            let mut outputs = self.plan.outputs();
            for batch in &batches {
                self.plan.run(batch, &mut outputs)?;
            }
            self.jobs.push_back(Job::Done(outputs));
            return self.merge_ready();
        }
        let (sender, receiver) = oneshot::channel();
        let (shared, running, preselect) = (
            Arc::clone(shared),
            Arc::clone(&self.running),
            self.preselect,
        );
        running.fetch_add(1, Ordering::AcqRel);
        pool.spawn(move || {
            let outputs = run(&shared.0, &shared.1, preselect, &batches);
            running.fetch_sub(1, Ordering::AcqRel);
            let _ = sender.send(outputs);
        });
        self.jobs.push_back(Job::Running(receiver));
        // Finished jobs' outputs fold into this one now rather than
        // queueing until the read ends.
        self.merge_ready()
    }

    /// Merges the jobs that have finished ahead of any still running.
    fn merge_ready(&mut self) -> Result<()> {
        while let Some(job) = self.jobs.front_mut() {
            let outputs = match job {
                Job::Done(outputs) => std::mem::replace(outputs, Outputs::empty()),
                Job::Running(receiver) => match receiver.try_recv() {
                    Ok(outputs) => outputs?,
                    Err(oneshot::error::TryRecvError::Empty) => return Ok(()),
                    Err(oneshot::error::TryRecvError::Closed) => return Err(panicked()),
                },
            };
            self.jobs.pop_front();
            self.outputs.extend(outputs);
        }
        Ok(())
    }

    /// Log rows kept so far, once rows read up to now are merged where ready.
    fn kept_logs(&mut self, direction: Direction, limit: usize) -> Result<usize> {
        self.merge_ready()?;
        if self.outputs.kept() >= limit {
            // Duplicates must not count toward the limit.
            self.outputs.truncate_logs(direction, limit);
        }
        Ok(self.outputs.kept())
    }

    async fn finish(mut self) -> Result<Self> {
        if !self.pending.is_empty() {
            if self.jobs.is_empty() {
                for batch in std::mem::take(&mut self.pending) {
                    self.plan.run(&batch, &mut self.outputs)?;
                }
                self.outputs.settle();
            } else {
                self.dispatch()?;
            }
        }
        for job in std::mem::take(&mut self.jobs) {
            let outputs = match job {
                Job::Done(outputs) => outputs,
                Job::Running(receiver) => receiver.await.map_err(|_| panicked())??,
            };
            self.outputs.extend(outputs);
        }
        Ok(self)
    }
}

fn run(
    query: &Query,
    request: &QueryRequest,
    preselect: Option<(usize, Direction)>,
    batches: &[BatchSlice],
) -> Result<Outputs> {
    let plan = Plan::new(query, request, preselect)?;
    let mut outputs = plan.outputs();
    for batch in batches {
        plan.run(batch, &mut outputs)?;
    }
    Ok(outputs)
}
