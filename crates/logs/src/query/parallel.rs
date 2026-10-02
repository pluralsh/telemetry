//! Runs log pipelines on a pool sized to the CPUs this process may use, so
//! parsing overlaps reading and one database's rows spread across cores.

use std::collections::VecDeque;
use std::num::NonZero;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use tokio::sync::oneshot;

use super::{Direction, PipelineSink, Query, QueryRequest, Row};
use crate::{Error, Result};

/// Rows per pipeline job: enough that dispatch is noise next to parsing,
/// few enough that a database's rows spread across the pool.
const BATCH_ROWS: usize = 2048;

struct Pool {
    threads: usize,
    pool: rayon::ThreadPool,
}

/// `None` with a single usable CPU, where batching only adds overhead.
/// `available_parallelism` honours CPU affinity and cgroup quotas.
static POOL: LazyLock<Option<Pool>> = LazyLock::new(|| {
    let threads = std::thread::available_parallelism().map_or(1, NonZero::get);
    if threads < 2 {
        return None;
    }
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|index| format!("logs-pipeline-{index}"))
        // A panicking job drops its sender, which fails its query instead of
        // aborting the process.
        .panic_handler(|_| {})
        .build()
        .ok()
        .map(|pool| Pool { threads, pool })
});

pub(super) fn enabled() -> bool {
    POOL.is_some()
}

type Outputs = Vec<Vec<Row>>;

enum Batch {
    Done(Outputs),
    Running(oneshot::Receiver<Result<Outputs>>),
}

fn panicked() -> Error {
    Error::Query("log pipeline worker panicked".into())
}

/// Collects one database's rows and runs them through the query's pipelines
/// in batches, merging results in read order so output matches running them
/// in sequence.
pub(super) struct ParallelSink<'a> {
    sink: PipelineSink<'a>,
    shared: Option<Arc<(Query, QueryRequest)>>,
    rows: Vec<Row>,
    batches: VecDeque<Batch>,
    running: Arc<AtomicUsize>,
    /// Rows still run inline before batching starts.
    inline: usize,
}

impl<'a> ParallelSink<'a> {
    pub(super) fn new(sink: PipelineSink<'a>, shared: Option<Arc<(Query, QueryRequest)>>) -> Self {
        Self {
            sink,
            shared: shared.filter(|_| enabled()),
            rows: Vec::new(),
            batches: VecDeque::new(),
            running: Arc::default(),
            inline: 0,
        }
    }

    /// For a limited log query: its first batch of rows runs inline, so one
    /// that fills its limit early stops reading exactly where it would in
    /// sequence; only longer scans are batched.
    pub(super) fn for_logs(
        sink: PipelineSink<'a>,
        shared: Option<Arc<(Query, QueryRequest)>>,
    ) -> Self {
        Self {
            inline: BATCH_ROWS,
            ..Self::new(sink, shared)
        }
    }

    pub(super) fn push(&mut self, row: Row) -> Result<()> {
        if self.shared.is_none() {
            return self.sink.push(row);
        }
        if self.inline > 0 {
            self.inline -= 1;
            return self.sink.push(row);
        }
        self.rows.push(row);
        if self.rows.len() >= BATCH_ROWS {
            self.dispatch()?;
        }
        Ok(())
    }

    /// Hands the pending rows to the pool, or runs them here once this sink
    /// has a job per pool thread in flight, which bounds the rows queued
    /// behind a slow pipeline.
    fn dispatch(&mut self) -> Result<()> {
        let (Some(shared), Some(pool)) = (&self.shared, POOL.as_ref()) else {
            return Ok(());
        };
        let rows = std::mem::replace(&mut self.rows, Vec::with_capacity(BATCH_ROWS));
        if self.running.load(Ordering::Acquire) >= pool.threads {
            let outputs = run(&shared.0, &shared.1, rows)?;
            self.batches.push_back(Batch::Done(outputs));
            return Ok(());
        }
        let (sender, receiver) = oneshot::channel();
        let (shared, running) = (Arc::clone(shared), Arc::clone(&self.running));
        running.fetch_add(1, Ordering::AcqRel);
        pool.pool.spawn(move || {
            let outputs = run(&shared.0, &shared.1, rows);
            running.fetch_sub(1, Ordering::AcqRel);
            let _ = sender.send(outputs);
        });
        self.batches.push_back(Batch::Running(receiver));
        Ok(())
    }

    /// Merges the batches that have finished ahead of any still running.
    fn merge_ready(&mut self) -> Result<()> {
        while let Some(batch) = self.batches.front_mut() {
            let outputs = match batch {
                Batch::Done(outputs) => std::mem::take(outputs),
                Batch::Running(receiver) => match receiver.try_recv() {
                    Ok(outputs) => outputs?,
                    Err(oneshot::error::TryRecvError::Empty) => return Ok(()),
                    Err(oneshot::error::TryRecvError::Closed) => return Err(panicked()),
                },
            };
            self.batches.pop_front();
            self.sink.extend_outputs(outputs);
        }
        Ok(())
    }

    /// Log rows kept so far, once rows read up to now are merged where ready;
    /// the rows a log query needs arrive in order, so this only grows.
    pub(super) fn kept_logs(&mut self, direction: Direction, limit: usize) -> Result<usize> {
        self.merge_ready()?;
        if self.sink.kept() >= limit {
            // Duplicates must not count toward the limit.
            self.sink.truncate_logs(direction, limit);
        }
        Ok(self.sink.kept())
    }

    pub(super) async fn finish(self) -> Result<PipelineSink<'a>> {
        self.finish_until(|_| false).await
    }

    /// Like [`Self::finish`], but stops merging once a log query has its
    /// first `limit` rows; later batches only hold rows past them.
    pub(super) async fn finish_logs(
        self,
        direction: Direction,
        limit: usize,
    ) -> Result<PipelineSink<'a>> {
        let mut sink = self
            .finish_until(|sink| {
                if sink.kept() >= limit {
                    sink.truncate_logs(direction, limit);
                }
                sink.kept() >= limit
            })
            .await?;
        sink.truncate_logs(direction, limit);
        Ok(sink)
    }

    async fn finish_until(
        mut self,
        mut done: impl FnMut(&mut PipelineSink<'a>) -> bool,
    ) -> Result<PipelineSink<'a>> {
        if self.batches.is_empty() {
            for row in std::mem::take(&mut self.rows) {
                self.sink.push(row)?;
            }
            return Ok(self.sink);
        }
        if !self.rows.is_empty() {
            self.dispatch()?;
        }
        for batch in self.batches {
            if done(&mut self.sink) {
                break;
            }
            let outputs = match batch {
                Batch::Done(outputs) => outputs,
                Batch::Running(receiver) => receiver.await.map_err(|_| panicked())??,
            };
            self.sink.extend_outputs(outputs);
        }
        Ok(self.sink)
    }
}

fn run(query: &Query, request: &QueryRequest, rows: Vec<Row>) -> Result<Outputs> {
    let mut sink = PipelineSink::new(query, request)?;
    for row in rows {
        sink.push(row)?;
    }
    Ok(sink.outputs)
}
