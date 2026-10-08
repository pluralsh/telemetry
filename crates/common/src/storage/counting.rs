//! A [`StorageRead`] wrapper that tallies reads by operation and key class,
//! for attributing a query's storage I/O when profiling.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::FilterContext;

use super::slate::SlateReadHandle;
use super::{ReadHints, Record, StorageIterator, StorageRead, StorageResult};
use crate::BytesRange;

/// Names the kind of record a key belongs to, e.g. `"posting"`.
pub type KeyClassifier = fn(&[u8]) -> &'static str;

/// Reads of one `(operation, class)` pair.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadStats {
    /// `get`s issued, or scans opened.
    pub calls: u64,
    /// `get`s that found a record, or records a scan yielded.
    pub records: u64,
    /// Key and value bytes returned.
    pub bytes: u64,
    /// Time spent awaiting the backend, summed over concurrent calls.
    pub elapsed: Duration,
}

impl ReadStats {
    fn add(&mut self, other: &ReadStats) {
        self.calls += other.calls;
        self.records += other.records;
        self.bytes += other.bytes;
        self.elapsed += other.elapsed;
    }
}

type Tally = Arc<Mutex<BTreeMap<(&'static str, &'static str), ReadStats>>>;

/// Wraps a [`StorageRead`], forwarding every call unchanged.
pub struct CountingStorage {
    inner: Arc<dyn StorageRead>,
    classify: KeyClassifier,
    tally: Tally,
}

impl CountingStorage {
    pub fn new(inner: Arc<dyn StorageRead>, classify: KeyClassifier) -> Self {
        Self {
            inner,
            classify,
            tally: Tally::default(),
        }
    }

    /// Reads since the last call, keyed by `(operation, class)`. Scans
    /// still open keep counting into the next window.
    pub fn take(&self) -> BTreeMap<(&'static str, &'static str), ReadStats> {
        std::mem::take(&mut *self.tally.lock().expect("tally lock"))
    }

    fn record(&self, op: &'static str, key: &[u8], stats: ReadStats) {
        record(&self.tally, op, (self.classify)(key), stats);
    }

    fn counted(
        &self,
        key: &[u8],
        iter: Box<dyn StorageIterator + Send + 'static>,
        opened: Duration,
    ) -> Box<dyn StorageIterator + Send + 'static> {
        let class = (self.classify)(key);
        record(
            &self.tally,
            "scan",
            class,
            ReadStats {
                calls: 1,
                elapsed: opened,
                ..ReadStats::default()
            },
        );
        Box::new(CountingIterator {
            inner: iter,
            class,
            tally: self.tally.clone(),
        })
    }
}

fn record(tally: &Tally, op: &'static str, class: &'static str, stats: ReadStats) {
    tally
        .lock()
        .expect("tally lock")
        .entry((op, class))
        .or_default()
        .add(&stats);
}

struct CountingIterator {
    inner: Box<dyn StorageIterator + Send + 'static>,
    class: &'static str,
    tally: Tally,
}

#[async_trait]
impl StorageIterator for CountingIterator {
    async fn next(&mut self) -> StorageResult<Option<Record>> {
        let started = Instant::now();
        let next = self.inner.next().await?;
        let mut stats = ReadStats {
            elapsed: started.elapsed(),
            ..ReadStats::default()
        };
        if let Some(record) = &next {
            stats.records = 1;
            stats.bytes = (record.key.len() + record.value.len()) as u64;
        }
        record(&self.tally, "scan", self.class, stats);
        Ok(next)
    }
}

#[async_trait]
impl StorageRead for CountingStorage {
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let started = Instant::now();
        let found = self.inner.get(key.clone()).await?;
        self.record(
            "get",
            &key,
            ReadStats {
                calls: 1,
                records: u64::from(found.is_some()),
                bytes: found
                    .as_ref()
                    .map_or(0, |record| (record.key.len() + record.value.len()) as u64),
                elapsed: started.elapsed(),
            },
        );
        Ok(found)
    }

    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let key = match &range.start {
            std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) => key.clone(),
            std::ops::Bound::Unbounded => Bytes::new(),
        };
        let started = Instant::now();
        let iter = self.inner.scan_iter(range).await?;
        Ok(self.counted(&key, iter, started.elapsed()))
    }

    async fn scan_prefix_iter(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let started = Instant::now();
        let iter = self
            .inner
            .scan_prefix_iter(prefix.clone(), subrange, filter_context)
            .await?;
        Ok(self.counted(&prefix, iter, started.elapsed()))
    }

    async fn get_with(&self, key: Bytes, hints: ReadHints) -> StorageResult<Option<Record>> {
        let started = Instant::now();
        let found = self.inner.get_with(key.clone(), hints).await?;
        self.record(
            "get",
            &key,
            ReadStats {
                calls: 1,
                records: u64::from(found.is_some()),
                bytes: found
                    .as_ref()
                    .map_or(0, |record| (record.key.len() + record.value.len()) as u64),
                elapsed: started.elapsed(),
            },
        );
        Ok(found)
    }

    async fn scan_prefix_iter_with(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<FilterContext>,
        hints: ReadHints,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let started = Instant::now();
        let iter = self
            .inner
            .scan_prefix_iter_with(prefix.clone(), subrange, filter_context, hints)
            .await?;
        Ok(self.counted(&prefix, iter, started.elapsed()))
    }

    fn slate_read(&self) -> Option<SlateReadHandle> {
        self.inner.slate_read()
    }

    async fn close(&self) -> StorageResult<()> {
        self.inner.close().await
    }
}
