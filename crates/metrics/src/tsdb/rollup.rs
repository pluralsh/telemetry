//! Rollup of closed hour buckets into wider buckets; see
//! [`common::storage::rollup`], whose planner this shares.
//!
//! SlateDB compacts within a segment, and every bucket is its own segment,
//! so a day of hour buckets stays 24 segments however long it sits. Each
//! closed window of the day (4 hours by default) is re-written into one
//! bucket, or narrower ones within the configured size, and each closed day
//! into the fewest wider buckets its windows fit. Until sealed a rollup
//! bucket is invisible to reads; once merged into one sorted run and sealed
//! it replaces the buckets it covers, which compaction then drains. A
//! rollup interrupted before its seal resumes on the next pass.

use std::collections::HashMap;
use std::time::Duration;

use common::serde::scope::Segment;
use common::storage::rollup::Rollups;

use super::Tsdb;
use crate::config::RollupConfig;
use crate::discovery::MetricsDiscoveryCache;
use crate::error::Error;
use crate::minitsdb::MiniTsdb;
use crate::model::{
    BucketSize, HistogramSample, Label, MetricMetadata, Sample, Series, SeriesData, TimeBucket,
};
use crate::serde::TimeBucketScoped;
use crate::serde::key::TimeSeriesKey;
use crate::storage::{StorageRead, StorageSnapshot};
use crate::util::Result;
use common::storage::StorageError;

const MINUTE_NS: i64 = 60_000_000_000;
const HOUR_NS: i64 = 60 * MINUTE_NS;
/// Series copied per write into the rollup bucket.
const COPY_BATCH: usize = 2_048;
const COPY_WRITE_TIMEOUT: Duration = Duration::from_secs(60);

fn segment(bucket: TimeBucket) -> Segment {
    Segment::new(
        i64::from(bucket.start) * MINUTE_NS,
        i64::from(bucket.size) * HOUR_NS,
    )
}

fn bucket(segment: Segment) -> Option<TimeBucket> {
    Some(TimeBucket {
        start: u32::try_from(segment.start_ns / MINUTE_NS).ok()?,
        size: BucketSize::try_from(segment.width_ns / HOUR_NS).ok()?,
    })
}

impl Tsdb {
    /// Rolls up every window and day of this namespace that ended at least
    /// `config.delay` before `now_ms`, first finishing any interrupted
    /// rollup. Returns the rollup buckets sealed.
    pub(crate) async fn rollup(&self, config: &RollupConfig, now_ms: i64) -> Result<usize> {
        let _one = self.rollup_lock.lock().await;
        let mut done = 0;
        for wide in self.rollups().plan(
            HOUR_NS,
            None,
            &config.planner(),
            self.retention,
            now_ms.saturating_mul(1_000_000),
        ) {
            let Some(wide) = bucket(wide).filter(|wide| TimeBucket::is_valid_size(wide.size))
            else {
                continue;
            };
            self.roll_up(wide, config).await?;
            done += 1;
        }
        Ok(done)
    }

    /// This namespace's buckets as the shared planner sees them: a bucket
    /// wider than an hour is a rollup, pending until its seal.
    fn rollups(&self) -> Rollups {
        let (buckets, sealed) = self.storage.segments(&self.namespace);
        let sizes = self
            .storage
            .bucket_bytes(&self.namespace)
            .into_iter()
            .map(|(bucket, bytes)| (segment(bucket), bytes))
            .collect();
        Rollups::new(
            sealed.iter().copied().map(segment).collect(),
            buckets
                .iter()
                .filter(|bucket| bucket.size > 1)
                .copied()
                .map(segment)
                .collect(),
            sizes,
        )
    }

    /// Copies what reads see of every bucket `wide` covers into it, has it
    /// merged into one sorted run, then seals it.
    async fn roll_up(&self, wide: TimeBucket, config: &RollupConfig) -> Result<()> {
        let hold = self.storage.hold_merges(&self.namespace, wide);
        let sources: Vec<TimeBucket> = self
            .rollups()
            .sources(segment(wide), HOUR_NS)
            .into_iter()
            .filter_map(bucket)
            .collect();
        {
            // No ingest is in flight while this is held, so once the
            // buckets' pending writes are flushed every later write to them
            // goes to `wide` and the snapshot below holds all they will ever
            // get.
            let mut rolled = self.rolled.write().await;
            rolled.insert(wide);
            for covered in wide.hours().chain(sources.iter().copied()) {
                if let Some(mini) = self.ingest_cache.get(&covered).await {
                    mini.flush_written().await?;
                    self.ingest_cache.invalidate(&covered).await;
                }
            }
        }
        let snapshot = self.storage.snapshot().await?;
        let mini = self.get_or_create_for_ingest(wide).await?;
        let mut queued = 0;
        for source in sources {
            self.copy_bucket(
                &snapshot,
                source,
                &mini,
                config.copy_buffer_bytes,
                &mut queued,
            )
            .await?;
        }
        mini.flush_written().await?;
        self.storage.flush_memtable().await?;
        let copied_ms = common::time::now_ms().max(0).unsigned_abs();
        hold.merge();
        if !self
            .storage
            .await_merge(&self.namespace, wide, copied_ms, config.merge_wait)
            .await
        {
            tracing::warn!(
                namespace = %self.namespace,
                bucket = ?wide,
                "sealing rollup bucket before its merge"
            );
        }
        self.storage
            .seal(
                &self.namespace,
                wide,
                crate::flusher::bucket_ttl(wide, self.retention),
            )
            .await?;
        // Publishes the seal to the segment list, hiding the hours.
        self.storage.flush_memtable().await?;
        drop(hold);
        self.discovery_cache.clear();
        tracing::info!(namespace = %self.namespace, bucket = ?wide, "sealed rollup bucket");
        Ok(())
    }

    /// Copies `source` into `mini`. `ingest_batch` waits only until the
    /// coordinator accepts a batch, so once `queued` copied bytes exceed
    /// `buffer` the copy waits for them to be written.
    async fn copy_bucket(
        &self,
        snapshot: &StorageSnapshot,
        source: TimeBucket,
        mini: &MiniTsdb,
        buffer: u64,
        queued: &mut u64,
    ) -> Result<()> {
        let forward = snapshot.get_forward_index(&self.namespace, source).await?;
        if forward.series.is_empty() {
            return Ok(());
        }
        let metadata: HashMap<String, MetricMetadata> = crate::discovery::metadata(
            snapshot.clone(),
            &self.namespace,
            &[source],
            None,
            &MetricsDiscoveryCache::new(),
        )
        .await?
        .into_iter()
        .map(|metadata| (metadata.metric_name.clone(), metadata))
        .collect();

        let mut records = snapshot
            .scan_prefix(TimeSeriesKey::bucket_prefix(&self.namespace, &source))
            .await?;
        let mut batch = Vec::with_capacity(COPY_BATCH);
        while let Some(record) = records.next().await.map_err(StorageError::from_storage)? {
            let key = TimeSeriesKey::decode(record.key.as_ref())?;
            let Some(spec) = forward.series.get(&key.series_id) else {
                continue;
            };
            let data = SeriesData::decode(record.value.as_ref())
                .map_err(|e| Error::Internal(format!("Invalid timeseries data in storage: {e}")))?;
            let description = metadata
                .get(&key.metric_name)
                .filter(|m| m.metric_type == spec.metric_type && m.unit == spec.unit)
                .and_then(|m| m.description.clone());
            *queued += series_bytes(spec.labels.iter(), &data);
            batch.push(Series {
                labels: spec.labels.iter().cloned().collect(),
                metric_type: spec.metric_type,
                unit: spec.unit.clone(),
                description,
                samples: data
                    .timestamps
                    .iter()
                    .zip(&data.values)
                    .map(|(&timestamp_ms, &value)| Sample::new(timestamp_ms, value))
                    .collect(),
                histograms: data.histograms,
            });
            if batch.len() == COPY_BATCH || *queued >= buffer {
                mini.ingest_batch(&batch, Some(COPY_WRITE_TIMEOUT)).await?;
                batch.clear();
                if *queued >= buffer {
                    mini.flush_written().await?;
                    *queued = 0;
                }
            }
        }
        if !batch.is_empty() {
            mini.ingest_batch(&batch, Some(COPY_WRITE_TIMEOUT)).await?;
        }
        Ok(())
    }
}

/// Approximate in-memory size of one copied series.
fn series_bytes<'a>(labels: impl IntoIterator<Item = &'a Label>, data: &SeriesData) -> u64 {
    let labels: usize = labels
        .into_iter()
        .map(|label| label.name.len() + label.value.len())
        .sum();
    let samples = data.timestamps.len() * std::mem::size_of::<Sample>();
    let histograms = data.histograms.len() * std::mem::size_of::<HistogramSample>();
    (std::mem::size_of::<Series>() + labels + samples + histograms) as u64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::UNIX_EPOCH;

    use super::*;
    use crate::model::{MetricType, QueryOptions};
    use crate::storage::{Storage, in_memory_storage};
    use crate::tsdb::TsdbReadEngine;

    /// 2024-01-01 in minutes.
    const DAY: u32 = 19_723 * 24 * 60;
    const HOUR_MS: i64 = 3_600_000;

    fn day() -> TimeBucket {
        TimeBucket {
            start: DAY,
            size: 24,
        }
    }

    fn config(max_bucket_bytes: u64) -> RollupConfig {
        RollupConfig {
            max_bucket_bytes,
            ..RollupConfig::default()
        }
    }

    fn series(host: &str, timestamp_ms: i64, value: f64) -> Series {
        Series {
            labels: vec![
                Label::new("__name__", "rollup_metric"),
                Label::new("host", host),
            ],
            metric_type: Some(MetricType::Gauge),
            unit: Some("bytes".to_owned()),
            description: Some("rolled".to_owned()),
            samples: vec![Sample::new(timestamp_ms, value)],
            histograms: Vec::new(),
        }
    }

    async fn write_day(tsdb: &Tsdb, storage: &Storage) {
        let mut batch = Vec::new();
        for hour in 0..24_i64 {
            let start = day().start_ms() + hour * HOUR_MS;
            for (index, host) in ["a", "b", "c"].into_iter().enumerate() {
                for minute in [1, 31] {
                    let value = (hour * 100 + minute) as f64 + index as f64;
                    batch.push(series(host, start + minute * 60_000, value));
                }
            }
        }
        tsdb.ingest_samples(batch, None).await.unwrap();
        tsdb.flush().await.unwrap();
        storage.flush_memtable().await.unwrap();
    }

    async fn samples(tsdb: &Tsdb) -> Vec<(String, Vec<(i64, f64)>)> {
        let start = UNIX_EPOCH + Duration::from_millis(day().start_ms() as u64);
        let end = UNIX_EPOCH + Duration::from_millis(day().end_ms() as u64);
        let mut out: Vec<_> = tsdb
            .eval_query_range(
                "rollup_metric",
                start..=end,
                Duration::from_secs(60),
                &QueryOptions::default(),
            )
            .await
            .unwrap()
            .into_iter()
            .map(|sample| (format!("{:?}", sample.labels), sample.samples))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn after_delay() -> i64 {
        day().end_ms() + 2 * HOUR_MS
    }

    async fn sealed(storage: &Storage) -> Vec<TimeBucket> {
        let (_, mut sealed) = storage.segments(&Namespace::default());
        sealed.sort_by_key(|bucket| bucket.start);
        sealed
    }

    use crate::Namespace;

    #[tokio::test]
    async fn rolls_a_closed_day_into_one_bucket_serving_the_same_samples() {
        let storage = Arc::new(in_memory_storage().await);
        let tsdb = Tsdb::with_retention(storage.clone(), None);
        write_day(&tsdb, &storage).await;
        let before = samples(&tsdb).await;
        assert_eq!(before.len(), 3);
        assert_eq!(before[0].1.len(), 24 * 2 * 5);

        let sealed_now = tsdb.rollup(&config(u64::MAX), after_delay()).await.unwrap();

        assert_eq!(sealed_now, 1);
        assert_eq!(sealed(&storage).await, vec![day()]);
        let reader = tsdb
            .query_reader(day().start_ms() / 1000, day().end_ms() / 1000 - 1)
            .await
            .unwrap();
        assert_eq!(
            crate::query::QueryReader::list_buckets(&reader)
                .await
                .unwrap(),
            vec![day()]
        );
        assert_eq!(samples(&tsdb).await, before);
        let metadata = tsdb.find_metadata(Some("rollup_metric")).await.unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].description.as_deref(), Some("rolled"));
        assert_eq!(metadata[0].unit.as_deref(), Some("bytes"));
        assert_eq!(
            tsdb.rollup(&config(u64::MAX), after_delay()).await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn reads_many_series_of_a_rollup_bucket_in_parallel_parts() {
        let storage = Arc::new(in_memory_storage().await);
        let tsdb = Tsdb::with_retention(storage.clone(), None);
        let hosts: Vec<String> = (0..64).map(|host| format!("host-{host:02}")).collect();
        let batch = (0..24_i64)
            .flat_map(|hour| {
                hosts.iter().enumerate().map(move |(index, host)| {
                    let at = day().start_ms() + hour * HOUR_MS + 60_000;
                    series(host, at, (hour * 1_000) as f64 + index as f64)
                })
            })
            .collect();
        tsdb.ingest_samples(batch, None).await.unwrap();
        tsdb.flush().await.unwrap();
        storage.flush_memtable().await.unwrap();
        let before = samples(&tsdb).await;
        assert_eq!(before.len(), 64);

        tsdb.rollup(&config(u64::MAX), after_delay()).await.unwrap();

        assert_eq!(samples(&tsdb).await, before);
    }

    #[tokio::test]
    async fn waits_for_storage_whenever_the_copy_buffer_fills() {
        let storage = Arc::new(in_memory_storage().await);
        let tsdb = Tsdb::with_retention(storage.clone(), None);
        write_day(&tsdb, &storage).await;
        let before = samples(&tsdb).await;

        let config = RollupConfig {
            copy_buffer_bytes: 1,
            ..config(u64::MAX)
        };
        assert_eq!(tsdb.rollup(&config, after_delay()).await.unwrap(), 1);

        assert_eq!(sealed(&storage).await, vec![day()]);
        assert_eq!(samples(&tsdb).await, before);
    }

    #[tokio::test]
    async fn rolls_up_windows_as_the_day_goes_then_the_day_from_them() {
        let storage = Arc::new(in_memory_storage().await);
        let tsdb = Tsdb::with_retention(storage.clone(), None);
        write_day(&tsdb, &storage).await;
        let before = samples(&tsdb).await;

        // Five minutes past midnight, the last window is still in its delay.
        let now = day().end_ms() + 5 * 60_000;
        assert_eq!(tsdb.rollup(&config(u64::MAX), now).await.unwrap(), 5);
        let windows: Vec<_> = (0..5)
            .map(|index| TimeBucket {
                start: DAY + index * 4 * 60,
                size: 4,
            })
            .collect();
        assert_eq!(sealed(&storage).await, windows);
        assert_eq!(samples(&tsdb).await, before);

        // The day's last window first, then the day from its windows.
        assert_eq!(
            tsdb.rollup(&config(u64::MAX), after_delay()).await.unwrap(),
            1
        );
        assert_eq!(
            tsdb.rollup(&config(u64::MAX), after_delay()).await.unwrap(),
            1
        );
        assert!(sealed(&storage).await.contains(&day()));
        let reader = tsdb
            .query_reader(day().start_ms() / 1000, day().end_ms() / 1000 - 1)
            .await
            .unwrap();
        assert_eq!(
            crate::query::QueryReader::list_buckets(&reader)
                .await
                .unwrap(),
            vec![day()]
        );
        assert_eq!(samples(&tsdb).await, before);
        assert_eq!(
            tsdb.rollup(&config(u64::MAX), after_delay()).await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn routes_late_writes_to_the_rollup_bucket() {
        let storage = Arc::new(in_memory_storage().await);
        let tsdb = Tsdb::with_retention(storage.clone(), None);
        write_day(&tsdb, &storage).await;
        tsdb.rollup(&config(u64::MAX), after_delay()).await.unwrap();

        let late = day().start_ms() + 5 * HOUR_MS + 45 * 60_000;
        tsdb.ingest_samples(vec![series("a", late, -1.0)], None)
            .await
            .unwrap();
        tsdb.flush().await.unwrap();

        tsdb.ingest_cache.run_pending_tasks().await;
        assert!(tsdb.ingest_cache.contains_key(&day()));
        assert!(
            !tsdb
                .ingest_cache
                .contains_key(&TimeBucket::hour(DAY + 5 * 60))
        );
        let host_a = samples(&tsdb).await.remove(0).1;
        assert!(host_a.iter().any(|(_, value)| *value == -1.0), "{host_a:?}");
    }

    #[tokio::test]
    async fn splits_a_day_too_large_for_one_bucket() {
        let storage = Arc::new(in_memory_storage().await);
        let tsdb = Tsdb::with_retention(storage.clone(), None);
        write_day(&tsdb, &storage).await;
        let before = samples(&tsdb).await;
        let bytes = storage.bucket_bytes(&Namespace::default());
        let half = |first: u32| -> u64 {
            (first..first + 12)
                .map(|hour| bytes[&TimeBucket::hour(DAY + hour * 60)])
                .sum()
        };
        let cap = half(0).max(half(12));

        assert_eq!(tsdb.rollup(&config(cap), after_delay()).await.unwrap(), 2);

        let halves: Vec<_> = [0, 12 * 60]
            .into_iter()
            .map(|offset| TimeBucket {
                start: DAY + offset,
                size: 12,
            })
            .collect();
        assert_eq!(sealed(&storage).await, halves);
        assert_eq!(samples(&tsdb).await, before);
    }

    #[tokio::test]
    async fn finishes_an_interrupted_rollup_without_duplicating_samples() {
        let storage = Arc::new(in_memory_storage().await);
        let interrupted = Tsdb::with_retention(storage.clone(), None);
        write_day(&interrupted, &storage).await;
        let before = samples(&interrupted).await;
        // A rollup that copied a few hours, then stopped before its seal.
        interrupted.rolled.write().await.insert(day());
        let snapshot = storage.snapshot().await.unwrap();
        let mini = interrupted.get_or_create_for_ingest(day()).await.unwrap();
        for hour in day().hours().take(5) {
            interrupted
                .copy_bucket(&snapshot, hour, &mini, u64::MAX, &mut 0)
                .await
                .unwrap();
        }
        interrupted.flush().await.unwrap();
        storage.flush_memtable().await.unwrap();
        assert_eq!(samples(&interrupted).await, before);
        drop((snapshot, mini, interrupted));

        let restarted = Tsdb::with_retention(storage.clone(), None);
        assert!(restarted.rolled.read().await.contains(&day()));

        assert_eq!(
            restarted
                .rollup(&config(u64::MAX), after_delay())
                .await
                .unwrap(),
            1
        );
        assert_eq!(sealed(&storage).await, vec![day()]);
        assert_eq!(samples(&restarted).await, before);
        let snapshot = storage.snapshot().await.unwrap();
        let mut records = snapshot
            .scan_prefix(TimeSeriesKey::bucket_prefix(&Namespace::default(), &day()))
            .await
            .unwrap();
        let mut stored = 0;
        while let Some(record) = records.next().await.unwrap() {
            stored += SeriesData::decode(&record.value).unwrap().timestamps.len();
        }
        assert_eq!(stored, 3 * 24 * 2);
    }
}
