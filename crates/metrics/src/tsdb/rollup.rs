//! Rollup of closed hour buckets into wider buckets.
//!
//! SlateDB compacts within a segment, and every bucket is its own segment,
//! so a day of hour buckets stays 24 segments however long it sits. A
//! rollup re-writes a closed day's hours into the fewest aligned buckets of
//! 24, 12, 8, 6, 4, 3 or 2 hours that stay within the configured size,
//! then seals each one. Until sealed a rollup bucket is invisible to reads;
//! once sealed it replaces the hours it covers, which compaction then
//! drains. A rollup interrupted before its seal resumes on the next pass.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

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

/// Bucket widths in hours a day may be tiled with, widest first.
const WIDTHS: [BucketSize; 8] = [24, 12, 8, 6, 4, 3, 2, 1];
const HOURS_PER_DAY: usize = 24;
const MINUTES_PER_DAY: u32 = 24 * 60;
/// Series copied per write into the rollup bucket.
const COPY_BATCH: usize = 2_048;
const COPY_WRITE_TIMEOUT: Duration = Duration::from_secs(60);

/// The fewest aligned windows, as `(first hour, width)`, tiling a day whose
/// hours hold `bytes`, each window at most `max_bytes` unless an hour wide.
/// Ties go to the tiling whose windows are widest first.
pub(crate) fn tile_day(bytes: &[u64; HOURS_PER_DAY], max_bytes: u64) -> Vec<(usize, BucketSize)> {
    // best[end] = (window count, last window width) of the best tiling of
    // hours [0, end).
    let mut best: [Option<(usize, BucketSize)>; HOURS_PER_DAY + 1] = [None; HOURS_PER_DAY + 1];
    best[0] = Some((0, 0));
    for end in 1..=HOURS_PER_DAY {
        for width in WIDTHS {
            let width_hours = usize::from(width);
            if end % width_hours != 0 {
                continue;
            }
            let start = end - width_hours;
            let Some((count, _)) = best[start] else {
                continue;
            };
            let size: u64 = bytes[start..end].iter().sum();
            if width > 1 && size > max_bytes {
                continue;
            }
            if best[end].is_none_or(|(current, _)| count + 1 < current) {
                best[end] = Some((count + 1, width));
            }
        }
    }
    let mut windows = Vec::new();
    let mut end = HOURS_PER_DAY;
    while end > 0 {
        let (_, width) = best[end].expect("hour windows always tile a day");
        end -= usize::from(width);
        windows.push((end, width));
    }
    windows.reverse();
    windows
}

impl Tsdb {
    /// Rolls up every day of this namespace that ended at least
    /// `config.delay` before `now_ms`, first finishing any interrupted
    /// rollup. Returns the rollup buckets sealed.
    pub(crate) async fn rollup(&self, config: &RollupConfig, now_ms: i64) -> Result<usize> {
        let _one = self.rollup_lock.lock().await;
        let (buckets, sealed) = self.storage.segments(&self.namespace);
        let mut done = 0;
        for wide in buckets.iter().filter(|b| b.size > 1 && !sealed.contains(b)) {
            self.roll_up(*wide, config.copy_buffer_bytes).await?;
            done += 1;
        }

        let wide: Vec<TimeBucket> = buckets
            .iter()
            .chain(&sealed)
            .filter(|b| b.size > 1)
            .copied()
            .collect();
        let days: BTreeSet<u32> = buckets
            .iter()
            .filter(|b| b.size == 1)
            .map(|b| b.start - b.start % MINUTES_PER_DAY)
            .collect();
        let delay_ms = i64::try_from(config.delay.as_millis()).unwrap_or(i64::MAX);
        let sizes = self.storage.bucket_bytes(&self.namespace);
        for day_start in days {
            let day = TimeBucket {
                start: day_start,
                size: 24,
            };
            if day.end_ms().saturating_add(delay_ms) > now_ms || self.expired(day, now_ms) {
                continue;
            }
            if day.hours().any(|hour| wide.iter().any(|w| w.covers(&hour))) {
                continue;
            }
            let mut bytes = [0; HOURS_PER_DAY];
            let mut present = [false; HOURS_PER_DAY];
            for (index, hour) in day.hours().enumerate() {
                bytes[index] = sizes.get(&hour).copied().unwrap_or(0);
                present[index] = buckets.contains(&hour);
            }
            for (first, width) in tile_day(&bytes, config.max_bucket_bytes) {
                let hours = first..first + usize::from(width);
                if width == 1 || present[hours.clone()].iter().filter(|p| **p).count() < 2 {
                    continue;
                }
                let bucket = TimeBucket {
                    start: day_start + u32::try_from(first).unwrap_or(0) * 60,
                    size: width,
                };
                self.roll_up(bucket, config.copy_buffer_bytes).await?;
                done += 1;
            }
        }
        Ok(done)
    }

    /// Whether every record of `bucket` is past retention.
    fn expired(&self, bucket: TimeBucket, now_ms: i64) -> bool {
        matches!(
            crate::flusher::bucket_ttl(bucket, self.retention),
            common::Ttl::ExpireAt(expires_ms) if expires_ms <= now_ms
        )
    }

    /// Copies every hour `wide` covers into it, then seals it.
    async fn roll_up(&self, wide: TimeBucket, buffer: u64) -> Result<()> {
        {
            // No ingest is in flight while this is held, so once the hours'
            // pending writes are flushed every later write to them goes to
            // `wide` and the snapshot below holds all they will ever get.
            let mut rolled = self.rolled.write().await;
            rolled.insert(wide);
            for hour in wide.hours() {
                if let Some(mini) = self.ingest_cache.get(&hour).await {
                    mini.flush_written().await?;
                    self.ingest_cache.invalidate(&hour).await;
                }
            }
        }
        let snapshot = self.storage.snapshot().await?;
        let mini = self.get_or_create_for_ingest(wide).await?;
        let mut queued = 0;
        for hour in wide.hours() {
            self.copy_hour(&snapshot, hour, &mini, buffer, &mut queued)
                .await?;
        }
        mini.flush_written().await?;
        self.storage
            .seal(
                &self.namespace,
                wide,
                crate::flusher::bucket_ttl(wide, self.retention),
            )
            .await?;
        // Publishes the seal to the segment list, hiding the hours.
        self.storage.flush_memtable().await?;
        self.discovery_cache.clear();
        tracing::info!(namespace = %self.namespace, bucket = ?wide, "sealed rollup bucket");
        Ok(())
    }

    /// Copies `hour` into `mini`. `ingest_batch` waits only until the
    /// coordinator accepts a batch, so once `queued` copied bytes exceed
    /// `buffer` the copy waits for them to be written.
    async fn copy_hour(
        &self,
        snapshot: &StorageSnapshot,
        hour: TimeBucket,
        mini: &MiniTsdb,
        buffer: u64,
        queued: &mut u64,
    ) -> Result<()> {
        let forward = snapshot.get_forward_index(&self.namespace, hour).await?;
        if forward.series.is_empty() {
            return Ok(());
        }
        let metadata: HashMap<String, MetricMetadata> = crate::discovery::metadata(
            snapshot.clone(),
            &self.namespace,
            &[hour],
            None,
            &MetricsDiscoveryCache::new(),
        )
        .await?
        .into_iter()
        .map(|metadata| (metadata.metric_name.clone(), metadata))
        .collect();

        let mut records = snapshot
            .scan_prefix(TimeSeriesKey::bucket_prefix(&self.namespace, &hour))
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
    const DAY: u32 = 19_723 * MINUTES_PER_DAY;
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
    async fn leaves_days_inside_the_delay_alone() {
        let storage = Arc::new(in_memory_storage().await);
        let tsdb = Tsdb::with_retention(storage.clone(), None);
        write_day(&tsdb, &storage).await;

        let now = day().end_ms() + HOUR_MS / 2;

        assert_eq!(tsdb.rollup(&config(u64::MAX), now).await.unwrap(), 0);
        assert!(sealed(&storage).await.is_empty());
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
                .copy_hour(&snapshot, hour, &mini, u64::MAX, &mut 0)
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

    fn widths(windows: &[(usize, BucketSize)]) -> Vec<BucketSize> {
        windows.iter().map(|(_, width)| *width).collect()
    }

    #[test]
    fn small_day_rolls_into_one_bucket() {
        let windows = tile_day(&[10; 24], 1_000);
        assert_eq!(windows, vec![(0, 24)]);
    }

    #[test]
    fn halves_when_the_day_exceeds_the_cap() {
        assert_eq!(widths(&tile_day(&[10; 24], 120)), vec![12, 12]);
        assert_eq!(widths(&tile_day(&[10; 24], 119)), vec![8, 8, 8]);
    }

    #[test]
    fn one_hot_hour_only_narrows_its_own_window() {
        let mut bytes = [10; 24];
        bytes[20] = 5_000;
        let windows = tile_day(&bytes, 1_000);
        assert_eq!(windows.len(), 5, "{windows:?}");
        assert!(windows.contains(&(20, 1)));
        let covered: usize = windows.iter().map(|(_, w)| usize::from(*w)).sum();
        assert_eq!(covered, 24);
        for (first, width) in &windows {
            assert_eq!(first % usize::from(*width), 0, "{windows:?}");
            if *width > 1 {
                let size: u64 = bytes[*first..first + usize::from(*width)].iter().sum();
                assert!(size <= 1_000, "{windows:?}");
            }
        }
    }

    #[test]
    fn oversized_hours_stay_hours() {
        assert_eq!(tile_day(&[2_000; 24], 1_000).len(), 24);
    }
}
