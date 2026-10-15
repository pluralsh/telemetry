// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Rollup of closed segments into wider ones; see
//! [`common::storage::rollup`].
//!
//! A rollup re-writes every row of the segments it covers through the
//! ordinary write path, so the rollup segment gets its own streams,
//! objects, full-text index and discovery catalog. Queries deduplicate
//! identical rows, so a rollup interrupted before its seal is simply copied
//! again. Copied rows expire a retention after the copy rather than after
//! their first write.

use common::storage::rollup::{Marker, RollupConfig, marker_key};

use super::*;
use crate::model::locality_order;

/// Streams read and written per copy step. One stream can hold enough rows to
/// use the writer's whole memory budget, so never materialize several at once.
const COPY_STREAMS: usize = 1;
const COPY_WRITE_TIMEOUT: Duration = Duration::from_secs(60);

impl LogDb {
    /// Rolls up every window and day that ended `config.delay` ago in each
    /// namespace, first finishing any interrupted rollup. Returns the rollup
    /// segments sealed.
    pub async fn rollup(&self, config: &RollupConfig) -> Result<usize> {
        self.rollup_at(config, common::time::now_ns()).await
    }

    pub(crate) async fn rollup_at(&self, config: &RollupConfig, now_ns: i64) -> Result<usize> {
        if self.writer.is_none() || !config.enabled {
            return Ok(0);
        }
        let _one = self.rollup_lock.lock().await;
        let Some(slate) = self.storage.slate_read() else {
            return Ok(0);
        };
        let mut sealed = 0;
        for (namespace, rollups) in Rollups::load_all(&slate, crate::codec::KEY_SCOPE) {
            let planned = rollups.plan(
                self.segment_ns,
                self.rollup_ns,
                config,
                self.retention,
                now_ns,
            );
            for wide in planned {
                tracing::info!(%namespace, segment = %wide, "starting logs rollup segment");
                self.roll_up(&namespace, wide, config).await?;
                sealed += 1;
            }
        }
        if sealed > 0 {
            tracing::info!(sealed, "finished logs rollup pass");
        }
        Ok(sealed)
    }

    /// Copies what reads see of every segment `wide` covers into it, has it
    /// merged into one sorted run, then seals it.
    async fn roll_up(
        &self,
        namespace: &Namespace,
        wide: SegmentId,
        config: &RollupConfig,
    ) -> Result<()> {
        let buffer = config.copy_buffer_bytes;
        let writer = self.writer()?;
        let ttl = self.marker_ttl()?;
        let prefix = segment_prefix(namespace, wide);
        let hold = self.merge_holds.copying(prefix.clone());
        {
            // No write is in flight while this is held, so once applied
            // deltas are written every later write to the covered segments
            // goes to `wide`, and the copy below sees all they will hold.
            let mut rolled = self.rolled.write().await;
            writer
                .apply(vec![RecordOp::put_with_ttl(
                    marker_key(crate::codec::KEY_SCOPE, namespace, Marker::Pending, wide),
                    Bytes::new(),
                    ttl,
                )])
                .await?;
            rolled.entry(namespace.clone()).or_default().insert(wide);
            self.flush_written().await?;
        }
        let sources = self.rollups(namespace).sources(wide, self.segment_ns);
        self.register_streams(namespace, &sources, wide).await?;
        let mut queued = 0;
        for segment in sources {
            self.copy_segment(namespace, segment, wide, buffer, &mut queued)
                .await?;
        }
        self.flush_written().await?;
        writer.flush_memtable().await?;
        let copied_ms = unix_time_ms()?;
        hold.merge();
        if let Some(slate) = writer.slate_read() {
            let merged = common::storage::rollup::await_merge(
                || slate.manifest(),
                &prefix,
                copied_ms,
                config.merge_wait,
            )
            .await;
            if !merged {
                tracing::warn!(
                    %namespace,
                    segment = %wide,
                    "sealing logs rollup segment before its merge"
                );
            }
        }
        writer
            .apply_with_options(
                vec![RecordOp::put_with_ttl(
                    marker_key(crate::codec::KEY_SCOPE, namespace, Marker::Sealed, wide),
                    Bytes::new(),
                    ttl,
                )],
                WriteOptions {
                    await_durable: true,
                },
            )
            .await?;
        // Publishes the seal to the manifest, which is where reads find it.
        writer.flush_memtable().await?;
        drop(hold);
        tracing::info!(%namespace, segment = %wide, "sealed logs rollup segment");
        Ok(())
    }

    /// Gives every stream of `sources` its ID in `wide` before any row is
    /// copied, all in one [`locality_order`], so streams that the copy finds
    /// segment by segment still get neighbouring IDs when they share coarse
    /// labels.
    async fn register_streams(
        &self,
        namespace: &Namespace,
        sources: &[SegmentId],
        wide: SegmentId,
    ) -> Result<()> {
        let mut streams = BTreeSet::new();
        for segment in sources {
            for read in self.segment_reads(namespace, *segment, usize::MAX).await? {
                streams.extend(read.streams().map(|(_, labels)| labels.clone()));
            }
        }
        let streams: Vec<Labels> = streams.into_iter().collect();
        let order = locality_order(&streams.iter().collect::<Vec<_>>());
        let mut groups = StreamGroups::new();
        for position in order {
            let labels = streams[position].clone();
            groups.insert((wide, labels.fingerprint()), (labels, Vec::new()));
        }
        if !groups.is_empty() {
            self.apply_groups(namespace, groups, Some(COPY_WRITE_TIMEOUT))
                .await?;
        }
        self.flush_written().await
    }

    /// Copies `segment` into `wide`. `apply_groups` waits only until the
    /// coordinator accepts a chunk, so once `queued` copied bytes exceed
    /// `buffer` the copy waits for them to be written.
    async fn copy_segment(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        wide: SegmentId,
        buffer: u64,
        queued: &mut u64,
    ) -> Result<()> {
        for read in self.segment_reads(namespace, segment, COPY_STREAMS).await? {
            let (sender, mut rows) = tokio::sync::mpsc::channel(1);
            let budget = PageBudget::new(usize::MAX);
            let read = self.read_bounded_chunks(namespace, read, &budget, sender);
            let write = async {
                while let Some(rows) = rows.recv().await {
                    // Rows of one stream share their labels.
                    let mut streams: HashMap<usize, usize> = HashMap::new();
                    let mut batches: Vec<LogBatch> = Vec::new();
                    for row in rows {
                        *queued += row_bytes(&row.entry);
                        let stream = Arc::as_ptr(&row.labels) as usize;
                        let index = *streams.entry(stream).or_insert_with(|| {
                            batches.push(LogBatch::new((*row.labels).clone(), Vec::new()));
                            batches.len() - 1
                        });
                        batches[index].entries.push(row.entry);
                    }
                    let groups = group_by_stream(batches, self.segment_ns, |_| wide)?;
                    if !groups.is_empty() {
                        self.apply_groups(namespace, groups, Some(COPY_WRITE_TIMEOUT))
                            .await?;
                    }
                    if *queued >= buffer {
                        self.flush_written().await?;
                        *queued = 0;
                    }
                }
                Ok::<_, Error>(())
            };
            tokio::try_join!(read, write)?;
        }
        Ok(())
    }

    /// Publishes everything written so far to the manifest.
    #[cfg(test)]
    pub(crate) async fn flush_memtable(&self) -> Result<()> {
        self.flush_written().await?;
        self.writer()?.flush_memtable().await?;
        Ok(())
    }

    fn marker_ttl(&self) -> Result<Ttl> {
        self.retention
            .map(|retention| {
                u64::try_from(retention.as_millis())
                    .map(Ttl::ExpireAfter)
                    .map_err(|_| Error::Invalid("retention exceeds u64 milliseconds".to_owned()))
            })
            .transpose()
            .map(|ttl| ttl.unwrap_or(Ttl::NoExpiry))
    }
}

/// Approximate in-memory size of one copied entry.
fn row_bytes(entry: &LogEntry) -> u64 {
    let metadata: usize = entry
        .structured_metadata
        .iter()
        .map(|field| field.name.len() + field.value.len())
        .sum();
    (entry.line.len() + metadata + std::mem::size_of::<LogEntry>()) as u64
}

#[cfg(test)]
mod tests {
    use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig, StorageConfig};

    use super::*;
    use crate::config::CompactionConfig;
    use crate::model::{Label, LogEntry};
    use crate::{QueryOptions, QueryRequest, QueryResult};

    const HOUR: i64 = 3_600 * 1_000_000_000;
    /// 2024-01-01.
    const DAY: i64 = 19_723 * 24 * HOUR;

    fn config() -> Config {
        Config {
            storage: StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "logs-rollup-test".to_owned(),
                object_store: ObjectStoreConfig::InMemory,
                settings_path: None,
                block_cache: None,
                meta_cache: None,
                disk: Default::default(),
            }),
            segment_duration: Duration::from_secs(3_600),
            discovery_rollup: Some(Duration::from_secs(24 * 3_600)),
            retention: None,
            compaction: CompactionConfig {
                enabled: false,
                ..CompactionConfig::default()
            },
            block_cache_capacity_bytes: 0,
            result_cache_capacity_bytes: 0,
            ..Config::default()
        }
    }

    fn namespace() -> Namespace {
        Namespace::new("tenant").unwrap()
    }

    fn stream(service: &str) -> Labels {
        Labels::new(vec![
            Label::new("env", "prod"),
            Label::new("service", service),
        ])
        .unwrap()
    }

    async fn write_day(db: &LogDb) {
        let mut batches = Vec::new();
        for service in ["api", "worker"] {
            let entries = (0..24)
                .flat_map(|hour| {
                    [1, 31].map(|minute| {
                        let at = DAY + hour * HOUR + minute * 60_000_000_000;
                        LogEntry::new(at, format!("{service} {hour}:{minute}"))
                    })
                })
                .collect();
            batches.push(LogBatch::new(stream(service), entries));
        }
        db.write(&namespace(), batches).await.unwrap();
        db.flush_memtable().await.unwrap();
    }

    async fn rows(db: &LogDb) -> Vec<(String, i64, String)> {
        let mut rows: Vec<_> = db
            .read(
                &namespace(),
                DAY,
                DAY + 24 * HOUR - 1,
                &[Label::new("env", "prod")],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    format!("{:?}", row.labels),
                    row.entry.timestamp_ns,
                    row.entry.line,
                )
            })
            .collect();
        rows.sort();
        rows
    }

    async fn queried(db: &LogDb) -> usize {
        let result = db
            .query(
                &namespace(),
                &QueryRequest::range(r#"{env="prod"}"#, DAY, DAY + 24 * HOUR - 1, 60_000_000_000),
                QueryOptions {
                    limit: 10_000,
                    ..QueryOptions::default()
                },
            )
            .await
            .unwrap();
        let QueryResult::Streams(streams) = result else {
            panic!("expected streams");
        };
        streams.iter().map(|stream| stream.entries.len()).sum()
    }

    fn after_delay() -> i64 {
        DAY + 26 * HOUR
    }

    fn day() -> SegmentId {
        SegmentId::new(DAY, 24 * HOUR)
    }

    async fn runs_of(db: &LogDb, segment: SegmentId) -> Vec<crate::codec::StoredRun> {
        let mut records = db
            .storage
            .scan_prefix_iter(
                crate::codec::segment_run_prefix(&namespace(), segment),
                common::BytesRange::unbounded(),
                None,
            )
            .await
            .unwrap();
        let mut runs = Vec::new();
        while let Some(record) = records.next().await.unwrap() {
            runs.push(crate::codec::decode_run(&record.value).unwrap());
        }
        runs
    }

    #[tokio::test]
    async fn no_run_of_a_rollup_crosses_a_step_of_its_width() {
        let db = LogDb::open(Config {
            compaction: CompactionConfig {
                min_age: Duration::ZERO,
                fan_in: 2,
                ..CompactionConfig::default()
            },
            ..config()
        })
        .await
        .unwrap();
        write_day(&db).await;
        let before = rows(&db).await;
        let config = RollupConfig::default();

        assert_eq!(db.rollup_at(&config, DAY + 21 * HOUR).await.unwrap(), 5);
        let window = SegmentId::new(DAY, 4 * HOUR);
        let runs = runs_of(&db, window).await;
        // Two streams, each with rows at :01 and :31 of every hour.
        assert_eq!(runs.len(), 2 * 2 * 4);
        for run in &runs {
            assert_eq!(
                crate::object::slice_of(run.min_timestamp_ns, 300 * 1_000_000_000),
                crate::object::slice_of(run.max_timestamp_ns, 300 * 1_000_000_000),
            );
        }

        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 1);
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 1);
        let runs = runs_of(&db, day()).await;
        assert_eq!(runs.len(), 2 * 2 * 24);
        assert_eq!(rows(&db).await, before);
        db.close().await.unwrap();
    }

    #[test]
    fn slices_a_rollup_segment_at_the_step_of_its_width() {
        use crate::object::run_slice_ns;
        let minutes = |m: i64| m * 60_000_000_000;
        assert_eq!(run_slice_ns(SegmentId::new(DAY, HOUR), HOUR), None);
        assert_eq!(
            run_slice_ns(SegmentId::new(DAY, 2 * HOUR), HOUR),
            Some(minutes(1))
        );
        assert_eq!(
            run_slice_ns(SegmentId::new(DAY, 4 * HOUR), HOUR),
            Some(minutes(5))
        );
        assert_eq!(
            run_slice_ns(SegmentId::new(DAY, 8 * HOUR), HOUR),
            Some(minutes(5))
        );
        assert_eq!(
            run_slice_ns(SegmentId::new(DAY, 12 * HOUR), HOUR),
            Some(minutes(15))
        );
        assert_eq!(run_slice_ns(day(), HOUR), Some(minutes(30)));
        // A step's window ends on its boundary, so a slice does too.
        assert_eq!(
            crate::object::slice_of(DAY + minutes(5), minutes(5)),
            crate::object::slice_of(DAY + 1, minutes(5))
        );
        assert_ne!(
            crate::object::slice_of(DAY + minutes(5), minutes(5)),
            crate::object::slice_of(DAY + minutes(5) + 1, minutes(5))
        );
    }

    #[tokio::test]
    async fn waits_for_storage_whenever_the_copy_buffer_fills() {
        let db = LogDb::open(config()).await.unwrap();
        write_day(&db).await;
        let before = rows(&db).await;

        let config = RollupConfig {
            copy_buffer_bytes: 1,
            ..RollupConfig::default()
        };
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 1);

        assert_eq!(db.rollups(&namespace()).sealed(), &[day()]);
        assert_eq!(rows(&db).await, before);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn rolls_a_closed_day_into_one_segment_serving_the_same_rows() {
        let db = LogDb::open(config()).await.unwrap();
        write_day(&db).await;
        let before = rows(&db).await;
        assert_eq!(before.len(), 2 * 24 * 2);
        let names = db
            .label_names(&namespace(), DAY, DAY + 3 * HOUR)
            .await
            .unwrap();

        // Closed with no rollup yet, the day is copied once, directly.
        let config = RollupConfig::default();
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 1);

        let rollups = db.rollups(&namespace());
        assert_eq!(rollups.sealed(), &[day()]);
        assert_merged(&db, day());
        assert_eq!(
            db.visible_segment_ids(&namespace(), (DAY, DAY + 24 * HOUR - 1), false)
                .unwrap(),
            vec![day()]
        );
        assert_eq!(rows(&db).await, before);
        assert_eq!(queried(&db).await, before.len());
        assert_eq!(
            db.label_names(&namespace(), DAY, DAY + 3 * HOUR)
                .await
                .unwrap(),
            names
        );
        assert_eq!(
            db.label_values(&namespace(), "service", DAY + HOUR, DAY + 2 * HOUR)
                .await
                .unwrap(),
            vec!["api".to_owned(), "worker".to_owned()]
        );
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 0);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn gives_streams_sharing_coarse_labels_neighbouring_ids() {
        let db = LogDb::open(config()).await.unwrap();
        // Each hour brings in new apps on both clusters, so the copy finds
        // the clusters' streams interleaved.
        for hour in 0..8 {
            let batches = ["c0", "c1"]
                .map(|cluster| {
                    let labels = Labels::new(vec![
                        Label::new("app", format!("a{hour}")),
                        Label::new("cluster", cluster),
                    ])
                    .unwrap();
                    LogBatch::new(labels, vec![LogEntry::new(DAY + hour * HOUR, "line")])
                })
                .to_vec();
            db.write(&namespace(), batches).await.unwrap();
        }
        db.flush_memtable().await.unwrap();
        assert_eq!(
            db.rollup_at(&RollupConfig::default(), after_delay())
                .await
                .unwrap(),
            1
        );

        let mut ids: BTreeMap<String, Vec<StreamId>> = BTreeMap::new();
        for read in db
            .segment_reads(&namespace(), day(), usize::MAX)
            .await
            .unwrap()
        {
            for (id, labels) in read.streams() {
                let cluster = labels.iter().find(|label| label.name == "cluster").unwrap();
                ids.entry(cluster.value.clone()).or_default().push(id);
            }
        }
        for streams in ids.values_mut() {
            streams.sort_unstable();
        }
        let first = ids["c0"][0].min(ids["c1"][0]);
        assert_eq!(ids["c0"], (first..first + 8).collect::<Vec<_>>());
        assert_eq!(ids["c1"], (first + 8..first + 16).collect::<Vec<_>>());
        db.close().await.unwrap();
    }

    /// `segment` is one sorted run.
    fn assert_merged(db: &LogDb, segment: SegmentId) {
        let manifest = db.storage.slate_read().unwrap().manifest();
        let prefix = segment_prefix(&namespace(), segment);
        let live = manifest
            .segments()
            .iter()
            .find(|live| live.prefix().as_ref() == prefix.as_ref())
            .unwrap();
        assert_eq!(live.l0().len() + live.compacted().len(), 1, "{segment}");
    }

    #[tokio::test]
    async fn rolls_up_windows_as_the_day_goes_then_the_day_from_them() {
        let db = LogDb::open(config()).await.unwrap();
        write_day(&db).await;
        let before = rows(&db).await;
        let config = RollupConfig::default();

        assert_eq!(db.rollup_at(&config, DAY + 21 * HOUR).await.unwrap(), 5);
        let windows: Vec<SegmentId> = (0..5)
            .map(|index| SegmentId::new(DAY + index * 4 * HOUR, 4 * HOUR))
            .collect();
        assert_eq!(db.rollups(&namespace()).sealed(), windows.as_slice());
        for window in &windows {
            assert_merged(&db, *window);
        }
        let mut visible = windows.clone();
        visible.extend((20..24).map(|hour| SegmentId::new(DAY + hour * HOUR, HOUR)));
        assert_eq!(
            db.visible_segment_ids(&namespace(), (DAY, DAY + 24 * HOUR - 1), false)
                .unwrap(),
            visible
        );
        assert_eq!(rows(&db).await, before);

        // The day's last window first, then the day from its windows.
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 1);
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 1);
        assert!(db.rollups(&namespace()).sealed().contains(&day()));
        assert_merged(&db, day());
        assert_eq!(
            db.visible_segment_ids(&namespace(), (DAY, DAY + 24 * HOUR - 1), false)
                .unwrap(),
            vec![day()]
        );
        assert_eq!(rows(&db).await, before);
        assert_eq!(queried(&db).await, before.len());
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 0);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn routes_late_writes_to_the_rollup_segment() {
        let db = LogDb::open(config()).await.unwrap();
        write_day(&db).await;
        db.rollup_at(&RollupConfig::default(), after_delay())
            .await
            .unwrap();

        let late = DAY + 5 * HOUR + 45 * 60_000_000_000;
        db.write(
            &namespace(),
            vec![LogBatch::new(
                stream("api"),
                vec![LogEntry::new(late, "late")],
            )],
        )
        .await
        .unwrap();

        // Reads skip the covered hour, so the row is only found in the
        // rollup segment.
        assert!(
            rows(&db)
                .await
                .iter()
                .any(|(_, at, line)| *at == late && line == "late")
        );
        let hour = SegmentId::new(DAY + 5 * HOUR, HOUR);
        assert_eq!(
            db.segment_reads(&namespace(), hour, 256)
                .await
                .unwrap()
                .len(),
            1
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn finishes_an_interrupted_rollup_without_duplicating_results() {
        let db = LogDb::open(config()).await.unwrap();
        write_day(&db).await;
        let before = rows(&db).await;
        // A rollup that copied a few hours, then stopped before its seal.
        db.writer()
            .unwrap()
            .apply(vec![RecordOp::put_with_ttl(
                marker_key(
                    crate::codec::KEY_SCOPE,
                    &namespace(),
                    Marker::Pending,
                    day(),
                ),
                Bytes::new(),
                Ttl::NoExpiry,
            )])
            .await
            .unwrap();
        db.rolled
            .write()
            .await
            .entry(namespace())
            .or_default()
            .insert(day());
        for hour in 0..5 {
            let hour = SegmentId::new(DAY + hour * HOUR, HOUR);
            db.copy_segment(&namespace(), hour, day(), u64::MAX, &mut 0)
                .await
                .unwrap();
        }
        db.flush_memtable().await.unwrap();
        assert_eq!(rows(&db).await, before);

        assert_eq!(
            db.rollup_at(&RollupConfig::default(), after_delay())
                .await
                .unwrap(),
            1
        );
        assert_eq!(db.rollups(&namespace()).sealed(), &[day()]);
        // The hours copied twice are stored twice, and queried once.
        assert_eq!(rows(&db).await.len(), before.len() + 2 * 5 * 2);
        assert_eq!(queried(&db).await, before.len());
        db.close().await.unwrap();
    }
}
