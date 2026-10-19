// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

//! Rollup of closed segments into wider ones; see
//! [`common::storage::rollup`].
//!
//! A rollup re-writes every trace of the segments it covers through the
//! ordinary write path, so the rollup segment gets its own pages, postings
//! and catalog terms. A trace whose only page is in a covered segment gets
//! a head replacing its old one, so it stays one point read; any other
//! trace gets a continuation into the rollup segment. Reads skip locators
//! into segments a seal covers, and drop spans a trace's pages repeat
//! until the seal. A rollup interrupted before its seal is copied
//! again: replaced heads already point into the rollup segment and are
//! skipped, and merging a trace's continuations drops exact duplicate
//! spans. Copied pages expire a retention after the copy rather than after
//! their first write.

use common::storage::WriteOptions;
use common::storage::rollup::{Marker, RollupConfig, marker_key};

use super::*;

const COPY_WRITE_TIMEOUT: Duration = Duration::from_secs(60);

impl TraceDb {
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
        let base_ns = i64::try_from(self.segment_ns).unwrap_or(i64::MAX);
        let mut sealed = 0;
        for (namespace, rollups) in Rollups::load_all(&slate, crate::codec::KEY_SCOPE) {
            for wide in rollups.plan(base_ns, None, config, self.retention, now_ns) {
                self.roll_up(&namespace, wide, config).await?;
                sealed += 1;
            }
        }
        Ok(sealed)
    }

    pub(crate) fn rollup_writer(&self) -> Result<&Arc<dyn Storage>> {
        self.writer
            .as_ref()
            .ok_or_else(|| Error::Invalid("writes are unavailable on a read-only database".into()))
    }

    /// Copies what reads see of every segment `wide` covers into it, has it
    /// merged into one sorted run, then seals it.
    async fn roll_up(
        &self,
        namespace: &Namespace,
        wide: SegmentId,
        config: &RollupConfig,
    ) -> Result<()> {
        let writer = self.rollup_writer()?;
        let ttl = retention_values(self.retention)?.physical_ttl;
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
            self.flush().await?;
        }
        let base_ns = i64::try_from(self.segment_ns).unwrap_or(i64::MAX);
        for segment in self.rollups(namespace).sources(wide, base_ns) {
            self.copy_segment(namespace, segment, wide, config.copy_buffer_bytes)
                .await?;
        }
        self.flush().await?;
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
                    "sealing traces rollup segment before its merge"
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
        tracing::info!(%namespace, segment = %wide, "sealed traces rollup segment");
        Ok(())
    }

    /// Copies `segment` into `wide` in steps of about `buffer` page bytes.
    /// Each step waits for the one before it to be written.
    async fn copy_segment(
        &self,
        namespace: &Namespace,
        segment: SegmentId,
        wide: SegmentId,
        buffer: u64,
    ) -> Result<()> {
        let now = unix_time_ms()?;
        let mut sequences = Vec::new();
        let mut metadata = self
            .storage
            .scan_prefix_iter(
                metadata_prefix(namespace, segment),
                BytesRange::unbounded(),
                None,
            )
            .await?;
        while let Some(record) = metadata.next().await? {
            let page = decode_metadata(&record.value)?;
            if page
                .expires_at_unix_ms
                .is_none_or(|expires_at| expires_at > now)
            {
                sequences.push(decode_metadata_sequence(&record.key)?);
            }
        }
        let mut traces = Vec::new();
        let mut chunk = 0;
        for sequence in sequences {
            let Some(payload) = self
                .storage
                .get(payload_key(namespace, segment, sequence))
                .await?
            else {
                continue;
            };
            let page = Page::decode(payload.value)?;
            for index in 0..page.directory().len() {
                traces.push(page.decode_trace(index)?);
            }
            chunk += page.retained_bytes() as u64;
            if chunk >= buffer {
                self.copy_traces(namespace, (segment, wide), std::mem::take(&mut traces))
                    .await?;
                chunk = 0;
            }
        }
        if !traces.is_empty() {
            self.copy_traces(namespace, (segment, wide), traces).await?;
        }
        Ok(())
    }

    /// Copies `traces` of `segment` into `wide`.
    async fn copy_traces(
        &self,
        namespace: &Namespace,
        (segment, wide): (SegmentId, SegmentId),
        traces: Vec<Trace>,
    ) -> Result<()> {
        // A trace written in several flushes has a page in each.
        let traces = merge_traces(traces)?;
        // With no write in flight and none pending, a head read here stays
        // current until this copy is applied: a later write to the trace
        // lands in this copy's flush or after it.
        let _routing = self.rolled.write().await;
        let mut flush = self
            .write_handle()?
            .flush(false)
            .await
            .map_err(map_write_error)?;
        flush
            .wait(CoordinatorDurability::Written)
            .await
            .map_err(map_write_error)?;
        let now = unix_time_ms()?;
        let ids = traces
            .iter()
            .map(|trace| trace.trace_id)
            .collect::<Vec<_>>();
        let heads = stream::iter(ids)
            .map(|trace_id| async move { self.live_head(namespace, trace_id, now).await })
            .buffered(READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let mut rebased = Vec::new();
        let mut copied = Vec::with_capacity(traces.len());
        for (trace, head) in traces.into_iter().zip(heads) {
            match head {
                Some(head) if !head.continued && head.first.segment == wide => continue,
                Some(head) if !head.continued && head.first.segment == segment => {
                    rebased.push(trace.trace_id);
                }
                _ => {}
            }
            copied.push(trace);
        }
        if copied.is_empty() {
            return Ok(());
        }
        let report = WriteReport {
            traces: copied.len(),
            pages: 0,
            spans: copied.iter().map(|trace| trace.spans().count()).sum(),
        };
        self.apply_groups(
            namespace,
            BTreeMap::from([(wide, copied)]),
            report,
            rebased,
            Some(COPY_WRITE_TIMEOUT),
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig, StorageConfig};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    use super::*;
    use crate::PageConfig;

    const HOUR: u64 = 3_600 * 1_000_000_000;
    /// 2024-01-01.
    const DAY: u64 = 19_723 * 24 * HOUR;

    fn config() -> Config {
        Config {
            storage: StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "traces-rollup-test".to_owned(),
                object_store: ObjectStoreConfig::InMemory,
                settings_path: None,
                block_cache: None,
                meta_cache: None,
                disk: Default::default(),
            }),
            segment_duration: Duration::from_secs(3_600),
            retention: None,
            page: PageConfig {
                target_size_bytes: 64 * 1024,
                max_size_bytes: 128 * 1024,
                max_traces: 4,
            },
            write_buffer: Default::default(),
            read_cache: Default::default(),
        }
    }

    fn namespace() -> Namespace {
        Namespace::new("tenant").unwrap()
    }

    fn trace(id: u8, span: u8, start_ns: u64, name: &str) -> Trace {
        let trace_id = TraceId::new([id; 16]).unwrap();
        Trace::new(
            trace_id,
            vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: trace_id.as_bytes().to_vec(),
                        span_id: [span; 8].to_vec(),
                        name: name.to_owned(),
                        start_time_unix_nano: start_ns,
                        end_time_unix_nano: start_ns + 100,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        )
        .unwrap()
    }

    fn ids() -> Vec<TraceId> {
        (1..=24)
            .chain([99])
            .map(|id| TraceId::new([id; 16]).unwrap())
            .collect()
    }

    async fn publish(db: &TraceDb) {
        db.flush().await.unwrap();
        db.rollup_writer().unwrap().flush_memtable().await.unwrap();
    }

    /// A trace per hour, and one written in two flushes of hour 3.
    async fn write_day(db: &TraceDb) {
        let traces = (0..24)
            .map(|hour| {
                trace(
                    hour as u8 + 1,
                    1,
                    DAY + hour * HOUR + 60,
                    &format!("hour-{hour}"),
                )
            })
            .collect();
        db.write(&namespace(), vec![TraceBatch::new(traces)])
            .await
            .unwrap();
        for (span, offset) in [(1, 0), (2, 600)] {
            let part = trace(99, span, DAY + 3 * HOUR + offset, &format!("split-{span}"));
            db.write(&namespace(), vec![TraceBatch::new(vec![part])])
                .await
                .unwrap();
        }
        publish(db).await;
    }

    /// Each trace's spans, by span ID.
    async fn spans(db: &TraceDb) -> Vec<(TraceId, Vec<Vec<u8>>)> {
        let mut found = Vec::new();
        for trace_id in ids() {
            let trace = db.get_trace(&namespace(), trace_id).await.unwrap().unwrap();
            let mut spans: Vec<_> = trace.spans().map(|span| span.span_id.clone()).collect();
            spans.sort();
            found.push((trace_id, spans));
        }
        found
    }

    async fn names(db: &TraceDb, start_ns: u64, end_ns: u64) -> Vec<String> {
        let mut names: Vec<_> = db
            .query_traceql(
                &namespace(),
                start_ns,
                end_ns,
                "{}",
                QueryOptions::default(),
            )
            .await
            .unwrap()
            .into_iter()
            .flat_map(|result| result.matched_spans.into_iter().map(|span| span.name))
            .collect();
        names.sort();
        names
    }

    fn day() -> SegmentId {
        SegmentId::new(DAY as i64, 24 * HOUR as i64)
    }

    /// Single-page traces read from one head into the rollup segment; the
    /// trace written in two flushes keeps its continuations.
    async fn assert_heads_rebased(db: &TraceDb) {
        for trace_id in ids() {
            let head = db
                .live_head(&namespace(), trace_id, unix_time_ms().unwrap())
                .await
                .unwrap()
                .unwrap();
            if trace_id == TraceId::new([99; 16]).unwrap() {
                assert!(head.continued);
            } else {
                assert!(!head.continued, "{trace_id:?}");
                assert_eq!(head.first.segment, day());
            }
        }
    }

    fn after_delay() -> i64 {
        (DAY + 26 * HOUR) as i64
    }

    #[tokio::test]
    async fn copies_in_steps_of_the_copy_buffer() {
        let db = TraceDb::open(config()).await.unwrap();
        write_day(&db).await;
        let before = spans(&db).await;

        let config = RollupConfig {
            copy_buffer_bytes: 1,
            ..RollupConfig::default()
        };
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 1);

        assert_eq!(db.rollups(&namespace()).sealed(), &[day()]);
        assert_eq!(spans(&db).await, before);
        assert_heads_rebased(&db).await;
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn rolls_a_closed_day_into_one_segment_serving_the_same_traces() {
        let db = TraceDb::open(config()).await.unwrap();
        write_day(&db).await;
        let before = spans(&db).await;
        assert_eq!(before.last().unwrap().1.len(), 2);
        let window = names(&db, DAY + 3 * HOUR, DAY + 6 * HOUR - 1).await;
        assert_eq!(window.len(), 5, "{window:?}");

        // Closed with no rollup yet, the day is copied once, directly.
        let config = RollupConfig::default();
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 1);

        assert_eq!(db.rollups(&namespace()).sealed(), &[day()]);
        assert_eq!(
            db.catalog_segments(&namespace(), DAY + 5 * HOUR, DAY + 6 * HOUR)
                .await
                .unwrap(),
            BTreeSet::from([day()])
        );
        assert_eq!(spans(&db).await, before);
        assert_eq!(names(&db, DAY + 3 * HOUR, DAY + 6 * HOUR - 1).await, window);
        assert_heads_rebased(&db).await;
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 0);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn rolls_up_windows_as_the_day_goes_then_the_day_from_them() {
        let db = TraceDb::open(config()).await.unwrap();
        write_day(&db).await;
        let before = spans(&db).await;
        let window = names(&db, DAY + 3 * HOUR, DAY + 6 * HOUR - 1).await;
        let config = RollupConfig::default();

        assert_eq!(
            db.rollup_at(&config, (DAY + 21 * HOUR) as i64)
                .await
                .unwrap(),
            5
        );
        let windows: Vec<SegmentId> = (0..5)
            .map(|index| SegmentId::new((DAY + index * 4 * HOUR) as i64, 4 * HOUR as i64))
            .collect();
        assert_eq!(db.rollups(&namespace()).sealed(), windows.as_slice());
        assert_eq!(spans(&db).await, before);
        assert_eq!(names(&db, DAY + 3 * HOUR, DAY + 6 * HOUR - 1).await, window);

        // The day's last window first, then the day from its windows.
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 1);
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 1);
        assert!(db.rollups(&namespace()).sealed().contains(&day()));
        assert_eq!(
            db.catalog_segments(&namespace(), DAY + 5 * HOUR, DAY + 6 * HOUR)
                .await
                .unwrap(),
            BTreeSet::from([day()])
        );
        assert_eq!(spans(&db).await, before);
        assert_eq!(names(&db, DAY + 3 * HOUR, DAY + 6 * HOUR - 1).await, window);
        assert_heads_rebased(&db).await;
        assert_eq!(db.rollup_at(&config, after_delay()).await.unwrap(), 0);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn routes_late_writes_to_the_rollup_segment() {
        let db = TraceDb::open(config()).await.unwrap();
        write_day(&db).await;
        db.rollup_at(&RollupConfig::default(), after_delay())
            .await
            .unwrap();

        let late = trace(200, 1, DAY + 5 * HOUR + 1_000, "late");
        db.write(&namespace(), vec![TraceBatch::new(vec![late.clone()])])
            .await
            .unwrap();

        assert_eq!(
            db.get_trace(&namespace(), late.trace_id).await.unwrap(),
            Some(late)
        );
        assert!(
            names(&db, DAY + 5 * HOUR, DAY + 6 * HOUR - 1)
                .await
                .contains(&"late".to_owned())
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn finishes_an_interrupted_rollup_without_duplicating_spans() {
        let db = TraceDb::open(config()).await.unwrap();
        write_day(&db).await;
        let before = spans(&db).await;
        let window = names(&db, DAY, DAY + 24 * HOUR - 1).await;
        // A rollup that copied a few hours, then stopped before its seal.
        db.rollup_writer()
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
            let hour = SegmentId::new((DAY + hour * HOUR) as i64, HOUR as i64);
            db.copy_segment(&namespace(), hour, day(), u64::MAX)
                .await
                .unwrap();
        }
        publish(&db).await;
        assert_eq!(spans(&db).await, before);
        assert_eq!(names(&db, DAY, DAY + 24 * HOUR - 1).await, window);

        assert_eq!(
            db.rollup_at(&RollupConfig::default(), after_delay())
                .await
                .unwrap(),
            1
        );
        assert_eq!(spans(&db).await, before);
        assert_eq!(names(&db, DAY, DAY + 24 * HOUR - 1).await, window);
        assert_heads_rebased(&db).await;
        db.close().await.unwrap();
    }
}
