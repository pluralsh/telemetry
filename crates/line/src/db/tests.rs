// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::time::Duration;

use common::storage::config::{
    LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
};

use super::*;
use crate::config::{CompactionConfig, PageConfig};

fn test_config() -> Config {
    Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "line-test".to_owned(),
            object_store: ObjectStoreConfig::InMemory,
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        }),
        segment_duration: Duration::from_secs(10),
        retention: Some(Duration::from_secs(60)),
        page: PageConfig {
            target_size_bytes: 64,
            max_rows: 2,
            rows_per_block: 1,
        },
        compaction: CompactionConfig {
            enabled: false,
            ..CompactionConfig::default()
        },
        write_buffer: Default::default(),
    }
}

fn labels(service: &str, environment: &str) -> Labels {
    Labels::new(vec![
        Label::new("service", service),
        Label::new("environment", environment),
    ])
    .unwrap()
}

#[tokio::test]
async fn writes_and_reads_pages_through_slatedb() {
    let db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::new("tenant-a").unwrap();
    let report = db
        .write(
            &namespace,
            vec![
                LogBatch::new(
                    labels("api", "prod"),
                    vec![
                        LogEntry::new(1, "one"),
                        LogEntry::new(2, "two"),
                        LogEntry::new(11_000_000_000, "next segment"),
                    ],
                ),
                LogBatch::new(labels("worker", "prod"), vec![LogEntry::new(3, "work")]),
            ],
        )
        .await
        .unwrap();
    assert_eq!(report.rows, 4);
    assert_eq!(report.streams, 3);

    let prod = db
        .read(
            &namespace,
            0,
            12_000_000_000,
            &[Label::new("environment", "prod")],
        )
        .await
        .unwrap();
    assert_eq!(prod.len(), 4);

    let api = db
        .read(
            &namespace,
            0,
            12_000_000_000,
            &[
                Label::new("environment", "prod"),
                Label::new("service", "api"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        api.iter()
            .map(|row| row.entry.line.as_str())
            .collect::<Vec<_>>(),
        vec!["one", "two", "next segment"]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn repeated_writes_merge_label_postings() {
    let db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    db.write(
        &namespace,
        vec![LogBatch::new(
            labels("api", "prod"),
            vec![LogEntry::new(1, "a")],
        )],
    )
    .await
    .unwrap();
    db.write(
        &namespace,
        vec![LogBatch::new(
            labels("worker", "prod"),
            vec![LogEntry::new(2, "b")],
        )],
    )
    .await
    .unwrap();
    let rows = db
        .read(&namespace, 0, 5, &[Label::new("environment", "prod")])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn discovers_stream_labels_and_series_across_segments() {
    let db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::new("discovery").unwrap();
    db.write(
        &namespace,
        vec![
            LogBatch::new(labels("api", "prod"), vec![LogEntry::new(1, "first")]),
            LogBatch::new(
                labels("worker", "staging"),
                vec![LogEntry::new(11_000_000_000, "second")],
            ),
        ],
    )
    .await
    .unwrap();

    assert_eq!(
        db.label_names(&namespace, 0, 12_000_000_000).await.unwrap(),
        vec!["environment", "service"]
    );
    assert_eq!(
        db.label_values(&namespace, "service", 0, 12_000_000_000)
            .await
            .unwrap(),
        vec!["api", "worker"]
    );
    assert_eq!(
        db.label_values(&namespace, "environment", 0, 9_000_000_000)
            .await
            .unwrap(),
        vec!["prod"]
    );

    let series = db
        .series(
            &namespace,
            &[r#"{service=~"api|worker",environment!="staging"}"#.to_owned()],
            0,
            12_000_000_000,
        )
        .await
        .unwrap();
    assert_eq!(series, vec![labels("api", "prod")]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn streams_in_a_segment_share_one_id_space() {
    let db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::new("segment-ids").unwrap();

    db.write(
        &namespace,
        vec![
            LogBatch::new(labels("api", "prod"), vec![LogEntry::new(1, "first")]),
            LogBatch::new(labels("worker", "prod"), vec![LogEntry::new(2, "second")]),
        ],
    )
    .await
    .unwrap();

    let mut ids = db.stream_ids(&namespace, 0, &[]).await.unwrap();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1]);
    assert_eq!(db.read(&namespace, 0, 3, &[]).await.unwrap().len(), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn limited_log_queries_stop_within_a_segment() {
    use crate::{Direction, QueryOptions, QueryRequest, QueryResult};

    let db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::new("early-stop").unwrap();
    let entries = (1..=10)
        .map(|timestamp| LogEntry::new(timestamp, format!("line-{timestamp}")))
        .collect();
    db.write(
        &namespace,
        vec![LogBatch::new(labels("api", "prod"), entries)],
    )
    .await
    .unwrap();
    let first = |direction| {
        let db = &db;
        let namespace = &namespace;
        async move {
            let result = db
                .query(
                    namespace,
                    &QueryRequest::range(r#"{service="api"}"#, 0, 11, 1),
                    QueryOptions {
                        limit: 1,
                        max_pages: 2,
                        direction,
                        ..QueryOptions::default()
                    },
                )
                .await
                .unwrap();
            let QueryResult::Streams(streams) = result else {
                panic!("expected streams");
            };
            streams[0].entries[0].timestamp_ns
        }
    };

    // Five two-row pages share one segment; one page answers each query.
    assert_eq!(first(Direction::Forward).await, 1);
    assert_eq!(first(Direction::Backward).await, 10);
    db.close().await.unwrap();
}

#[tokio::test]
async fn regex_and_negative_matchers_prune_streams_before_page_reads() {
    use crate::{QueryOptions, QueryRequest};

    let db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::new("prune").unwrap();
    let worker = (1..=10)
        .map(|timestamp| LogEntry::new(timestamp, "worker"))
        .collect();
    db.write(
        &namespace,
        vec![
            LogBatch::new(labels("api", "prod"), vec![LogEntry::new(1, "api")]),
            LogBatch::new(labels("worker", "prod"), worker),
        ],
    )
    .await
    .unwrap();

    for query in [
        r#"count_over_time({service=~"a.i"}[10s])"#,
        r#"count_over_time({environment="prod", service!="worker"}[10s])"#,
        r#"count_over_time({environment="prod", service!~"work.*"}[10s])"#,
    ] {
        db.query(
            &namespace,
            &QueryRequest::instant(query, 10),
            QueryOptions {
                max_pages: 1,
                ..QueryOptions::default()
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn identical_writes_allocate_distinct_pages() {
    let db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    let batch = || {
        LogBatch::new(
            labels("api", "prod"),
            vec![LogEntry::new(1, "same accepted entry")],
        )
    };

    db.write(&namespace, vec![batch()]).await.unwrap();
    db.write(&namespace, vec![batch()]).await.unwrap();

    let rows = db
        .read(&namespace, 0, 5, &[Label::new("service", "api")])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].entry, rows[1].entry);
    db.close().await.unwrap();
}

#[tokio::test]
async fn applied_writes_coalesce_before_flush() {
    let db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::new("coalesced").unwrap();
    let labels = labels("api", "prod");

    for (timestamp, line) in [(1, "first"), (2, "second")] {
        let report = db
            .write_with_durability(
                &namespace,
                vec![LogBatch::new(
                    labels.clone(),
                    vec![LogEntry::new(timestamp, line)],
                )],
                Durability::Applied,
            )
            .await
            .unwrap();
        assert_eq!(report.rows, 1);
        assert_eq!(report.streams, 1);
        assert_eq!(report.pages, 0);
    }

    assert!(db.read(&namespace, 0, 3, &[]).await.unwrap().is_empty());
    db.flush().await.unwrap();

    let rows = db.read(&namespace, 0, 3, &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let stream_id = db.stream_ids(&namespace, 0, &[]).await.unwrap()[0];
    let mut pages = db
        .storage
        .scan_prefix_iter(
            metadata_prefix(&namespace, 0, stream_id),
            BytesRange::unbounded(),
            None,
        )
        .await
        .unwrap();
    let mut page_count = 0;
    while pages.next().await.unwrap().is_some() {
        page_count += 1;
    }
    assert_eq!(page_count, 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn written_and_durable_force_the_expected_flushes() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = test_config();
    config.storage = StorageConfig::SlateDb(SlateDbStorageConfig {
        path: "durability-levels".to_owned(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: directory.path().to_string_lossy().into_owned(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    });
    let namespace = Namespace::new("durability").unwrap();
    let db = LogDb::open(config.clone()).await.unwrap();

    db.write_with_durability(
        &namespace,
        vec![LogBatch::new(
            labels("api", "prod"),
            vec![LogEntry::new(1, "applied")],
        )],
        Durability::Applied,
    )
    .await
    .unwrap();
    assert!(db.read(&namespace, 0, 3, &[]).await.unwrap().is_empty());

    db.write_with_durability(
        &namespace,
        vec![LogBatch::new(
            labels("api", "prod"),
            vec![LogEntry::new(2, "written")],
        )],
        Durability::Written,
    )
    .await
    .unwrap();
    assert_eq!(db.read(&namespace, 0, 3, &[]).await.unwrap().len(), 2);

    db.write_with_durability(
        &namespace,
        vec![LogBatch::new(
            labels("api", "prod"),
            vec![LogEntry::new(3, "durable")],
        )],
        Durability::Durable,
    )
    .await
    .unwrap();
    db.close().await.unwrap();

    let reopened = LogDb::open(config).await.unwrap();
    assert_eq!(reopened.read(&namespace, 0, 4, &[]).await.unwrap().len(), 3);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn flush_drains_and_close_stops_the_coordinator() {
    let db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::new("flush-close").unwrap();
    db.write_with_durability(
        &namespace,
        vec![LogBatch::new(
            labels("api", "prod"),
            vec![LogEntry::new(1, "pending")],
        )],
        Durability::Applied,
    )
    .await
    .unwrap();

    db.flush().await.unwrap();
    assert_eq!(db.read(&namespace, 0, 2, &[]).await.unwrap().len(), 1);
    db.close().await.unwrap();

    let error = db
        .write(
            &namespace,
            vec![LogBatch::new(
                labels("api", "prod"),
                vec![LogEntry::new(2, "after close")],
            )],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("shut down"));
}

#[tokio::test]
async fn small_writes_top_up_the_trailing_posting_block() {
    let db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    for timestamp in 1..=5 {
        db.write(
            &namespace,
            vec![LogBatch::new(
                labels("search", "prod"),
                vec![LogEntry::new(timestamp, "needle in haystack")],
            )],
        )
        .await
        .unwrap();
    }
    let stats = db
        .storage
        .get(crate::codec::term_stats_key(&namespace, 0, "needle"))
        .await
        .unwrap()
        .unwrap();
    let stats = crate::search::decode_term_stats(&stats.value).unwrap();
    assert_eq!(stats.documents, 5);
    assert_eq!(stats.blocks, 1);

    let terms = vec!["needle".to_owned()];
    for top_k in [None, Some(2)] {
        let rows = db
            .read_match_bounded(
                &namespace,
                &db.scan_targets(&namespace, 0, 10, &StreamFilter::exact(Vec::new()))
                    .await
                    .unwrap(),
                (&terms, top_k),
                10,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rows.len(), 5);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn logical_retention_filters_reads_estimates_and_bm25_before_compaction() {
    let mut config = test_config();
    config.retention = Some(Duration::from_millis(20));
    let db = LogDb::open(config).await.unwrap();
    let namespace = Namespace::default();
    db.write(
        &namespace,
        vec![LogBatch::new(
            labels("search", "prod"),
            vec![LogEntry::new(1, "needle")],
        )],
    )
    .await
    .unwrap();
    assert_eq!(db.read(&namespace, 0, 2, &[]).await.unwrap().len(), 1);

    tokio::time::sleep(Duration::from_millis(40)).await;

    assert!(db.read(&namespace, 0, 2, &[]).await.unwrap().is_empty());
    assert_eq!(
        db.scan_targets(&namespace, 0, 2, &StreamFilter::exact(Vec::new()))
            .await
            .unwrap()
            .estimate(),
        QueryEstimate::default()
    );
    let terms = vec!["needle".to_owned()];
    assert_eq!(
        db.read_match_bounded(
            &namespace,
            &db.scan_targets(&namespace, 0, 2, &StreamFilter::exact(Vec::new()))
                .await
                .unwrap(),
            (&terms, None),
            10
        )
        .await
        .unwrap(),
        Some(Vec::new())
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn persisted_logical_expiry_survives_reopen_without_physical_ttl() {
    let directory = tempfile::tempdir().unwrap();
    let config = Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "logical-expiry-reopen".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: directory.path().to_string_lossy().into_owned(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        }),
        ..test_config()
    };
    let namespace = Namespace::default();
    let db = LogDb::open(config.clone()).await.unwrap();
    db.write(
        &namespace,
        vec![LogBatch::new(
            labels("search", "prod"),
            vec![LogEntry::new(1, "needle")],
        )],
    )
    .await
    .unwrap();

    let stream_id = db.stream_ids(&namespace, 0, &[]).await.unwrap()[0];
    let mut metadata_records = db
        .storage
        .scan_prefix_iter(
            metadata_prefix(&namespace, 0, stream_id),
            BytesRange::unbounded(),
            None,
        )
        .await
        .unwrap();
    let record = metadata_records.next().await.unwrap().unwrap();
    let mut metadata = decode_metadata(&record.value).unwrap();
    metadata.expires_at_unix_ms = Some(0);
    db.writer
        .as_ref()
        .unwrap()
        .apply(vec![RecordOp::put_with_ttl(
            record.key,
            encode_metadata(&metadata).unwrap(),
            Ttl::NoExpiry,
        )])
        .await
        .unwrap();
    db.flush().await.unwrap();
    assert!(db.read(&namespace, 0, 2, &[]).await.unwrap().is_empty());
    db.close().await.unwrap();

    let reopened = LogDb::open(config).await.unwrap();
    assert!(
        reopened
            .read(&namespace, 0, 2, &[])
            .await
            .unwrap()
            .is_empty()
    );
    let terms = vec!["needle".to_owned()];
    assert_eq!(
        reopened
            .read_match_bounded(
                &namespace,
                &reopened
                    .scan_targets(&namespace, 0, 2, &StreamFilter::exact(Vec::new()))
                    .await
                    .unwrap(),
                (&terms, None),
                10
            )
            .await
            .unwrap(),
        Some(Vec::new())
    );
    reopened.close().await.unwrap();
}

const SEGMENT_NS: i64 = 10_000_000_000;

fn compacting_config() -> Config {
    Config {
        page: PageConfig {
            target_size_bytes: 4096,
            max_rows: 64,
            rows_per_block: 4,
        },
        compaction: CompactionConfig {
            enabled: true,
            fan_in: 2,
            min_age: Duration::ZERO,
            finalize_after: Duration::from_secs(3600),
            delete_delay: Duration::ZERO,
            max_merges_per_flush: 256,
        },
        ..test_config()
    }
}

fn local_storage(directory: &tempfile::TempDir, path: &str) -> StorageConfig {
    StorageConfig::SlateDb(SlateDbStorageConfig {
        path: path.to_owned(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: directory.path().to_string_lossy().into_owned(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    })
}

/// Start of the segment containing now, so it is still open.
fn current_segment() -> SegmentId {
    let now_ns = common::time::now_ns();
    crate::codec::segment_for(now_ns, SEGMENT_NS)
}

async fn count_records(
    db: &LogDb,
    namespace: &Namespace,
    segment: SegmentId,
    record_type: crate::codec::RecordType,
) -> usize {
    let mut records = db
        .storage
        .scan_prefix_iter(
            crate::codec::record_type_prefix(namespace, segment, record_type),
            BytesRange::unbounded(),
            None,
        )
        .await
        .unwrap();
    let mut count = 0;
    while records.next().await.unwrap().is_some() {
        count += 1;
    }
    count
}

async fn write_line(db: &LogDb, namespace: &Namespace, service: &str, timestamp: i64, line: &str) {
    db.write(
        namespace,
        vec![LogBatch::new(
            labels(service, "prod"),
            vec![LogEntry::new(timestamp, line)],
        )],
    )
    .await
    .unwrap();
}

async fn lines(db: &LogDb, namespace: &Namespace, start: i64, end: i64) -> Vec<String> {
    db.read(namespace, start, end, &[Label::new("service", "api")])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.entry.line)
        .collect()
}

async fn match_lines(
    db: &LogDb,
    namespace: &Namespace,
    start: i64,
    end: i64,
    max_pages: usize,
) -> Result<Vec<String>> {
    let terms = vec!["needle".to_owned()];
    let targets = db
        .scan_targets(
            namespace,
            start,
            end,
            &StreamFilter::exact(vec![Label::new("service", "api")]),
        )
        .await?;
    let mut rows = db
        .read_match_bounded(namespace, &targets, (&terms, None), max_pages)
        .await?
        .expect("indexed match");
    rows.sort_by_key(|(row, _)| row.entry.timestamp_ns);
    Ok(rows.into_iter().map(|(row, _)| row.entry.line).collect())
}

#[tokio::test]
async fn open_segments_merge_equal_level_pages_and_delete_replaced_payloads() {
    use crate::codec::RecordType;

    let db = LogDb::open(compacting_config()).await.unwrap();
    let namespace = Namespace::new("compaction").unwrap();
    let segment = current_segment();
    // Two writes share a timestamp; their order must survive merging.
    let timestamps = [1, 2, 3, 3, 5, 6, 7, 8].map(|offset| segment + offset);
    let expected = (0..timestamps.len())
        .map(|index| format!("needle {index}"))
        .collect::<Vec<_>>();
    for (timestamp, line) in timestamps.iter().zip(&expected) {
        write_line(&db, &namespace, "api", *timestamp, line).await;
    }

    // Eight level-0 pages fold into one level-3 page within the flushes.
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::PageMetadata).await,
        1
    );
    let end = segment + 100;
    assert_eq!(lines(&db, &namespace, segment, end).await, expected);
    assert_eq!(
        match_lines(&db, &namespace, segment, end, 1).await.unwrap(),
        expected
    );
    // Replaced payloads stay readable until a later flush deletes them.
    assert!(count_records(&db, &namespace, segment, RecordType::PageTombstone).await > 0);

    write_line(&db, &namespace, "worker", segment + 50, "other stream").await;
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::PageTombstone).await,
        0
    );
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::PagePayload).await,
        2
    );
    assert_eq!(lines(&db, &namespace, segment, end).await, expected);
    db.close().await.unwrap();
}

#[tokio::test]
async fn settled_segments_merge_below_fan_in() {
    use crate::codec::RecordType;

    let mut config = compacting_config();
    config.compaction.fan_in = 4;
    config.compaction.min_age = Duration::from_secs(3600);
    config.compaction.finalize_after = Duration::ZERO;
    let db = LogDb::open(config).await.unwrap();
    let namespace = Namespace::new("settled").unwrap();
    let expected = (1..=6)
        .map(|index| format!("needle {index}"))
        .collect::<Vec<_>>();
    for (timestamp, line) in (1..).zip(&expected) {
        write_line(&db, &namespace, "api", timestamp, line).await;
    }

    let pages = count_records(&db, &namespace, 0, RecordType::PageMetadata).await;
    assert!(pages <= 3, "six late writes left {pages} pages");
    assert_eq!(lines(&db, &namespace, 0, 10).await, expected);
    assert_eq!(
        match_lines(&db, &namespace, 0, 10, 3).await.unwrap(),
        expected
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn reopened_writers_rebuild_the_index_and_pending_deletes() {
    use crate::codec::RecordType;

    let directory = tempfile::tempdir().unwrap();
    let config = Config {
        storage: local_storage(&directory, "compaction-reopen"),
        ..compacting_config()
    };
    let namespace = Namespace::new("reopen").unwrap();
    let segment = current_segment();
    let expected = (1..=4)
        .map(|index| format!("needle {index}"))
        .collect::<Vec<_>>();

    let db = LogDb::open(config.clone()).await.unwrap();
    for (offset, line) in (1..).zip(&expected[..2]) {
        write_line(&db, &namespace, "api", segment + offset, line).await;
    }
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::PageTombstone).await,
        2
    );
    db.close().await.unwrap();

    let db = LogDb::open(config).await.unwrap();
    write_line(&db, &namespace, "api", segment + 3, &expected[2]).await;
    // Recovery re-queued the tombstones, and the merged page is tracked.
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::PageTombstone).await,
        0
    );
    write_line(&db, &namespace, "api", segment + 4, &expected[3]).await;
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::PageMetadata).await,
        1
    );
    let end = segment + 100;
    assert_eq!(lines(&db, &namespace, segment, end).await, expected);
    assert_eq!(
        match_lines(&db, &namespace, segment, end, 1).await.unwrap(),
        expected
    );
    db.close().await.unwrap();
}
