// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::time::Duration;

use common::storage::config::{
    LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
};

use super::*;
use crate::codec::{decode_run, encode_run, segment_run_prefix, stream_run_prefix};
use crate::config::{CompactionConfig, PageConfig};

fn test_config() -> Config {
    Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "logs-test".to_owned(),
            object_store: ObjectStoreConfig::InMemory,
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        }),
        segment_duration: Duration::from_secs(10),
        discovery_rollup: Some(Duration::from_secs(20)),
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
        // Read-counting tests count storage reads.
        block_cache_capacity_bytes: 0,
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
async fn discovery_unions_many_segments_and_overlapping_selectors() {
    let db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::new("discovery-many").unwrap();
    let segment = 10_000_000_000_i64;
    let batches = (0..40)
        .map(|index| {
            let environment = if index % 2 == 0 { "prod" } else { "staging" };
            LogBatch::new(
                labels(&format!("svc-{:02}", index % 7), environment),
                vec![LogEntry::new(index * segment + 1, "line")],
            )
        })
        .collect::<Vec<_>>();
    db.write(&namespace, batches).await.unwrap();
    let end = 40 * segment;

    let values = db
        .label_values(&namespace, "service", 0, end)
        .await
        .unwrap();
    assert_eq!(
        values,
        (0..7)
            .map(|index| format!("svc-{index:02}"))
            .collect::<Vec<_>>()
    );
    let series = db
        .series(
            &namespace,
            &[
                r#"{environment="prod"}"#.to_owned(),
                r#"{environment="prod",service="svc-01"}"#.to_owned(),
                r#"{service=~"svc-0[01]"}"#.to_owned(),
            ],
            0,
            end,
        )
        .await
        .unwrap();
    let mut expected = (0..40)
        .filter_map(|index: i64| {
            let service = format!("svc-{:02}", index % 7);
            let environment = if index % 2 == 0 { "prod" } else { "staging" };
            (environment == "prod" || index % 7 <= 1).then(|| labels(&service, environment))
        })
        .collect::<Vec<_>>();
    expected.sort();
    expected.dedup();
    assert_eq!(series, expected);
    db.close().await.unwrap();
}

#[tokio::test]
async fn reader_discovery_sees_late_writes_to_closed_partitions() {
    const SECOND: i64 = 1_000_000_000;
    let directory = tempfile::tempdir().unwrap();
    let mut config = test_config();
    config.retention = None;
    config.storage = local_storage(&directory, "logs-late-discovery");
    let namespace = Namespace::new("late-discovery").unwrap();
    let writer = LogDb::open(config.clone()).await.unwrap();
    writer
        .write_with_durability(
            &namespace,
            vec![
                LogBatch::new(labels("api", "prod"), vec![LogEntry::new(1, "a")]),
                LogBatch::new(
                    labels("cart", "prod"),
                    vec![LogEntry::new(12 * SECOND, "b")],
                ),
            ],
            Durability::Durable,
        )
        .await
        .unwrap();
    let reader = LogDb::open_reader(
        config,
        DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(50),
            ..DbReaderOptions::default()
        },
    )
    .await
    .unwrap();
    let selector = [r#"{environment="prod"}"#.to_owned()];
    // 0..15s reads both 10s segments; 0..20s reads their 20s rollup period.
    let ranges = [(0, 15 * SECOND), (0, 20 * SECOND - 1)];
    for (start, end) in ranges {
        assert_eq!(
            reader.label_names(&namespace, start, end).await.unwrap(),
            vec!["environment", "service"]
        );
        assert_eq!(
            reader
                .label_values(&namespace, "service", start, end)
                .await
                .unwrap(),
            vec!["api", "cart"]
        );
        assert_eq!(
            reader
                .series(&namespace, &selector, start, end)
                .await
                .unwrap(),
            vec![labels("api", "prod"), labels("cart", "prod")]
        );
    }

    // Long after segment 0s..10s closed, a late export adds a stream there.
    let late = Labels::new(vec![
        Label::new("service", "worker"),
        Label::new("environment", "prod"),
        Label::new("region", "eu"),
    ])
    .unwrap();
    let counter = next_stream_id_key(&namespace, 0);
    let before = reader
        .partition_version(&namespace, Partition::Segment(0))
        .await
        .unwrap();
    writer
        .write_with_durability(
            &namespace,
            vec![LogBatch::new(late.clone(), vec![LogEntry::new(2, "c")])],
            Durability::Durable,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while reader
            .storage
            .get(counter.clone())
            .await
            .unwrap()
            .map(|record| decode_stream_id(&record.value).unwrap())
            == before
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("reader observes the late stream");

    let mut expected = vec![labels("api", "prod"), labels("cart", "prod"), late];
    expected.sort();
    for (start, end) in ranges {
        assert_eq!(
            reader.label_names(&namespace, start, end).await.unwrap(),
            vec!["environment", "region", "service"]
        );
        assert_eq!(
            reader
                .label_values(&namespace, "service", start, end)
                .await
                .unwrap(),
            vec!["api", "cart", "worker"]
        );
        let mut series = reader
            .series(&namespace, &selector, start, end)
            .await
            .unwrap();
        series.sort();
        assert_eq!(series, expected);
    }
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn cached_closed_segments_observe_late_writes() {
    let db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::new("late").unwrap();
    let other = Namespace::new("late-other").unwrap();
    let selector = [r#"{environment="prod"}"#.to_owned()];
    for ns in [&namespace, &other] {
        db.write(
            ns,
            vec![LogBatch::new(
                labels("api", "prod"),
                vec![LogEntry::new(1, "a")],
            )],
        )
        .await
        .unwrap();
    }
    assert_eq!(
        db.label_values(&namespace, "service", 0, 5).await.unwrap(),
        vec!["api"]
    );
    assert_eq!(
        db.series(&namespace, &selector, 0, 5).await.unwrap(),
        vec![labels("api", "prod")]
    );
    db.label_values(&other, "service", 0, 5).await.unwrap();
    let series_key = (
        namespace.clone(),
        Partition::Segment(0),
        vec![Label::new("environment", "prod")],
    );
    assert!(db.caches.series.get(&series_key).is_some());
    let other_key = (other.clone(), Partition::Segment(0), "service".to_owned());
    assert!(db.caches.label_values.get(&other_key).is_some());

    db.write(
        &namespace,
        vec![LogBatch::new(
            labels("worker", "prod"),
            vec![LogEntry::new(2, "b")],
        )],
    )
    .await
    .unwrap();
    assert_eq!(
        db.label_values(&other, "service", 0, 5).await.unwrap(),
        vec!["api"]
    );
    assert_eq!(
        db.label_names(&namespace, 0, 5).await.unwrap(),
        vec!["environment", "service"]
    );
    assert_eq!(
        db.label_values(&namespace, "service", 0, 5).await.unwrap(),
        vec!["api", "worker"]
    );
    assert_eq!(
        db.series(&namespace, &selector, 0, 5).await.unwrap(),
        vec![labels("api", "prod"), labels("worker", "prod")]
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limited_backward_queries_keep_the_newest_rows_of_a_large_page() {
    use crate::{Direction, QueryOptions, QueryRequest, QueryResult};

    // One page larger than a pipeline batch is read as a single ascending
    // chunk, so its newest rows are in its last batches.
    let db = LogDb::open(Config {
        retention: None,
        page: PageConfig {
            target_size_bytes: 1 << 20,
            max_rows: 10_000,
            rows_per_block: 512,
        },
        ..test_config()
    })
    .await
    .unwrap();
    let namespace = Namespace::new("large-page").unwrap();
    let entries = (1..=5_000)
        .map(|timestamp| LogEntry::new(timestamp, format!("line-{timestamp}")))
        .collect();
    db.write(
        &namespace,
        vec![LogBatch::new(labels("api", "prod"), entries)],
    )
    .await
    .unwrap();
    let QueryResult::Streams(streams) = db
        .query(
            &namespace,
            &QueryRequest::range(r#"{service="api"}"#, 0, 5_001, 1),
            QueryOptions {
                limit: 100,
                direction: Direction::Backward,
                ..QueryOptions::default()
            },
        )
        .await
        .unwrap()
    else {
        panic!("expected streams");
    };
    let timestamps = streams[0]
        .entries
        .iter()
        .map(|entry| entry.timestamp_ns)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, (4_901..=5_000).rev().collect::<Vec<_>>());
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
    let mut runs = db
        .storage
        .scan_prefix_iter(
            stream_run_prefix(&namespace, 0, stream_id),
            BytesRange::unbounded(),
            None,
        )
        .await
        .unwrap();
    let mut run_count = 0;
    while runs.next().await.unwrap().is_some() {
        run_count += 1;
    }
    assert_eq!(run_count, 1);
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
async fn each_flush_appends_posting_blocks_without_reading_the_index() {
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
    assert_eq!(stats.blocks, 5);

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
            .estimate(false),
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
async fn reopened_writers_resume_stream_and_object_ids() {
    let directory = tempfile::tempdir().unwrap();
    let config = Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "resume-ids".to_owned(),
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
    let api = Label::new("service", "api");
    let worker = Label::new("service", "worker");
    let db = LogDb::open(config.clone()).await.unwrap();
    db.write(
        &namespace,
        vec![LogBatch::new(
            labels("api", "prod"),
            vec![LogEntry::new(1, "needle first")],
        )],
    )
    .await
    .unwrap();
    let api_id = db
        .stream_ids(&namespace, 0, std::slice::from_ref(&api))
        .await
        .unwrap();
    db.close().await.unwrap();

    let reopened = LogDb::open(config).await.unwrap();
    reopened
        .write(
            &namespace,
            vec![
                LogBatch::new(
                    labels("api", "prod"),
                    vec![LogEntry::new(2, "needle again")],
                ),
                LogBatch::new(
                    labels("worker", "prod"),
                    vec![LogEntry::new(3, "needle new")],
                ),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        reopened.stream_ids(&namespace, 0, &[api]).await.unwrap(),
        api_id
    );
    let worker_id = reopened.stream_ids(&namespace, 0, &[worker]).await.unwrap();
    assert_eq!(worker_id.len(), 1);
    assert!(!api_id.contains(&worker_id[0]));

    let rows = reopened
        .read(&namespace, 0, 5, &[Label::new("environment", "prod")])
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.entry.line.as_str())
            .collect::<Vec<_>>(),
        vec!["needle first", "needle again", "needle new"]
    );
    let terms = vec!["needle".to_owned()];
    let matched = reopened
        .read_match_bounded(
            &namespace,
            &reopened
                .scan_targets(&namespace, 0, 5, &StreamFilter::exact(Vec::new()))
                .await
                .unwrap(),
            (&terms, None),
            10,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(matched.len(), 3);
    reopened.close().await.unwrap();
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
    let mut run_records = db
        .storage
        .scan_prefix_iter(
            stream_run_prefix(&namespace, 0, stream_id),
            BytesRange::unbounded(),
            None,
        )
        .await
        .unwrap();
    let record = run_records.next().await.unwrap().unwrap();
    let mut run = decode_run(&record.value).unwrap();
    run.expires_at_unix_ms = Some(0);
    db.writer
        .as_ref()
        .unwrap()
        .apply(vec![RecordOp::put_with_ttl(
            record.key,
            encode_run(&run).unwrap(),
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

    // Eight level-0 objects fold into one level-3 object within the flushes.
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::Run).await,
        1
    );
    let end = segment + 100;
    assert_eq!(lines(&db, &namespace, segment, end).await, expected);
    assert_eq!(
        match_lines(&db, &namespace, segment, end, 1).await.unwrap(),
        expected
    );
    // Replaced objects stay readable until a later flush deletes them.
    assert!(count_records(&db, &namespace, segment, RecordType::ObjectTombstone).await > 0);

    write_line(&db, &namespace, "worker", segment + 50, "other stream").await;
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::ObjectTombstone).await,
        0
    );
    // The merged object and the worker's object; replaced blocks are gone,
    // leaving a meta and a lines value per remaining block.
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::ObjectDirectory).await,
        2
    );
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::ObjectBlock).await,
        6
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

    let runs = count_records(&db, &namespace, 0, RecordType::Run).await;
    assert!(runs <= 3, "six late writes left {runs} runs");
    assert_eq!(lines(&db, &namespace, 0, 10).await, expected);
    assert_eq!(
        match_lines(&db, &namespace, 0, 10, 3).await.unwrap(),
        expected
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn open_segments_merge_runs_the_page_limits_cut_short() {
    use crate::codec::RecordType;

    let mut config = compacting_config();
    config.compaction.fan_in = 8;
    let db = LogDb::open(config).await.unwrap();
    let namespace = Namespace::new("cut-short").unwrap();
    let segment = current_segment();
    // Twenty-row objects are small, but only three fit within `max_rows`.
    let expected = (0..80)
        .map(|index| format!("needle {index}"))
        .collect::<Vec<_>>();
    for (batch, lines) in (0..).zip(expected.chunks(20)) {
        let entries = (0..)
            .zip(lines)
            .map(|(offset, line)| LogEntry::new(segment + batch * 20 + offset, line))
            .collect();
        db.write(
            &namespace,
            vec![LogBatch::new(labels("api", "prod"), entries)],
        )
        .await
        .unwrap();
    }

    // The first three merge once the fourth cannot join them.
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::Run).await,
        2
    );
    let end = segment + 100;
    assert_eq!(lines(&db, &namespace, segment, end).await, expected);
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
        count_records(&db, &namespace, segment, RecordType::ObjectTombstone).await,
        2
    );
    db.close().await.unwrap();

    let db = LogDb::open(config).await.unwrap();
    write_line(&db, &namespace, "api", segment + 3, &expected[2]).await;
    // Recovery re-queued the tombstones, and the merged object is tracked.
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::ObjectTombstone).await,
        0
    );
    write_line(&db, &namespace, "api", segment + 4, &expected[3]).await;
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::Run).await,
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

/// Counts gets and prefix scans whose key or prefix falls under each watched
/// prefix.
struct CountingStorage {
    inner: Arc<dyn StorageRead>,
    watched: Vec<Bytes>,
    gets: Vec<std::sync::atomic::AtomicUsize>,
    scans: Vec<std::sync::atomic::AtomicUsize>,
}

impl CountingStorage {
    fn record(&self, counts: &[std::sync::atomic::AtomicUsize], key: &[u8]) {
        for (prefix, count) in self.watched.iter().zip(counts) {
            if key.starts_with(prefix) {
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    fn take(&self) -> Vec<(usize, usize)> {
        use std::sync::atomic::Ordering::Relaxed;
        self.gets
            .iter()
            .zip(&self.scans)
            .map(|(gets, scans)| (gets.swap(0, Relaxed), scans.swap(0, Relaxed)))
            .collect()
    }
}

#[async_trait::async_trait]
impl StorageRead for CountingStorage {
    async fn get(
        &self,
        key: Bytes,
    ) -> common::storage::StorageResult<Option<common::storage::Record>> {
        self.record(&self.gets, &key);
        self.inner.get(key).await
    }

    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> common::storage::StorageResult<Box<dyn common::storage::StorageIterator + Send + 'static>>
    {
        self.inner.scan_iter(range).await
    }

    async fn scan_prefix_iter(
        &self,
        prefix: Bytes,
        subrange: BytesRange,
        filter_context: Option<slatedb::FilterContext>,
    ) -> common::storage::StorageResult<Box<dyn common::storage::StorageIterator + Send + 'static>>
    {
        self.record(&self.scans, &prefix);
        self.inner
            .scan_prefix_iter(prefix, subrange, filter_context)
            .await
    }
}

fn count_reads(db: &mut LogDb, watched: Vec<Bytes>) -> Arc<CountingStorage> {
    let counting = Arc::new(CountingStorage {
        inner: Arc::clone(&db.storage),
        gets: watched.iter().map(|_| Default::default()).collect(),
        scans: watched.iter().map(|_| Default::default()).collect(),
        watched,
    });
    db.storage = counting.clone();
    counting
}

/// Forty streams in one segment, alternating environments, so `prod`
/// selects every other stream ID.
async fn write_forty_streams(db: &LogDb, namespace: &Namespace) {
    let batches = (0..40)
        .map(|index| {
            let environment = if index % 2 == 0 { "prod" } else { "staging" };
            LogBatch::new(
                labels(&format!("s{index:02}"), environment),
                vec![
                    LogEntry::new(1_000 + index, format!("line {index}")),
                    LogEntry::new(2_000 + index, format!("again {index}")),
                ],
            )
        })
        .collect();
    db.write(namespace, batches).await.unwrap();
}

#[tokio::test]
async fn span_scans_and_point_reads_select_the_same_rows() {
    let mut db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    write_forty_streams(&db, &namespace).await;
    let selectors = [
        vec![],
        vec![Label::new("environment", "prod")],
        vec![Label::new("environment", "staging")],
        vec![Label::new("service", "s07")],
        vec![
            Label::new("service", "s08"),
            Label::new("environment", "prod"),
        ],
        vec![
            Label::new("service", "s08"),
            Label::new("environment", "staging"),
        ],
    ];
    for matchers in &selectors {
        let mut by_path = Vec::new();
        for min_streams in [usize::MAX, 1] {
            db.span_scan_min_streams = min_streams;
            let rows = db.read(&namespace, 0, 5_000, matchers).await.unwrap();
            by_path.push(
                rows.into_iter()
                    .map(|row| (row.entry.timestamp_ns, row.entry.line, row.labels))
                    .collect::<Vec<_>>(),
            );
        }
        assert!(!by_path[0].is_empty() || matchers.len() == 2);
        assert_eq!(by_path[0], by_path[1], "{matchers:?}");
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn many_selected_streams_cost_two_scans_per_segment() {
    let mut db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    write_forty_streams(&db, &namespace).await;
    let counting = count_reads(
        &mut db,
        vec![
            forward_prefix(&namespace, 0),
            segment_run_prefix(&namespace, 0),
        ],
    );
    let prod = [Label::new("environment", "prod")];

    let rows = db.read(&namespace, 0, 5_000, &prod).await.unwrap();
    assert_eq!(rows.len(), 40);
    // (forward gets, forward scans), (run gets, run scans)
    assert_eq!(counting.take(), vec![(0, 1), (0, 1)]);
    let rows = db.read(&namespace, 0, 5_000, &[]).await.unwrap();
    assert_eq!(rows.len(), 80);
    assert_eq!(counting.take(), vec![(0, 1), (0, 1)]);
    // Selected streams are cached until the segment gains one.
    let rows = db.read(&namespace, 0, 5_000, &prod).await.unwrap();
    assert_eq!(rows.len(), 40);
    assert_eq!(counting.take(), vec![(0, 0), (0, 1)]);

    db.span_scan_min_streams = usize::MAX;
    db.caches.streams.invalidate_all();
    let rows = db.read(&namespace, 0, 5_000, &prod).await.unwrap();
    assert_eq!(rows.len(), 40);
    assert_eq!(counting.take(), vec![(20, 0), (0, 20)]);

    db.write(
        &namespace,
        vec![LogBatch::new(
            labels("s40", "prod"),
            vec![LogEntry::new(1_040, "line 40")],
        )],
    )
    .await
    .unwrap();
    let rows = db.read(&namespace, 0, 5_000, &prod).await.unwrap();
    assert_eq!(rows.len(), 41);
    db.close().await.unwrap();
}

#[tokio::test]
async fn streams_flushed_together_are_read_with_one_object_scan() {
    use crate::codec::{RecordType, record_type_prefix};

    let mut config = test_config();
    config.page = PageConfig {
        target_size_bytes: 1 << 20,
        max_rows: 1_024,
        rows_per_block: 1,
    };
    let mut db = LogDb::open(config).await.unwrap();
    let namespace = Namespace::default();
    write_forty_streams(&db, &namespace).await;
    assert_eq!(
        count_records(&db, &namespace, 0, RecordType::ObjectDirectory).await,
        1
    );
    let counting = count_reads(
        &mut db,
        vec![record_type_prefix(&namespace, 0, RecordType::ObjectBlock)],
    );
    let ascending = |rows: &[LogRow]| {
        rows.windows(2)
            .all(|pair| pair[0].entry.timestamp_ns <= pair[1].entry.timestamp_ns)
    };

    let rows = db.read(&namespace, 0, 5_000, &[]).await.unwrap();
    assert_eq!(rows.len(), 80);
    assert!(ascending(&rows));
    // One scan each of the meta and lines groups.
    assert_eq!(counting.take(), vec![(0, 2)]);
    // Half the streams: runs separated by few unselected blocks share a scan.
    let prod = [Label::new("environment", "prod")];
    let rows = db.read(&namespace, 0, 5_000, &prod).await.unwrap();
    assert_eq!(rows.len(), 40);
    assert!(ascending(&rows));
    let [(gets, scans)] = counting.take()[..] else {
        unreachable!()
    };
    assert_eq!(gets, 0);
    assert!(scans < 10, "20 streams took {scans} scans");
    let one = [Label::new("service", "s07")];
    let rows = db.read(&namespace, 1_500, 5_000, &one).await.unwrap();
    assert_eq!(
        rows.into_iter()
            .map(|row| row.entry.line)
            .collect::<Vec<_>>(),
        ["again 7"]
    );
    assert_eq!(counting.take(), vec![(0, 2)]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn cached_blocks_answer_repeat_reads_and_gain_lines_when_needed() {
    use crate::codec::{RecordType, record_type_prefix};

    let mut config = test_config();
    config.block_cache_capacity_bytes = crate::DEFAULT_BLOCK_CACHE_CAPACITY_BYTES;
    config.page = PageConfig {
        target_size_bytes: 1 << 20,
        max_rows: 1_024,
        rows_per_block: 4,
    };
    let mut db = LogDb::open(config).await.unwrap();
    let namespace = Namespace::default();
    let second = 1_000_000_000;
    db.write(
        &namespace,
        vec![LogBatch::new(
            labels("api", "prod"),
            (1..=3)
                .map(|index| LogEntry::new(index * second, format!("line {index}")))
                .collect(),
        )],
    )
    .await
    .unwrap();
    let counting = count_reads(
        &mut db,
        vec![record_type_prefix(&namespace, 0, RecordType::ObjectBlock)],
    );
    // The window splits the block, so it is decoded from its meta value.
    let count = async |db: &LogDb| {
        let request =
            crate::QueryRequest::instant(r#"count_over_time({service="api"}[1s])"#, 2 * second + 1);
        format!(
            "{:?}",
            db.query(&namespace, &request, crate::QueryOptions::default())
                .await
                .unwrap()
        )
    };
    let lines = async |db: &LogDb| {
        db.read(&namespace, 0, 4 * second, &[])
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.entry.line)
            .collect::<Vec<_>>()
    };

    let counted = count(&db).await;
    assert_eq!(counting.take(), vec![(1, 0)]);
    assert_eq!(count(&db).await, counted);
    assert_eq!(counting.take(), vec![(0, 0)]);
    // A block cached without lines is read again with them.
    assert_eq!(lines(&db).await, ["line 1", "line 2", "line 3"]);
    assert_eq!(counting.take(), vec![(2, 0)]);
    assert_eq!(lines(&db).await, ["line 1", "line 2", "line 3"]);
    assert_eq!(count(&db).await, counted);
    assert_eq!(counting.take(), vec![(0, 0)]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn cross_stream_merges_keep_write_order_for_out_of_order_rows() {
    use crate::codec::RecordType;

    let db = LogDb::open(compacting_config()).await.unwrap();
    let namespace = Namespace::new("cross-stream").unwrap();
    let segment = current_segment();
    // Rows arrive out of order across flushes, two at one timestamp.
    let offsets = [8, 3, 5, 5];
    for (index, offset) in offsets.iter().enumerate() {
        db.write(
            &namespace,
            vec![
                LogBatch::new(
                    labels("api", "prod"),
                    vec![LogEntry::new(
                        segment + offset,
                        format!("needle api {index}"),
                    )],
                ),
                LogBatch::new(
                    labels("worker", "prod"),
                    vec![LogEntry::new(
                        segment + offset,
                        format!("needle worker {index}"),
                    )],
                ),
            ],
        )
        .await
        .unwrap();
    }
    // Four two-stream objects fold into one holding a run per stream.
    assert_eq!(
        count_records(&db, &namespace, segment, RecordType::Run).await,
        2
    );
    let expected = [
        "needle api 1",
        "needle api 2",
        "needle api 3",
        "needle api 0",
    ];
    let end = segment + 100;
    assert_eq!(lines(&db, &namespace, segment, end).await, expected);
    assert_eq!(
        match_lines(&db, &namespace, segment, end, 1).await.unwrap(),
        expected
    );
    // Reverse scans hand over ascending chunks, latest chunk first.
    let mut chunks = Vec::new();
    db.read_segments(
        &namespace,
        (segment, end),
        &StreamFilter::exact(vec![Label::new("service", "api")]),
        &PageBudget::new(usize::MAX),
        true,
        |rows| {
            chunks.push(
                rows.into_iter()
                    .map(|row| row.entry.line)
                    .collect::<Vec<_>>(),
            );
            Ok(ControlFlow::Continue(()))
        },
    )
    .await
    .unwrap();
    assert_eq!(
        chunks.into_iter().rev().flatten().collect::<Vec<_>>(),
        expected
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_satisfied_read_selects_at_most_one_segment_ahead() {
    let mut db = LogDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    let segment_ns = db.segment_ns;
    let segments = (0..6).map(|index| index * segment_ns).collect::<Vec<_>>();
    for &segment in &segments {
        write_line(&db, &namespace, "api", segment + 1, "line").await;
    }
    let counting = count_reads(
        &mut db,
        segments
            .iter()
            .map(|&segment| segment_run_prefix(&namespace, segment))
            .collect(),
    );
    db.read_segments(
        &namespace,
        (0, segments[5] + segment_ns - 1),
        &StreamFilter::exact(Vec::new()),
        &PageBudget::new(usize::MAX),
        false,
        |_| Ok(ControlFlow::Break(())),
    )
    .await
    .unwrap();
    let scans = counting
        .take()
        .into_iter()
        .map(|(_, scans)| scans)
        .collect::<Vec<_>>();
    assert_eq!(scans[0], 1);
    assert!(scans[1] <= 1);
    assert_eq!(scans[2..], [0, 0, 0, 0]);
    db.close().await.unwrap();
}

/// Deterministic xorshift, so failures reproduce from the seed.
struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }

    fn pick<'a>(&mut self, values: &[&'a str]) -> &'a str {
        values[self.below(values.len() as u64) as usize]
    }
}

fn random_batches(rng: &mut Rng, span_ns: i64) -> Vec<LogBatch> {
    (0..1 + rng.below(6))
        .map(|_| {
            let mut pairs = vec![
                Label::new("service", rng.pick(&["api", "cart", "auth", "billing"])),
                Label::new("environment", rng.pick(&["prod", "dev"])),
            ];
            if rng.below(3) == 0 {
                pairs.push(Label::new("zone", rng.pick(&["a", "b", "c"])));
            }
            let entries = (0..1 + rng.below(4))
                .map(|index| {
                    LogEntry::new(rng.below(span_ns as u64) as i64, format!("line {index}"))
                })
                .collect();
            LogBatch::new(Labels::new(pairs).unwrap(), entries)
        })
        .collect()
}

#[tokio::test]
async fn rollups_answer_discovery_exactly_like_segments() {
    let segment_ns = 10 * 1_000_000_000;
    let span_ns = 8 * segment_ns;
    let selectors = [
        r#"{environment="prod"}"#,
        r#"{service="api", environment="dev"}"#,
        r#"{service=~"a.*"}"#,
        r#"{zone="b"}"#,
        r#"{service="cart", zone!="a"}"#,
    ]
    .map(str::to_owned);
    for seed in [1, 7, 42, 1_234, 99_991] {
        let rolled = LogDb::open(test_config()).await.unwrap();
        let plain = LogDb::open(Config {
            discovery_rollup: None,
            ..test_config()
        })
        .await
        .unwrap();
        assert!(rolled.rollup_ns.is_some() && plain.rollup_ns.is_none());
        let namespace = Namespace::default();
        let mut rng = Rng(seed);
        // Reads between writes fill the caches, so later writes into closed
        // segments and periods also exercise invalidation.
        for _ in 0..6 {
            let batches = random_batches(&mut rng, span_ns);
            for db in [&rolled, &plain] {
                db.write(&namespace, batches.clone()).await.unwrap();
            }
            for _ in 0..8 {
                let start = rng.below(span_ns as u64) as i64;
                let end = start + rng.below((span_ns - start) as u64 + 1) as i64;
                let names = plain.label_names(&namespace, start, end).await.unwrap();
                assert_eq!(
                    rolled.label_names(&namespace, start, end).await.unwrap(),
                    names,
                    "seed {seed} [{start}, {end}]"
                );
                for name in names.iter().map(String::as_str).chain(["missing"]) {
                    assert_eq!(
                        rolled
                            .label_values(&namespace, name, start, end)
                            .await
                            .unwrap(),
                        plain
                            .label_values(&namespace, name, start, end)
                            .await
                            .unwrap(),
                        "seed {seed} {name} [{start}, {end}]"
                    );
                }
                for selector in &selectors {
                    let selector = std::slice::from_ref(selector);
                    assert_eq!(
                        rolled
                            .series(&namespace, selector, start, end)
                            .await
                            .unwrap(),
                        plain
                            .series(&namespace, selector, start, end)
                            .await
                            .unwrap(),
                        "seed {seed} {selector:?} [{start}, {end}]"
                    );
                }
            }
        }
        rolled.close().await.unwrap();
        plain.close().await.unwrap();
    }
}

#[tokio::test]
async fn whole_periods_are_read_from_the_rollup() {
    let db = LogDb::open(test_config()).await.unwrap();
    let second = 1_000_000_000;
    // Periods are 20s over 10s segments. Discovery reads whole touched
    // segments, so a range covers a period once it touches all its segments.
    assert_eq!(
        db.discovery_partitions(0, 40 * second - 1).unwrap(),
        vec![Partition::Rollup(0), Partition::Rollup(20 * second)]
    );
    assert_eq!(
        db.discovery_partitions(5 * second, 45 * second).unwrap(),
        vec![
            Partition::Rollup(0),
            Partition::Rollup(20 * second),
            Partition::Segment(40 * second),
        ]
    );
    assert_eq!(
        db.discovery_partitions(10 * second, 29 * second).unwrap(),
        vec![
            Partition::Segment(10 * second),
            Partition::Segment(20 * second)
        ]
    );
    assert_eq!(
        db.discovery_partitions(-20 * second, -1).unwrap(),
        vec![Partition::Rollup(-20 * second)]
    );

    // A late write into a cached closed period is visible afterwards.
    let namespace = Namespace::default();
    write_line(&db, &namespace, "api", second, "early").await;
    let whole = (0, 20 * second - 1);
    assert_eq!(
        db.label_values(&namespace, "service", whole.0, whole.1)
            .await
            .unwrap(),
        vec!["api"]
    );
    assert!(
        db.caches
            .label_values
            .get(&(
                namespace.clone(),
                Partition::Rollup(0),
                "service".to_owned()
            ))
            .is_some()
    );
    write_line(&db, &namespace, "worker", 15 * second, "late").await;
    assert_eq!(
        db.label_values(&namespace, "service", whole.0, whole.1)
            .await
            .unwrap(),
        vec!["api", "worker"]
    );
    db.close().await.unwrap();
}

#[test]
fn rollups_must_align_with_segments() {
    let config = |rollup| Config {
        discovery_rollup: rollup,
        ..test_config()
    };
    assert!(config(Some(Duration::from_secs(30))).validate().is_ok());
    assert!(config(None).validate().is_ok());
    assert!(config(Some(Duration::from_secs(25))).validate().is_err());
    assert!(config(Some(Duration::ZERO)).validate().is_err());
}

#[test]
fn window_boundaries_split_only_the_spans_they_fall_inside() {
    // Windows (5, 10], (15, 20], (25, 30]: boundaries 5, 15, 25 and 10, 20, 30.
    let read = SampleRead {
        metadata: false,
        boundaries: vec![
            Boundaries {
                first: 10,
                step: 10,
                count: 3,
            },
            Boundaries {
                first: 5,
                step: 10,
                count: 3,
            },
        ],
    };
    assert!(!read.splits(6, 10));
    assert!(read.splits(9, 11));
    assert!(read.splits(5, 6));
    assert!(!read.splits(11, 15));
    assert!(!read.splits(31, 99));
    assert!(!read.splits(-50, 5));
    assert!(read.splits(-50, 6));
    assert!(!read.splits(10, 10));
    let instant = SampleRead {
        metadata: false,
        boundaries: vec![Boundaries {
            first: i64::MAX,
            step: 1,
            count: 1,
        }],
    };
    assert!(!instant.splits(i64::MIN, i64::MAX));
    assert!(!instant.splits(i64::MAX, i64::MAX));
    let near_max = SampleRead {
        metadata: false,
        boundaries: vec![Boundaries {
            first: i64::MAX - 1,
            step: 1,
            count: 1,
        }],
    };
    assert!(near_max.splits(i64::MIN, i64::MAX));
    assert!(!near_max.splits(i64::MIN, i64::MAX - 1));
}

/// Writes of two streams over three segments, with lines of equal length,
/// repeated timestamps and structured metadata on some rows; a repeated
/// write duplicates rows across objects. With `errors`, a rare row carries
/// `__error__` metadata, which fails the metric queries that see it.
fn lineless_batches(rng: &mut Rng, previous: &[LogBatch], errors: bool) -> Vec<LogBatch> {
    if !previous.is_empty() && rng.below(4) == 0 {
        return previous.to_vec();
    }
    let services: Vec<_> = ["api", "cart"]
        .into_iter()
        .filter(|_| rng.below(3) > 0)
        .collect();
    services
        .into_iter()
        .map(|service| {
            let entries = (0..1 + rng.below(9))
                .map(|_| {
                    let timestamp = rng.below(120) as i64 * 250_000_000;
                    let line = rng.pick(&["GET /a", "GET /b", "POST /checkout", ""]);
                    match rng.below(if errors { 60 } else { 3 }) {
                        59 => LogEntry::with_structured_metadata(
                            timestamp,
                            line,
                            crate::Fields::new(vec![crate::Field::new("__error__", "bad")])
                                .unwrap(),
                        ),
                        0..20 if errors => LogEntry::new(timestamp, line),
                        0 | 20..40 => LogEntry::with_structured_metadata(
                            timestamp,
                            line,
                            crate::Fields::new(vec![crate::Field::new(
                                "pod",
                                rng.pick(&["x", "y"]),
                            )])
                            .unwrap(),
                        ),
                        _ => LogEntry::new(timestamp, line),
                    }
                })
                .collect();
            LogBatch::new(labels(service, "prod"), entries)
        })
        .collect()
}

/// Lineless metric reads count from run records, block headers and meta
/// values; a no-op line filter forces the full-row read, which must agree.
#[tokio::test]
async fn lineless_metric_reads_match_full_row_reads() {
    use crate::query::{QueryOptions, QueryRequest};

    const MS: i64 = 1_000_000;
    let ops = ["count_over_time", "rate", "bytes_over_time", "bytes_rate"];
    let wrappers = [
        "{}",
        "sum({})",
        "sum by (service) ({})",
        "sum by (pod) ({})",
        "sum without (pod) ({})",
        "sum({}) + sum by (service) ({})",
    ];
    let selectors = [r#"{environment="prod"}"#, r#"{service="api"}"#];
    for seed in 1..=30u64 {
        let db = LogDb::open(Config {
            retention: None,
            page: PageConfig {
                target_size_bytes: 4096,
                max_rows: 12,
                rows_per_block: 3,
            },
            ..test_config()
        })
        .await
        .unwrap();
        let namespace = Namespace::default();
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let mut previous = Vec::new();
        for _ in 0..1 + rng.below(6) {
            let batches = lineless_batches(&mut rng, &previous, seed % 5 == 0);
            db.write(&namespace, batches.clone()).await.unwrap();
            previous = batches;
        }
        for _ in 0..40 {
            let op = ops[rng.below(ops.len() as u64) as usize];
            let selector = selectors[rng.below(2) as usize];
            let range = [1_000, 2_500, 5_000, 7_000][rng.below(4) as usize];
            let offset = [0, 1_000, 750][rng.below(3) as usize];
            let window = |filter: &str| {
                let offset = if offset == 0 {
                    String::new()
                } else {
                    format!(" offset {offset}ms")
                };
                format!("{op}({selector}{filter} [{range}ms]{offset})")
            };
            let wrapper = wrappers[rng.below(wrappers.len() as u64) as usize];
            let (fast, slow) = (
                wrapper.replace("{}", &window("")),
                wrapper.replace("{}", &window(r#" |= """#)),
            );
            let start = rng.below(140) as i64 * 250 * MS;
            let (fast, slow) = if rng.below(4) == 0 {
                (
                    QueryRequest::instant(fast, start),
                    QueryRequest::instant(slow, start),
                )
            } else {
                let end = start + rng.below(80) as i64 * 250 * MS;
                let step = [250, 1_000, 3_000, 5_000][rng.below(4) as usize] * MS;
                (
                    QueryRequest::range(fast, start, end, step),
                    QueryRequest::range(slow, start, end, step),
                )
            };
            let result = async |request| {
                db.query(&namespace, request, QueryOptions::default())
                    .await
                    .map_err(|error| error.to_string())
            };
            assert_eq!(
                result(&fast).await,
                result(&slow).await,
                "seed {seed}: {fast:?}"
            );
        }
        db.close().await.unwrap();
    }
}

/// Runs counted from their records leave gaps between the runs decoded
/// around them, which the query's page estimate covered in one read unit.
#[tokio::test]
async fn lineless_reads_stay_within_the_page_estimate() {
    use crate::query::{QueryOptions, QueryRequest};

    // One object of three streams' runs in one-row blocks. The window
    // (5s, 30s] splits two of them; the third lies inside it and is counted,
    // leaving a 12-block gap when its run falls between the other two.
    for counted in 0..3 {
        let db = LogDb::open(Config {
            segment_duration: Duration::from_secs(60),
            discovery_rollup: None,
            retention: None,
            page: PageConfig {
                target_size_bytes: 1 << 20,
                max_rows: 1_000,
                rows_per_block: 1,
            },
            ..test_config()
        })
        .await
        .unwrap();
        let namespace = Namespace::default();
        let batches = (0..3)
            .map(|stream| {
                let seconds = if stream == counted { 6..18 } else { 0..36 };
                let entries = seconds
                    .map(|second| LogEntry::new(second * 1_000_000_000, "GET /a"))
                    .collect();
                LogBatch::new(labels(&format!("s{stream}"), "prod"), entries)
            })
            .collect();
        db.write(&namespace, batches).await.unwrap();
        let result = async |query: &str, max_pages| {
            db.query(
                &namespace,
                &QueryRequest::instant(query, 30_000_000_000),
                QueryOptions {
                    max_pages,
                    ..QueryOptions::default()
                },
            )
            .await
            .map_err(|error| error.to_string())
        };
        let full = r#"sum(count_over_time({environment="prod"} |= "" [25s]))"#;
        let mut max_pages = 1;
        let expected = loop {
            match result(full, max_pages).await {
                Err(error) if error.contains("max_pages") => max_pages += 1,
                other => break other,
            }
        };
        assert!(expected.is_ok(), "{expected:?}");
        assert_eq!(
            result(
                r#"sum(count_over_time({environment="prod"} [25s]))"#,
                max_pages
            )
            .await,
            expected,
            "stream {counted} counted, {max_pages} pages"
        );
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn page_limit_counts_objects_instead_of_sparse_read_ranges() {
    use crate::query::{QueryOptions, QueryRequest};

    let db = LogDb::open(Config {
        segment_duration: Duration::from_secs(60),
        discovery_rollup: None,
        retention: None,
        page: PageConfig {
            target_size_bytes: 1 << 20,
            max_rows: 1_000,
            rows_per_block: 1,
        },
        ..test_config()
    })
    .await
    .unwrap();
    let namespace = Namespace::default();

    // Pick labels whose fingerprint order is selected, unselected, selected.
    // The middle twelve-block run forces two sparse read ranges in one object.
    let mut candidates = (0..100)
        .map(|index| {
            labels(
                &format!("s{index}"),
                if index % 2 == 0 { "prod" } else { "dev" },
            )
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(Labels::fingerprint);
    let selected = candidates
        .windows(3)
        .find(|window| {
            let environment = |labels: &Labels, wanted: &str| {
                labels
                    .iter()
                    .any(|label| label.name == "environment" && label.value == wanted)
            };
            environment(&window[0], "prod")
                && environment(&window[1], "dev")
                && environment(&window[2], "prod")
        })
        .expect("candidate fingerprints contain prod/dev/prod")
        .to_vec();
    let batches = selected
        .into_iter()
        .map(|labels| {
            LogBatch::new(
                labels,
                (1..=12)
                    .map(|second| LogEntry::new(second * 1_000_000_000, "line"))
                    .collect(),
            )
        })
        .collect();
    db.write(&namespace, batches).await.unwrap();

    let targets = db
        .scan_targets(
            &namespace,
            0,
            20_000_000_000,
            &StreamFilter::exact(vec![Label::new("environment", "prod")]),
        )
        .await
        .unwrap();
    let estimate = targets.estimate(false);
    assert_eq!(estimate.pages, 1);
    assert_eq!(estimate.read_units, 2);
    assert_eq!(targets.estimate(true).read_units, 1);

    let result = db
        .query(
            &namespace,
            &QueryRequest::instant(
                r#"sum(count_over_time({environment="prod"}[20s]))"#,
                20_000_000_000,
            ),
            QueryOptions {
                max_pages: 1,
                ..QueryOptions::default()
            },
        )
        .await;
    assert!(result.is_ok(), "{result:?}");
    db.close().await.unwrap();
}

/// A line of words from a small vocabulary plus a rare request ID, so some
/// interior terms are rare enough to prefilter.
fn prefilter_line(rng: &mut Rng) -> String {
    format!(
        "{} {} {} req{} {}",
        rng.pick(&["GET", "POST", "get"]),
        rng.pick(&["/api/users", "/api/cart", "/Api/users"]),
        rng.pick(&["200", "500", "timeout"]),
        rng.below(300),
        rng.pick(&["user@example.com", "https://x.io/a?b=1", "done", "-"]),
    )
}

/// `|=` filters whose interior terms narrow reads through postings return
/// what the same filter returns unnarrowed, as a two-branch filter is.
#[tokio::test]
async fn prefiltered_line_filters_match_unfiltered_reads() {
    use crate::codec::RecordType;
    use crate::query::{Direction, QueryOptions, QueryRequest};

    let mut narrowed_reads = 0;
    let mut plain_reads = 0;
    for seed in 1..=10u64 {
        let mut db = LogDb::open(compacting_config()).await.unwrap();
        let namespace = Namespace::default();
        let segment = current_segment();
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let mut written = Vec::new();
        for _ in 0..12 {
            let batches = ["api", "cart", "web"]
                .into_iter()
                .map(|service| {
                    let entries = (0..1 + rng.below(12))
                        .map(|_| {
                            let line = prefilter_line(&mut rng);
                            written.push(line.clone());
                            LogEntry::new(segment + rng.below(20_000) as i64 * 1_000_000, line)
                        })
                        .collect();
                    LogBatch::new(labels(service, "prod"), entries)
                })
                .collect();
            db.write(&namespace, batches).await.unwrap();
        }
        let counting = count_reads(
            &mut db,
            vec![crate::codec::record_type_prefix(
                &namespace,
                segment,
                RecordType::ObjectBlock,
            )],
        );
        let end = segment + 20_000_000_000;
        for _ in 0..30 {
            let line = &written[rng.below(written.len() as u64) as usize];
            let needle = if rng.below(5) == 0 {
                "GET /api/users req999 x".to_owned()
            } else {
                let start = rng.below(line.len() as u64 / 2) as usize;
                line[start..line.len() - rng.below(line.len() as u64 / 3) as usize].to_owned()
            };
            let needle = needle.replace('"', "");
            let selector = r#"{environment="prod"}"#;
            let shape = rng.below(4);
            let query = |filter: String| match shape {
                0 => format!("{selector} {filter}"),
                1 => format!("count_over_time({selector} {filter} [5s])"),
                2 => format!("sum by (service) (bytes_over_time({selector} {filter} [3s]))"),
                _ => format!("{selector} {filter} | logfmt | __error__=\"\""),
            };
            let narrowed = query(format!("|= \"{needle}\""));
            let plain = query(format!("|= \"{needle}\" or \"{needle}\""));
            let options = QueryOptions {
                limit: [3, 1_000][rng.below(2) as usize],
                direction: [Direction::Forward, Direction::Backward][rng.below(2) as usize],
                ..QueryOptions::default()
            };
            let run = async |query: String| {
                let request = QueryRequest::range(query, segment, end, 1_000_000_000);
                let result = db.query(&namespace, &request, options.clone()).await;
                (
                    result.map_err(|error| error.to_string()),
                    counting.take()[0],
                )
            };
            let (narrowed_result, (gets, scans)) = run(narrowed.clone()).await;
            narrowed_reads += gets + scans;
            let (plain_result, (gets, scans)) = run(plain).await;
            plain_reads += gets + scans;
            assert_eq!(narrowed_result, plain_result, "seed {seed}: {narrowed}");
        }
        db.close().await.unwrap();
    }
    assert!(
        narrowed_reads < plain_reads,
        "{narrowed_reads} block reads with postings, {plain_reads} without"
    );
}
