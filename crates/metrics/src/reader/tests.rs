use super::*;
use crate::model::Series;
use crate::storage::{SharedInMemoryStorage, in_memory_shared_storage};

/// Writer storage plus the shared object store, so the tests can open
/// real (non-fencing) `DbReader`-backed readers over the same data.
async fn create_shared_storage() -> SharedInMemoryStorage {
    in_memory_shared_storage().await
}

#[tokio::test]
async fn reader_sees_written_data() {
    // Write data through internal Tsdb, then verify reader sees it.
    let shared = create_shared_storage().await;

    let tsdb = crate::tsdb::Tsdb::new(shared.storage.clone());
    let series = vec![
        Series::builder("http_requests_total")
            .label("method", "GET")
            .label("status", "200")
            .sample(1700000000000, 100.0)
            .sample(1700000001000, 101.0)
            .build(),
    ];
    tsdb.ingest_samples(series, None).await.unwrap();
    tsdb.flush().await.unwrap();

    // Open reader on the same storage
    let reader = TimeSeriesDbReader::from_storage(shared.reader().await);

    // Query should find the data
    let query_time = SystemTime::UNIX_EPOCH + Duration::from_millis(1700000001000);
    let result = reader
        .query(
            &crate::Namespace::default(),
            "http_requests_total",
            Some(query_time),
        )
        .await
        .unwrap();

    match result {
        QueryValue::Vector(samples) => {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 101.0);
        }
        _ => panic!("expected Vector result"),
    }
}

#[tokio::test]
async fn reader_series_discovery() {
    let shared = create_shared_storage().await;

    let tsdb = crate::tsdb::Tsdb::new(shared.storage.clone());
    let series = vec![
        Series::builder("http_requests_total")
            .label("method", "GET")
            .sample(1700000000000, 100.0)
            .build(),
        Series::builder("http_requests_total")
            .label("method", "POST")
            .sample(1700000000000, 50.0)
            .build(),
        Series::builder("cpu_usage")
            .label("host", "server1")
            .sample(1700000000000, 0.75)
            .build(),
    ];
    tsdb.ingest_samples(series, None).await.unwrap();
    tsdb.flush().await.unwrap();

    let reader = TimeSeriesDbReader::from_storage(shared.reader().await);

    // Series discovery
    let series = reader
        .series(
            &crate::Namespace::default(),
            &["{__name__=~\"http_requests_total|cpu_usage\"}"],
            (SystemTime::UNIX_EPOCH + Duration::from_secs(1699999000))
                ..=(SystemTime::UNIX_EPOCH + Duration::from_secs(1700001000)),
        )
        .await
        .unwrap();
    assert_eq!(series.len(), 3);

    // Label names
    let labels = reader
        .labels(
            &crate::Namespace::default(),
            None,
            (SystemTime::UNIX_EPOCH + Duration::from_secs(1699999000))
                ..=(SystemTime::UNIX_EPOCH + Duration::from_secs(1700001000)),
        )
        .await
        .unwrap();
    assert!(labels.contains(&"__name__".to_string()));
    assert!(labels.contains(&"method".to_string()));
    assert!(labels.contains(&"host".to_string()));

    // Label values
    let values = reader
        .label_values(
            &crate::Namespace::default(),
            "method",
            None,
            (SystemTime::UNIX_EPOCH + Duration::from_secs(1699999000))
                ..=(SystemTime::UNIX_EPOCH + Duration::from_secs(1700001000)),
        )
        .await
        .unwrap();
    assert!(values.contains(&"GET".to_string()));
    assert!(values.contains(&"POST".to_string()));
}

#[tokio::test]
async fn writer_and_reader_coexist_on_shared_storage() {
    // Verify that a writer and a non-fencing reader can both operate on
    // the same shared object store without interfering with each other.
    let shared = create_shared_storage().await;

    let tsdb = crate::tsdb::Tsdb::new(shared.storage.clone());

    // Write initial data
    let series = vec![
        Series::builder("metric_a")
            .label("env", "prod")
            .sample(1700000000000, 1.0)
            .build(),
    ];
    tsdb.ingest_samples(series, None).await.unwrap();
    tsdb.flush().await.unwrap();

    // Open reader
    let reader = TimeSeriesDbReader::from_storage(shared.reader().await);

    // Reader sees initial data
    let query_time = SystemTime::UNIX_EPOCH + Duration::from_millis(1700000000000);
    let result = reader
        .query(&crate::Namespace::default(), "metric_a", Some(query_time))
        .await
        .unwrap();
    match &result {
        QueryValue::Vector(samples) => assert_eq!(samples.len(), 1),
        _ => panic!("expected Vector"),
    }

    // Writer can still write more data (the DbReader does not fence it)
    let more_series = vec![
        Series::builder("metric_a")
            .label("env", "prod")
            .sample(1700000002000, 2.0)
            .build(),
    ];
    tsdb.ingest_samples(more_series, None).await.unwrap();
    tsdb.flush().await.unwrap();

    // A reader opened after the new flush sees the new data. (The first
    // reader's view advances only on manifest polls, so a fresh reader is
    // used to assert visibility deterministically.)
    let late_reader = TimeSeriesDbReader::from_storage(shared.reader().await);
    let query_time2 = SystemTime::UNIX_EPOCH + Duration::from_millis(1700000002000);
    let result2 = late_reader
        .query(&crate::Namespace::default(), "metric_a", Some(query_time2))
        .await
        .unwrap();
    match &result2 {
        QueryValue::Vector(samples) => {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 2.0);
        }
        _ => panic!("expected Vector"),
    }

    // The first reader still serves its original view.
    let result3 = reader
        .query(&crate::Namespace::default(), "metric_a", Some(query_time))
        .await
        .unwrap();
    match &result3 {
        QueryValue::Vector(samples) => {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 1.0);
        }
        _ => panic!("expected Vector"),
    }
}

#[tokio::test]
async fn reader_query_range() {
    let shared = create_shared_storage().await;

    let tsdb = crate::tsdb::Tsdb::new(shared.storage.clone());
    let series = vec![
        Series::builder("counter")
            .label("job", "test")
            .sample(1700000000000, 100.0)
            .sample(1700000015000, 115.0)
            .sample(1700000030000, 130.0)
            .sample(1700000045000, 145.0)
            .sample(1700000060000, 160.0)
            .build(),
    ];
    tsdb.ingest_samples(series, None).await.unwrap();
    tsdb.flush().await.unwrap();

    let reader = TimeSeriesDbReader::from_storage(shared.reader().await);

    let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000000);
    let end = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000060);

    let result = reader
        .query_range(
            &crate::Namespace::default(),
            "counter",
            start..=end,
            Duration::from_secs(15),
        )
        .await
        .unwrap();

    assert!(!result.is_empty());
    // Each RangeSample should have multiple data points
    for rs in &result {
        assert!(!rs.samples.is_empty());
    }
}

/// Integration test using real SlateDB storage via TimeSeriesDb::open and
/// TimeSeriesDbReader::open on the same local path. This exercises the
/// actual DbReader open path and verifies writer + reader coexistence
/// without fencing.
///
#[tokio::test]
async fn slatedb_writer_and_reader_coexist_no_fencing() {
    use crate::config::Config;
    use crate::timeseries::TimeSeriesDb;
    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
    };

    let tmp_dir = tempfile::tempdir().unwrap();
    let storage_config = SlateDbStorageConfig {
        path: "data".to_string(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: tmp_dir.path().to_str().unwrap().to_string(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    };

    // 1. Open writer and write data
    let writer = TimeSeriesDb::open(Config {
        storage: storage_config.clone(),
        flush_interval: Duration::from_secs(60),
        retention: None,
        write_buffer: Default::default(),
    })
    .await
    .unwrap();

    let series = vec![
        Series::builder("http_requests_total")
            .label("method", "GET")
            .label("status", "200")
            .sample(1700000000000, 100.0)
            .sample(1700000001000, 101.0)
            .build(),
    ];
    writer
        .write(&crate::Namespace::default(), series)
        .await
        .unwrap();
    writer.flush().await.unwrap();

    // 2. Open reader via the public API (exercises create_storage_read + DbReader)
    let reader_options = slatedb::config::DbReaderOptions {
        manifest_poll_interval: Duration::from_millis(100),
        skip_wal_replay: false,
        ..Default::default()
    };
    let reader = TimeSeriesDbReader::open(storage_config.clone(), reader_options, 50)
        .await
        .unwrap();

    // 3. Reader sees written data
    let query_time = SystemTime::UNIX_EPOCH + Duration::from_millis(1700000001000);
    let result = reader
        .query(
            &crate::Namespace::default(),
            "http_requests_total",
            Some(query_time),
        )
        .await
        .unwrap();
    match &result {
        QueryValue::Vector(samples) => {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 101.0);
        }
        _ => panic!("expected Vector, got {:?}", result),
    }

    // 4. Writer can still write after reader opened (no fencing)
    let more_series = vec![
        Series::builder("http_requests_total")
            .label("method", "GET")
            .label("status", "200")
            .sample(1700000002000, 102.0)
            .build(),
    ];
    writer
        .write(&crate::Namespace::default(), more_series)
        .await
        .unwrap();
    writer.flush().await.unwrap();

    // 5. Verify writer is still functional by querying through the writer
    let query_time2 = SystemTime::UNIX_EPOCH + Duration::from_millis(1700000002000);
    let writer_result = writer
        .query(
            &crate::Namespace::default(),
            "http_requests_total",
            Some(query_time2),
        )
        .await
        .unwrap();
    match &writer_result {
        QueryValue::Vector(samples) => {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 102.0);
        }
        _ => panic!("expected Vector from writer, got {:?}", writer_result),
    }

    // 6. Verify reader can still query its original snapshot (not fenced).
    //
    // NOTE: Ideally we'd also verify that the reader sees the *new* data
    // (value=102.0 at t=1700000002000) after its manifest_poll_interval
    // elapses.  However, SlateDB's DbReader currently hits a
    // "invalid sequence number ordering during merge" error when it
    // encounters SSTs written after it opened, due to the merge operator
    // seeing ascending sequence numbers.  Once that is resolved upstream
    // this test should be extended to assert refresh visibility.
    let reader_result = reader
        .query(
            &crate::Namespace::default(),
            "http_requests_total",
            Some(query_time),
        )
        .await
        .unwrap();
    match &reader_result {
        QueryValue::Vector(samples) => {
            assert_eq!(
                samples.len(),
                1,
                "reader should still work after writer writes more data"
            );
            assert_eq!(samples[0].value, 101.0);
        }
        _ => panic!("expected Vector from reader, got {:?}", reader_result),
    }
}

#[tokio::test]
async fn should_persist_data_after_flush_and_writer_reopen() {
    use crate::config::Config;
    use crate::timeseries::TimeSeriesDb;
    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
    };

    // given
    let tmp_dir = tempfile::tempdir().unwrap();
    let storage_config = SlateDbStorageConfig {
        path: "data".to_string(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: tmp_dir.path().to_str().unwrap().to_string(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    };

    let writer = TimeSeriesDb::open(Config {
        storage: storage_config.clone(),
        flush_interval: Duration::from_secs(60),
        retention: None,
        write_buffer: Default::default(),
    })
    .await
    .unwrap();

    let series = vec![
        Series::builder("flush_durability_metric")
            .label("env", "test")
            .sample(1700000001000, 7.0)
            .build(),
    ];
    writer
        .write(&crate::Namespace::default(), series)
        .await
        .unwrap();

    // when
    writer.flush().await.unwrap();
    drop(writer);

    let reopened = TimeSeriesDb::open(Config {
        storage: storage_config,
        flush_interval: Duration::from_secs(60),
        retention: None,
        write_buffer: Default::default(),
    })
    .await
    .unwrap();
    let query_time = SystemTime::UNIX_EPOCH + Duration::from_millis(1700000001000);
    let result = reopened
        .query(
            &crate::Namespace::default(),
            "flush_durability_metric",
            Some(query_time),
        )
        .await
        .unwrap();

    // then
    match result {
        QueryValue::Vector(samples) => {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 7.0);
        }
        _ => panic!("expected Vector result after reopen"),
    }
}

/// Writer and reader must return identical timestamps for query_range
/// with an inclusive end (`..=end`) where end is step-aligned. This
/// guards against the regression where double-converting through
/// `range_bounds_to_system_time` shifted the inclusive end by 1ms.
#[tokio::test]
async fn writer_and_reader_query_range_parity_at_boundary() {
    use crate::model::QueryOptions;

    let shared = create_shared_storage().await;
    let tsdb = crate::tsdb::Tsdb::new(shared.storage.clone());

    // 5 samples at 15s intervals: t+0, t+15, t+30, t+45, t+60
    let series = vec![
        Series::builder("gauge")
            .label("job", "test")
            .sample(1700000000000, 1.0)
            .sample(1700000015000, 2.0)
            .sample(1700000030000, 3.0)
            .sample(1700000045000, 4.0)
            .sample(1700000060000, 5.0)
            .build(),
    ];
    tsdb.ingest_samples(series, None).await.unwrap();
    tsdb.flush().await.unwrap();

    let reader = TimeSeriesDbReader::from_storage(shared.reader().await);

    // Use Tsdb directly for the writer side (same data, same storage)
    let writer_tsdb = crate::tsdb::Tsdb::new(shared.storage.clone());

    let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000000);
    let end = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000060);
    let step = Duration::from_secs(15);

    let reader_result = reader
        .query_range(&crate::Namespace::default(), "gauge", start..=end, step)
        .await
        .unwrap();
    let writer_result = writer_tsdb
        .eval_query_range("gauge", start..=end, step, &QueryOptions::default())
        .await
        .unwrap();

    // Extract sorted (timestamp_ms, value) pairs for comparison
    let extract_timestamps = |samples: &[RangeSample]| -> Vec<Vec<i64>> {
        let mut result: Vec<Vec<i64>> = samples
            .iter()
            .map(|rs| rs.samples.iter().map(|(ts, _)| *ts).collect())
            .collect();
        result.sort();
        result
    };

    let reader_ts = extract_timestamps(&reader_result);
    let writer_ts = extract_timestamps(&writer_result);

    assert_eq!(
        reader_ts, writer_ts,
        "reader and writer must produce identical timestamps for the same range query"
    );
    // The final step at t+60s must be present (inclusive end).
    let all_ts: Vec<i64> = reader_result
        .iter()
        .flat_map(|rs| rs.samples.iter().map(|(ts, _)| *ts))
        .collect();
    assert!(
        all_ts.contains(&1700000060000),
        "inclusive end (t+60s) must be included; got timestamps: {:?}",
        all_ts,
    );
}

#[tokio::test]
async fn query_range_rejects_zero_step() {
    let shared = create_shared_storage().await;

    let tsdb = crate::tsdb::Tsdb::new(shared.storage.clone());
    let series = vec![
        Series::builder("counter")
            .label("job", "test")
            .sample(1700000000000, 100.0)
            .build(),
    ];
    tsdb.ingest_samples(series, None).await.unwrap();
    tsdb.flush().await.unwrap();

    let reader = TimeSeriesDbReader::from_storage(shared.reader().await);

    let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000000);
    let end = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000060);

    let result = reader
        .query_range(
            &crate::Namespace::default(),
            "counter",
            start..=end,
            Duration::ZERO,
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, QueryError::InvalidQuery(_)),
        "expected InvalidQuery, got {:?}",
        err
    );
}

/// Writes data, creates a checkpoint, writes more data, then opens a
/// reader pinned to the checkpoint and verifies it sees only the data
/// that was durable at checkpoint time (not later writes).
#[tokio::test]
async fn should_open_reader_pinned_to_checkpoint() {
    use crate::config::Config;
    use crate::timeseries::TimeSeriesDb;
    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
    };

    // given
    let tmp_dir = tempfile::tempdir().unwrap();
    let storage_config = SlateDbStorageConfig {
        path: "data".to_string(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: tmp_dir.path().to_str().unwrap().to_string(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    };

    let writer = TimeSeriesDb::open(Config {
        storage: storage_config.clone(),
        flush_interval: Duration::from_secs(60),
        retention: None,
        write_buffer: Default::default(),
    })
    .await
    .unwrap();

    writer
        .write(
            &crate::Namespace::default(),
            vec![
                Series::builder("checkpointed")
                    .label("env", "test")
                    .sample(1700000001000, 1.0)
                    .build(),
            ],
        )
        .await
        .unwrap();

    // when — capture a checkpoint, then write a sample that must NOT be visible.
    let checkpoint = writer.create_checkpoint().await.unwrap();

    writer
        .write(
            &crate::Namespace::default(),
            vec![
                Series::builder("checkpointed")
                    .label("env", "test")
                    .sample(1700000002000, 2.0)
                    .build(),
            ],
        )
        .await
        .unwrap();
    writer.flush().await.unwrap();

    let reader_options = slatedb::config::DbReaderOptions {
        manifest_poll_interval: Duration::from_millis(100),
        skip_wal_replay: true,
        ..Default::default()
    };
    let reader =
        TimeSeriesDbReader::open_at_checkpoint(storage_config, reader_options, 50, checkpoint.id)
            .await
            .unwrap();

    // then — the pre-checkpoint sample is visible
    let pre = reader
        .query(
            &crate::Namespace::default(),
            "checkpointed",
            Some(SystemTime::UNIX_EPOCH + Duration::from_millis(1700000001000)),
        )
        .await
        .unwrap();
    match &pre {
        QueryValue::Vector(samples) => {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 1.0);
        }
        _ => panic!("expected Vector, got {:?}", pre),
    }

    // and — the post-checkpoint sample is NOT visible. PromQL lookback
    // will surface the earlier (checkpointed) sample, so we assert the
    // value is the pre-checkpoint one rather than the new write.
    let post = reader
        .query(
            &crate::Namespace::default(),
            "checkpointed",
            Some(SystemTime::UNIX_EPOCH + Duration::from_millis(1700000002000)),
        )
        .await
        .unwrap();
    match &post {
        QueryValue::Vector(samples) => {
            assert_eq!(samples.len(), 1);
            assert_eq!(
                samples[0].value, 1.0,
                "reader pinned to checkpoint must not see writes made after the checkpoint",
            );
            assert_eq!(samples[0].timestamp_ms, 1700000002000);
        }
        _ => panic!("expected Vector, got {:?}", post),
    }
}

#[tokio::test]
async fn from_storage_uses_default_cache_capacity() {
    let shared = create_shared_storage().await;
    let reader = TimeSeriesDbReader::from_storage(shared.reader().await);
    assert_eq!(
        reader.query_cache.policy().max_capacity(),
        Some(DEFAULT_CACHE_CAPACITY)
    );
}

#[tokio::test]
async fn from_storage_with_capacity_honors_custom_value() {
    let shared = create_shared_storage().await;
    let reader = TimeSeriesDbReader::from_storage_with_capacity(shared.reader().await, 123);
    assert_eq!(reader.query_cache.policy().max_capacity(), Some(123));
}
