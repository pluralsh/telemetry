use super::*;
use slatedb::object_store::memory::InMemory;
use slatedb::{MergeOperator as SlateDbMergeOperator, MergeOperatorError};
use slatedb_common::clock::MockSystemClock;

fn storage_from_db(db: Db) -> Storage {
    Storage::from_db(Arc::new(db))
}

fn reader_from_db_reader(reader: DbReader) -> StorageReader {
    let reader = Arc::new(reader);
    let status_reader = Arc::clone(&reader);
    StorageReader {
        reader: StorageReaderInner {
            db: reader,
            segments: Arc::new(move || status_reader.status().list_segments()),
        },
    }
}

fn settings_config(path: Option<String>) -> SlateDbStorageConfig {
    SlateDbStorageConfig {
        settings_path: path,
        ..SlateDbStorageConfig::default()
    }
}

fn min_compaction_sources(settings: &Settings) -> Option<&str> {
    settings
        .compactor_options
        .as_ref()?
        .scheduler_options
        .get("min_compaction_sources")
        .map(String::as_str)
}

#[test]
fn should_apply_metrics_slatedb_defaults_without_settings_file() {
    let settings = load_settings(&settings_config(None)).unwrap();

    assert_eq!(settings.l0_sst_size_bytes, DEFAULT_L0_SST_SIZE_BYTES);
    assert_eq!(
        min_compaction_sources(&settings),
        Some(DEFAULT_MIN_COMPACTION_SOURCES.to_string().as_str())
    );
    assert_eq!(settings.l0_max_ssts, Settings::default().l0_max_ssts);
}

#[test]
fn should_keep_metrics_defaults_for_keys_a_settings_file_leaves_unset() {
    // given: a file overriding one top-level key and one other scheduler key
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("slatedb.toml");
    std::fs::write(
        &path,
        "l0_max_ssts = 16\n[compactor_options.scheduler_options]\nmax_compaction_sources = \"6\"\n",
    )
    .unwrap();

    // when
    let settings =
        load_settings(&settings_config(Some(path.to_string_lossy().into_owned()))).unwrap();

    // then
    assert_eq!(settings.l0_max_ssts, 16);
    assert_eq!(settings.l0_sst_size_bytes, DEFAULT_L0_SST_SIZE_BYTES);
    let scheduler = &settings
        .compactor_options
        .as_ref()
        .unwrap()
        .scheduler_options;
    assert_eq!(scheduler.get("max_compaction_sources").unwrap(), "6");
    assert_eq!(
        min_compaction_sources(&settings),
        Some(DEFAULT_MIN_COMPACTION_SOURCES.to_string().as_str())
    );
}

#[test]
fn should_let_a_settings_file_override_metrics_defaults() {
    // given
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("slatedb.json");
    std::fs::write(
        &path,
        r#"{"l0_sst_size_bytes": 67108864,
            "compactor_options": {"scheduler_options": {"min_compaction_sources": "4"}}}"#,
    )
    .unwrap();

    // when
    let settings =
        load_settings(&settings_config(Some(path.to_string_lossy().into_owned()))).unwrap();

    // then
    assert_eq!(settings.l0_sst_size_bytes, 64 << 20);
    assert_eq!(min_compaction_sources(&settings), Some("4"));
}

#[test]
fn should_load_example_settings_file_over_metrics_defaults() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../config/SlateDb.example.toml"
    );

    let settings = load_settings(&settings_config(Some(path.to_string()))).unwrap();

    assert_eq!(
        settings.compression_codec,
        Some(slatedb::config::CompressionCodec::Zstd)
    );
    assert_eq!(settings.l0_sst_size_bytes, DEFAULT_L0_SST_SIZE_BYTES);
    assert_eq!(
        min_compaction_sources(&settings),
        Some(DEFAULT_MIN_COMPACTION_SOURCES.to_string().as_str())
    );
}

#[test]
fn should_reject_settings_file_with_unknown_extension() {
    let err = load_settings(&settings_config(Some("slatedb.ini".to_string()))).unwrap_err();

    assert!(err.to_string().contains("unknown format"), "{err}");
}

#[test]
fn metadata_only_cache_warm_targets_avoid_data_cache() {
    let namespace = Namespace::new("tenant").unwrap();
    let bucket = TimeBucket {
        start: 12345,
        size: 1,
    };
    assert_eq!(bucket_cache_targets(&namespace, &bucket, false).len(), 2);
    assert_eq!(bucket_cache_targets(&namespace, &bucket, true).len(), 3);
}

#[tokio::test]
async fn should_read_data_written_by_storage_via_reader() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/db";

    let db = DbBuilder::new(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage = storage_from_db(db);

    storage
        .put(vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
            Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
        ])
        .await
        .unwrap();
    storage.flush().await.unwrap();

    let reader = DbReader::builder(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage_reader = reader_from_db_reader(reader);

    let value = storage_reader.get(Bytes::from("key1")).await.unwrap();
    assert_eq!(value, Some(Bytes::from("value1")));
    let value = storage_reader.get(Bytes::from("key2")).await.unwrap();
    assert_eq!(value, Some(Bytes::from("value2")));
    let value = storage_reader.get(Bytes::from("key3")).await.unwrap();
    assert!(value.is_none());

    storage.close().await.unwrap();
}

#[tokio::test]
async fn should_scan_data_written_by_storage_via_reader() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/db";

    let db = DbBuilder::new(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage = storage_from_db(db);

    storage
        .put(vec![
            Record::new(Bytes::from("a"), Bytes::from("1")).into(),
            Record::new(Bytes::from("b"), Bytes::from("2")).into(),
            Record::new(Bytes::from("c"), Bytes::from("3")).into(),
        ])
        .await
        .unwrap();
    storage.flush().await.unwrap();

    let reader = DbReader::builder(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage_reader = reader_from_db_reader(reader);

    let mut iter = storage_reader.scan(BytesRange::unbounded()).await.unwrap();
    let mut results = Vec::new();
    while let Some(record) = iter.next().await.unwrap() {
        results.push((record.key, record.value));
    }

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], (Bytes::from("a"), Bytes::from("1")));
    assert_eq!(results[1], (Bytes::from("b"), Bytes::from("2")));
    assert_eq!(results[2], (Bytes::from("c"), Bytes::from("3")));

    storage.close().await.unwrap();
}

#[tokio::test]
async fn should_set_expire_ts_based_on_ttl() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/ttl_db";
    let clock = Arc::new(MockSystemClock::new());

    let db = DbBuilder::new(path, object_store.clone())
        .with_settings(Settings {
            default_ttl: Some(30_000),
            ..Default::default()
        })
        .with_system_clock(clock.clone())
        .build()
        .await
        .unwrap();
    let storage = storage_from_db(db);

    storage
        .put(vec![
            PutRecordOp::new_with_options(
                Record::new(Bytes::from("key1"), Bytes::from("value1")),
                PutOptions {
                    ttl: Ttl::ExpireAfter(20_000),
                },
            ),
            PutRecordOp::new_with_options(
                Record::new(Bytes::from("key2"), Bytes::from("value2")),
                PutOptions { ttl: Ttl::Default },
            ),
            PutRecordOp::new_with_options(
                Record::new(Bytes::from("key3"), Bytes::from("value3")),
                PutOptions { ttl: Ttl::NoExpiry },
            ),
        ])
        .await
        .unwrap();

    let kv1 = storage.db.get_key_value(b"key1").await.unwrap().unwrap();
    assert_eq!(kv1.expire_ts, Some(20_000));
    let kv2 = storage.db.get_key_value(b"key2").await.unwrap().unwrap();
    assert_eq!(kv2.expire_ts, Some(30_000));
    let kv3 = storage.db.get_key_value(b"key3").await.unwrap().unwrap();
    assert_eq!(kv3.expire_ts, None);

    storage.close().await.unwrap();
}

/// Simple merge operator that concatenates existing and new values.
/// Implements SlateDB's `MergeOperator` directly (test-only).
struct ConcatMergeOperator;

impl SlateDbMergeOperator for ConcatMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let mut result = existing_value.unwrap_or_default().to_vec();
        result.extend_from_slice(&value);
        Ok(Bytes::from(result))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        if operands.is_empty() && existing_value.is_none() {
            return Err(MergeOperatorError::EmptyBatch);
        }
        let mut result = existing_value.unwrap_or_default().to_vec();
        for operand in operands {
            result.extend_from_slice(operand);
        }
        Ok(Bytes::from(result))
    }
}

#[tokio::test]
async fn should_set_expire_ts_on_merge_records_based_on_ttl() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/merge_ttl_db";
    let clock = Arc::new(MockSystemClock::new());

    let db = DbBuilder::new(path, object_store.clone())
        .with_settings(Settings {
            default_ttl: Some(30_000),
            ..Default::default()
        })
        .with_system_clock(clock.clone())
        .with_merge_operator(Arc::new(ConcatMergeOperator))
        .build()
        .await
        .unwrap();
    let storage = storage_from_db(db);

    storage
        .merge(vec![
            MergeRecordOp::new_with_ttl(
                Record::new(Bytes::from("key1"), Bytes::from("v1")),
                MergeOptions {
                    ttl: Ttl::ExpireAfter(20_000),
                },
            ),
            MergeRecordOp::new_with_ttl(
                Record::new(Bytes::from("key2"), Bytes::from("v2")),
                MergeOptions { ttl: Ttl::Default },
            ),
            MergeRecordOp::new_with_ttl(
                Record::new(Bytes::from("key3"), Bytes::from("v3")),
                MergeOptions { ttl: Ttl::NoExpiry },
            ),
        ])
        .await
        .unwrap();

    let kv1 = storage.db.get_key_value(b"key1").await.unwrap().unwrap();
    assert_eq!(kv1.value, Bytes::from("v1"));
    assert_eq!(kv1.expire_ts, Some(20_000));
    let kv2 = storage.db.get_key_value(b"key2").await.unwrap().unwrap();
    assert_eq!(kv2.value, Bytes::from("v2"));
    assert_eq!(kv2.expire_ts, Some(30_000));
    let kv3 = storage.db.get_key_value(b"key3").await.unwrap().unwrap();
    assert_eq!(kv3.value, Bytes::from("v3"));
    assert_eq!(kv3.expire_ts, None);

    storage.close().await.unwrap();
}

async fn reader_can_see(path: &str, object_store: Arc<InMemory>, key: &str) -> bool {
    let reader = DbReader::builder(path, object_store).build().await.unwrap();
    let storage_reader = reader_from_db_reader(reader);
    storage_reader
        .get(Bytes::from(key.to_owned()))
        .await
        .unwrap()
        .is_some()
}

#[tokio::test]
async fn put_defaults_to_not_await_durable() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/put_default_durability";

    let db = DbBuilder::new(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage = storage_from_db(db);

    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();

    assert!(!reader_can_see(path, object_store.clone(), "k1").await);
    storage.flush().await.unwrap();
    assert!(reader_can_see(path, object_store.clone(), "k1").await);

    storage.close().await.unwrap();
}

#[tokio::test]
async fn apply_with_await_durable_true_is_visible_to_reader() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/apply_durable";

    let db = DbBuilder::new(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage = storage_from_db(db);

    storage
        .apply_with_options(
            vec![RecordOp::Put(
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            )],
            WriteOptions {
                await_durable: true,
            },
        )
        .await
        .unwrap();

    assert!(reader_can_see(path, object_store.clone(), "k1").await);
    storage.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_sees_writes_made_before_it() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/snapshot";

    let db = DbBuilder::new(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage = storage_from_db(db);

    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();

    let snapshot = storage.snapshot().await.unwrap();
    let value = snapshot.get(Bytes::from("k1")).await.unwrap();
    assert_eq!(value, Some(Bytes::from("v1")));

    storage.close().await.unwrap();
}

#[tokio::test]
async fn should_flush_memtable_to_l0_periodically_only_after_writes() {
    // given: a storage whose periodic flusher runs every 50 ms
    let db = DbBuilder::new("periodic-flush", Arc::new(InMemory::new()))
        .build()
        .await
        .unwrap();
    let storage = storage_from_db(db);
    storage.spawn_memtable_flusher(Duration::from_millis(50));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(storage.merge_barrier().1, 0, "no writes, so no L0 flush");

    // when
    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();

    // then
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while storage.merge_barrier().1 == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the write never reached L0"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    storage.close().await.unwrap();
}

#[tokio::test]
async fn should_warm_buckets_resolved_from_segments() {
    use crate::storage::in_memory_storage;
    use slatedb::config::{FlushOptions, FlushType};

    // given: a record in a known bucket. `in_memory_storage` wires the
    // timeseries segment extractor, so the record lands in its own SlateDB
    // segment. A memtable flush then writes it out as an L0 SST that
    // appears in the manifest (a plain WAL flush would not).
    let storage = in_memory_storage().await;
    let bucket = TimeBucket {
        start: 100,
        size: 1,
    };
    let key = ForwardIndexKey {
        namespace: crate::Namespace::default(),
        bucket,
        series_id: 1,
    }
    .encode();
    storage
        .put(vec![Record::new(key, Bytes::from_static(b"v")).into()])
        .await
        .unwrap();
    storage
        .db
        .flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

    // the segment is in the manifest, so warm has an SST to resolve.
    assert!(
        !storage.db.status().current_manifest.segments().is_empty(),
        "flush should have produced a segment in the manifest"
    );

    // the bucket is discoverable from the segment list.
    let namespace = crate::Namespace::default();
    let buckets = storage
        .get_buckets_in_range(&namespace, None, None)
        .await
        .unwrap();
    assert!(buckets.contains(&bucket));

    // when / then: warming the live bucket (with and without samples), an
    // empty set, and an unknown bucket all succeed. Resolution and the
    // per-SST fanout run; `warm_sst` itself no-ops because the in-memory
    // config has no block cache.
    let cancel = CancellationToken::new();
    storage
        .warm(&namespace, buckets.clone(), true, 2, &cancel, None)
        .await
        .unwrap();
    let tracker = common::SstWarmTracker::new(true);
    storage
        .warm(
            &namespace,
            buckets.clone(),
            true,
            2,
            &cancel,
            Some(&tracker),
        )
        .await
        .unwrap();
    tracker.finish_pass();
    storage
        .warm(&namespace, buckets, false, 2, &cancel, Some(&tracker))
        .await
        .unwrap();
    storage
        .warm(&namespace, vec![], true, 2, &cancel, None)
        .await
        .unwrap();
    storage
        .warm(
            &namespace,
            vec![TimeBucket {
                start: 999_999,
                size: 1,
            }],
            false,
            2,
            &cancel,
            None,
        )
        .await
        .unwrap();

    // an already-cancelled token short-circuits the warm and still
    // returns Ok.
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let buckets = storage
        .get_buckets_in_range(&namespace, None, None)
        .await
        .unwrap();
    storage
        .warm(&namespace, buckets, true, 2, &cancelled, None)
        .await
        .unwrap();

    storage.close().await.unwrap();
}
