use super::*;
use crate::BytesRange;
use slatedb::DbBuilder;
use slatedb::config::Settings;
use slatedb::object_store::memory::InMemory;
use slatedb_common::clock::MockSystemClock;

#[tokio::test]
async fn should_read_data_written_by_storage_via_reader() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/db";

    // Create writer and write data
    let db = DbBuilder::new(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage = SlateDbStorage::new(Arc::new(db));

    storage
        .put(vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
            Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
        ])
        .await
        .unwrap();
    storage.flush().await.unwrap();

    // Create reader and verify data
    let reader = DbReader::builder(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage_reader = SlateDbStorageReader::new(Arc::new(reader));

    let record = storage_reader.get(Bytes::from("key1")).await.unwrap();
    assert!(record.is_some());
    assert_eq!(record.unwrap().value, Bytes::from("value1"));

    let record = storage_reader.get(Bytes::from("key2")).await.unwrap();
    assert!(record.is_some());
    assert_eq!(record.unwrap().value, Bytes::from("value2"));

    let record = storage_reader.get(Bytes::from("key3")).await.unwrap();
    assert!(record.is_none());

    storage.close().await.unwrap();
}

#[tokio::test]
async fn should_flush_memtable_to_l0_periodically_only_after_writes() {
    // given: a storage whose periodic flusher runs every 50 ms
    let db = DbBuilder::new("periodic-flush", Arc::new(InMemory::new()))
        .build()
        .await
        .unwrap();
    let storage = SlateDbStorage::new(Arc::new(db)).with_memtable_flush(Duration::from_millis(50));
    let last_l0_seq = || storage.db().manifest().last_l0_seq();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(last_l0_seq(), 0, "no writes, so no L0 flush");

    // when
    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();

    // then
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while last_l0_seq() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the write never reached L0"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    storage.close().await.unwrap();
}

#[tokio::test]
async fn should_scan_data_written_by_storage_via_reader() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/db";

    // Create writer and write data
    let db = DbBuilder::new(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage = SlateDbStorage::new(Arc::new(db));

    storage
        .put(vec![
            Record::new(Bytes::from("a"), Bytes::from("1")).into(),
            Record::new(Bytes::from("b"), Bytes::from("2")).into(),
            Record::new(Bytes::from("c"), Bytes::from("3")).into(),
        ])
        .await
        .unwrap();
    storage.flush().await.unwrap();

    // Create reader and scan data
    let reader = DbReader::builder(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage_reader = SlateDbStorageReader::new(Arc::new(reader));

    let mut iter = storage_reader
        .scan_iter(BytesRange::unbounded())
        .await
        .unwrap();
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
async fn should_coexist_writer_and_reader_without_fencing_error() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/db";

    // Create writer
    let db = DbBuilder::new(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage = SlateDbStorage::new(Arc::new(db));

    // Write initial data
    storage
        .put(vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
        ])
        .await
        .unwrap();
    storage.flush().await.unwrap();

    // Create reader while writer is still open - this should NOT cause fencing error
    let reader = DbReader::builder(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage_reader = SlateDbStorageReader::new(Arc::new(reader));

    // Reader can read the data
    let record = storage_reader.get(Bytes::from("key1")).await.unwrap();
    assert!(record.is_some());
    assert_eq!(record.unwrap().value, Bytes::from("value1"));

    // Writer can still write more data
    storage
        .put(vec![
            Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
        ])
        .await
        .unwrap();
    storage.flush().await.unwrap();

    storage.close().await.unwrap();
}

#[tokio::test]
async fn should_set_expire_ts_based_on_ttl() {
    // given - storage configured with a 30 second default TTL
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
    let storage = SlateDbStorage::new(Arc::new(db));

    // Write three keys at time=0:
    //   key1: expires after 20 seconds
    //   key2: uses default TTL (30 seconds)
    //   key3: never expires
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

    // then - key1 has expire_ts = 20_000 (time=0 + 20s TTL)
    let kv1 = storage.db.get_key_value(b"key1").await.unwrap().unwrap();
    assert_eq!(kv1.expire_ts, Some(20_000));

    // then - key2 has expire_ts = 30_000 (time=0 + 30s default TTL)
    let kv2 = storage.db.get_key_value(b"key2").await.unwrap().unwrap();
    assert_eq!(kv2.expire_ts, Some(30_000));

    // then - key3 has no expire_ts (NoExpiry)
    let kv3 = storage.db.get_key_value(b"key3").await.unwrap().unwrap();
    assert_eq!(kv3.expire_ts, None);

    storage.close().await.unwrap();
}

/// Simple merge operator that concatenates existing and new values.
struct ConcatMergeOperator;

impl MergeOperator for ConcatMergeOperator {
    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Bytes {
        let mut result = existing_value.unwrap_or_default().to_vec();
        for operand in operands {
            result.extend_from_slice(operand);
        }
        Bytes::from(result)
    }
}

#[tokio::test]
async fn should_set_expire_ts_on_merge_records_based_on_ttl() {
    // given - storage configured with a 30 second default TTL and a merge operator
    let object_store = Arc::new(InMemory::new());
    let path = "/test/merge_ttl_db";
    let clock = Arc::new(MockSystemClock::new());

    let merge_op: Arc<dyn MergeOperator> = Arc::new(ConcatMergeOperator);
    let slate_merge_op = SlateDbStorage::merge_operator_adapter(merge_op);
    let db = DbBuilder::new(path, object_store.clone())
        .with_settings(Settings {
            default_ttl: Some(30_000),
            ..Default::default()
        })
        .with_system_clock(clock.clone())
        .with_merge_operator(Arc::new(slate_merge_op))
        .build()
        .await
        .unwrap();
    let storage = SlateDbStorage::new(Arc::new(db));

    // Merge three keys at time=0:
    //   key1: expires after 20 seconds
    //   key2: uses default TTL (30 seconds)
    //   key3: never expires
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

    // then - key1 has expire_ts = 20_000 (time=0 + 20s TTL)
    let kv1 = storage.db.get_key_value(b"key1").await.unwrap().unwrap();
    assert_eq!(kv1.value, Bytes::from("v1"));
    assert_eq!(kv1.expire_ts, Some(20_000));

    // then - key2 has expire_ts = 30_000 (time=0 + 30s default TTL)
    let kv2 = storage.db.get_key_value(b"key2").await.unwrap().unwrap();
    assert_eq!(kv2.value, Bytes::from("v2"));
    assert_eq!(kv2.expire_ts, Some(30_000));

    // then - key3 has no expire_ts (NoExpiry)
    let kv3 = storage.db.get_key_value(b"key3").await.unwrap().unwrap();
    assert_eq!(kv3.value, Bytes::from("v3"));
    assert_eq!(kv3.expire_ts, None);

    storage.close().await.unwrap();
}

/// Helper: open a DbReader against the same path/object_store and try to
/// read a key. Returns `true` if the key is present.
async fn reader_can_see(path: &str, object_store: Arc<InMemory>, key: &str) -> bool {
    reader_can_see_with_merge_op(path, object_store, key, None).await
}

async fn reader_can_see_with_merge_op(
    path: &str,
    object_store: Arc<InMemory>,
    key: &str,
    merge_op: Option<Arc<dyn SlateDbMergeOperator + Send + Sync>>,
) -> bool {
    let mut builder = DbReader::builder(path, object_store);
    if let Some(op) = merge_op {
        builder = builder.with_merge_operator(op);
    }
    let reader = builder.build().await.unwrap();
    let storage_reader = SlateDbStorageReader::new(Arc::new(reader));
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
    let storage = SlateDbStorage::new(Arc::new(db));

    // put() uses WriteOptions::default() which is await_durable: false
    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();

    // Data is in memtable only — a reader (which reads from durable state) should NOT see it
    assert!(!reader_can_see(path, object_store.clone(), "k1").await);

    // After explicit flush, reader can see it
    storage.flush().await.unwrap();
    assert!(reader_can_see(path, object_store.clone(), "k1").await);

    storage.close().await.unwrap();
}

#[tokio::test]
async fn put_with_await_durable_true_is_visible_to_reader() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/put_durable";

    let db = DbBuilder::new(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage = SlateDbStorage::new(Arc::new(db));

    // Write with await_durable: true — should be flushed before returning
    storage
        .put_with_options(
            vec![Record::new(Bytes::from("k1"), Bytes::from("v1")).into()],
            WriteOptions {
                await_durable: true,
            },
        )
        .await
        .unwrap();

    // Reader should see it immediately without explicit flush
    assert!(reader_can_see(path, object_store.clone(), "k1").await);

    storage.close().await.unwrap();
}

#[tokio::test]
async fn apply_defaults_to_not_await_durable() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/apply_default_durability";

    let db = DbBuilder::new(path, object_store.clone())
        .build()
        .await
        .unwrap();
    let storage = SlateDbStorage::new(Arc::new(db));

    // apply() delegates with WriteOptions::default() (await_durable: false)
    storage
        .apply(vec![RecordOp::Put(
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        )])
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
    let storage = SlateDbStorage::new(Arc::new(db));

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
async fn merge_defaults_to_not_await_durable() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/merge_default_durability";

    let merge_op: Arc<dyn MergeOperator> = Arc::new(ConcatMergeOperator);
    let slate_merge_op = Arc::new(SlateDbStorage::merge_operator_adapter(merge_op.clone()));
    let db = DbBuilder::new(path, object_store.clone())
        .with_merge_operator(slate_merge_op.clone())
        .build()
        .await
        .unwrap();
    let storage = SlateDbStorage::new(Arc::new(db));

    // merge() delegates with WriteOptions::default() (await_durable: false)
    storage
        .merge(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();

    let reader_merge_op: Arc<dyn SlateDbMergeOperator + Send + Sync> =
        Arc::new(SlateDbStorage::merge_operator_adapter(merge_op.clone()));
    assert!(
        !reader_can_see_with_merge_op(
            path,
            object_store.clone(),
            "k1",
            Some(reader_merge_op.clone()),
        )
        .await
    );

    storage.flush().await.unwrap();
    assert!(
        reader_can_see_with_merge_op(path, object_store.clone(), "k1", Some(reader_merge_op),)
            .await
    );

    storage.close().await.unwrap();
}

#[tokio::test]
async fn merge_with_await_durable_true_is_visible_to_reader() {
    let object_store = Arc::new(InMemory::new());
    let path = "/test/merge_durable";

    let merge_op: Arc<dyn MergeOperator> = Arc::new(ConcatMergeOperator);
    let slate_merge_op = Arc::new(SlateDbStorage::merge_operator_adapter(merge_op.clone()));
    let db = DbBuilder::new(path, object_store.clone())
        .with_merge_operator(slate_merge_op.clone())
        .build()
        .await
        .unwrap();
    let storage = SlateDbStorage::new(Arc::new(db));

    storage
        .merge_with_options(
            vec![Record::new(Bytes::from("k1"), Bytes::from("v1")).into()],
            WriteOptions {
                await_durable: true,
            },
        )
        .await
        .unwrap();

    let reader_merge_op: Arc<dyn SlateDbMergeOperator + Send + Sync> =
        Arc::new(SlateDbStorage::merge_operator_adapter(merge_op));
    assert!(
        reader_can_see_with_merge_op(path, object_store.clone(), "k1", Some(reader_merge_op),)
            .await
    );

    storage.close().await.unwrap();
}
