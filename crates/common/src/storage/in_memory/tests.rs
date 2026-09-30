use super::*;
use bytes::BytesMut;
use std::ops::Bound;

/// Test merge operator that appends new value to existing value with a separator.
struct AppendMergeOperator;

impl MergeOperator for AppendMergeOperator {
    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Bytes {
        operands
            .iter()
            .fold(existing_value.unwrap_or_default(), |acc, operand| {
                let mut result = BytesMut::from(acc);
                if !result.is_empty() {
                    result.extend_from_slice(b",");
                }
                result.extend_from_slice(operand);
                result.freeze()
            })
    }
}

#[tokio::test]
async fn should_return_none_when_key_not_found() {
    // given
    let storage = InMemoryStorage::new();

    // when
    let result = storage.get(Bytes::from("missing_key")).await;

    // then
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn should_store_and_retrieve_record() {
    // given
    let storage = InMemoryStorage::new();
    let key = Bytes::from("test_key");
    let value = Bytes::from("test_value");

    // when
    storage
        .put(vec![Record::new(key.clone(), value.clone()).into()])
        .await
        .unwrap();
    let result = storage.get(key).await.unwrap();

    // then
    assert!(result.is_some());
    let record = result.unwrap();
    assert_eq!(record.key, Bytes::from("test_key"));
    assert_eq!(record.value, value);
}

#[tokio::test]
async fn should_overwrite_existing_key() {
    // given
    let storage = InMemoryStorage::new();
    let key = Bytes::from("test_key");
    let initial_value = Bytes::from("initial_value");
    let updated_value = Bytes::from("updated_value");

    // when
    storage
        .put(vec![Record::new(key.clone(), initial_value).into()])
        .await
        .unwrap();
    storage
        .put(vec![Record::new(key.clone(), updated_value.clone()).into()])
        .await
        .unwrap();
    let result = storage.get(key).await.unwrap();

    // then
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, updated_value);
}

#[tokio::test]
async fn should_store_multiple_records() {
    // given
    let storage = InMemoryStorage::new();
    let records = vec![
        Record::new(Bytes::from("key1"), Bytes::from("value1")),
        Record::new(Bytes::from("key2"), Bytes::from("value2")),
        Record::new(Bytes::from("key3"), Bytes::from("value3")),
    ];

    // when
    storage
        .put(records.iter().cloned().map(PutRecordOp::new).collect())
        .await
        .unwrap();

    // then
    for record in records {
        let retrieved = storage.get(record.key.clone()).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().value, record.value);
    }
}

#[tokio::test]
async fn should_scan_all_records_when_unbounded() {
    // given
    let storage = InMemoryStorage::new();
    let records = [
        Record::new(Bytes::from("a"), Bytes::from("value_a")),
        Record::new(Bytes::from("b"), Bytes::from("value_b")),
        Record::new(Bytes::from("c"), Bytes::from("value_c")),
    ];
    storage
        .put(records.iter().cloned().map(PutRecordOp::new).collect())
        .await
        .unwrap();

    // when
    let scanned = storage.scan(BytesRange::unbounded()).await.unwrap();

    // then
    assert_eq!(scanned.len(), 3);
    assert_eq!(scanned[0].key, Bytes::from("a"));
    assert_eq!(scanned[1].key, Bytes::from("b"));
    assert_eq!(scanned[2].key, Bytes::from("c"));
}

#[tokio::test]
async fn should_scan_records_with_prefix() {
    // given
    let storage = InMemoryStorage::new();
    let records = vec![
        Record::new(Bytes::from("prefix_a"), Bytes::from("value1")),
        Record::new(Bytes::from("prefix_b"), Bytes::from("value2")),
        Record::new(Bytes::from("other_c"), Bytes::from("value3")),
    ];
    storage
        .put(records.into_iter().map(PutRecordOp::new).collect())
        .await
        .unwrap();

    // when
    let scanned = storage
        .scan(BytesRange::prefix(Bytes::from("prefix_")))
        .await
        .unwrap();

    // then
    assert_eq!(scanned.len(), 2);
    assert_eq!(scanned[0].key, Bytes::from("prefix_a"));
    assert_eq!(scanned[1].key, Bytes::from("prefix_b"));
}

#[tokio::test]
async fn should_scan_records_in_bounded_range() {
    // given
    let storage = InMemoryStorage::new();
    let records = vec![
        Record::new(Bytes::from("a"), Bytes::from("value_a")),
        Record::new(Bytes::from("b"), Bytes::from("value_b")),
        Record::new(Bytes::from("c"), Bytes::from("value_c")),
        Record::new(Bytes::from("d"), Bytes::from("value_d")),
    ];
    storage
        .put(records.into_iter().map(PutRecordOp::new).collect())
        .await
        .unwrap();

    // when
    let range = BytesRange::new(
        Bound::Included(Bytes::from("b")),
        Bound::Excluded(Bytes::from("d")),
    );
    let scanned = storage.scan(range).await.unwrap();

    // then
    assert_eq!(scanned.len(), 2);
    assert_eq!(scanned[0].key, Bytes::from("b"));
    assert_eq!(scanned[1].key, Bytes::from("c"));
}

#[tokio::test]
async fn should_return_empty_vec_when_scanning_empty_storage() {
    // given
    let storage = InMemoryStorage::new();

    // when
    let scanned = storage.scan(BytesRange::unbounded()).await.unwrap();

    // then
    assert!(scanned.is_empty());
}

#[tokio::test]
async fn should_iterate_over_records() {
    // given
    let storage = InMemoryStorage::new();
    let records = vec![
        Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
        Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
    ];
    storage.put(records).await.unwrap();

    // when
    let mut iter = storage.scan_iter(BytesRange::unbounded()).await.unwrap();
    let first = iter.next().await.unwrap();
    let second = iter.next().await.unwrap();
    let third = iter.next().await.unwrap();

    // then
    assert!(first.is_some());
    assert_eq!(first.unwrap().key, Bytes::from("key1"));
    assert!(second.is_some());
    assert_eq!(second.unwrap().key, Bytes::from("key2"));
    assert!(third.is_none());
}

#[tokio::test]
async fn should_create_snapshot_with_current_data() {
    // given
    let storage = InMemoryStorage::new();
    storage
        .put(vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
        ])
        .await
        .unwrap();

    // when
    let snapshot = storage.snapshot().await.unwrap();

    // then
    let result = snapshot.get(Bytes::from("key1")).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, Bytes::from("value1"));
}

#[tokio::test]
async fn should_not_see_writes_after_snapshot() {
    // given
    let storage = InMemoryStorage::new();
    storage
        .put(vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
        ])
        .await
        .unwrap();

    // when
    let snapshot = storage.snapshot().await.unwrap();
    storage
        .put(vec![
            Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
        ])
        .await
        .unwrap();

    // then
    let snapshot_result = snapshot.get(Bytes::from("key2")).await.unwrap();
    assert!(snapshot_result.is_none());

    let storage_result = storage.get(Bytes::from("key2")).await.unwrap();
    assert!(storage_result.is_some());
}

#[tokio::test]
async fn should_scan_snapshot_independently() {
    // given
    let storage = InMemoryStorage::new();
    storage
        .put(vec![
            Record::new(Bytes::from("a"), Bytes::from("value_a")).into(),
        ])
        .await
        .unwrap();

    // when
    let snapshot = storage.snapshot().await.unwrap();
    storage
        .put(vec![
            Record::new(Bytes::from("b"), Bytes::from("value_b")).into(),
        ])
        .await
        .unwrap();

    // then
    let snapshot_records = snapshot.scan(BytesRange::unbounded()).await.unwrap();
    assert_eq!(snapshot_records.len(), 1);
    assert_eq!(snapshot_records[0].key, Bytes::from("a"));

    let storage_records = storage.scan(BytesRange::unbounded()).await.unwrap();
    assert_eq!(storage_records.len(), 2);
}

#[tokio::test]
async fn should_handle_empty_record() {
    // given
    let storage = InMemoryStorage::new();
    let key = Bytes::from("empty_key");

    // when
    storage
        .put(vec![Record::empty(key.clone()).into()])
        .await
        .unwrap();
    let result = storage.get(key).await.unwrap();

    // then
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, Bytes::new());
}

#[tokio::test]
async fn should_return_error_when_merge_operator_not_configured() {
    // given
    let storage = InMemoryStorage::new();
    let record = Record::new(Bytes::from("key1"), Bytes::from("value1"));

    // when
    let result = storage.merge(vec![record.into()]).await;

    // then
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Merge operator not configured")
    );
}

#[tokio::test]
async fn should_merge_when_key_does_not_exist() {
    // given
    let merge_op = Arc::new(AppendMergeOperator);
    let storage = InMemoryStorage::with_merge_operator(merge_op);
    let key = Bytes::from("new_key");
    let value = Bytes::from("value1");

    // when
    storage
        .merge(vec![Record::new(key.clone(), value.clone()).into()])
        .await
        .unwrap();
    let result = storage.get(key).await.unwrap();

    // then
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, value);
}

#[tokio::test]
async fn should_merge_when_key_exists() {
    // given
    let merge_op = Arc::new(AppendMergeOperator);
    let storage = InMemoryStorage::with_merge_operator(merge_op);
    let key = Bytes::from("key1");
    let initial_value = Bytes::from("value1");
    let new_value = Bytes::from("value2");

    storage
        .put(vec![Record::new(key.clone(), initial_value).into()])
        .await
        .unwrap();

    // when
    storage
        .merge(vec![Record::new(key.clone(), new_value).into()])
        .await
        .unwrap();
    let result = storage.get(key).await.unwrap();

    // then
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, Bytes::from("value1,value2"));
}

#[tokio::test]
async fn should_merge_multiple_keys() {
    // given
    let merge_op = Arc::new(AppendMergeOperator);
    let storage = InMemoryStorage::with_merge_operator(merge_op);
    let records = vec![
        Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
        Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
    ];
    storage.put(records).await.unwrap();

    // when
    storage
        .merge(vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1a")).into(),
            Record::new(Bytes::from("key2"), Bytes::from("value2a")).into(),
        ])
        .await
        .unwrap();

    // then
    let result1 = storage.get(Bytes::from("key1")).await.unwrap();
    assert_eq!(result1.unwrap().value, Bytes::from("value1,value1a"));

    let result2 = storage.get(Bytes::from("key2")).await.unwrap();
    assert_eq!(result2.unwrap().value, Bytes::from("value2,value2a"));
}

#[tokio::test]
async fn should_return_monotonically_increasing_seqnums_from_put() {
    let storage = InMemoryStorage::new();

    let r1 = storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();
    let r2 = storage
        .put(vec![
            Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
        ])
        .await
        .unwrap();
    let r3 = storage
        .put(vec![
            Record::new(Bytes::from("k3"), Bytes::from("v3")).into(),
        ])
        .await
        .unwrap();

    assert_eq!(r1.seqnum, 1);
    assert_eq!(r2.seqnum, 2);
    assert_eq!(r3.seqnum, 3);
}

#[tokio::test]
async fn should_return_monotonically_increasing_seqnums_from_apply() {
    let storage = InMemoryStorage::new();

    let r1 = storage
        .apply(vec![RecordOp::Put(PutRecordOp::new(Record::new(
            Bytes::from("k1"),
            Bytes::from("v1"),
        )))])
        .await
        .unwrap();
    let r2 = storage
        .apply(vec![RecordOp::Put(PutRecordOp::new(Record::new(
            Bytes::from("k2"),
            Bytes::from("v2"),
        )))])
        .await
        .unwrap();

    assert_eq!(r1.seqnum, 1);
    assert_eq!(r2.seqnum, 2);
}

#[tokio::test]
async fn should_share_seqnum_counter_across_write_methods() {
    let merge_op = Arc::new(AppendMergeOperator);
    let storage = InMemoryStorage::with_merge_operator(merge_op);

    let r1 = storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();
    let r2 = storage
        .merge(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v2")).into(),
        ])
        .await
        .unwrap();
    let r3 = storage
        .apply(vec![RecordOp::Put(PutRecordOp::new(Record::new(
            Bytes::from("k2"),
            Bytes::from("v3"),
        )))])
        .await
        .unwrap();

    assert_eq!(r1.seqnum, 1);
    assert_eq!(r2.seqnum, 2);
    assert_eq!(r3.seqnum, 3);
}

#[tokio::test]
async fn should_start_durable_subscriber_at_zero() {
    let storage = InMemoryStorage::new();
    let rx = storage.subscribe_durable();
    assert_eq!(*rx.borrow(), 0);
}

#[tokio::test]
async fn should_advance_durable_watermark_on_each_write() {
    let storage = InMemoryStorage::new();
    let rx = storage.subscribe_durable();

    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();
    assert_eq!(*rx.borrow(), 1);

    storage
        .put(vec![
            Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
        ])
        .await
        .unwrap();
    assert_eq!(*rx.borrow(), 2);
}

#[tokio::test]
async fn should_merge_empty_values() {
    // given
    let merge_op = Arc::new(AppendMergeOperator);
    let storage = InMemoryStorage::with_merge_operator(merge_op);
    let key = Bytes::from("key1");

    // when
    storage
        .merge(vec![Record::empty(key.clone()).into()])
        .await
        .unwrap();
    let result = storage.get(key).await.unwrap();

    // then
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, Bytes::new());
}

#[tokio::test]
async fn should_not_advance_durable_watermark_when_deferred() {
    let storage = InMemoryStorage::new().with_deferred_durability();
    let rx = storage.subscribe_durable();

    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();
    assert_eq!(*rx.borrow(), 0, "durable watermark should not advance");

    storage
        .put(vec![
            Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
        ])
        .await
        .unwrap();
    assert_eq!(*rx.borrow(), 0, "durable watermark should still be 0");
}

#[tokio::test]
async fn should_advance_durable_watermark_on_flush_when_deferred() {
    let storage = InMemoryStorage::new().with_deferred_durability();
    let rx = storage.subscribe_durable();

    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();
    storage
        .put(vec![
            Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
        ])
        .await
        .unwrap();
    assert_eq!(*rx.borrow(), 0);

    storage.flush().await.unwrap();
    assert_eq!(*rx.borrow(), 2, "flush should advance to current seqnum");
}

#[tokio::test]
async fn should_advance_durable_watermark_to_specific_seq() {
    let storage = InMemoryStorage::new().with_deferred_durability();
    let rx = storage.subscribe_durable();

    // Write 3 records
    for i in 1..=3 {
        storage
            .put(vec![
                Record::new(Bytes::from(format!("k{i}")), Bytes::from(format!("v{i}"))).into(),
            ])
            .await
            .unwrap();
    }
    assert_eq!(*rx.borrow(), 0);

    // Partially flush through seqnum 2
    storage.flush_to(2);
    assert_eq!(*rx.borrow(), 2, "should advance to requested seqnum");

    // Flush the rest
    storage.flush_to(3);
    assert_eq!(*rx.borrow(), 3);
}

#[tokio::test]
#[should_panic(expected = "cannot move durable seqnum backwards")]
async fn should_panic_when_flush_to_moves_backwards() {
    let storage = InMemoryStorage::new().with_deferred_durability();
    for i in 1..=3 {
        storage
            .put(vec![
                Record::new(Bytes::from(format!("k{i}")), Bytes::from(format!("v{i}"))).into(),
            ])
            .await
            .unwrap();
    }

    storage.flush_to(2);
    storage.flush_to(1);
}

#[tokio::test]
#[should_panic(expected = "cannot flush beyond written seqnum")]
async fn should_panic_when_flush_to_exceeds_written() {
    let storage = InMemoryStorage::new().with_deferred_durability();
    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();

    storage.flush_to(5);
}

#[tokio::test]
async fn should_see_data_in_snapshot_before_flush_when_deferred() {
    let storage = InMemoryStorage::new().with_deferred_durability();

    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();

    // Data is written but not durable — snapshots should still see it
    let snapshot = storage.snapshot().await.unwrap();
    let result = snapshot.get(Bytes::from("k1")).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, Bytes::from("v1"));

    // But durable watermark is still 0
    let rx = storage.subscribe_durable();
    assert_eq!(*rx.borrow(), 0);
}

#[tokio::test]
async fn should_not_advance_durable_watermark_on_apply_when_deferred() {
    let storage = InMemoryStorage::new().with_deferred_durability();
    let rx = storage.subscribe_durable();

    storage
        .apply(vec![RecordOp::Put(PutRecordOp::new(Record::new(
            Bytes::from("k1"),
            Bytes::from("v1"),
        )))])
        .await
        .unwrap();
    assert_eq!(*rx.borrow(), 0);

    storage
        .apply(vec![RecordOp::Delete(Bytes::from("k1"))])
        .await
        .unwrap();
    assert_eq!(*rx.borrow(), 0);

    storage.flush().await.unwrap();
    assert_eq!(*rx.borrow(), 2);
}

#[tokio::test]
async fn should_not_advance_durable_watermark_on_merge_when_deferred() {
    let merge_op = Arc::new(AppendMergeOperator);
    let storage = InMemoryStorage::with_merge_operator(merge_op).with_deferred_durability();
    let rx = storage.subscribe_durable();

    storage
        .merge(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();
    assert_eq!(*rx.borrow(), 0);

    storage
        .merge(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v2")).into(),
        ])
        .await
        .unwrap();
    assert_eq!(*rx.borrow(), 0);

    storage.flush().await.unwrap();
    assert_eq!(*rx.borrow(), 2);
}

#[tokio::test]
async fn should_support_multiple_flush_cycles_when_deferred() {
    let storage = InMemoryStorage::new().with_deferred_durability();
    let rx = storage.subscribe_durable();

    // First cycle: write and flush
    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();
    storage.flush().await.unwrap();
    assert_eq!(*rx.borrow(), 1);

    // Second cycle: write more and flush again
    storage
        .put(vec![
            Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
        ])
        .await
        .unwrap();
    storage
        .put(vec![
            Record::new(Bytes::from("k3"), Bytes::from("v3")).into(),
        ])
        .await
        .unwrap();
    assert_eq!(*rx.borrow(), 1, "should not advance before second flush");

    storage.flush().await.unwrap();
    assert_eq!(*rx.borrow(), 3);
}

#[tokio::test]
async fn should_flush_on_empty_storage_when_deferred() {
    let storage = InMemoryStorage::new().with_deferred_durability();
    let rx = storage.subscribe_durable();

    // Flushing with no writes should be fine (sends 0 again)
    storage.flush().await.unwrap();
    assert_eq!(*rx.borrow(), 0);

    // flush_to(0) should also be fine
    storage.flush_to(0);
    assert_eq!(*rx.borrow(), 0);
}

#[tokio::test]
async fn should_defer_durability_across_mixed_write_methods() {
    let merge_op = Arc::new(AppendMergeOperator);
    let storage = InMemoryStorage::with_merge_operator(merge_op).with_deferred_durability();
    let rx = storage.subscribe_durable();

    // put, apply, merge — all should defer
    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
        ])
        .await
        .unwrap();
    storage
        .apply(vec![RecordOp::Put(PutRecordOp::new(Record::new(
            Bytes::from("k2"),
            Bytes::from("v2"),
        )))])
        .await
        .unwrap();
    storage
        .merge(vec![
            Record::new(Bytes::from("k3"), Bytes::from("v3")).into(),
        ])
        .await
        .unwrap();
    assert_eq!(*rx.borrow(), 0);

    // Partially flush through seqnum 2
    storage.flush_to(2);
    assert_eq!(*rx.borrow(), 2);

    // Flush the rest
    storage.flush().await.unwrap();
    assert_eq!(*rx.borrow(), 3);
}

#[tokio::test]
async fn should_read_data_written_before_flush_when_deferred() {
    let storage = InMemoryStorage::new().with_deferred_durability();

    storage
        .put(vec![
            Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
        ])
        .await
        .unwrap();

    // get and scan see data even though it is not yet durable
    let result = storage.get(Bytes::from("k1")).await.unwrap();
    assert_eq!(result.unwrap().value, Bytes::from("v1"));

    let scanned = storage.scan(BytesRange::unbounded()).await.unwrap();
    assert_eq!(scanned.len(), 2);
}
