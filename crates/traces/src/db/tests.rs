// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::time::Duration;

use common::storage::config::{
    LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};

use super::*;
use crate::PageConfig;

fn test_config() -> Config {
    Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "traces-test".to_owned(),
            object_store: ObjectStoreConfig::InMemory,
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        }),
        segment_duration: Duration::from_secs(10),
        retention: Some(Duration::from_secs(60)),
        page: PageConfig {
            target_size_bytes: 64 * 1024,
            max_size_bytes: 128 * 1024,
            max_traces: 16,
        },
        write_buffer: Default::default(),
        read_cache: Default::default(),
    }
}

fn value(value: any_value::Value) -> Option<AnyValue> {
    Some(AnyValue { value: Some(value) })
}

fn attr(name: &str, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: name.to_owned(),
        value: self::value(value),
    }
}

fn trace(
    id: u8,
    start_ns: u64,
    name: &str,
    resource_attributes: Vec<KeyValue>,
    span_attributes: Vec<KeyValue>,
) -> Trace {
    let trace_id = TraceId::new([id; 16]).unwrap();
    Trace::new(
        trace_id,
        vec![ResourceSpans {
            resource: Some(Resource {
                attributes: resource_attributes,
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: trace_id.as_bytes().to_vec(),
                    span_id: [id; 8].to_vec(),
                    name: name.to_owned(),
                    start_time_unix_nano: start_ns,
                    end_time_unix_nano: start_ns + 100,
                    attributes: span_attributes,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    )
    .unwrap()
}

#[tokio::test]
async fn stores_traces_in_pages_and_isolates_namespaces() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let tenant_a = Namespace::new("tenant-a").unwrap();
    let tenant_b = Namespace::new("tenant-b").unwrap();
    let first = trace(1, 1, "one", Vec::new(), Vec::new());
    let second = trace(2, 2, "two", Vec::new(), Vec::new());
    let report = db
        .write(
            &tenant_a,
            vec![TraceBatch::new(vec![first.clone(), second.clone()])],
        )
        .await
        .unwrap();
    assert_eq!(report.pages, 0);
    assert_eq!(report.traces, 2);
    db.write(
        &tenant_b,
        vec![TraceBatch::new(vec![trace(
            3,
            3,
            "other",
            Vec::new(),
            Vec::new(),
        )])],
    )
    .await
    .unwrap();

    assert_eq!(
        db.get_trace(&tenant_a, first.trace_id).await.unwrap(),
        Some(first)
    );
    assert!(
        db.get_trace(&tenant_b, second.trace_id)
            .await
            .unwrap()
            .is_none()
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn traces_in_a_segment_share_one_page_sequence() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::new("segment-pages").unwrap();
    let first = trace(1, 1, "first", Vec::new(), Vec::new());
    let second = trace(2, 2, "second", Vec::new(), Vec::new());

    for trace in [&first, &second] {
        db.write(&namespace, vec![TraceBatch::new(vec![trace.clone()])])
            .await
            .unwrap();
    }

    let mut records = db
        .storage
        .scan_prefix_iter(
            metadata_prefix(&namespace, 0),
            BytesRange::unbounded(),
            None,
        )
        .await
        .unwrap();
    let mut sequences = Vec::new();
    while let Some(record) = records.next().await.unwrap() {
        sequences.push(crate::codec::decode_metadata_sequence(&record.key).unwrap());
    }
    assert_eq!(sequences, vec![0, 1]);
    assert_eq!(
        db.get_trace(&namespace, first.trace_id).await.unwrap(),
        Some(first)
    );
    assert_eq!(
        db.get_trace(&namespace, second.trace_id).await.unwrap(),
        Some(second)
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn applied_writes_coalesce_into_pages_across_requests_in_timestamp_order() {
    let mut config = test_config();
    config.page.max_traces = 16;
    let db = TraceDb::open(config).await.unwrap();
    let namespace = Namespace::new("coalesced").unwrap();
    let mut early = trace(1, 1, "span", Vec::new(), Vec::new());
    let mut late = trace(2, 2, "span", Vec::new(), Vec::new());
    early.resource_spans[0].scope_spans[0].spans[0].start_time_unix_nano = 10;
    early.resource_spans[0].scope_spans[0].spans[0].end_time_unix_nano = 11;
    late.resource_spans[0].scope_spans[0].spans[0].start_time_unix_nano = 20;
    late.resource_spans[0].scope_spans[0].spans[0].end_time_unix_nano = 21;
    let late_report = db
        .write_with_durability(
            &namespace,
            vec![TraceBatch::new(vec![late.clone()])],
            Durability::Applied,
        )
        .await
        .unwrap();
    let early_report = db
        .write_with_durability(
            &namespace,
            vec![TraceBatch::new(vec![early.clone()])],
            Durability::Applied,
        )
        .await
        .unwrap();
    assert_eq!(
        late_report,
        WriteReport {
            traces: 1,
            pages: 0,
            spans: 1
        }
    );
    assert_eq!(
        early_report,
        WriteReport {
            traces: 1,
            pages: 0,
            spans: 1
        }
    );
    assert!(
        db.get_trace(&namespace, early.trace_id)
            .await
            .unwrap()
            .is_none(),
        "Applied acknowledges the in-memory delta only"
    );

    db.flush().await.unwrap();
    let mut records = db
        .storage
        .scan_prefix_iter(
            metadata_prefix(&namespace, 0),
            BytesRange::unbounded(),
            None,
        )
        .await
        .unwrap();
    let page = records.next().await.unwrap().unwrap();
    let metadata = decode_metadata(&page.value).unwrap();
    assert_eq!(
        metadata
            .traces
            .iter()
            .map(|trace| trace.trace_id)
            .collect::<Vec<_>>(),
        vec![early.trace_id, late.trace_id]
    );
    assert!(records.next().await.unwrap().is_none());
    db.close().await.unwrap();
}

#[tokio::test]
async fn written_write_flushes_delta_before_returning() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::new("written").unwrap();
    let original = trace(1, 1, "written", Vec::new(), Vec::new());

    db.write_with_durability(
        &namespace,
        vec![TraceBatch::new(vec![original.clone()])],
        Durability::Written,
    )
    .await
    .unwrap();

    assert_eq!(
        db.get_trace(&namespace, original.trace_id).await.unwrap(),
        Some(original)
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn durable_write_is_visible_to_a_new_storage_reader() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = test_config();
    config.retention = None;
    config.storage = StorageConfig::SlateDb(SlateDbStorageConfig {
        path: "traces-durable".to_owned(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: directory.path().to_string_lossy().into_owned(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    });
    let namespace = Namespace::new("durable").unwrap();
    let original = trace(1, 1, "durable", Vec::new(), Vec::new());
    let db = TraceDb::open(config.clone()).await.unwrap();

    db.write_with_durability(
        &namespace,
        vec![TraceBatch::new(vec![original.clone()])],
        Durability::Durable,
    )
    .await
    .unwrap();
    let reader = TraceDb::open_reader(config, DbReaderOptions::default())
        .await
        .unwrap();

    assert_eq!(
        reader
            .get_trace(&namespace, original.trace_id)
            .await
            .unwrap(),
        Some(original)
    );
    reader.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn close_drains_applied_delta_before_storage_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = test_config();
    config.retention = None;
    config.storage = StorageConfig::SlateDb(SlateDbStorageConfig {
        path: "traces-shutdown".to_owned(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: directory.path().to_string_lossy().into_owned(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    });
    let namespace = Namespace::new("shutdown").unwrap();
    let original = trace(1, 1, "pending", Vec::new(), Vec::new());
    let db = TraceDb::open(config.clone()).await.unwrap();
    db.write_with_durability(
        &namespace,
        vec![TraceBatch::new(vec![original.clone()])],
        Durability::Applied,
    )
    .await
    .unwrap();

    db.close().await.unwrap();
    let reopened = TraceDb::open(config).await.unwrap();
    assert_eq!(
        reopened
            .get_trace(&namespace, original.trace_id)
            .await
            .unwrap(),
        Some(original)
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn routes_time_segments_and_finds_trace_by_id() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    let early = trace(1, 1, "early", Vec::new(), Vec::new());
    let late = trace(2, 11_000_000_000, "late", Vec::new(), Vec::new());
    let report = db
        .write(
            &namespace,
            vec![TraceBatch::new(vec![early.clone(), late.clone()])],
        )
        .await
        .unwrap();
    assert_eq!(report.pages, 0);
    assert_eq!(
        db.get_trace(&namespace, late.trace_id).await.unwrap(),
        Some(late.clone())
    );
    assert_eq!(
        db.search(&namespace, 10_000_000_000, 12_000_000_000, &[])
            .await
            .unwrap(),
        vec![late.clone()]
    );
    assert_eq!(
        db.search(&namespace, 0, u64::MAX, &[]).await.unwrap(),
        vec![early, late]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn duplicate_and_continuation_writes_merge_without_losing_spans() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    let first = trace(1, 10, "first", Vec::new(), Vec::new());
    let continuation = trace(1, 20, "second", Vec::new(), Vec::new());
    db.write(
        &namespace,
        vec![TraceBatch::new(vec![first.clone(), first.clone()])],
    )
    .await
    .unwrap();
    db.write(&namespace, vec![TraceBatch::new(vec![continuation])])
        .await
        .unwrap();
    let merged = db
        .get_trace(&namespace, first.trace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(merged.spans().count(), 2);
    assert_eq!(
        merged
            .spans()
            .map(|span| span.name.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["first", "second"])
    );
    db.close().await.unwrap();
}

#[test]
fn shard_merge_drops_spans_resent_to_both_shards() {
    let first = trace(1, 10, "first", Vec::new(), Vec::new());
    let continuation = trace(1, 20, "second", Vec::new(), Vec::new());
    let merged = merge_traces(vec![first.clone(), first, continuation]).unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged[0]
            .spans()
            .map(|span| span.name.as_str())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
}

#[tokio::test]
async fn search_merges_continuations_across_pages_and_batches() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    let count = MATERIALIZE_BATCH as u64 + 3;
    let single_span = |index: u64, span_id: u8, start_ns: u64| {
        let mut id = [0; 16];
        id[..8].copy_from_slice(&(index + 1).to_be_bytes());
        Trace::new(
            TraceId::new(id).unwrap(),
            vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: id.to_vec(),
                        span_id: vec![span_id; 8],
                        start_time_unix_nano: start_ns,
                        end_time_unix_nano: start_ns + 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        )
        .unwrap()
    };
    let traces: Vec<_> = (0..count)
        .map(|index| single_span(index, 1, 1 + index))
        .collect();
    db.write(&namespace, vec![TraceBatch::new(traces.clone())])
        .await
        .unwrap();
    db.write(
        &namespace,
        vec![TraceBatch::new(vec![single_span(0, 2, 5)])],
    )
    .await
    .unwrap();

    let found = db.search(&namespace, 0, 1_000, &[]).await.unwrap();
    assert_eq!(found.len(), count as usize);
    let merged = found
        .iter()
        .find(|trace| trace.trace_id == traces[0].trace_id)
        .unwrap();
    assert_eq!(merged.spans().count(), 2);
    assert_eq!(
        db.scan_traces(&namespace, 2).await.unwrap().len(),
        2,
        "scan limit counts distinct traces"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn exact_typed_search_distinguishes_values_and_intersects_matchers() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    let variants = vec![
        trace(
            1,
            1,
            "string",
            vec![attr(
                "service",
                any_value::Value::StringValue("api".to_owned()),
            )],
            vec![attr("value", any_value::Value::StringValue("7".to_owned()))],
        ),
        trace(
            2,
            2,
            "int",
            vec![attr(
                "service",
                any_value::Value::StringValue("api".to_owned()),
            )],
            vec![attr("value", any_value::Value::IntValue(7))],
        ),
        trace(
            3,
            3,
            "double",
            Vec::new(),
            vec![attr("value", any_value::Value::DoubleValue(7.0))],
        ),
        trace(
            4,
            4,
            "bool",
            Vec::new(),
            vec![attr("value", any_value::Value::BoolValue(true))],
        ),
    ];
    db.write(&namespace, vec![TraceBatch::new(variants.clone())])
        .await
        .unwrap();
    for (expected, value) in [
        (variants[0].trace_id, AttributeValue::String("7".to_owned())),
        (variants[1].trace_id, AttributeValue::Int(7)),
        (variants[2].trace_id, AttributeValue::Double(7.0)),
        (variants[3].trace_id, AttributeValue::Bool(true)),
    ] {
        let found = db
            .search(
                &namespace,
                0,
                10,
                &[AttributeMatcher::new(AttributeScope::Span, "value", value).unwrap()],
            )
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].trace_id, expected);
    }
    let found = db
        .search(
            &namespace,
            0,
            10,
            &[
                AttributeMatcher::new(
                    AttributeScope::Resource,
                    "service",
                    AttributeValue::String("api".to_owned()),
                )
                .unwrap(),
                AttributeMatcher::new(AttributeScope::Span, "value", AttributeValue::Int(7))
                    .unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].trace_id, variants[1].trace_id);
    db.close().await.unwrap();
}

#[tokio::test]
async fn catalog_deduplicates_pages_and_segments_without_payload_reads() {
    let mut config = test_config();
    config.page.max_traces = 1;
    let db = TraceDb::open(config).await.unwrap();
    let namespace = Namespace::new("catalog").unwrap();
    let traces = vec![
        trace(
            1,
            1,
            "first",
            vec![attr(
                "service.name",
                any_value::Value::StringValue("api".into()),
            )],
            vec![attr("code", any_value::Value::IntValue(200))],
        ),
        trace(
            2,
            2,
            "second",
            vec![attr(
                "service.name",
                any_value::Value::StringValue("api".into()),
            )],
            vec![attr("error", any_value::Value::BoolValue(true))],
        ),
        trace(
            3,
            11_000_000_000,
            "later",
            vec![attr(
                "service.name",
                any_value::Value::StringValue("worker".into()),
            )],
            vec![attr("ratio", any_value::Value::DoubleValue(0.5))],
        ),
    ];
    db.write(&namespace, vec![TraceBatch::new(traces.clone())])
        .await
        .unwrap();

    // Make every data page undecodable. Catalog reads must continue to
    // work because they only consult locators and catalog records.
    let mut corrupt_pages = Vec::new();
    let now = unix_time_ms().unwrap();
    for trace in &traces {
        for locator in db.locate(&namespace, trace.trace_id, now).await.unwrap() {
            corrupt_pages.push(RecordOp::put_with_ttl(
                payload_key(&namespace, locator.segment, locator.page_sequence),
                Bytes::from_static(b"not-a-page"),
                Ttl::NoExpiry,
            ));
        }
    }
    db.writer
        .as_ref()
        .unwrap()
        .apply(corrupt_pages)
        .await
        .unwrap();

    assert_eq!(
        db.catalog_names(&namespace, 0, 20_000_000_000, None)
            .await
            .unwrap(),
        vec!["code", "error", "ratio", "service.name"]
    );
    assert_eq!(
        db.catalog_names(&namespace, 0, 9_999_999_999, Some(AttributeScope::Resource))
            .await
            .unwrap(),
        vec!["service.name"]
    );
    assert_eq!(
        db.catalog_values(
            &namespace,
            0,
            9_999_999_999,
            Some(AttributeScope::Resource),
            "service.name"
        )
        .await
        .unwrap(),
        vec![DiscoveryValue::String("api".into())]
    );
    assert_eq!(
        db.catalog_values(
            &namespace,
            10_000_000_000,
            20_000_000_000,
            Some(AttributeScope::Resource),
            "service.name"
        )
        .await
        .unwrap(),
        vec![DiscoveryValue::String("worker".into())]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn reader_catalog_sees_late_writes_to_closed_segments() {
    const SECOND: u64 = 1_000_000_000;
    let directory = tempfile::tempdir().unwrap();
    let mut config = test_config();
    config.retention = None;
    config.segment_duration = Duration::from_secs(60);
    config.page.max_traces = 2;
    config.storage = StorageConfig::SlateDb(SlateDbStorageConfig {
        path: "traces-late-catalog".to_owned(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: directory.path().to_string_lossy().into_owned(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    });
    let namespace = Namespace::new("late-catalog").unwrap();
    let service = |name: &str| {
        vec![attr(
            "service.name",
            any_value::Value::StringValue(name.to_owned()),
        )]
    };
    let writer = TraceDb::open(config.clone()).await.unwrap();
    let early = (0..6u8)
        .map(|id| {
            let name = if id % 2 == 0 { "auth" } else { "cart" };
            trace(
                id + 1,
                u64::from(id) * 30 * SECOND + SECOND,
                name,
                service(name),
                Vec::new(),
            )
        })
        .collect::<Vec<_>>();
    writer
        .write_with_durability(
            &namespace,
            vec![TraceBatch::new(early)],
            Durability::Durable,
        )
        .await
        .unwrap();
    let reader = TraceDb::open_reader(
        config,
        DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(50),
            ..DbReaderOptions::default()
        },
    )
    .await
    .unwrap();
    let (start, end) = (30 * SECOND, 150 * SECOND);
    let values = || {
        reader.catalog_values(
            &namespace,
            start,
            end,
            Some(AttributeScope::Resource),
            "service.name",
        )
    };
    let strings = |names: &[&str]| {
        names
            .iter()
            .map(|name| DiscoveryValue::String((*name).to_owned()))
            .collect::<Vec<_>>()
    };
    assert_eq!(values().await.unwrap(), strings(&["auth", "cart"]));
    assert_eq!(
        reader
            .catalog_names(&namespace, start, end, Some(AttributeScope::Resource))
            .await
            .unwrap(),
        vec!["service.name"]
    );

    // Long past segment 60s..120s, a late export lands spans of a new
    // service there, across more than one page.
    let mut search = service("search");
    search.push(attr(
        "search.tier",
        any_value::Value::StringValue("hot".to_owned()),
    ));
    let late = (0..3u8)
        .map(|id| {
            trace(
                id + 20,
                70 * SECOND + u64::from(id),
                "late",
                search.clone(),
                Vec::new(),
            )
        })
        .collect::<Vec<_>>();
    writer
        .write_with_durability(&namespace, vec![TraceBatch::new(late)], Durability::Durable)
        .await
        .unwrap();
    let sequence = next_sequence_key(&namespace, 1);
    tokio::time::timeout(Duration::from_secs(10), async {
        while reader
            .storage
            .get(sequence.clone())
            .await
            .unwrap()
            .map(|record| decode_sequence(&record.value).unwrap())
            < Some(3)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("reader observes the late pages");

    assert_eq!(
        values().await.unwrap(),
        strings(&["auth", "cart", "search"])
    );
    assert_eq!(
        reader
            .catalog_names(&namespace, start, end, Some(AttributeScope::Resource))
            .await
            .unwrap(),
        vec!["search.tier", "service.name"]
    );
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn unindexed_values_remain_in_payload() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    let original = trace(
        1,
        1,
        "bytes",
        vec![attr(
            "opaque",
            any_value::Value::BytesValue(vec![0, 1, 2, 255]),
        )],
        Vec::new(),
    );
    db.write(&namespace, vec![TraceBatch::new(vec![original.clone()])])
        .await
        .unwrap();
    assert_eq!(
        db.get_trace(&namespace, original.trace_id).await.unwrap(),
        Some(original)
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn logical_retention_hides_trace_and_search_results() {
    let mut config = test_config();
    config.retention = Some(Duration::from_millis(20));
    let db = TraceDb::open(config).await.unwrap();
    let namespace = Namespace::default();
    let trace = trace(
        1,
        1,
        "short-lived",
        Vec::new(),
        vec![attr("live", any_value::Value::BoolValue(true))],
    );
    db.write(&namespace, vec![TraceBatch::new(vec![trace.clone()])])
        .await
        .unwrap();
    assert!(
        db.get_trace(&namespace, trace.trace_id)
            .await
            .unwrap()
            .is_some()
    );
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        db.get_trace(&namespace, trace.trace_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.search(
            &namespace,
            0,
            10,
            &[
                AttributeMatcher::new(AttributeScope::Span, "live", AttributeValue::Bool(true),)
                    .unwrap()
            ],
        )
        .await
        .unwrap()
        .is_empty()
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn persists_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = test_config();
    config.retention = None;
    config.storage = StorageConfig::SlateDb(SlateDbStorageConfig {
        path: "traces-reopen".to_owned(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: directory.path().to_string_lossy().into_owned(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    });
    let namespace = Namespace::default();
    let original = trace(1, 1, "persistent", Vec::new(), Vec::new());
    let db = TraceDb::open(config.clone()).await.unwrap();
    db.write(&namespace, vec![TraceBatch::new(vec![original.clone()])])
        .await
        .unwrap();
    db.close().await.unwrap();

    let reopened = TraceDb::open(config).await.unwrap();
    assert_eq!(
        reopened
            .get_trace(&namespace, original.trace_id)
            .await
            .unwrap(),
        Some(original)
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn should_not_pin_retention_barrier_with_idle_coordinator() {
    // given: a coordinator that flushed once and then went idle while the
    // database kept advancing, with memtables small enough to reach L0
    let directory = tempfile::tempdir().unwrap();
    let settings = directory.path().join("slatedb.json");
    std::fs::write(&settings, r#"{"l0_sst_size_bytes": 1024}"#).unwrap();
    let mut config = test_config();
    config.storage = StorageConfig::SlateDb(SlateDbStorageConfig {
        path: "traces-barrier".to_owned(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: directory.path().to_string_lossy().into_owned(),
        }),
        settings_path: Some(settings.to_string_lossy().into_owned()),
        block_cache: None,
        meta_cache: None,
    });
    let namespace = Namespace::default();
    let original = trace(1, 1, "idle", Vec::new(), Vec::new());
    let db = TraceDb::open(config).await.unwrap();
    db.write(&namespace, vec![TraceBatch::new(vec![original.clone()])])
        .await
        .unwrap();
    let writer = db.writer.clone().unwrap();
    let mut after_coordinator_seq = 0;
    for sequence in 0..16 {
        let ops = (100..116)
            .map(|segment| {
                RecordOp::put_with_ttl(
                    next_sequence_key(&namespace, segment),
                    encode_sequence(sequence),
                    Ttl::NoExpiry,
                )
            })
            .collect();
        let written = writer.apply(ops).await.unwrap();
        if sequence == 0 {
            after_coordinator_seq = written.seqnum;
        }
    }

    // when
    db.flush().await.unwrap();
    let slate = db.storage.slate_read().unwrap();
    let manifest = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let manifest = slate.manifest();
            if manifest.last_l0_seq() >= after_coordinator_seq {
                return manifest;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    // then: the coordinator holds no snapshot, so L0 flushes and compaction
    // may drop overwritten versions instead of retaining them behind its
    // last flush
    assert_eq!(manifest.recent_snapshot_min_seq(), manifest.last_l0_seq());
    assert_eq!(
        db.get_trace(&namespace, original.trace_id).await.unwrap(),
        Some(original)
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn traceql_query_executes_with_index_pushdown() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    db.write(
        &namespace,
        vec![TraceBatch::new(vec![
            trace(
                1,
                1,
                "wanted",
                vec![attr(
                    "service.name",
                    any_value::Value::StringValue("api".to_owned()),
                )],
                vec![attr("code", any_value::Value::IntValue(200))],
            ),
            trace(
                2,
                2,
                "other",
                vec![attr(
                    "service.name",
                    any_value::Value::StringValue("worker".to_owned()),
                )],
                vec![attr("code", any_value::Value::IntValue(500))],
            ),
        ])],
    )
    .await
    .unwrap();

    let results = db
        .query_traceql(
            &namespace,
            0,
            10,
            r#"{ resource."service.name" = "api" && span.code = 200 }"#,
            QueryOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].matched_spans[0].name, "wanted");

    for query in [
        r#"{ span.code = 200 || resource."service.name" = "worker" }"#,
        r#"{ span.code = 200 } || { span.code = 500 }"#,
    ] {
        let results = db
            .query_traceql(&namespace, 0, 10, query, QueryOptions::default())
            .await
            .unwrap();
        let mut names = results
            .iter()
            .map(|result| result.matched_spans[0].name.as_str())
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, ["other", "wanted"], "{query}");
    }
    db.close().await.unwrap();
}

fn intrinsic_traces() -> Vec<Trace> {
    let mut slow_error = trace(
        1,
        1,
        "GET /api/users",
        vec![],
        vec![attr(
            "path",
            any_value::Value::StringValue("/api/users".into()),
        )],
    );
    {
        let span = &mut slow_error.resource_spans[0].scope_spans[0].spans[0];
        span.end_time_unix_nano = span.start_time_unix_nano + 5_000_000;
        span.kind = 2;
        span.status = Some(opentelemetry_proto::tonic::trace::v1::Status {
            code: 2,
            ..Default::default()
        });
    }
    let fast_ok = trace(
        2,
        2,
        "POST /health",
        vec![],
        vec![attr("code", any_value::Value::IntValue(200))],
    );
    vec![slow_error, fast_ok]
}

#[tokio::test]
async fn traceql_pushdown_narrows_scans_and_ranges_and_intrinsics() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    db.write(&namespace, vec![TraceBatch::new(intrinsic_traces())])
        .await
        .unwrap();
    for (query, wanted) in [
        (r#"{ span.path =~ "/api/.*" }"#, "GET /api/users"),
        ("{ span.path != nil }", "GET /api/users"),
        ("{ span.code >= 200 }", "POST /health"),
        ("{ 300 > span.code }", "POST /health"),
        (r#"{ name = "POST /health" }"#, "POST /health"),
        (r#"{ name =~ "GET.*" }"#, "GET /api/users"),
        ("{ status = error }", "GET /api/users"),
        ("{ status != error }", "POST /health"),
        ("{ kind = server }", "GET /api/users"),
        ("{ duration > 1ms }", "GET /api/users"),
        ("{ duration < 1us }", "POST /health"),
        (
            r#"{ status = error || span.code = 500 } && { .path =~ "/api.*" }"#,
            "GET /api/users",
        ),
    ] {
        // One candidate proves the index, not the evaluator, did the pruning.
        let results = db
            .query_traceql(
                &namespace,
                0,
                10,
                query,
                QueryOptions {
                    max_candidate_traces: 1,
                    ..QueryOptions::default()
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        let names = results
            .iter()
            .map(|result| result.matched_spans[0].name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, [wanted], "{query}");
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn traceql_query_limit_and_order_are_deterministic() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    db.write(
        &namespace,
        vec![TraceBatch::new(vec![
            trace(2, 2, "second", Vec::new(), Vec::new()),
            trace(1, 1, "first", Vec::new(), Vec::new()),
        ])],
    )
    .await
    .unwrap();
    let results = db
        .query_traceql(
            &namespace,
            0,
            10,
            "{}",
            QueryOptions {
                limit: 1,
                ..QueryOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].matched_spans[0].name, "first");
    db.close().await.unwrap();
}

#[tokio::test]
async fn search_finds_traces_that_started_in_an_earlier_segment() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::new("long-traces").unwrap();
    let second = 1_000_000_000;
    let mut long = trace(1, second, "long", vec![], vec![]);
    long.resource_spans[0].scope_spans[0].spans[0].end_time_unix_nano = 25 * second;
    let short = trace(2, 2 * second, "short", vec![], vec![]);
    db.write(&namespace, vec![TraceBatch::new(vec![long, short])])
        .await
        .unwrap();

    for (start, wanted) in [(0, vec!["long", "short"]), (21, vec!["long"]), (26, vec![])] {
        let results = db
            .query_traceql(
                &namespace,
                start * second,
                40 * second,
                "{}",
                QueryOptions::default(),
            )
            .await
            .unwrap();
        let mut names = results
            .iter()
            .map(|result| result.matched_spans[0].name.as_str())
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, wanted, "window starting at {start}s");
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn traceql_limit_stops_loading_without_changing_results() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    // Start order is the reverse of ID order, spread over several pages.
    for chunk in (1..=30u8).collect::<Vec<_>>().chunks(10) {
        db.write(
            &namespace,
            vec![TraceBatch::new(
                chunk
                    .iter()
                    .map(|&id| trace(id, u64::from(31 - id) * 10, "span", vec![], vec![]))
                    .collect(),
            )],
        )
        .await
        .unwrap();
    }
    // A later continuation moves trace 1 from last to first.
    let mut early = trace(1, 5, "early", vec![], vec![]);
    early.resource_spans[0].scope_spans[0].spans[0].span_id = vec![0xee; 8];
    db.write(&namespace, vec![TraceBatch::new(vec![early])])
        .await
        .unwrap();
    let query = |options| db.query_traceql(&namespace, 0, 1_000, "{}", options);
    let key = |results: Vec<TraceQlResult>| {
        results
            .into_iter()
            .map(|result| (result.start_ns, result.trace_id))
            .collect::<Vec<_>>()
    };
    let all = key(query(QueryOptions::default()).await.unwrap());
    assert_eq!(all.len(), 30);
    assert_eq!(all[0], (5, TraceId::new([1; 16]).unwrap()));
    assert!(all.is_sorted());
    for limit in [1, 3, 8, 29, 30] {
        let limited = key(query(QueryOptions {
            limit,
            max_concurrency: 2,
            ..QueryOptions::default()
        })
        .await
        .unwrap());
        assert_eq!(limited, all[..limit], "limit {limit}");
    }
    // Only the first batch is loaded, so a cap below the match count holds.
    let limited = query(QueryOptions {
        limit: 3,
        max_candidate_traces: 10,
        ..QueryOptions::default()
    })
    .await
    .unwrap();
    assert_eq!(key(limited), all[..3]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn traceql_candidate_limit_is_explicit() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    db.write(
        &namespace,
        vec![TraceBatch::new(vec![
            trace(1, 1, "one", Vec::new(), Vec::new()),
            trace(2, 2, "two", Vec::new(), Vec::new()),
        ])],
    )
    .await
    .unwrap();
    let error = db
        .query_traceql(
            &namespace,
            0,
            10,
            "{}",
            QueryOptions {
                max_candidate_traces: 1,
                ..QueryOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::TraceQl(crate::traceql::QueryError::Limit(_))
    ));
    db.close().await.unwrap();
}

/// Counts reads of the fixed locator segment, which hold every head and
/// continuation record, and page metadata gets in segment 0.
struct CountingStorage {
    inner: Arc<dyn StorageRead>,
    locator_scope: Bytes,
    metadata_scope: Bytes,
    locator_gets: std::sync::atomic::AtomicUsize,
    locator_scans: std::sync::atomic::AtomicUsize,
    metadata_gets: std::sync::atomic::AtomicUsize,
    metadata_scans: std::sync::atomic::AtomicUsize,
}

impl CountingStorage {
    fn counts(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.locator_gets.load(Relaxed),
            self.locator_scans.load(Relaxed),
        )
    }
}

#[async_trait]
impl StorageRead for CountingStorage {
    async fn get(
        &self,
        key: Bytes,
    ) -> common::storage::StorageResult<Option<common::storage::Record>> {
        if key.starts_with(&self.locator_scope) {
            self.locator_gets
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if key.starts_with(&self.metadata_scope) {
            self.metadata_gets
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
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
        if prefix.starts_with(&self.locator_scope) {
            self.locator_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if prefix.starts_with(&self.metadata_scope) {
            self.metadata_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.inner
            .scan_prefix_iter(prefix, subrange, filter_context)
            .await
    }
}

fn count_reads(db: &mut TraceDb, namespace: &Namespace) -> Arc<CountingStorage> {
    let counting = Arc::new(CountingStorage {
        inner: Arc::clone(&db.storage),
        locator_scope: segment_prefix(namespace, LOCATOR_SEGMENT),
        metadata_scope: metadata_prefix(namespace, 0),
        locator_gets: Default::default(),
        locator_scans: Default::default(),
        metadata_gets: Default::default(),
        metadata_scans: Default::default(),
    });
    db.storage = counting.clone();
    counting
}

#[tokio::test]
async fn metadata_reads_follow_the_most_selective_clause() {
    let mut db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    for round in 0..10u8 {
        let batch = (0..4u8)
            .map(|offset| {
                let id = round * 4 + offset + 1;
                trace(
                    id,
                    u64::from(id) * 10,
                    "span",
                    vec![
                        attr("run", any_value::Value::StringValue("r".into())),
                        attr("round", any_value::Value::IntValue(i64::from(round))),
                    ],
                    vec![attr("method", any_value::Value::StringValue("POST".into()))],
                )
            })
            .collect::<Vec<_>>();
        db.write(&namespace, vec![TraceBatch::new(batch)])
            .await
            .unwrap();
    }
    let counting = count_reads(&mut db, &namespace);
    let results = db
        .query_traceql(
            &namespace,
            0,
            1_000,
            r#"{ resource.run = "r" && resource.round = 3 && span.method = "POST" }"#,
            QueryOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        results
            .iter()
            .map(|result| result.trace_id)
            .collect::<Vec<_>>(),
        (13..=16u8)
            .map(|id| TraceId::new([id; 16]).unwrap())
            .collect::<Vec<_>>()
    );
    // Ten pages are posted by `run` and `method`, but only the one page
    // `round` posts has its metadata read.
    assert_eq!(
        counting
            .metadata_gets
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn nearby_metadata_is_scanned_and_distant_metadata_is_fetched() {
    let mut db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    for round in 0..10u8 {
        let mut resource = vec![attr("run", any_value::Value::StringValue("r".into()))];
        if matches!(round, 0 | 1 | 9) {
            resource.push(attr("edge", any_value::Value::BoolValue(true)));
        }
        let batch = (0..4u8)
            .map(|offset| {
                let id = round * 4 + offset + 1;
                trace(id, u64::from(id) * 10, "span", resource.clone(), Vec::new())
            })
            .collect::<Vec<_>>();
        db.write(&namespace, vec![TraceBatch::new(batch)])
            .await
            .unwrap();
    }
    let counting = count_reads(&mut db, &namespace);
    let results = db
        .query_traceql(
            &namespace,
            0,
            1_000,
            "{ resource.edge = true }",
            QueryOptions::default(),
        )
        .await
        .unwrap();
    let mut found = results
        .iter()
        .map(|result| result.trace_id)
        .collect::<Vec<_>>();
    found.sort();
    assert_eq!(
        found,
        [1..=8u8, 37..=40]
            .into_iter()
            .flatten()
            .map(|id| TraceId::new([id; 16]).unwrap())
            .collect::<Vec<_>>()
    );
    // Pages 0 and 1 share one scan; seven pages separate page 9, so it is
    // fetched on its own.
    use std::sync::atomic::Ordering::Relaxed;
    assert_eq!(counting.metadata_scans.load(Relaxed), 1);
    assert_eq!(counting.metadata_gets.load(Relaxed), 1);
    db.close().await.unwrap();
}

async fn head(db: &TraceDb, namespace: &Namespace, trace_id: TraceId) -> TraceHead {
    decode_head(
        &db.storage
            .get(head_key(namespace, trace_id))
            .await
            .unwrap()
            .unwrap()
            .value,
    )
    .unwrap()
}

async fn prefix_count(db: &TraceDb, prefix: Bytes) -> usize {
    db.storage
        .scan(BytesRange::from_prefix_and_subrange(
            &prefix,
            &BytesRange::unbounded(),
        ))
        .await
        .unwrap()
        .len()
}

async fn page_trace(db: &TraceDb, namespace: &Namespace, locator: TraceLocator) -> PageTrace {
    let record = db
        .storage
        .get(metadata_key(
            namespace,
            locator.segment,
            locator.page_sequence,
        ))
        .await
        .unwrap()
        .unwrap();
    decode_metadata(&record.value).unwrap().traces[locator.trace_index as usize]
}

fn with_span_id(mut trace: Trace, span_id: u8) -> Trace {
    trace.resource_spans[0].scope_spans[0].spans[0].span_id = vec![span_id; 8];
    trace
}

#[tokio::test]
async fn every_page_gets_a_continuation_and_heads_count_pages_across_flushes() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    let once = trace(1, 1_000, "once", vec![], vec![]);
    let later = trace(2, 2_000, "first", vec![], vec![]);
    // Same trace twice in one batch: the page builder cuts a second page.
    let split = trace(3, 3_000, "split", vec![], vec![]);
    db.write(
        &namespace,
        vec![TraceBatch::new(vec![
            once.clone(),
            later.clone(),
            split.clone(),
            with_span_id(split.clone(), 0x33),
        ])],
    )
    .await
    .unwrap();

    let first = head(&db, &namespace, once.trace_id).await;
    assert!(!first.continued);
    assert_eq!(
        prefix_count(&db, continuation_prefix(&namespace, once.trace_id)).await,
        1
    );
    assert!(!page_trace(&db, &namespace, first.first).await.continued);
    assert_eq!(first.pages, 1);
    let split_head = head(&db, &namespace, split.trace_id).await;
    assert!(split_head.continued);
    assert_eq!(split_head.pages, 2);
    assert_eq!(
        prefix_count(&db, continuation_prefix(&namespace, split.trace_id)).await,
        2
    );
    // Locate `later` while it has one page, so its continuation in a later
    // flush must replace the cached locators.
    assert_eq!(
        db.locate(&namespace, later.trace_id, unix_time_ms().unwrap())
            .await
            .unwrap()
            .len(),
        1
    );

    // Continue `later` twice, in a later segment, across separate flushes,
    // locating it in between so the second continuation must replace the
    // cached locators.
    for (pages, (start, span_id)) in [(2, (25_000_000_000, 0x21)), (3, (35_000_000_000, 0x22))] {
        db.write(
            &namespace,
            vec![TraceBatch::new(vec![with_span_id(
                trace(2, start, "next", vec![], vec![]),
                span_id,
            )])],
        )
        .await
        .unwrap();
        let located = db
            .cached_locate(&namespace, later.trace_id, unix_time_ms().unwrap())
            .await
            .unwrap();
        assert_eq!(located.len(), pages);
    }
    let later_head = head(&db, &namespace, later.trace_id).await;
    assert!(later_head.continued);
    assert_eq!(later_head.pages, 3);
    let locators = db
        .locate(&namespace, later.trace_id, unix_time_ms().unwrap())
        .await
        .unwrap();
    assert_eq!(locators.len(), 3);
    let first_page = later_head.first.page();
    assert!(locators.iter().any(|locator| locator.page() == first_page));
    // Each page of `later` was the first of its own flush.
    for locator in &locators {
        assert!(!page_trace(&db, &namespace, *locator).await.continued);
    }
    assert_eq!(
        later_head.first.expires_at_unix_ms,
        locators
            .iter()
            .map(|locator| locator.expires_at_unix_ms)
            .max()
            .unwrap()
    );
    assert_eq!(
        db.get_trace(&namespace, later.trace_id)
            .await
            .unwrap()
            .unwrap()
            .spans()
            .count(),
        3
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn the_writer_reads_each_trace_head_once() {
    let mut db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    let traces = (1..=20u8)
        .map(|id| {
            trace(
                id,
                u64::from(id) * 10,
                "span",
                vec![],
                vec![attr("k", any_value::Value::StringValue("v".into()))],
            )
        })
        .collect::<Vec<_>>();
    db.write(&namespace, vec![TraceBatch::new(traces)])
        .await
        .unwrap();
    db.write(
        &namespace,
        vec![TraceBatch::new(vec![with_span_id(
            trace(4, 15, "late", vec![], vec![]),
            0x44,
        )])],
    )
    .await
    .unwrap();
    let counting = count_reads(&mut db, &namespace);
    for (query, reads) in [("{}", (20, 3)), (r#"{ span.k = "v" }"#, (0, 2))] {
        let before = counting.counts();
        let results = db
            .query_traceql(&namespace, 0, 1_000, query, QueryOptions::default())
            .await
            .unwrap();
        assert_eq!(results.len(), 20, "{query}");
        assert_eq!(
            results[1].trace_id,
            TraceId::new([4; 16]).unwrap(),
            "{query}"
        );
        let after = counting.counts();
        // The catalog scan of the locator segment, plus, until the writer
        // has cached their locators, every candidate's head and the one
        // continued trace's continuation scan.
        assert_eq!((after.0 - before.0, after.1 - before.1), reads, "{query}");
    }
    let before = counting.counts();
    db.get_trace(&namespace, TraceId::new([1; 16]).unwrap())
        .await
        .unwrap()
        .unwrap();
    let after = counting.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 0));
    db.close().await.unwrap();
}

#[tokio::test]
async fn expired_first_page_of_a_continued_trace_is_skipped() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    let first = trace(9, 1_000, "first", vec![], vec![]);
    db.write(&namespace, vec![TraceBatch::new(vec![first.clone()])])
        .await
        .unwrap();
    db.write(
        &namespace,
        vec![TraceBatch::new(vec![with_span_id(
            trace(9, 2_000, "second", vec![], vec![]),
            0x99,
        )])],
    )
    .await
    .unwrap();
    let mut expired = head(&db, &namespace, first.trace_id).await.first;
    expired.expires_at_unix_ms = Some(0);
    db.writer
        .as_ref()
        .unwrap()
        .apply(vec![RecordOp::put_with_ttl(
            continuation_key(
                &namespace,
                first.trace_id,
                expired.segment,
                expired.page_sequence,
            ),
            encode_locator(&expired).unwrap(),
            Ttl::NoExpiry,
        )])
        .await
        .unwrap();
    let found = db
        .get_trace(&namespace, first.trace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        found
            .spans()
            .map(|span| span.name.as_str())
            .collect::<Vec<_>>(),
        ["second"]
    );
    let scanned = db.scan_trace_ids(&namespace, 10).await.unwrap();
    assert_eq!(scanned.len(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn unconfirmed_candidates_are_kept_only_when_their_head_counts_other_pages() {
    const SECOND: u64 = 1_000_000_000;
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    let k = |value: &str| vec![attr("k", any_value::Value::StringValue(value.into()))];
    db.write(
        &namespace,
        vec![TraceBatch::new(vec![
            trace(1, SECOND, "single", vec![], k("v")),
            trace(2, SECOND, "split", vec![], k("v")),
        ])],
    )
    .await
    .unwrap();
    // A later flush continues trace 2 in another segment, so each clause
    // sees it on a different unflagged page and neither confirms it.
    db.write(
        &namespace,
        vec![TraceBatch::new(vec![
            with_span_id(trace(2, 15 * SECOND, "split", vec![], k("w")), 0x22),
            trace(3, 15 * SECOND, "single", vec![], k("w")),
        ])],
    )
    .await
    .unwrap();
    let query = r#"{ span.k = "v" } && { span.k = "w" }"#;
    let plan = crate::traceql::plan(crate::traceql::parse(query).unwrap()).unwrap();
    let located = db
        .ordered_candidates(
            &namespace,
            0,
            20 * SECOND,
            &plan.pushdown,
            unix_time_ms().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        located
            .iter()
            .map(|located| located.trace_id)
            .collect::<Vec<_>>(),
        vec![TraceId::new([2; 16]).unwrap()]
    );
    let results = db
        .query_traceql(&namespace, 0, 20 * SECOND, query, QueryOptions::default())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].trace_id, TraceId::new([2; 16]).unwrap());
    db.close().await.unwrap();
}

#[tokio::test]
async fn lazy_locating_matches_evaluating_every_trace() {
    const SECOND: u64 = 1_000_000_000;
    for seed in 1..=6u64 {
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        // Continuations land in other segments and often start earlier, on
        // pages whose postings a predicate may not match.
        for write in 0..4u8 {
            let ids = (1..=40u8)
                .filter(|_| write == 0 || next(3) == 0)
                .collect::<Vec<_>>();
            let batch = ids
                .into_iter()
                .map(|id| {
                    let value = if next(2) == 0 { "v" } else { "w" };
                    with_span_id(
                        trace(
                            id,
                            next(30) * SECOND + next(SECOND),
                            "span",
                            vec![],
                            vec![attr("k", any_value::Value::StringValue(value.into()))],
                        ),
                        write * 64 + id,
                    )
                })
                .collect::<Vec<_>>();
            db.write(&namespace, vec![TraceBatch::new(batch)])
                .await
                .unwrap();
        }
        let stored = db.scan_traces(&namespace, 1_000).await.unwrap();
        for (query, window) in [
            ("{}", (0, 40 * SECOND)),
            ("{}", (5 * SECOND, 15 * SECOND)),
            (r#"{ span.k = "v" }"#, (0, 40 * SECOND)),
            (r#"{ span.k = "v" } && { span.k = "w" }"#, (0, 40 * SECOND)),
        ] {
            let plan = crate::traceql::plan(crate::traceql::parse(query).unwrap()).unwrap();
            // A trace is in range when one of its stored pages is.
            let now = unix_time_ms().unwrap();
            let mut in_window = HashSet::new();
            for trace in &stored {
                for locator in db.locate(&namespace, trace.trace_id, now).await.unwrap() {
                    if page_trace(&db, &namespace, locator)
                        .await
                        .overlaps(window.0, window.1)
                    {
                        in_window.insert(trace.trace_id);
                    }
                }
            }
            let mut expected = stored
                .iter()
                .filter(|trace| in_window.contains(&trace.trace_id))
                .filter_map(|trace| {
                    crate::traceql::execute(trace, &plan.query, 10_000)
                        .unwrap()
                        .map(|result| (result.start_ns, result.trace_id))
                })
                .collect::<Vec<_>>();
            expected.sort_unstable();
            let run = |limit| {
                db.query_traceql(
                    &namespace,
                    window.0,
                    window.1,
                    query,
                    QueryOptions {
                        limit,
                        max_concurrency: 3,
                        ..QueryOptions::default()
                    },
                )
            };
            let all = run(1_000)
                .await
                .unwrap()
                .into_iter()
                .map(|result| (result.start_ns, result.trace_id))
                .collect::<Vec<_>>();
            assert_eq!(all, expected, "seed {seed} {query} {window:?}");
            for limit in [1, 5, 13] {
                let limited = run(limit)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|result| (result.start_ns, result.trace_id))
                    .collect::<Vec<_>>();
                assert_eq!(
                    limited.len(),
                    limit.min(expected.len()),
                    "seed {seed} {query}"
                );
                assert!(limited.is_sorted(), "seed {seed} {query}");
                assert!(
                    limited.iter().all(|found| expected.contains(found)),
                    "seed {seed} {query}"
                );
            }
        }
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn column_pruning_matches_full_evaluation() {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;

    use crate::traceql::columns::testing;

    const SECOND: u64 = 1_000_000_000;
    let mut runner = TestRunner::deterministic();
    let mut generate = |id: u8| {
        let trace = testing::trace(id).new_tree(&mut runner).unwrap().current();
        let mut resource_spans = trace.resource_spans;
        for span in resource_spans
            .iter_mut()
            .flat_map(|resource| &mut resource.scope_spans)
            .flat_map(|scope| &mut scope.spans)
        {
            span.end_time_unix_nano = span
                .end_time_unix_nano
                .min(span.start_time_unix_nano + 5 * SECOND);
        }
        Trace::new(trace.trace_id, resource_spans).unwrap()
    };
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    // Later writes continue a third of the traces on new pages.
    for write in 0..3u8 {
        let batch = (1..=60u8)
            .filter(|id| write == 0 || id % 3 == write)
            .map(&mut generate)
            .collect::<Vec<_>>();
        db.write(&namespace, vec![TraceBatch::new(batch)])
            .await
            .unwrap();
    }
    let stored = db.scan_traces(&namespace, 1_000).await.unwrap();
    let now = unix_time_ms().unwrap();
    let mut pages = BTreeSet::new();
    for trace in &stored {
        for locator in db.locate(&namespace, trace.trace_id, now).await.unwrap() {
            pages.insert(locator.page());
        }
    }
    assert!(pages.len() > 3, "{} pages", pages.len());

    let mut queries = [
        "{ duration > 1s }",
        "{ status = error && kind = server }",
        "{ kind = consumer && status = error && duration > 1ms }",
        r#"{ name = "get" } >> { status = error }"#,
        r#"{ resource.service.name = "api" && duration >= 1ms }"#,
        "{ duration != 1ms } | count() > 1",
        "{ status = ok } !>> { kind = client }",
    ]
    .map(str::to_owned)
    .to_vec();
    let generated = testing::query();
    queries.extend((0..80).map(|_| generated.new_tree(&mut runner).unwrap().current()));
    let queries = queries
        .into_iter()
        .filter(|query| crate::traceql::plan(crate::traceql::parse(query).unwrap()).is_ok())
        .collect::<Vec<_>>();

    let window = (0, 100 * SECOND);
    let mut pruned = 0;
    for query in &queries {
        let parsed = crate::traceql::parse(query).unwrap();
        let mut expected = stored
            .iter()
            .filter_map(|trace| {
                crate::traceql::execute(trace, &parsed, 10_000)
                    .unwrap()
                    .map(|result| (result.start_ns, result.trace_id))
            })
            .collect::<Vec<_>>();
        expected.sort_unstable();
        let found = db
            .query_traceql(
                &namespace,
                window.0,
                window.1,
                query,
                QueryOptions::default(),
            )
            .await
            .unwrap()
            .into_iter()
            .map(|result| (result.start_ns, result.trace_id))
            .collect::<Vec<_>>();
        assert_eq!(found, expected, "{query}");

        let Some(filter) = crate::traceql::prefilter(&parsed) else {
            continue;
        };
        let located = db
            .ordered_candidates(&namespace, window.0, window.1, &[], now)
            .await
            .unwrap();
        let complete = stored.iter().map(|trace| trace.trace_id).collect();
        let all = db
            .load_candidates(
                &namespace,
                located.clone(),
                &query::PageMemo::default(),
                None,
            )
            .await
            .unwrap();
        let kept = db
            .load_candidates(
                &namespace,
                located,
                &query::PageMemo::default(),
                Some(query::Prune::new(&filter, 10_000, &complete)),
            )
            .await
            .unwrap();
        let matching = |traces: &[Trace]| {
            traces
                .iter()
                .filter_map(|trace| crate::traceql::execute(trace, &parsed, 10_000).unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(matching(&kept), matching(&all), "{query}");
        assert!(kept.iter().all(|trace| all.contains(trace)), "{query}");
        pruned += all.len() - kept.len();
    }
    assert!(pruned > 0);
    db.close().await.unwrap();
}

mod value_ranges {
    use proptest::prelude::*;

    use super::*;
    use crate::traceql::{BinaryOp, StaticValue};

    const EDGE_INTS: [i64; 9] = [
        i64::MIN,
        -(1 << 53) - 1,
        -1,
        0,
        1,
        1 << 53,
        (1 << 53) + 1,
        i64::MAX - 1,
        i64::MAX,
    ];
    const EDGE_DOUBLES: [f64; 14] = [
        0.0,
        -0.0,
        0.5,
        -1.5,
        1.5,
        f64::MIN_POSITIVE,
        9_007_199_254_740_992.0,
        9_223_372_036_854_775_807.0,
        -9_223_372_036_854_775_808.0,
        1e300,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
        -f64::NAN,
    ];

    fn literal() -> impl Strategy<Value = StaticValue> {
        prop_oneof![
            any::<i64>().prop_map(StaticValue::Int),
            prop::sample::select(EDGE_INTS.to_vec()).prop_map(StaticValue::Int),
            (-3_i64..3).prop_map(StaticValue::Duration),
            any::<f64>().prop_map(StaticValue::Float),
            prop::sample::select(EDGE_DOUBLES.to_vec()).prop_map(StaticValue::Float),
        ]
    }

    /// Stored values of every type, many next to the literal.
    fn value(literal: &StaticValue) -> BoxedStrategy<AttributeValue> {
        let (near_int, near_double) = match *literal {
            StaticValue::Int(value) | StaticValue::Duration(value) => (value, value as f64),
            StaticValue::Float(value) => (value as i64, value),
            _ => (0, 0.0),
        };
        prop_oneof![
            any::<i64>().prop_map(AttributeValue::Int),
            prop::sample::select(EDGE_INTS.to_vec()).prop_map(AttributeValue::Int),
            (-5_000_i64..5_000)
                .prop_map(move |delta| AttributeValue::Int(near_int.saturating_add(delta))),
            any::<f64>().prop_map(AttributeValue::Double),
            prop::sample::select(EDGE_DOUBLES.to_vec()).prop_map(AttributeValue::Double),
            (-3_i64..=3).prop_map(move |steps| {
                let bits = near_double.to_bits() as i64;
                AttributeValue::Double(f64::from_bits(bits.wrapping_add(steps) as u64))
            }),
            Just(AttributeValue::String("5".to_owned())),
            Just(AttributeValue::Bool(true)),
        ]
        .boxed()
    }

    fn in_ranges(predicate: &IndexPredicate, value: &AttributeValue) -> Option<bool> {
        let namespace = Namespace::default();
        let prefix = field_scan_prefix(&namespace, 0, predicate.field, &predicate.name);
        let key = field_posting_key(
            &namespace,
            0,
            (predicate.field, &predicate.name, value),
            u64::MAX,
        );
        let suffix = &key[prefix.len()..];
        let ranges = predicate.value_ranges()?;
        Some(ranges.iter().any(|(low, high)| {
            let (lower, upper) = value_subrange(low, high);
            lower.as_ref() <= suffix && suffix <= upper.as_ref()
        }))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4_000))]

        #[test]
        fn numeric_ranges_hold_every_admitted_value(
            (literal, values) in literal().prop_flat_map(|literal| {
                let values = prop::collection::vec(value(&literal), 1..16);
                (Just(literal), values)
            }),
            op in prop::sample::select(vec![
                BinaryOp::Less,
                BinaryOp::LessEqual,
                BinaryOp::Greater,
                BinaryOp::GreaterEqual,
                BinaryOp::Equal,
            ]),
        ) {
            let predicate = IndexPredicate {
                field: IndexField::Span,
                name: "n".to_owned(),
                test: IndexTest::Compare(op, literal.clone()),
            };
            for value in &values {
                let Some(found) = in_ranges(&predicate, value) else {
                    prop_assert!(op == BinaryOp::Equal && matches!(literal, StaticValue::Float(value) if value.is_nan()));
                    continue;
                };
                prop_assert!(
                    found || !predicate.admits(value),
                    "{:?} {:?} {:?} admitted outside its ranges", value, op, literal
                );
            }
        }

        #[test]
        fn duration_ranges_hold_every_admitted_bucket(
            nanoseconds in prop_oneof![
                any::<f64>(),
                (0_u64..=u64::MAX).prop_map(|value| value as f64),
                prop::sample::select(vec![0.0, 1.0, 1.5, 1023.0, 1024.0, f64::NAN]),
            ],
            op in prop::sample::select(vec![
                BinaryOp::Less,
                BinaryOp::LessEqual,
                BinaryOp::Greater,
                BinaryOp::GreaterEqual,
                BinaryOp::Equal,
            ]),
        ) {
            let predicate = IndexPredicate {
                field: IndexField::Intrinsic,
                name: "duration".to_owned(),
                test: IndexTest::Duration(op, nanoseconds),
            };
            for bucket in 0..=64 {
                let bucket = AttributeValue::Int(bucket);
                prop_assert_eq!(in_ranges(&predicate, &bucket), Some(predicate.admits(&bucket)));
            }
        }
    }

    #[test]
    fn string_and_inequality_tests_scan_the_field() {
        for test in [
            IndexTest::Compare(BinaryOp::NotEqual, StaticValue::Int(1)),
            IndexTest::Compare(BinaryOp::Greater, StaticValue::String("a".to_owned())),
            IndexTest::Compare(BinaryOp::Regex, StaticValue::String("a.*".to_owned())),
        ] {
            let predicate = IndexPredicate {
                field: IndexField::Span,
                name: "n".to_owned(),
                test,
            };
            assert_eq!(predicate.value_ranges(), None, "{predicate:?}");
        }
    }

    /// Queries with different bounds share segments and the posting cache,
    /// and each still returns exactly what evaluating every trace does.
    #[tokio::test]
    async fn bounded_scans_match_full_evaluation() {
        let values = [
            any_value::Value::IntValue(i64::MIN),
            any_value::Value::IntValue(-7),
            any_value::Value::IntValue(0),
            any_value::Value::IntValue(1),
            any_value::Value::IntValue(2),
            any_value::Value::IntValue(500),
            any_value::Value::IntValue(1 << 53),
            any_value::Value::IntValue((1 << 53) + 1),
            any_value::Value::IntValue(i64::MAX),
            any_value::Value::DoubleValue(-0.0),
            any_value::Value::DoubleValue(0.0),
            any_value::Value::DoubleValue(1.5),
            any_value::Value::DoubleValue(-2.5),
            any_value::Value::DoubleValue(499.999),
            any_value::Value::DoubleValue(f64::INFINITY),
            any_value::Value::DoubleValue(f64::NEG_INFINITY),
            any_value::Value::DoubleValue(f64::NAN),
            any_value::Value::StringValue("3".to_owned()),
            any_value::Value::BoolValue(true),
        ];
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        let traces = values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                trace(
                    index as u8 + 1,
                    index as u64,
                    "op",
                    Vec::new(),
                    vec![attr("n", value.clone())],
                )
            })
            .collect::<Vec<_>>();
        db.write(&namespace, vec![TraceBatch::new(traces.clone())])
            .await
            .unwrap();
        let mut checked = 0;
        for literal in [
            "0",
            "1",
            "2",
            "500",
            "-0.0",
            "0.0",
            "1.5",
            "2.0",
            "499.999",
            "500.0",
            "1e300",
            "9007199254740992",
            "9007199254740993",
            "9223372036854775807",
            "1ms",
        ] {
            for op in ["<", "<=", ">", ">=", "="] {
                let source = format!("{{ span.n {op} {literal} }}");
                let Ok(parsed) = crate::traceql::parse(&source) else {
                    continue;
                };
                checked += 1;
                let mut expected = traces
                    .iter()
                    .filter(|trace| {
                        crate::traceql::execute(trace, &parsed, 1_000)
                            .unwrap()
                            .is_some()
                    })
                    .map(|trace| trace.trace_id)
                    .collect::<Vec<_>>();
                expected.sort_unstable();
                let mut found = db
                    .query_traceql(&namespace, 0, 100, &source, QueryOptions::default())
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|result| result.trace_id)
                    .collect::<Vec<_>>();
                found.sort_unstable();
                assert_eq!(found, expected, "{source}");
            }
        }
        assert!(checked > 40, "{checked} queries parsed");
        db.close().await.unwrap();
    }

    /// Search, deciding and summarizing from columns where it can, returns
    /// exactly what evaluating every merged trace does, for generated traces
    /// (some continued across flushes) and queries.
    #[tokio::test]
    async fn generated_queries_match_the_interpreter() {
        use proptest::strategy::{Strategy, ValueTree};
        use proptest::test_runner::TestRunner;

        use crate::traceql::columns::testing;

        let mut runner = TestRunner::deterministic();
        let mut trace = |id: u8| testing::trace(id).new_tree(&mut runner).unwrap().current();
        let first = (1..=30).map(&mut trace).collect::<Vec<_>>();
        let second = (1..=6).map(&mut trace).collect::<Vec<_>>();
        let queries = (0..120)
            .map(|_| testing::query().new_tree(&mut runner).unwrap().current())
            .collect::<Vec<_>>();
        let db = TraceDb::open(test_config()).await.unwrap();
        let namespace = Namespace::default();
        for batch in [&first, &second] {
            db.write(&namespace, vec![TraceBatch::new(batch.clone())])
                .await
                .unwrap();
        }
        let merged = crate::db::merge_traces(first.into_iter().chain(second).collect()).unwrap();
        let options = QueryOptions {
            limit: 100,
            ..QueryOptions::default()
        };
        let mut matched = 0;
        for source in queries {
            let parsed = crate::traceql::parse(&source).unwrap();
            let mut expected = merged
                .iter()
                .filter_map(|trace| crate::traceql::execute(trace, &parsed, 1_000).unwrap())
                .collect::<Vec<_>>();
            expected.sort_by_key(|result| (result.start_ns, result.trace_id));
            let found = db
                .query_traceql(&namespace, 0, u64::MAX, &source, options)
                .await
                .unwrap();
            assert_eq!(found, expected, "{source}");
            let summaries = db
                .search_traceql(&namespace, 0, u64::MAX, &source, options)
                .await
                .unwrap();
            let expected = expected
                .into_iter()
                .map(TraceSummary::from)
                .collect::<Vec<_>>();
            assert_eq!(summaries, expected, "{source}");
            matched += expected.len();
        }
        assert!(matched > 0);
        db.close().await.unwrap();
    }
}
