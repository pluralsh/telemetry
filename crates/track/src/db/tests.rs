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
            path: "track-test".to_owned(),
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
        path: "track-durable".to_owned(),
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
        path: "track-shutdown".to_owned(),
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
    for trace in &traces {
        let mut locators = db
            .storage
            .scan_prefix_iter(
                locator_prefix(&namespace, trace.trace_id),
                BytesRange::unbounded(),
                None,
            )
            .await
            .unwrap();
        while let Some(record) = locators.next().await.unwrap() {
            let locator = decode_locator(&record.value).unwrap();
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
        path: "track-reopen".to_owned(),
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
async fn traceql_intrinsic_pushdown_keeps_pages_without_intrinsic_postings() {
    let db = TraceDb::open(test_config()).await.unwrap();
    let namespace = Namespace::default();
    let traces = intrinsic_traces();
    db.write(&namespace, vec![TraceBatch::new(traces.clone())])
        .await
        .unwrap();

    // Pages written before intrinsics were indexed have no intrinsic postings.
    let mut segments = HashSet::new();
    for trace in &traces {
        let mut locators = db
            .storage
            .scan_prefix_iter(
                locator_prefix(&namespace, trace.trace_id),
                BytesRange::unbounded(),
                None,
            )
            .await
            .unwrap();
        while let Some(record) = locators.next().await.unwrap() {
            segments.insert(decode_locator(&record.value).unwrap().segment);
        }
    }
    let mut deletes = Vec::new();
    for segment in segments {
        for name in ["name", "status", "kind", "duration"] {
            let mut postings = db
                .storage
                .scan_prefix_iter(
                    field_scan_prefix(&namespace, segment, IndexField::Intrinsic, name).freeze(),
                    BytesRange::unbounded(),
                    None,
                )
                .await
                .unwrap();
            while let Some(record) = postings.next().await.unwrap() {
                deletes.push(RecordOp::Delete(record.key));
            }
        }
    }
    assert!(!deletes.is_empty());
    db.writer.as_ref().unwrap().apply(deletes).await.unwrap();

    for (query, wanted) in [
        ("{ status = error }", "GET /api/users"),
        (r#"{ name = "POST /health" }"#, "POST /health"),
        ("{ duration > 1ms }", "GET /api/users"),
    ] {
        let results = db
            .query_traceql(&namespace, 0, 10, query, QueryOptions::default())
            .await
            .unwrap();
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
