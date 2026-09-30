use super::*;
use crate::model::MetricType;
use crate::model::Sample;
use crate::storage::in_memory_storage;

fn create_sample(
    metric_name: &str,
    label_pairs: Vec<(&str, &str)>,
    timestamp: i64,
    value: f64,
) -> Series {
    let mut labels = vec![Label {
        name: "__name__".to_string(),
        value: metric_name.to_string(),
    }];
    for (key, val) in label_pairs {
        labels.push(Label {
            name: key.to_string(),
            value: val.to_string(),
        });
    }
    Series {
        labels,
        unit: None,
        metric_type: Some(MetricType::Gauge),
        description: None,
        samples: vec![Sample {
            timestamp_ms: timestamp,
            value,
        }],
        histograms: Vec::new(),
    }
}

#[tokio::test]
async fn should_create_tsdb_with_caches() {
    let storage = Arc::new(in_memory_storage().await);
    // when
    let tsdb = Tsdb::new(storage);

    // then: tsdb is created successfully
    tsdb.ingest_cache.run_pending_tasks().await;
    assert_eq!(tsdb.ingest_cache.entry_count(), 0);
}

#[tokio::test]
async fn should_get_or_create_bucket_for_ingest() {
    let storage = Arc::new(in_memory_storage().await);
    // given
    let tsdb = Tsdb::new(storage);
    let bucket = TimeBucket::hour(1000);

    // when
    let mini1 = tsdb.get_or_create_for_ingest(bucket).await.unwrap();
    let mini2 = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

    // then: same Arc is returned (cached)
    assert!(Arc::ptr_eq(&mini1, &mini2));
    tsdb.ingest_cache.run_pending_tasks().await;
    assert_eq!(tsdb.ingest_cache.entry_count(), 1);
}

#[tokio::test]
async fn should_use_ingest_cache_during_queries() {
    let storage = Arc::new(in_memory_storage().await);
    // given: a bucket in the ingest cache with ingested data
    let tsdb = Tsdb::new(storage);
    let bucket = TimeBucket::hour(60);
    let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

    let sample = create_sample("test_metric", vec![("env", "prod")], 4_000_000, 1.0);
    mini.ingest(&sample).await.unwrap();
    tsdb.flush().await.unwrap();

    // when: building a query reader that covers this bucket
    let reader = tsdb.query_reader(3600, 7200).await.unwrap();

    // then: reader should see the ingested data (via ingest cache)
    let buckets = reader.list_buckets().await.unwrap();
    assert_eq!(buckets.len(), 1);
}

#[tokio::test]
async fn should_ingest_and_query_single_bucket() {
    let storage = Arc::new(in_memory_storage().await);
    // given
    let tsdb = Tsdb::new(storage);

    // Use hour-aligned bucket (60 minutes = 1 hour)
    // Bucket at minute 60 covers minutes 60-119, i.e., seconds 3600-7199
    let bucket = TimeBucket::hour(60);
    let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

    // Ingest a sample with timestamp in the bucket range (seconds 3600-7199)
    // Using 4000 seconds = 4000000 ms
    let sample = create_sample("http_requests", vec![("env", "prod")], 4000000, 42.0);
    mini.ingest(&sample).await.unwrap();

    // Flush to make data visible
    tsdb.flush().await.unwrap();

    // when: query the data with range covering the bucket (seconds 3600-7200)
    let reader = tsdb.query_reader(3600, 7200).await.unwrap();
    let terms = vec![Label {
        name: "__name__".to_string(),
        value: "http_requests".to_string(),
    }];
    let bucket = TimeBucket::hour(60);
    let index = reader.inverted_index(&bucket, &terms).await.unwrap();
    let series_ids: Vec<_> = index.intersect(terms).iter().collect();

    // then
    assert_eq!(series_ids.len(), 1);
}

#[tokio::test]
async fn should_match_per_series_reads_for_batched_scans() {
    // given: two metrics with interleaved series ids, so `m`'s forward-index
    // id range also covers `other`'s series
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);
    let bucket = TimeBucket::hour(60);
    let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();
    for i in 0..24 {
        let name = if i % 3 == 0 { "other" } else { "m" };
        let instance = i.to_string();
        for t in 0..3 {
            let sample = create_sample(
                name,
                vec![("instance", instance.as_str())],
                3_600_000 + t * 15_000,
                (i * 10 + t) as f64,
            );
            mini.ingest(&sample).await.unwrap();
        }
    }
    tsdb.flush().await.unwrap();
    let reader = tsdb.query_reader(3600, 7200).await.unwrap();
    let term = Label::metric_name("m");
    let mut ids: Vec<SeriesId> = reader
        .inverted_index(&bucket, std::slice::from_ref(&term))
        .await
        .unwrap()
        .intersect(vec![term])
        .iter()
        .collect();
    ids.sort_unstable();
    assert_eq!(ids.len(), 16);
    let (start_ms, end_ms) = (3_600_000, 3_615_000);
    let other_id = (ids[0]..ids[ids.len() - 1])
        .find(|id| !ids.contains(id))
        .unwrap();
    let requests: Vec<(&str, Vec<SeriesId>)> = vec![
        ("dense", ids.clone()),
        (
            "unordered with duplicates and an absent key",
            ids.iter()
                .rev()
                .copied()
                .chain([ids[3], ids[3], other_id])
                .collect(),
        ),
        ("sparse fallback", {
            let mut sparse = ids[..8].to_vec();
            sparse.push(ids[0] + 10_000);
            sparse
        }),
    ];

    for (case, request) in requests {
        // when
        let batched = reader
            .samples_many(&bucket, "m", &request, start_ms, end_ms)
            .await
            .unwrap();
        let specs = reader.forward_index_many(&bucket, &request).await.unwrap();

        // then: identical to one read per series, in request order
        assert_eq!(batched.len(), request.len(), "{case}");
        assert_eq!(specs.len(), request.len(), "{case}");
        // `(start_ms, end_ms]` holds the second of three samples.
        assert!(batched.iter().any(|s| s.floats.len() == 1), "{case}");
        for ((&id, samples), spec) in request.iter().zip(&batched).zip(&specs) {
            let expected = reader
                .samples(&bucket, id, "m", start_ms, end_ms)
                .await
                .unwrap();
            assert_eq!(samples, &expected, "{case}: series {id}");
            let expected_spec = reader.forward_index_one(&bucket, id).await.unwrap();
            assert_eq!(
                format!("{spec:?}"),
                format!("{expected_spec:?}"),
                "{case}: series {id}"
            );
        }
    }
}

#[tokio::test]
async fn should_query_across_multiple_buckets() {
    let storage = Arc::new(in_memory_storage().await);
    use crate::test_utils::assertions::assert_approx_eq;
    use std::time::{Duration, UNIX_EPOCH};

    // given: 4 hour-aligned buckets
    // Bucket layout (each bucket is 1 hour = 60 minutes):
    //   Bucket 60:  minutes 60-119,  seconds 3600-7199,   ms 3,600,000-7,199,999
    //   Bucket 120: minutes 120-179, seconds 7200-10799,  ms 7,200,000-10,799,999
    //   Bucket 180: minutes 180-239, seconds 10800-14399, ms 10,800,000-14,399,999
    //   Bucket 240: minutes 240-299, seconds 14400-17999, ms 14,400,000-17,999,999
    let tsdb = Tsdb::new(storage);

    // Buckets 1 & 2: will end up in query cache (ingest, flush, then invalidate from ingest cache)
    let bucket1 = TimeBucket::hour(60);
    let bucket2 = TimeBucket::hour(120);

    // Buckets 3 & 4: will stay in ingest cache
    let bucket3 = TimeBucket::hour(180);
    let bucket4 = TimeBucket::hour(240);

    // Ingest data into buckets 1 & 2
    // Sample timestamps should be well within the bucket and reachable by lookback
    // Bucket 60: covers 3,600,000-7,199,999 ms -> sample at 3,900,000 ms (3900s)
    // Bucket 120: covers 7,200,000-10,799,999 ms -> sample at 7,900,000 ms (7900s)
    let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
    mini1
        .ingest(&create_sample(
            "http_requests",
            vec![("env", "prod")],
            3_900_000,
            10.0,
        ))
        .await
        .unwrap();
    mini1
        .ingest(&create_sample(
            "http_requests",
            vec![("env", "staging")],
            3_900_001,
            15.0,
        ))
        .await
        .unwrap();

    let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
    mini2
        .ingest(&create_sample(
            "http_requests",
            vec![("env", "prod")],
            7_900_000,
            20.0,
        ))
        .await
        .unwrap();
    mini2
        .ingest(&create_sample(
            "http_requests",
            vec![("env", "staging")],
            7_900_001,
            25.0,
        ))
        .await
        .unwrap();

    // Flush buckets 1 & 2 to storage
    tsdb.flush().await.unwrap();

    // Invalidate buckets 1 & 2 from ingest cache so they'll be loaded from query cache
    tsdb.ingest_cache.invalidate(&bucket1).await;
    tsdb.ingest_cache.invalidate(&bucket2).await;
    tsdb.ingest_cache.run_pending_tasks().await;

    // Ingest data into buckets 3 & 4 (these stay in ingest cache)
    // Bucket 180: covers 10,800,000-14,399,999 ms -> sample at 11,900,000 ms (11900s)
    // Bucket 240: covers 14,400,000-17,999,999 ms -> sample at 15,900,000 ms (15900s)
    let mini3 = tsdb.get_or_create_for_ingest(bucket3).await.unwrap();
    mini3
        .ingest(&create_sample(
            "http_requests",
            vec![("env", "prod")],
            11_900_000,
            30.0,
        ))
        .await
        .unwrap();
    mini3
        .ingest(&create_sample(
            "http_requests",
            vec![("env", "staging")],
            11_900_001,
            35.0,
        ))
        .await
        .unwrap();

    let mini4 = tsdb.get_or_create_for_ingest(bucket4).await.unwrap();
    mini4
        .ingest(&create_sample(
            "http_requests",
            vec![("env", "prod")],
            15_900_000,
            40.0,
        ))
        .await
        .unwrap();
    mini4
        .ingest(&create_sample(
            "http_requests",
            vec![("env", "staging")],
            15_900_001,
            45.0,
        ))
        .await
        .unwrap();

    // Flush buckets 3 & 4 to storage (data is now visible for queries)
    tsdb.flush().await.unwrap();

    // Verify cache state: 2 in ingest cache (buckets 3 & 4)
    tsdb.ingest_cache.run_pending_tasks().await;
    assert_eq!(tsdb.ingest_cache.entry_count(), 2);

    // when: query across all 4 buckets via the public eval_query surface
    let query = r#"http_requests"#;
    let lookback = Duration::from_secs(1000);

    // Query times: one point in each bucket where we expect to find data
    // Bucket 60:  sample at 3,900,000 ms (3900s) -> query at 4000s
    // Bucket 120: sample at 7,900,000 ms (7900s) -> query at 8000s
    // Bucket 180: sample at 11,900,000 ms (11900s) -> query at 12000s
    // Bucket 240: sample at 15,900,000 ms (15900s) -> query at 16000s
    // Lookback of 1000s ensures samples are within the window
    let query_times_secs = [4000u64, 8000, 12000, 16000];
    let expected_prod_values = [10.0, 20.0, 30.0, 40.0];
    let expected_staging_values = [15.0, 25.0, 35.0, 45.0];

    let opts = QueryOptions {
        lookback_delta: lookback,
        ..Default::default()
    };
    for (i, &query_time_secs) in query_times_secs.iter().enumerate() {
        let query_time = UNIX_EPOCH + Duration::from_secs(query_time_secs);
        let result = tsdb
            .eval_query(query, Some(query_time), &opts)
            .await
            .unwrap();
        let mut results = match result {
            QueryValue::Vector(s) => s,
            other => panic!("expected Vector, got {:?}", other),
        };
        results.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));

        assert_eq!(
            results.len(),
            2,
            "query {} at {}s: expected 2 results",
            i,
            query_time_secs
        );

        // env=prod
        assert_eq!(results[0].labels.get("env"), Some("prod"));
        assert_approx_eq(results[0].value, expected_prod_values[i]);

        // env=staging
        assert_eq!(results[1].labels.get("env"), Some("staging"));
        assert_approx_eq(results[1].value, expected_staging_values[i]);
    }
}

#[tokio::test]
async fn should_query_across_multiple_buckets_with_different_series_id_mappings() {
    use std::time::{Duration, UNIX_EPOCH};

    // given: Two time buckets with overlapping series but different series IDs
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);
    // Bucket 1: hour 60 (covers 3,600,000-7,199,999 ms)
    let bucket1 = TimeBucket::hour(60);
    let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
    mini1
        .ingest(&create_sample(
            "foo",
            vec![("a", "b"), ("x", "y")],
            3_900_000,
            1.0,
        ))
        .await
        .unwrap();
    mini1
        .ingest(&create_sample(
            "foo",
            vec![("a", "c"), ("x", "z")],
            3_900_001,
            2.0,
        ))
        .await
        .unwrap();
    // Bucket 2: hour 120 (covers 7,200,000-10,799,999 ms)
    let bucket2 = TimeBucket::hour(120);
    let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
    mini2
        .ingest(&create_sample(
            "foo",
            vec![("a", "c"), ("x", "z")],
            7_900_000,
            3.0,
        ))
        .await
        .unwrap();
    mini2
        .ingest(&create_sample(
            "foo",
            vec![("a", "d"), ("x", "w")],
            7_900_001,
            4.0,
        ))
        .await
        .unwrap();
    // Flush to storage
    tsdb.flush().await.unwrap();

    // when: query foo{a="c"} at t=8000s (inside bucket 2)
    let query = r#"foo{a="c"}"#;
    let query_time = UNIX_EPOCH + Duration::from_secs(8000);
    let opts = QueryOptions {
        lookback_delta: Duration::from_secs(5000),
        ..Default::default()
    };
    let result = tsdb
        .eval_query(query, Some(query_time), &opts)
        .await
        .unwrap();
    let samples = match result {
        QueryValue::Vector(s) => s,
        other => panic!("expected Vector, got {:?}", other),
    };

    // then: we should only get the series foo{a="c",x="z"} with value 3.0
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].labels.get("a"), Some("c"));
    assert_eq!(samples[0].labels.get("x"), Some("z"));
    assert_eq!(samples[0].labels.metric_name(), "foo");
    assert_eq!(samples[0].value, 3.0);
}

// ── Native read method tests ─────────────────────────────────────

async fn create_tsdb_with_data() -> Tsdb {
    let tsdb = Tsdb::new(Arc::new(in_memory_storage().await));

    // Ingest two series into bucket at minute 60 (covers 3,600,000–7,199,999 ms)
    let series = vec![
        create_sample("http_requests", vec![("env", "prod")], 4_000_000, 42.0),
        create_sample("http_requests", vec![("env", "staging")], 4_000_000, 10.0),
    ];
    tsdb.ingest_samples(series, None).await.unwrap();
    tsdb.flush().await.unwrap();
    tsdb
}

#[tokio::test]
async fn eval_query_should_return_instant_vector() {
    let tsdb = create_tsdb_with_data().await;
    let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);

    let opts = QueryOptions::default();
    let result = tsdb
        .eval_query("http_requests", Some(query_time), &opts)
        .await
        .unwrap();
    let mut samples = match result {
        QueryValue::Vector(samples) => samples,
        other => panic!("expected Vector, got {:?}", other),
    };
    samples.sort_by(|a, b| {
        a.labels
            .metric_name()
            .cmp(b.labels.metric_name())
            .then_with(|| a.labels.get("env").cmp(&b.labels.get("env")))
    });

    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0].labels.get("env"), Some("prod"));
    assert_eq!(samples[0].value, 42.0);
    assert_eq!(samples[1].labels.get("env"), Some("staging"));
    assert_eq!(samples[1].value, 10.0);
}

#[tokio::test]
async fn eval_query_should_respect_lookback_delta() {
    // Sample at t=4000s, query at t=4100s (100s later).
    // Default 5m lookback finds it; 10s lookback should not.
    let tsdb = create_tsdb_with_data().await;
    let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);

    let wide = QueryOptions::default(); // 5m
    let results = tsdb
        .eval_query("http_requests", Some(query_time), &wide)
        .await
        .unwrap()
        .into_matrix();
    assert_eq!(results.len(), 2);

    let narrow = QueryOptions {
        lookback_delta: std::time::Duration::from_secs(10),
        ..Default::default()
    };
    let results = tsdb
        .eval_query("http_requests", Some(query_time), &narrow)
        .await
        .unwrap()
        .into_matrix();
    assert_eq!(
        results.len(),
        0,
        "10s lookback should miss samples 100s ago"
    );
}

#[tokio::test]
async fn eval_query_range_should_respect_lookback_delta() {
    // Same idea but for range queries: narrow lookback → no results.
    let tsdb = create_tsdb_with_data().await;
    let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);
    let end = start;
    let step = std::time::Duration::from_secs(60);

    let wide = QueryOptions::default();
    let results = tsdb
        .eval_query_range("http_requests", start..=end, step, &wide)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);

    let narrow = QueryOptions {
        lookback_delta: std::time::Duration::from_secs(10),
        ..Default::default()
    };
    let results = tsdb
        .eval_query_range("http_requests", start..=end, step, &narrow)
        .await
        .unwrap();
    assert!(
        results.is_empty(),
        "10s lookback should miss samples 100s ago"
    );
}

#[tokio::test]
async fn eval_query_should_return_scalar() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);
    let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);

    let opts = QueryOptions::default();
    let result = tsdb
        .eval_query("1+1", Some(query_time), &opts)
        .await
        .unwrap();

    match result {
        QueryValue::Scalar {
            timestamp_ms,
            value,
        } => {
            assert_eq!(value, 2.0);
            assert_eq!(
                timestamp_ms,
                query_time
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
            );
        }
        other => panic!("expected Scalar, got {:?}", other),
    }
}

#[tokio::test]
async fn eval_query_should_return_error_for_invalid_query() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);

    let opts = QueryOptions::default();
    let result = tsdb.eval_query("invalid{", None, &opts).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        crate::error::QueryError::InvalidQuery(_)
    ));
}

#[tokio::test]
async fn eval_query_range_should_return_range_samples() {
    let tsdb = create_tsdb_with_data().await;
    let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4000);
    let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4000);
    let step = std::time::Duration::from_secs(60);

    let opts = QueryOptions::default();
    let mut results = tsdb
        .eval_query_range("http_requests", start..=end, step, &opts)
        .await
        .unwrap();
    results.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].labels.get("env"), Some("prod"));
    assert!(!results[0].samples.is_empty());
    assert_eq!(results[1].labels.get("env"), Some("staging"));
}

#[tokio::test]
async fn eval_query_range_should_return_scalar() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);
    let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
    let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(160);
    let step = std::time::Duration::from_secs(60);

    let opts = QueryOptions::default();
    let results = tsdb
        .eval_query_range("1+1", start..=end, step, &opts)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].labels.metric_name(), "");
    assert_eq!(results[0].samples.len(), 2); // two steps: 100s and 160s
    assert_eq!(results[0].samples[0].1, 2.0);
    assert_eq!(results[0].samples[1].1, 2.0);
}

#[tokio::test]
async fn eval_query_range_should_return_error_for_invalid_query() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);
    let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
    let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(200);

    let opts = QueryOptions::default();
    let result = tsdb
        .eval_query_range(
            "invalid{",
            start..=end,
            std::time::Duration::from_secs(60),
            &opts,
        )
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        crate::error::QueryError::InvalidQuery(_)
    ));
}

#[tokio::test]
async fn find_series_should_return_matching_series() {
    let tsdb = create_tsdb_with_data().await;

    let mut results = tsdb
        .find_series(&["http_requests"], 3600, 7200)
        .await
        .unwrap();
    results.sort_by(|a, b| a.get("env").cmp(&b.get("env")));

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("env"), Some("prod"));
    assert_eq!(results[0].metric_name(), "http_requests");
    assert_eq!(results[1].get("env"), Some("staging"));
}

#[tokio::test]
async fn find_series_should_error_on_empty_matchers() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);

    let result = tsdb.find_series(&[], 0, i64::MAX).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        crate::error::QueryError::InvalidQuery(_)
    ));
}

#[tokio::test]
async fn find_series_should_dedup_across_buckets() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);

    // Same series in two different buckets
    let series1 = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
    let series2 = create_sample("cpu", vec![("host", "a")], 7_500_000, 2.0);
    tsdb.ingest_samples(vec![series1, series2], None)
        .await
        .unwrap();
    tsdb.flush().await.unwrap();

    let results = tsdb.find_series(&["cpu"], 0, i64::MAX).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].metric_name(), "cpu");
}

#[tokio::test]
async fn should_see_series_late_written_into_a_cached_bucket() {
    // given: a queried bucket, so its postings are cached across queries
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);
    let host = |value| create_sample("cpu", vec![("host", value)], 4_000_000, 1.0);
    tsdb.ingest_samples(vec![host("a")], None).await.unwrap();
    tsdb.flush().await.unwrap();
    let selectors = ["cpu", r#"cpu{host=~"a|b"}"#, r#"cpu{host=~".+"}"#];
    for selector in selectors {
        let found = tsdb.find_series(&[selector], 0, i64::MAX).await.unwrap();
        assert_eq!(found.len(), 1, "{selector}");
    }

    // when: a late sample adds a series to that bucket
    tsdb.ingest_samples(vec![host("b")], None).await.unwrap();
    tsdb.flush().await.unwrap();

    // then: every matcher shape sees it
    for selector in selectors {
        let found = tsdb.find_series(&[selector], 0, i64::MAX).await.unwrap();
        assert_eq!(found.len(), 2, "{selector}");
    }
}

#[tokio::test]
async fn should_serve_repeat_selector_postings_from_the_shared_cache() {
    // given
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);
    let sample = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
    tsdb.ingest_samples(vec![sample], None).await.unwrap();
    tsdb.flush().await.unwrap();
    let bucket = TimeBucket::round_to_hour(
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(4_000_000),
    )
    .unwrap();

    // when
    tsdb.find_series(&[r#"cpu{host=~".+"}"#], 0, i64::MAX)
        .await
        .unwrap();

    // then
    assert!(tsdb.postings_cache.label(bucket, "host").await.is_some());
    assert!(
        tsdb.postings_cache
            .term(bucket, &Label::metric_name("cpu"))
            .await
            .is_some()
    );
}

#[tokio::test]
async fn find_labels_should_return_all_label_names() {
    let tsdb = create_tsdb_with_data().await;

    let mut results = tsdb.find_labels(None, 3600, 7200).await.unwrap();
    results.sort();

    assert!(results.contains(&"__name__".to_string()));
    assert!(results.contains(&"env".to_string()));
}

#[tokio::test]
async fn find_labels_should_filter_by_matcher() {
    let tsdb = create_tsdb_with_data().await;

    let mut results = tsdb
        .find_labels(Some(&[r#"http_requests{env="prod"}"#]), 3600, 7200)
        .await
        .unwrap();
    results.sort();

    assert!(results.contains(&"__name__".to_string()));
    assert!(results.contains(&"env".to_string()));
}

#[tokio::test]
async fn find_labels_should_return_empty_for_no_data() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);

    let results = tsdb.find_labels(None, 0, 100).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn find_label_values_should_return_values() {
    let tsdb = create_tsdb_with_data().await;

    let mut results = tsdb
        .find_label_values("env", None, 3600, 7200)
        .await
        .unwrap();
    results.sort();

    assert_eq!(results, vec!["prod", "staging"]);
}

#[tokio::test]
async fn find_label_values_should_filter_by_matcher() {
    let tsdb = create_tsdb_with_data().await;

    let results = tsdb
        .find_label_values("env", Some(&[r#"http_requests{env="prod"}"#]), 3600, 7200)
        .await
        .unwrap();

    assert_eq!(results, vec!["prod"]);
}

#[tokio::test]
async fn find_label_values_should_return_empty_for_no_data() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);

    let results = tsdb.find_label_values("env", None, 0, 100).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn find_metadata_should_return_all() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);

    let mut series = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
    series.description = Some("CPU usage".to_string());
    series.unit = Some("percent".to_string());
    tsdb.ingest_samples(vec![series], None).await.unwrap();

    assert!(
        tsdb.find_metadata(None).await.unwrap().is_empty(),
        "Applied-only metadata is not catalog-visible"
    );
    tsdb.flush().await.unwrap();
    let results = tsdb.find_metadata(None).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].metric_name, "cpu");
    assert_eq!(results[0].metric_type, Some(MetricType::Gauge));
    assert_eq!(results[0].description, Some("CPU usage".to_string()));
    assert_eq!(results[0].unit, Some("percent".to_string()));
}

// -----------------------------------------------------------------------
// Offset / @ modifier tests (bucket preloading)
// -----------------------------------------------------------------------

#[tokio::test]
async fn eval_query_with_offset_should_load_correct_bucket() {
    // Data at 4000s (bucket hour-60: 3600–7199s).
    // Query at 7600s with offset 1h → effective time = 4000s.
    let tsdb = create_tsdb_with_data().await;
    let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(7600);
    let opts = QueryOptions::default();

    let result = tsdb
        .eval_query("http_requests offset 1h", Some(query_time), &opts)
        .await
        .unwrap();
    let samples = result.into_matrix();
    assert_eq!(samples.len(), 2, "offset 1h should find data at 4000s");
}

#[tokio::test]
async fn eval_query_range_with_offset_crossing_bucket() {
    // Data at 4000s. Range [7600,7660] with offset 1h → effective [4000,4060].
    let tsdb = create_tsdb_with_data().await;
    let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(7600);
    let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(7660);
    let step = std::time::Duration::from_secs(60);
    let opts = QueryOptions::default();

    let results = tsdb
        .eval_query_range("http_requests offset 1h", start..=end, step, &opts)
        .await
        .unwrap();
    assert!(!results.is_empty(), "offset range query should find data");
    for rs in &results {
        assert!(!rs.samples.is_empty());
    }
}

#[tokio::test]
async fn eval_query_with_offset_before_epoch_should_not_error() {
    let storage = Arc::new(in_memory_storage().await);
    // Query at 100s with offset 1h → effective time = -3500s (before epoch).
    let tsdb = Tsdb::new(storage);
    let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
    let opts = QueryOptions::default();

    let result = tsdb
        .eval_query("up offset 1h", Some(query_time), &opts)
        .await
        .unwrap();
    let samples = result.into_matrix();
    assert!(
        samples.is_empty(),
        "before-epoch offset should return empty"
    );
}

#[tokio::test]
async fn eval_query_range_with_at_before_epoch_should_not_error() {
    let storage = Arc::new(in_memory_storage().await);
    // `@ 0 offset 1h` pins evaluation to t=0, then offset pushes to -3600.
    let tsdb = Tsdb::new(storage);
    let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1000);
    let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(2000);
    let step = std::time::Duration::from_secs(60);
    let opts = QueryOptions::default();

    let results = tsdb
        .eval_query_range("up @ 0 offset 1h", start..=end, step, &opts)
        .await
        .unwrap();
    assert!(
        results.is_empty() || results.iter().all(|rs| rs.samples.is_empty()),
        "@ 0 offset 1h should return empty matrix"
    );
}

#[tokio::test]
async fn eval_query_range_with_at_end_should_load_correct_bucket() {
    // Data at 4000s. Range [4000,8000]. `@ end()` pins to 8000s.
    // With default 5m lookback, sample at 4000s is within range from 8000.
    // Actually, @ end() pins evaluation to t=8000 for each step, but
    // lookback only covers 5min=300s. Data at 4000s is 4000s before 8000s,
    // so it won't be found by lookback. Let's use a range where @ end()
    // helps: range [3900,4100], data at 4000s, `@ end()` pins to 4100s,
    // lookback 5min covers it.
    let tsdb = create_tsdb_with_data().await;
    let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(3900);
    let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);
    let step = std::time::Duration::from_secs(60);
    let opts = QueryOptions::default();

    let results = tsdb
        .eval_query_range("http_requests @ end()", start..=end, step, &opts)
        .await
        .unwrap();
    assert!(!results.is_empty(), "@ end() should find data");
    // All steps should see the same sample (pinned to end)
    for rs in &results {
        assert!(!rs.samples.is_empty());
    }
}

// -----------------------------------------------------------------------
// Multi-bucket dedup for labels and label_values
// -----------------------------------------------------------------------

#[tokio::test]
async fn find_labels_should_dedup_across_buckets() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);

    // Same series in two different buckets
    let series1 = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
    let series2 = create_sample("cpu", vec![("host", "a")], 7_500_000, 2.0);
    tsdb.ingest_samples(vec![series1, series2], None)
        .await
        .unwrap();
    tsdb.flush().await.unwrap();

    let results = tsdb.find_labels(None, 0, i64::MAX).await.unwrap();

    // Label names should not be duplicated
    let mut sorted = results.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        results.len(),
        sorted.len(),
        "label names should be deduplicated across buckets"
    );
    assert!(results.contains(&"__name__".to_string()));
    assert!(results.contains(&"host".to_string()));
}

#[tokio::test]
async fn find_label_values_should_dedup_across_buckets() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);

    // Same series in two different buckets
    let series1 = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
    let series2 = create_sample("cpu", vec![("host", "a")], 7_500_000, 2.0);
    tsdb.ingest_samples(vec![series1, series2], None)
        .await
        .unwrap();
    tsdb.flush().await.unwrap();

    let results = tsdb
        .find_label_values("host", None, 0, i64::MAX)
        .await
        .unwrap();

    assert_eq!(
        results,
        vec!["a"],
        "label values should be deduplicated across buckets"
    );
}

#[tokio::test]
async fn find_metadata_should_filter_by_metric() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);

    tsdb.ingest_samples(
        vec![
            create_sample("cpu", vec![], 4_000_000, 1.0),
            create_sample("mem", vec![], 4_000_000, 2.0),
        ],
        None,
    )
    .await
    .unwrap();

    assert!(
        tsdb.find_metadata(Some("cpu")).await.unwrap().is_empty(),
        "Applied-only metadata is not catalog-visible"
    );
    tsdb.flush().await.unwrap();
    let results = tsdb.find_metadata(Some("cpu")).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].metric_name, "cpu");

    let results = tsdb.find_metadata(Some("nonexistent")).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn eval_query_range_rejects_zero_step() {
    let storage = Arc::new(in_memory_storage().await);
    let tsdb = Tsdb::new(storage);

    tsdb.ingest_samples(vec![create_sample("cpu", vec![], 1_000_000, 1.0)], None)
        .await
        .unwrap();
    tsdb.flush().await.unwrap();

    let start = UNIX_EPOCH + Duration::from_secs(1000);
    let end = UNIX_EPOCH + Duration::from_secs(1060);
    let opts = QueryOptions::default();

    let result = tsdb
        .eval_query_range("cpu", start..=end, Duration::ZERO, &opts)
        .await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), QueryError::InvalidQuery(_)));
}

/// End-to-end test: Writer → BufferConsumer → Tsdb → query.
///
/// Runs the real consumer poll loop (metadata validation, ack/flush) and
/// shuts it down gracefully via [`ConsumerHandle`].
#[cfg(all(feature = "otel", any()))]
#[tokio::test]
async fn should_ingest_via_consumer_and_query_back() {
    use bytes::Bytes;
    use opentelemetry_proto::tonic::{
        collector::metrics::v1::ExportMetricsServiceRequest,
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        metrics::v1::{Gauge, Metric, NumberDataPoint, ScopeMetrics, metric, number_data_point},
        resource::v1::Resource,
    };
    use prost::Message;

    use slatedb::object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;

    // given — shared in-memory object store for writer and consumer
    let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifest = "ingest/manifest".to_string();

    // Build an OTLP gauge: cpu_temperature{host="server1"} = 72.5 at t=3_900s
    let ts_nanos = 3_900_000_000_000u64; // 3900s in nanos
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "test".to_string(),
                    version: "1.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                metrics: vec![Metric {
                    name: "cpu_temperature".to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![KeyValue {
                                key: "host".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue(
                                        "server1".to_string(),
                                    )),
                                }),
                            }],
                            start_time_unix_nano: 0,
                            time_unix_nano: ts_nanos,
                            value: Some(number_data_point::Value::AsDouble(72.5)),
                            exemplars: vec![],
                            flags: 0,
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let proto_bytes = request.encode_to_vec();

    // Metadata: version=1, signal_type=metrics(1), encoding=otlp_proto(1), reserved=0
    let metadata = Bytes::from_static(&[1, 1, 1, 0]);

    // Produce via Producer
    let producer_config = buffer::ProducerConfig {
        object_store: common::ObjectStoreConfig::InMemory,
        data_path_prefix: "ingest".to_string(),
        manifest_path: manifest.clone(),
        flush_interval: Duration::from_millis(10),
        flush_size_bytes: 64 * 1024 * 1024,
        max_buffered_inputs: 1000,
        batch_compression: buffer::CompressionType::None,
    };
    let producer = buffer::Producer::with_object_store(
        producer_config,
        obj_store.clone(),
        Arc::new(common::clock::SystemClock),
    )
    .unwrap();
    producer
        .produce(vec![Bytes::from(proto_bytes)], metadata)
        .await
        .unwrap();
    producer.close().await.unwrap();

    // Start the real BufferConsumer against the shared object store
    let tsdb = Arc::new(Tsdb::new(Arc::new(in_memory_storage().await)));
    let converter = Arc::new(crate::otel::OtelConverter::new(
        crate::otel::OtelConfig::default(),
    ));
    let consumer_config = crate::promql::config::BufferConsumerConfig {
        object_store: common::ObjectStoreConfig::InMemory,
        manifest_path: manifest,
        poll_interval: Duration::from_millis(10),
        data_path_prefix: "ingest".to_string(),
        gc_interval: Duration::from_secs(300),
        gc_grace_period: Duration::from_secs(600),
    };
    let consumer = Arc::new(crate::server::buffer_consumer::BufferConsumer::new(
        tsdb.clone(),
        converter,
        consumer_config,
    ));
    let handle = consumer.run_with_object_store(obj_store).await.unwrap();

    // Wait for the consumer to process the batch
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Graceful shutdown — flushes pending acks
    handle.shutdown().await;

    tsdb.flush().await.unwrap();

    // when — query back
    let query_time = UNIX_EPOCH + Duration::from_secs(3900);
    let opts = QueryOptions::default();
    let result = tsdb
        .eval_query("cpu_temperature", Some(query_time), &opts)
        .await
        .unwrap();

    // then
    let samples = match result {
        QueryValue::Vector(s) => s,
        other => panic!("expected Vector, got {:?}", other),
    };
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].value, 72.5);
    assert_eq!(samples[0].labels.get("host"), Some("server1"));
}

// ── Engine wiring tests ───────────────────────────────────────────

mod wiring_tests {
    use super::*;
    use crate::testing::columnar_stress::{
        Oracle, Scenario, ScenarioConfig, assert_grouped_count_matrix, assert_grouped_count_vector,
        assert_grouped_rate_matrix, assert_grouped_sum_matrix, assert_grouped_sum_vector,
        assert_raw_selector_cardinality, build_oracle, build_scenario, concurrency_profiles,
        create_tsdb_for_scenario, query_end_time, query_start_time, query_step,
    };

    async fn create_tsdb_with_counter() -> Tsdb {
        let tsdb = Tsdb::new(Arc::new(in_memory_storage().await));
        // Counter over bucket at minute 60 (covers 3.6M-7.2M ms).
        // Samples every 10s at 4_000_000, 4_010_000, 4_020_000.
        let series = vec![
            create_sample("req_total", vec![("env", "prod")], 4_000_000, 10.0),
            create_sample("req_total", vec![("env", "prod")], 4_010_000, 20.0),
            create_sample("req_total", vec![("env", "prod")], 4_020_000, 30.0),
        ];
        tsdb.ingest_samples(series, None).await.unwrap();
        tsdb.flush().await.unwrap();
        tsdb
    }

    #[tokio::test]
    async fn should_eval_instant_query_selector_returns_one_sample_per_series() {
        // given: two series in a single bucket
        let tsdb = create_tsdb_with_data().await;
        let query_time = UNIX_EPOCH + Duration::from_secs(4100);

        // when: run the instant selector via the engine entry point
        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("http_requests", Some(query_time), &opts)
            .await
            .unwrap();

        // then: two instant samples, one per series, both at the
        // evaluation timestamp
        let mut samples = match result {
            QueryValue::Vector(s) => s,
            other => panic!("expected Vector, got {:?}", other),
        };
        samples.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));
        assert_eq!(samples.len(), 2);
        let expected_ts = 4_100_000i64;
        assert_eq!(samples[0].timestamp_ms, expected_ts);
        assert_eq!(samples[0].labels.get("env"), Some("prod"));
        assert_eq!(samples[0].value, 42.0);
        assert_eq!(samples[1].labels.get("env"), Some("staging"));
        assert_eq!(samples[1].value, 10.0);
    }

    #[tokio::test]
    async fn should_eval_range_query_produces_samples_per_step() {
        // given: a counter with 3 samples in one bucket
        let tsdb = create_tsdb_with_counter().await;
        // Query window straddling the samples — step 10s.
        let start = UNIX_EPOCH + Duration::from_secs(4_000);
        let end = UNIX_EPOCH + Duration::from_secs(4_020);
        let step = Duration::from_secs(10);

        // when
        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query_range_value("req_total", start..=end, step, &opts)
            .await
            .unwrap();

        // then: one range sample (one series) with 3 points
        let series = match result {
            QueryValue::Matrix(m) => m,
            other => panic!("expected Matrix, got {:?}", other),
        };
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].labels.get("env"), Some("prod"));
        assert_eq!(series[0].samples.len(), 3);
        assert_eq!(series[0].samples[0].1, 10.0);
        assert_eq!(series[0].samples[1].1, 20.0);
        assert_eq!(series[0].samples[2].1, 30.0);
    }

    #[tokio::test]
    async fn should_eval_rate_over_counter_matches_expected_rate() {
        // given: counter 10 → 20 → 30 over 20 seconds
        let tsdb = create_tsdb_with_counter().await;
        // Evaluate instant rate over a [1m] window ending at 4020s;
        // Prometheus extrapolates but for an evenly-sampled counter
        // the result is ≈ 1 req/s (20 total increase / 20s ≈ 1).
        let query_time = UNIX_EPOCH + Duration::from_secs(4_020);

        // when
        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("rate(req_total[1m])", Some(query_time), &opts)
            .await
            .unwrap();

        // then: one series, rate close to 1 per second (extrapolation
        // of a 2-sample-per-10s counter over a 1m window)
        let samples = match result {
            QueryValue::Vector(v) => v,
            other => panic!("expected Vector, got {:?}", other),
        };
        assert_eq!(samples.len(), 1);
        // Rate is per-second, extrapolated across the range window.
        // For this fixture Prometheus emits ~0.417 (20-count increase
        // over 60s extrapolation). Tolerate a wide plausibility band
        // rather than pinning a precise golden.
        assert!(
            samples[0].value > 0.0 && samples[0].value < 10.0,
            "rate should be plausible (>0 and <10), got {}",
            samples[0].value
        );
    }

    #[tokio::test]
    async fn should_propagate_memory_limit_at_exec_time() {
        // given: a small-but-real fixture and an aggressively tiny
        // memory cap so every operator allocation trips.
        let tsdb = create_tsdb_with_data().await;
        let at_ms = 4_100_000i64;
        let ctx = crate::promql::plan::LoweringContext::for_instant(at_ms, 300_000);
        let reader = tsdb.query_reader_for_ranges(&[(0, 10_000)]).await.unwrap();
        let source = Arc::new(crate::promql::source_adapter::QueryReaderSource::new(
            Arc::new(reader),
        ));
        // 1 byte cap — any allocation fails.
        let reservation = crate::promql::memory::MemoryReservation::new(1);
        let expr = promql_parser::parser::parse("http_requests").unwrap();
        let plan = crate::promql::plan::lower(&expr, &ctx).unwrap();

        // when: build + drive. We expect the operator to surface a
        // `MemoryLimit` error either at build time (scratch reservation)
        // or on the first `next()` poll (per-batch output reservation).
        let built =
            crate::promql::plan::build_physical_plan(plan, &source, reservation, &ctx).await;
        let err: String = match built {
            Err(e) => e.to_string(),
            Ok(mut phys) => {
                use std::future::poll_fn;
                let polled = poll_fn(|cx| phys.root.next(cx)).await;
                match polled {
                    Some(Err(e)) => e.to_string(),
                    Some(Ok(_)) => panic!("expected a memory-limit error, got a batch"),
                    None => panic!("expected an error, got end-of-stream"),
                }
            }
        };

        // then: error mentions memory limit
        assert!(
            err.to_lowercase().contains("memory"),
            "expected a memory-limit error, got: {err}"
        );
    }

    #[tokio::test]
    async fn should_return_query_error_for_unknown_function() {
        // given: a query using a function the engine does not lower
        let tsdb = create_tsdb_with_data().await;
        // when
        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("holt_winters(http_requests[5m], 0.5, 0.5)", None, &opts)
            .await;
        // then
        let err = result.unwrap_err();
        assert!(
            matches!(err, QueryError::InvalidQuery(_)),
            "expected InvalidQuery, got {:?}",
            err
        );
    }

    #[tokio::test]
    async fn should_eval_time_function_as_scalar() {
        // given
        let tsdb = create_tsdb_with_data().await;
        let query_time = UNIX_EPOCH + Duration::from_millis(1);

        // when
        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("time()", Some(query_time), &opts)
            .await
            .unwrap();

        // then
        match result {
            QueryValue::Scalar {
                timestamp_ms,
                value,
            } => {
                assert_eq!(timestamp_ms, 1);
                assert_eq!(value, 0.001);
            }
            other => panic!("expected Scalar, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn should_eval_vector_time_as_vector() {
        // given
        let tsdb = create_tsdb_with_data().await;
        let query_time = UNIX_EPOCH + Duration::from_secs(5);

        // when
        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("vector(time())", Some(query_time), &opts)
            .await
            .unwrap();

        // then
        match result {
            QueryValue::Vector(samples) => {
                assert_eq!(samples.len(), 1);
                assert!(samples[0].labels.is_empty());
                assert_eq!(samples[0].value, 5.0);
            }
            other => panic!("expected Vector, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn should_eval_calendar_function() {
        // given
        let tsdb = create_tsdb_with_data().await;
        let query_time = UNIX_EPOCH;

        // when
        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("minute(vector(1136239445))", Some(query_time), &opts)
            .await
            .unwrap();

        // then
        match result {
            QueryValue::Vector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 4.0);
            }
            other => panic!("expected Vector, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn should_eval_label_replace() {
        // given
        let tsdb = create_tsdb_with_data().await;
        let query_time = UNIX_EPOCH + Duration::from_secs(4100);

        // when
        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query(
                r#"label_replace(http_requests, "tier", "$1-app", "env", "(.*)")"#,
                Some(query_time),
                &opts,
            )
            .await
            .unwrap();

        // then
        let mut samples = match result {
            QueryValue::Vector(samples) => samples,
            other => panic!("expected Vector, got {:?}", other),
        };
        samples.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].labels.get("tier"), Some("prod-app"));
        assert_eq!(samples[1].labels.get("tier"), Some("staging-app"));
    }

    #[tokio::test]
    async fn should_eval_label_join() {
        // given
        let tsdb = create_tsdb_with_data().await;
        let query_time = UNIX_EPOCH + Duration::from_secs(4100);

        // when
        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query(
                r#"label_join(http_requests, "joined", "/", "__name__", "env")"#,
                Some(query_time),
                &opts,
            )
            .await
            .unwrap();

        // then
        let mut samples = match result {
            QueryValue::Vector(samples) => samples,
            other => panic!("expected Vector, got {:?}", other),
        };
        samples.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].labels.get("joined"), Some("http_requests/prod"));
        assert_eq!(
            samples[1].labels.get("joined"),
            Some("http_requests/staging")
        );
    }

    #[tokio::test]
    async fn should_eval_sum_by_label() {
        // given: two series under the same metric name, grouped by env
        let tsdb = create_tsdb_with_data().await;
        let query_time = UNIX_EPOCH + Duration::from_secs(4100);

        // when
        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("sum by (env) (http_requests)", Some(query_time), &opts)
            .await
            .unwrap();

        // then: two groups, values match the input series
        let mut samples = match result {
            QueryValue::Vector(v) => v,
            other => panic!("expected Vector, got {:?}", other),
        };
        samples.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].labels.get("env"), Some("prod"));
        assert_eq!(samples[0].value, 42.0);
        assert_eq!(samples[1].labels.get("env"), Some("staging"));
        assert_eq!(samples[1].value, 10.0);
    }

    // ── RFC 0007 §4 unit 6.3.9 — tile-boundary stress end-to-end ──

    /// Ingest a large-roster metric: `n_series` series under a single
    /// metric name `metric`, one sample per series at `(base_ts,
    /// base_ts + step)` — two samples per series so `sum_over_time`
    /// has real values to aggregate inside the 10s sub-window.
    async fn create_tsdb_with_large_roster(metric: &str, n_series: usize) -> Tsdb {
        let tsdb = Tsdb::new(Arc::new(in_memory_storage().await));
        let mut samples = Vec::with_capacity(n_series * 2);
        for i in 0..n_series {
            samples.push(create_sample(
                metric,
                vec![("i", &i.to_string())],
                4_000_000,
                1.0,
            ));
            samples.push(create_sample(
                metric,
                vec![("i", &i.to_string())],
                4_005_000,
                1.0,
            ));
        }
        tsdb.ingest_samples(samples, None).await.unwrap();
        tsdb.flush().await.unwrap();
        tsdb
    }

    /// Subquery end-to-end at >512 series (RFC 0007 6.3.9 audit item
    /// 10): each outer step's child must resume across storage waits
    /// and return its reservation when dropped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_eval_sum_over_time_subquery_with_over_512_series() {
        const N_SERIES: usize = 600;
        let tsdb = create_tsdb_with_large_roster("m", N_SERIES).await;
        let query_time = UNIX_EPOCH + Duration::from_secs(4_020);

        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("sum_over_time(m[10s:2s])", Some(query_time), &opts)
            .await
            .unwrap();

        let samples = match result {
            QueryValue::Vector(v) => v,
            other => panic!("expected Vector, got {:?}", other),
        };
        assert_eq!(
            samples.len(),
            N_SERIES,
            "expected one output series per input",
        );
        for s in &samples {
            assert!(
                s.value >= 2.0,
                "series {:?} sum_over_time = {}, expected >= 2.0",
                s.labels.get("i"),
                s.value,
            );
        }
    }

    #[tokio::test]
    async fn should_eval_sum_by_label_over_large_roster_end_to_end() {
        // given: 1500 series under `m`, bucketed into 3 groups
        // (round-robin by `i % 3`). Each series has value 1.0 at the
        // query time, so sum by (g) (m) should emit 3 groups with
        // counts 500 each.
        const N_SERIES: usize = 1500;
        const GROUPS: usize = 3;
        let tsdb = {
            let tsdb = Tsdb::new(Arc::new(in_memory_storage().await));
            let mut samples = Vec::with_capacity(N_SERIES);
            for i in 0..N_SERIES {
                samples.push(create_sample(
                    "m",
                    vec![("i", &i.to_string()), ("g", &((i % GROUPS).to_string()))],
                    4_000_000,
                    1.0,
                ));
            }
            tsdb.ingest_samples(samples, None).await.unwrap();
            tsdb.flush().await.unwrap();
            tsdb
        };
        let query_time = UNIX_EPOCH + Duration::from_secs(4_010);

        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("sum by (g) (m)", Some(query_time), &opts)
            .await
            .unwrap();

        // then: three groups, each carrying N_SERIES / GROUPS = 500.
        let mut samples = match result {
            QueryValue::Vector(v) => v,
            other => panic!("expected Vector, got {:?}", other),
        };
        samples.sort_by(|a, b| a.labels.get("g").cmp(&b.labels.get("g")));
        assert_eq!(samples.len(), GROUPS);
        for s in &samples {
            assert_eq!(
                s.value,
                (N_SERIES / GROUPS) as f64,
                "group {:?} did not reach expected count",
                s.labels.get("g"),
            );
        }
    }

    async fn create_columnar_stress_fixture(config: ScenarioConfig) -> (Scenario, Oracle, Tsdb) {
        let scenario = build_scenario(config);
        let oracle = build_oracle(&scenario);
        let tsdb = create_tsdb_for_scenario(&scenario).await;
        (scenario, oracle, tsdb)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_eval_long_window_stress_regression_instant_probes() {
        // given
        let (scenario, oracle, tsdb) =
            create_columnar_stress_fixture(ScenarioConfig::regression()).await;

        for (profile_name, opts) in concurrency_profiles() {
            for &timestamp_ms in &scenario.probe_timestamps {
                let query_time = UNIX_EPOCH + Duration::from_millis(timestamp_ms as u64);

                // when
                let raw_result = tsdb
                    .eval_query("stress_gauge", Some(query_time), &opts)
                    .await
                    .unwrap();
                let count_result = tsdb
                    .eval_query(
                        "count by (applicationid) (stress_gauge)",
                        Some(query_time),
                        &opts,
                    )
                    .await
                    .unwrap();
                let sum_result = tsdb
                    .eval_query(
                        "sum by (applicationid) (stress_gauge)",
                        Some(query_time),
                        &opts,
                    )
                    .await
                    .unwrap();

                // then
                let raw_samples = match raw_result {
                    QueryValue::Vector(samples) => samples,
                    other => panic!("expected Vector for raw selector, got {:?}", other),
                };
                assert_raw_selector_cardinality(&raw_samples, &scenario, timestamp_ms);

                let count_samples = match count_result {
                    QueryValue::Vector(samples) => samples,
                    other => panic!("expected Vector for count query, got {:?}", other),
                };
                assert_grouped_count_vector(&count_samples, &scenario, &oracle, timestamp_ms);

                let sum_samples = match sum_result {
                    QueryValue::Vector(samples) => samples,
                    other => panic!("expected Vector for sum query, got {:?}", other),
                };
                assert_grouped_sum_vector(&sum_samples, &scenario, &oracle, timestamp_ms);
            }

            assert!(
                !scenario.probe_timestamps.is_empty(),
                "expected probe timestamps for profile {profile_name}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_eval_long_window_stress_regression_range_queries() {
        // given
        let (scenario, oracle, tsdb) =
            create_columnar_stress_fixture(ScenarioConfig::regression()).await;
        let start = query_start_time(&scenario);
        let end = query_end_time(&scenario);
        let step = query_step(&scenario);

        for (_profile_name, opts) in concurrency_profiles() {
            // when
            let count_result = tsdb
                .eval_query_range_value(
                    "count by (applicationid) (stress_gauge)",
                    start..=end,
                    step,
                    &opts,
                )
                .await
                .unwrap();
            let sum_result = tsdb
                .eval_query_range_value(
                    "sum by (applicationid) (stress_gauge)",
                    start..=end,
                    step,
                    &opts,
                )
                .await
                .unwrap();
            let rate_result = tsdb
                .eval_query_range_value(
                    "sum by (applicationid) (rate(stress_counter_total[15m]))",
                    start..=end,
                    step,
                    &opts,
                )
                .await
                .unwrap();

            // then
            let count_matrix = match count_result {
                QueryValue::Matrix(matrix) => matrix,
                other => panic!("expected Matrix for count range, got {:?}", other),
            };
            assert_grouped_count_matrix(&count_matrix, &scenario, &oracle);

            let sum_matrix = match sum_result {
                QueryValue::Matrix(matrix) => matrix,
                other => panic!("expected Matrix for sum range, got {:?}", other),
            };
            assert_grouped_sum_matrix(&sum_matrix, &scenario, &oracle);

            let rate_matrix = match rate_result {
                QueryValue::Matrix(matrix) => matrix,
                other => panic!("expected Matrix for rate range, got {:?}", other),
            };
            assert_grouped_rate_matrix(&rate_matrix, &scenario, &oracle);
        }
    }

    /// Larger ignored soak for RFC 0008. Invoke with:
    /// `cargo test -p opendata-timeseries should_eval_long_window_stress_soak -- --ignored --nocapture`
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn should_eval_long_window_stress_soak() {
        // given
        let (scenario, oracle, tsdb) = create_columnar_stress_fixture(ScenarioConfig::soak()).await;
        let start = query_start_time(&scenario);
        let end = query_end_time(&scenario);
        let step = query_step(&scenario);
        let opts = QueryOptions::default();

        // when
        let count_result = tsdb
            .eval_query_range_value(
                "count by (applicationid) (stress_gauge)",
                start..=end,
                step,
                &opts,
            )
            .await
            .unwrap();
        let sum_result = tsdb
            .eval_query_range_value(
                "sum by (applicationid) (stress_gauge)",
                start..=end,
                step,
                &opts,
            )
            .await
            .unwrap();
        let rate_result = tsdb
            .eval_query_range_value(
                "sum by (applicationid) (rate(stress_counter_total[15m]))",
                start..=end,
                step,
                &opts,
            )
            .await
            .unwrap();

        // then
        let count_matrix = match count_result {
            QueryValue::Matrix(matrix) => matrix,
            other => panic!("expected Matrix for count soak, got {:?}", other),
        };
        assert_grouped_count_matrix(&count_matrix, &scenario, &oracle);

        let sum_matrix = match sum_result {
            QueryValue::Matrix(matrix) => matrix,
            other => panic!("expected Matrix for sum soak, got {:?}", other),
        };
        assert_grouped_sum_matrix(&sum_matrix, &scenario, &oracle);

        let rate_matrix = match rate_result {
            QueryValue::Matrix(matrix) => matrix,
            other => panic!("expected Matrix for rate soak, got {:?}", other),
        };
        assert_grouped_rate_matrix(&rate_matrix, &scenario, &oracle);
    }

    // ── Read-only reader tests (RFC 0007 unit 7.0) ───────────────
    //
    // These verify that the engine runs through the
    // `TsdbReadEngine` trait defaults on both the writer (`Tsdb`)
    // and the read-only `TimeSeriesDbReader`, so reader-only prod
    // binaries exercise the columnar engine.

    /// Build a writer + reader pair on shared storage, ingest a
    /// counter spanning 3 samples, flush, and return both handles.
    async fn create_writer_and_reader_with_counter() -> (Tsdb, crate::reader::TimeSeriesDbReader) {
        let shared = crate::storage::in_memory_shared_storage().await;
        let tsdb = Tsdb::new(shared.storage.clone());
        let series = vec![
            create_sample("req_total", vec![("env", "prod")], 4_000_000, 10.0),
            create_sample("req_total", vec![("env", "prod")], 4_010_000, 20.0),
            create_sample("req_total", vec![("env", "prod")], 4_020_000, 30.0),
        ];
        tsdb.ingest_samples(series, None).await.unwrap();
        tsdb.flush().await.unwrap();
        // Open the reader only after the flush so its view includes the data.
        let reader = crate::reader::TimeSeriesDbReader::from_storage(shared.reader().await);
        (tsdb, reader)
    }

    #[tokio::test]
    async fn should_eval_instant_query_on_read_only_reader_returns_one_sample_per_series() {
        // given: counter ingested via writer, read through a reader
        let (_tsdb, reader) = create_writer_and_reader_with_counter().await;
        let query_time = UNIX_EPOCH + Duration::from_secs(4_020);

        // when: the reader's entry point evaluates the selector
        let opts = QueryOptions::default();
        let result = reader
            .eval_query("req_total", Some(query_time), &opts)
            .await
            .unwrap();

        // then: one instant sample at the evaluation timestamp
        let samples = match result {
            QueryValue::Vector(s) => s,
            other => panic!("expected Vector, got {:?}", other),
        };
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].timestamp_ms, 4_020_000);
        assert_eq!(samples[0].labels.get("env"), Some("prod"));
        assert_eq!(samples[0].value, 30.0);
    }

    #[tokio::test]
    async fn should_eval_range_query_on_read_only_reader_produces_samples_per_step() {
        // given: counter ingested via writer, read through a reader
        let (_tsdb, reader) = create_writer_and_reader_with_counter().await;
        let start = UNIX_EPOCH + Duration::from_secs(4_000);
        let end = UNIX_EPOCH + Duration::from_secs(4_020);
        let step = Duration::from_secs(10);

        // when: the reader's range entry point evaluates the query
        let opts = QueryOptions::default();
        let result = reader
            .eval_query_range_value("req_total", start..=end, step, &opts)
            .await
            .unwrap();

        // then: one range sample (one series) with 3 points
        let series = match result {
            QueryValue::Matrix(m) => m,
            other => panic!("expected Matrix, got {:?}", other),
        };
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].labels.get("env"), Some("prod"));
        assert_eq!(series[0].samples.len(), 3);
        assert_eq!(series[0].samples[0].1, 10.0);
        assert_eq!(series[0].samples[1].1, 20.0);
        assert_eq!(series[0].samples[2].1, 30.0);
    }

    #[tokio::test]
    async fn should_return_identical_results_for_read_write_and_read_only_paths_on_same_storage() {
        // given: writer and reader on the same in-memory storage
        let (tsdb, reader) = create_writer_and_reader_with_counter().await;
        let start = UNIX_EPOCH + Duration::from_secs(4_000);
        let end = UNIX_EPOCH + Duration::from_secs(4_020);
        let step = Duration::from_secs(10);

        // when: both engines run the identical range query
        let opts = QueryOptions::default();
        let query = "rate(req_total[1m])";
        let writer_result = tsdb
            .eval_query_range_value(query, start..=end, step, &opts)
            .await
            .unwrap();
        let reader_result = reader
            .eval_query_range_value(query, start..=end, step, &opts)
            .await
            .unwrap();

        // then: both produce matrices that agree on labels and samples
        let writer_matrix = match writer_result {
            QueryValue::Matrix(m) => m,
            other => panic!("expected Matrix from writer, got {:?}", other),
        };
        let reader_matrix = match reader_result {
            QueryValue::Matrix(m) => m,
            other => panic!("expected Matrix from reader, got {:?}", other),
        };
        assert_eq!(writer_matrix.len(), reader_matrix.len());
        let mut w_sorted = writer_matrix.clone();
        let mut r_sorted = reader_matrix.clone();
        w_sorted.sort_by(|a, b| a.labels.cmp(&b.labels));
        r_sorted.sort_by(|a, b| a.labels.cmp(&b.labels));
        for (w, r) in w_sorted.iter().zip(r_sorted.iter()) {
            assert_eq!(w.labels, r.labels);
            assert_eq!(w.samples, r.samples);
        }
    }
}
