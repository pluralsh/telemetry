// ---------------------------------------------------------------------------
// Integration tests
//
// These exercise the [`SeriesSource`] contract end-to-end against the
// existing in-memory [`MockQueryReader`] fixture (the same one that backs
// `promql/selector.rs` and `promql/pipeline.rs` tests). They verify:
//
//   - RFC §"Storage Contract" caller → source and source → caller
//     guarantees (series set, time window, ordering, stale markers).
//   - The inclusive-start / exclusive-end translation over `QueryReader`'s
//     `(start, end]` contract.
//   - Cross-bucket stitching (per-bucket batches in chronological order).
//   - `selector_util` matcher parity with the shapes covered in
//     `promql/selector.rs`: metric-only, equality, negation, regex OR,
//     empty-string, AND-combination.
//
// ---------------------------------------------------------------------------

use super::*;
use crate::model::{Label, MetricType, STALE_NAN, Sample, TimeBucket};
use crate::query::test_utils::{MockMultiBucketQueryReaderBuilder, MockQueryReader};
use futures::StreamExt;
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher, Matchers};
use promql_parser::parser::VectorSelector;
use regex::Regex;

// ---------- Fixture helpers -----------------------------------------

/// Build an empty `Matchers`. Mirrors `empty_matchers()` in
/// `promql/selector.rs` tests.
fn empty_matchers() -> Matchers {
    Matchers {
        matchers: vec![],
        or_matchers: vec![],
    }
}

/// Build a bare metric-name-only selector.
fn sel_metric(name: &str) -> VectorSelector {
    VectorSelector {
        name: Some(name.to_string()),
        matchers: empty_matchers(),
        offset: None,
        at: None,
    }
}

/// Build a selector with a metric name plus one or more matchers.
fn sel_with(name: &str, matchers: Vec<Matcher>) -> VectorSelector {
    VectorSelector {
        name: Some(name.to_string()),
        matchers: Matchers {
            matchers,
            or_matchers: vec![],
        },
        offset: None,
        at: None,
    }
}

/// Build a `Matcher::Re(..)` pair for a `label=~pattern` matcher.
fn re_matcher(name: &str, pattern: &str) -> Matcher {
    let regex = Regex::new(pattern).expect("valid regex");
    Matcher {
        op: MatchOp::Re(regex),
        name: name.to_string(),
        value: pattern.to_string(),
    }
}

/// Labels that identify one test series: `{__name__, ...extra}`.
fn labels(name: &str, extra: &[(&str, &str)]) -> Vec<Label> {
    let mut out = vec![Label {
        name: METRIC_NAME.to_string(),
        value: name.to_string(),
    }];
    for (k, v) in extra {
        out.push(Label {
            name: (*k).to_string(),
            value: (*v).to_string(),
        });
    }
    out
}

/// One-bucket, single-series builder. Convenience for simple boundary
/// / staleness tests.
fn one_series_reader(bucket: TimeBucket, name: &str, samples: &[(i64, f64)]) -> MockQueryReader {
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    for &(ts, v) in samples {
        b.add_sample(
            bucket,
            labels(name, &[]),
            MetricType::Gauge,
            Sample::new(ts, v),
        );
    }
    b.build()
}

/// Drain a `Stream<Item=Result<T, QueryError>>` into `Vec<T>`,
/// unwrapping errors inline (tests panic with the adapter's message).
/// The stream is pinned on the heap inside so callers don't have to
/// care about `Unpin` — the adapter's RPITIT-returned streams aren't.
async fn collect_ok<S, T>(stream: S) -> Vec<T>
where
    S: futures::Stream<Item = Result<T, QueryError>>,
{
    let mut stream = Box::pin(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item.expect("source adapter returned error"));
    }
    out
}

/// Build a `QueryReaderSource` from a concrete `MockQueryReader`.
fn source(reader: MockQueryReader) -> QueryReaderSource<MockQueryReader> {
    QueryReaderSource::new(Arc::new(reader))
}

// Bucket `h0` covers [0, 3_600_000) ms. `h60` covers [3_600_000,
// 7_200_000). `h120` covers [7_200_000, 10_800_000).
fn h0() -> TimeBucket {
    TimeBucket::hour(0)
}
fn h60() -> TimeBucket {
    TimeBucket::hour(60)
}
fn h120() -> TimeBucket {
    TimeBucket::hour(120)
}

// ---------- RFC source → caller contract ----------------------------

#[tokio::test]
async fn should_return_series_range_covering_contiguous_slice_of_input_series() {
    // given: a single-bucket reader with three series under one metric name
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    for (env, ts, v) in [("a", 1_000i64, 1.0), ("b", 1_000, 2.0), ("c", 1_000, 3.0)] {
        b.add_sample(
            h0(),
            labels("m", &[("env", env)]),
            MetricType::Gauge,
            Sample::new(ts, v),
        );
    }
    let src = source(b.build());

    // when: resolve then ask for all three series' samples
    let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
    assert_eq!(chunks.len(), 1);
    let series: Arc<[ResolvedSeriesRef]> = chunks[0].series.clone();
    let batches = collect_ok(src.samples(SamplesRequest::new(
        series.clone(),
        TimeRange::new(0, 3_600_000),
    )))
    .await;

    // then: each batch's series_range is a contiguous slice of the request
    assert!(!batches.is_empty());
    let total: usize = batches.iter().map(|b| b.series_range.len()).sum();
    assert_eq!(total, series.len());
    for batch in &batches {
        assert!(batch.series_range.end <= series.len());
        assert!(batch.series_range.start < batch.series_range.end);
        assert_eq!(batch.samples.series_count(), batch.series_range.len());
    }
}

#[tokio::test]
async fn should_return_samples_in_timestamp_order_per_series() {
    // given: one series ingested in timestamp order (production
    // `QueryReader::samples` always returns sorted; the adapter's
    // job is to forward that without reordering, which is what this
    // test verifies — ingest order == expected output order).
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    let ingested: Vec<(i64, f64)> = vec![(500, 1.0), (1_000, 2.0), (1_500, 3.0), (2_000, 4.0)];
    for (ts, v) in &ingested {
        b.add_sample(
            h0(),
            labels("m", &[]),
            MetricType::Gauge,
            Sample::new(*ts, *v),
        );
    }
    let src = source(b.build());

    // when: resolve and fetch samples
    let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
    let request = SamplesRequest::new(chunks[0].series.clone(), TimeRange::new(0, 3_600_000));
    let batches = collect_ok(src.samples(request)).await;

    // then: per-series timestamps are monotone non-decreasing, in
    // the order the backing store produced them — the adapter did
    // not reorder or de-duplicate.
    assert_eq!(batches.len(), 1);
    let ts_col = &batches[0].samples.timestamps[0];
    let val_col = &batches[0].samples.values[0];
    for w in ts_col.windows(2) {
        assert!(w[0] <= w[1], "timestamps not monotone: {:?}", ts_col);
    }
    // Ordering is preserved end-to-end: the adapter's output column
    // matches the (already-sorted) ingest sequence.
    let expected_ts: Vec<i64> = ingested.iter().map(|(t, _)| *t).collect();
    let expected_val: Vec<f64> = ingested.iter().map(|(_, v)| *v).collect();
    assert_eq!(ts_col, &expected_ts);
    assert_eq!(val_col, &expected_val);
}

#[tokio::test]
async fn should_preserve_stale_nan_markers() {
    // given: a series with a real value then a stale marker
    let stale = f64::from_bits(STALE_NAN);
    let reader = one_series_reader(h0(), "m", &[(1_000, 42.0), (2_000, stale)]);
    let src = source(reader);

    // when: resolve + fetch the full bucket window
    let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
    let request = SamplesRequest::new(chunks[0].series.clone(), TimeRange::new(0, 3_600_000));
    let batches = collect_ok(src.samples(request)).await;

    // then: the stale bit pattern round-trips verbatim, not normalised
    // to NaN or dropped
    let values = &batches[0].samples.values[0];
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], 42.0);
    assert_eq!(
        values[1].to_bits(),
        STALE_NAN,
        "stale marker must round-trip bit-exact"
    );
    assert!(crate::model::is_stale_nan(values[1]));
}

// ---------- RFC caller → source contract ----------------------------

#[tokio::test]
async fn should_not_widen_series_set_beyond_selector_match() {
    // given: a reader with two metrics; only one matches the selector
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    b.add_sample(
        h0(),
        labels("m1", &[]),
        MetricType::Gauge,
        Sample::new(1_000, 1.0),
    );
    b.add_sample(
        h0(),
        labels("m2", &[]),
        MetricType::Gauge,
        Sample::new(1_000, 2.0),
    );
    let src = source(b.build());

    // when: resolve `m1`
    let chunks = collect_ok(src.resolve(&sel_metric("m1"), TimeRange::new(0, 3_600_000))).await;

    // then: exactly one series returned; `m2` must not leak in
    let total: usize = chunks.iter().map(|c| c.series.len()).sum();
    assert_eq!(total, 1);
    for chunk in &chunks {
        for lab in chunk.labels.iter() {
            assert_eq!(lab.metric_name(), "m1");
        }
    }
}

#[tokio::test]
async fn should_respect_caller_time_range() {
    // given: samples at 500, 1_500, 2_500
    let reader = one_series_reader(h0(), "m", &[(500, 1.0), (1_500, 2.0), (2_500, 3.0)]);
    let src = source(reader);

    // when: query window [1_000, 2_000)
    let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
    let request = SamplesRequest::new(chunks[0].series.clone(), TimeRange::new(1_000, 2_000));
    let batches = collect_ok(src.samples(request)).await;

    // then: only the sample at 1_500 survives — 500 is before the
    // window and 2_500 is after
    let ts = &batches[0].samples.timestamps[0];
    assert_eq!(ts, &vec![1_500]);
}

// ---------- Boundary / edge cases ---------------

#[tokio::test]
async fn should_include_sample_at_start_ms_boundary() {
    // given: a single sample at exactly time_range.start_ms
    let reader = one_series_reader(h0(), "m", &[(1_000, 7.0)]);
    let src = source(reader);

    // when: request [1_000, 2_000)
    let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
    let request = SamplesRequest::new(chunks[0].series.clone(), TimeRange::new(1_000, 2_000));
    let batches = collect_ok(src.samples(request)).await;

    // then: sample at 1_000 is included (inclusive start)
    let ts = &batches[0].samples.timestamps[0];
    assert_eq!(ts, &vec![1_000]);
}

#[tokio::test]
async fn should_exclude_sample_at_end_ms_exclusive_boundary() {
    // given: a single sample at exactly time_range.end_ms_exclusive
    let reader = one_series_reader(h0(), "m", &[(2_000, 7.0)]);
    let src = source(reader);

    // when: request [1_000, 2_000) — note exclusive end
    let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
    let request = SamplesRequest::new(chunks[0].series.clone(), TimeRange::new(1_000, 2_000));
    let batches = collect_ok(src.samples(request)).await;

    // then: the sample is NOT included
    let ts = &batches[0].samples.timestamps[0];
    assert!(ts.is_empty(), "sample at end_ms_exclusive leaked: {:?}", ts);
}

#[tokio::test]
async fn should_include_sample_just_before_end_ms_exclusive() {
    // given: a single sample at end_ms_exclusive - 1 (the last
    // included timestamp)
    let reader = one_series_reader(h0(), "m", &[(1_999, 7.0)]);
    let src = source(reader);

    // when: request [1_000, 2_000)
    let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
    let request = SamplesRequest::new(chunks[0].series.clone(), TimeRange::new(1_000, 2_000));
    let batches = collect_ok(src.samples(request)).await;

    // then: sanity pair with the excluded-end test above — 1_999 IS
    // included
    let ts = &batches[0].samples.timestamps[0];
    assert_eq!(ts, &vec![1_999]);
}

#[tokio::test]
async fn should_emit_empty_stream_for_empty_time_range() {
    // given: a reader with real samples
    let reader = one_series_reader(h0(), "m", &[(1_000, 1.0), (2_000, 2.0)]);
    let src = source(reader);

    // resolve the series first (over a non-empty window)
    let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;
    let series = chunks[0].series.clone();

    // when: ask for samples over an empty window [1_500, 1_500)
    let request = SamplesRequest::new(series, TimeRange::new(1_500, 1_500));
    let batches = collect_ok(src.samples(request)).await;

    // then: the stream terminates immediately with no batches
    assert!(
        batches.is_empty(),
        "expected empty stream, got {} batch(es)",
        batches.len()
    );
}

// ---------- Cross-bucket stitching ---------------------------------

#[tokio::test]
async fn should_emit_one_batch_per_bucket_for_multi_bucket_series() {
    // given: the *same* label set ingested into two buckets
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    b.add_sample(
        h0(),
        labels("m", &[]),
        MetricType::Gauge,
        Sample::new(1_000, 1.0),
    );
    b.add_sample(
        h60(),
        labels("m", &[]),
        MetricType::Gauge,
        Sample::new(3_700_000, 2.0),
    );
    let src = source(b.build());

    // when: resolve (two chunks — one per bucket, chronological) and
    // concatenate their handles before requesting samples
    let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 7_200_000))).await;
    assert_eq!(chunks.len(), 2);
    // chronological: h0 (start=0) before h60 (start=60)
    let h0_id = encode_bucket(h0());
    let h60_id = encode_bucket(h60());
    assert_eq!(chunks[0].bucket_id, h0_id);
    assert_eq!(chunks[1].bucket_id, h60_id);

    let mut merged: Vec<ResolvedSeriesRef> = Vec::new();
    for c in &chunks {
        merged.extend(c.series.iter().cloned());
    }
    let request = SamplesRequest::new(Arc::from(merged), TimeRange::new(0, 7_200_000));
    let batches = collect_ok(src.samples(request)).await;

    // then: one batch per bucket in the order the caller supplied the
    // runs (which was chronological after our merge)
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].samples.timestamps[0], vec![1_000]);
    assert_eq!(batches[1].samples.timestamps[0], vec![3_700_000]);
}

#[tokio::test]
async fn should_skip_buckets_that_do_not_overlap_time_range() {
    // given: series in h0, h60, h120. Query window covers only h0+h60.
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    for (bucket, ts) in [(h0(), 1_000i64), (h60(), 3_700_000), (h120(), 7_300_000)] {
        b.add_sample(
            bucket,
            labels("m", &[]),
            MetricType::Gauge,
            Sample::new(ts, 1.0),
        );
    }
    let src = source(b.build());

    // when: resolve over [0, 7_200_000) — h120 is entirely outside
    let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 7_200_000))).await;

    // then: exactly the two overlapping buckets emit chunks; h120 is
    // not represented
    let ids: Vec<u64> = chunks.iter().map(|c| c.bucket_id).collect();
    assert!(ids.contains(&encode_bucket(h0())));
    assert!(ids.contains(&encode_bucket(h60())));
    assert!(
        !ids.contains(&encode_bucket(h120())),
        "h120 bucket leaked into resolve: {:?}",
        ids
    );
    assert_eq!(chunks.len(), 2);
}

// ---------- Selector-matcher parity --------------------------------
//
// `selector_util` is validated behaviourally against hand-built
// fixtures rather than by calling into `promql::selector`,
// whose `CachedQueryReader`-coupled API is private to the
// evaluator). The existing selector.rs tests already cover
// `evaluate_selector_with_reader` over the same matcher shapes, so a
// failure here alongside a green `promql::selector` test strongly
// implies a `selector_util` drift.

#[tokio::test]
async fn should_match_metric_name_only() {
    // given: two series under the same metric name plus one under a
    // different name
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    b.add_sample(
        h0(),
        labels("m", &[("env", "a")]),
        MetricType::Gauge,
        Sample::new(1_000, 1.0),
    );
    b.add_sample(
        h0(),
        labels("m", &[("env", "b")]),
        MetricType::Gauge,
        Sample::new(1_000, 1.0),
    );
    b.add_sample(
        h0(),
        labels("other", &[]),
        MetricType::Gauge,
        Sample::new(1_000, 1.0),
    );
    let src = source(b.build());

    // when: resolve `m`
    let chunks = collect_ok(src.resolve(&sel_metric("m"), TimeRange::new(0, 3_600_000))).await;

    // then: two series returned, both `m`
    let total: usize = chunks.iter().map(|c| c.series.len()).sum();
    assert_eq!(total, 2);
}

#[tokio::test]
async fn should_match_label_equality() {
    // given: three series, one matches method=GET
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    b.add_sample(
        h0(),
        labels("m", &[("method", "GET")]),
        MetricType::Gauge,
        Sample::new(1_000, 1.0),
    );
    b.add_sample(
        h0(),
        labels("m", &[("method", "POST")]),
        MetricType::Gauge,
        Sample::new(1_000, 1.0),
    );
    b.add_sample(
        h0(),
        labels("m", &[("method", "DELETE")]),
        MetricType::Gauge,
        Sample::new(1_000, 1.0),
    );
    let src = source(b.build());

    // when: resolve m{method="GET"}
    let sel = sel_with("m", vec![Matcher::new(MatchOp::Equal, "method", "GET")]);
    let chunks = collect_ok(src.resolve(&sel, TimeRange::new(0, 3_600_000))).await;

    // then: exactly one match
    let mut got_methods: Vec<String> = Vec::new();
    for c in &chunks {
        for lab in c.labels.iter() {
            if let Some(v) = lab.get("method") {
                got_methods.push(v.to_string());
            }
        }
    }
    assert_eq!(got_methods, vec!["GET".to_string()]);
}

#[tokio::test]
async fn should_match_label_negation() {
    // given: three series with method labels
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    for method in ["GET", "POST", "DELETE"] {
        b.add_sample(
            h0(),
            labels("m", &[("method", method)]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
    }
    let src = source(b.build());

    // when: resolve m{method!="GET"}
    let sel = sel_with("m", vec![Matcher::new(MatchOp::NotEqual, "method", "GET")]);
    let chunks = collect_ok(src.resolve(&sel, TimeRange::new(0, 3_600_000))).await;

    // then: POST + DELETE match, GET is excluded
    let mut methods: Vec<String> = Vec::new();
    for c in &chunks {
        for lab in c.labels.iter() {
            if let Some(v) = lab.get("method") {
                methods.push(v.to_string());
            }
        }
    }
    methods.sort();
    assert_eq!(methods, vec!["DELETE".to_string(), "POST".to_string()]);
}

#[tokio::test]
async fn should_match_label_regex() {
    // given: three series with env labels
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    for env in ["prod-a", "prod-b", "staging-a"] {
        b.add_sample(
            h0(),
            labels("m", &[("env", env)]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
    }
    let src = source(b.build());

    // when: resolve m{env=~"prod-a|prod-b"}
    let sel = sel_with("m", vec![re_matcher("env", "prod-a|prod-b")]);
    let chunks = collect_ok(src.resolve(&sel, TimeRange::new(0, 3_600_000))).await;

    // then: both prod-* series match, staging-a does not
    let mut envs: Vec<String> = Vec::new();
    for c in &chunks {
        for lab in c.labels.iter() {
            if let Some(v) = lab.get("env") {
                envs.push(v.to_string());
            }
        }
    }
    envs.sort();
    assert_eq!(envs, vec!["prod-a".to_string(), "prod-b".to_string()]);
}

#[tokio::test]
async fn should_match_empty_string_matcher() {
    // given: two series — one with a label `foo`, one without
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    b.add_sample(
        h0(),
        labels("m", &[("foo", "bar")]),
        MetricType::Gauge,
        Sample::new(1_000, 1.0),
    );
    b.add_sample(
        h0(),
        labels("m", &[]),
        MetricType::Gauge,
        Sample::new(1_000, 1.0),
    );
    let src = source(b.build());

    // when: resolve m{foo=""}
    let sel = sel_with("m", vec![Matcher::new(MatchOp::Equal, "foo", "")]);
    let chunks = collect_ok(src.resolve(&sel, TimeRange::new(0, 3_600_000))).await;

    // then: only the series without `foo` matches (`{foo=""}` matches
    // absent)
    let total: usize = chunks.iter().map(|c| c.series.len()).sum();
    assert_eq!(total, 1);
    for c in &chunks {
        for lab in c.labels.iter() {
            assert!(lab.get("foo").is_none());
        }
    }
}

#[tokio::test]
async fn should_match_combined_and_of_matchers() {
    // given: four series with (env, method) combinations
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    for (env, method) in [
        ("prod", "GET"),
        ("prod", "POST"),
        ("staging", "GET"),
        ("staging", "POST"),
    ] {
        b.add_sample(
            h0(),
            labels("m", &[("env", env), ("method", method)]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
    }
    let src = source(b.build());

    // when: resolve m{env="prod", method="GET"}
    let sel = sel_with(
        "m",
        vec![
            Matcher::new(MatchOp::Equal, "env", "prod"),
            Matcher::new(MatchOp::Equal, "method", "GET"),
        ],
    );
    let chunks = collect_ok(src.resolve(&sel, TimeRange::new(0, 3_600_000))).await;

    // then: exactly one match (intersection)
    let total: usize = chunks.iter().map(|c| c.series.len()).sum();
    assert_eq!(total, 1);
    for c in &chunks {
        for lab in c.labels.iter() {
            assert_eq!(lab.get("env"), Some("prod"));
            assert_eq!(lab.get("method"), Some("GET"));
        }
    }
}

#[tokio::test]
async fn should_match_regex_or_group() {
    // given: three instances
    let mut b = MockMultiBucketQueryReaderBuilder::new();
    for inst in ["host-38", "host-39", "host-40"] {
        b.add_sample(
            h0(),
            labels("m", &[("instance", inst)]),
            MetricType::Gauge,
            Sample::new(1_000, 1.0),
        );
    }
    b.add_sample(
        h0(),
        labels("m", &[("instance", "host-99")]),
        MetricType::Gauge,
        Sample::new(1_000, 1.0),
    );
    let src = source(b.build());

    // when: resolve m{instance=~"host-38|host-39|host-40"}
    let sel = sel_with("m", vec![re_matcher("instance", "host-38|host-39|host-40")]);
    let chunks = collect_ok(src.resolve(&sel, TimeRange::new(0, 3_600_000))).await;

    // then: three matches, host-99 excluded
    let mut insts: Vec<String> = Vec::new();
    for c in &chunks {
        for lab in c.labels.iter() {
            if let Some(v) = lab.get("instance") {
                insts.push(v.to_string());
            }
        }
    }
    insts.sort();
    assert_eq!(
        insts,
        vec![
            "host-38".to_string(),
            "host-39".to_string(),
            "host-40".to_string(),
        ]
    );
}
