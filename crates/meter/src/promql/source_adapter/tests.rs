use super::*;

#[test]
fn should_parse_single_character_regex_alternation() {
    assert_eq!(selector_util::regex_literals("a|b").unwrap(), ["a", "b"]);
}

#[test]
fn should_bound_expanded_regex_classes() {
    assert!(selector_util::regex_literals(r"[\x00-\u{10FFFF}]").is_none());
}

#[test]
fn should_leave_non_literal_regexes_to_enumeration() {
    assert!(selector_util::regex_literals("foo.*").is_none());
    assert!(selector_util::regex_literals("(?i)foo").is_none());
}

#[test]
fn should_encode_and_decode_bucket_round_trip() {
    // given: a TimeBucket with realistic start + size values
    let bucket = TimeBucket {
        start: 1_234_567u32,
        size: 3u8,
    };

    // when: round-trip through encode/decode
    let id = encode_bucket(bucket);
    let decoded = decode_bucket(id).expect("decode should succeed");

    // then: fields round-trip exactly
    assert_eq!(decoded.start, bucket.start);
    assert_eq!(decoded.size, bucket.size);
}

#[test]
fn should_reject_decoded_bucket_with_zero_size() {
    // given: a bucket id with size = 0 (invalid)
    // when: decode it
    let decoded = decode_bucket(42u64 << 8);

    // then: decoding returns None
    assert!(decoded.is_none());
}

#[test]
fn should_detect_bucket_overlap_with_time_range() {
    // given: a 1-hour bucket starting at minute 60 (→ [3_600_000, 7_200_000) ms)
    let bucket = TimeBucket {
        start: 60u32,
        size: 1u8,
    };
    let (b_start, b_end) = bucket_ms_window(bucket);
    assert_eq!(b_start, 3_600_000);
    assert_eq!(b_end, 7_200_000);

    // when / then: windows fully inside, overlapping, and disjoint
    assert!(bucket_overlaps(
        bucket,
        TimeRange::new(4_000_000, 5_000_000),
    ));
    assert!(bucket_overlaps(bucket, TimeRange::new(0, 4_000_000),));
    assert!(bucket_overlaps(
        bucket,
        TimeRange::new(6_000_000, 8_000_000),
    ));
    // touching at start: bucket_end == time.start_ms → no overlap
    assert!(!bucket_overlaps(
        bucket,
        TimeRange::new(7_200_000, 8_000_000),
    ));
    // touching at end: bucket_start == time.end_ms_exclusive → no overlap
    assert!(!bucket_overlaps(bucket, TimeRange::new(0, 3_600_000),));
    // empty range never overlaps
    assert!(!bucket_overlaps(
        bucket,
        TimeRange::new(4_000_000, 4_000_000),
    ));
}

#[test]
fn should_group_contiguous_bucket_runs_preserving_order() {
    // given: a request-series slice with three buckets in mixed order
    let name: Arc<str> = Arc::from("m");
    let series = vec![
        ResolvedSeriesRef::new(
            encode_bucket(TimeBucket { start: 0, size: 1 }),
            1,
            name.clone(),
        ),
        ResolvedSeriesRef::new(
            encode_bucket(TimeBucket { start: 0, size: 1 }),
            2,
            name.clone(),
        ),
        ResolvedSeriesRef::new(
            encode_bucket(TimeBucket { start: 60, size: 1 }),
            7,
            name.clone(),
        ),
        ResolvedSeriesRef::new(
            encode_bucket(TimeBucket { start: 60, size: 1 }),
            8,
            name.clone(),
        ),
        ResolvedSeriesRef::new(
            encode_bucket(TimeBucket {
                start: 120,
                size: 1,
            }),
            5,
            name.clone(),
        ),
        // back to the first bucket — a new run, not merged.
        ResolvedSeriesRef::new(
            encode_bucket(TimeBucket { start: 0, size: 1 }),
            9,
            name.clone(),
        ),
    ];

    // when: partition into runs
    let runs = contiguous_bucket_runs(&series);

    // then: four runs, each a contiguous sub-range
    assert_eq!(runs.len(), 4);
    assert_eq!(runs[0].range, 0..2);
    assert_eq!(runs[1].range, 2..4);
    assert_eq!(runs[2].range, 4..5);
    assert_eq!(runs[3].range, 5..6);
}

#[test]
fn should_return_empty_runs_for_empty_series_slice() {
    // given: no series
    let series: Vec<ResolvedSeriesRef> = Vec::new();

    // when: ask for runs
    let runs = contiguous_bucket_runs(&series);

    // then: no runs emitted
    assert!(runs.is_empty());
}

#[test]
fn should_treat_whole_series_as_one_run_when_all_share_bucket() {
    // given: all series in the same bucket
    let bid = encode_bucket(TimeBucket { start: 0, size: 1 });
    let name: Arc<str> = Arc::from("m");
    let series = vec![
        ResolvedSeriesRef::new(bid, 1, name.clone()),
        ResolvedSeriesRef::new(bid, 2, name.clone()),
        ResolvedSeriesRef::new(bid, 3, name.clone()),
    ];

    // when: partition
    let runs = contiguous_bucket_runs(&series);

    // then: one run spanning the whole slice
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].range, 0..3);
    assert_eq!(runs[0].bucket_id, bid);
}
