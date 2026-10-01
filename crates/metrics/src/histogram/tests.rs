use super::*;

fn buckets(start: i32, counts: &[f64]) -> Vec<Bucket> {
    counts
        .iter()
        .enumerate()
        .map(|(i, &count)| Bucket {
            index: start + i as i32,
            count,
        })
        .collect()
}

fn exponential(schema: i32, count: f64, sum: f64, positive: Vec<Bucket>) -> FloatHistogram {
    FloatHistogram {
        schema,
        count,
        sum,
        positive,
        ..FloatHistogram::default()
    }
}

#[test]
fn should_compute_exponential_bucket_bounds() {
    // schema 0: bucket i covers (2^(i-1), 2^i]
    assert_eq!(exponential_bound(0, 0), 1.0);
    assert_eq!(exponential_bound(1, 0), 2.0);
    assert_eq!(exponential_bound(-1, 0), 0.5);
    // schema 1: boundaries every sqrt(2)
    assert!((exponential_bound(1, 1) - std::f64::consts::SQRT_2).abs() < 1e-15);
    // schema -1: boundaries every 4x
    assert_eq!(exponential_bound(1, -1), 4.0);
    // last finite bucket is pinned to f64::MAX
    assert_eq!(exponential_bound(1024, 0), f64::MAX);
    assert_eq!(exponential_bound(1025, 0), f64::INFINITY);
}

#[test]
fn should_round_trip_spans() {
    // given
    let spans = [
        Span {
            offset: -2,
            length: 2,
        },
        Span {
            offset: 3,
            length: 1,
        },
    ];

    // when
    let sparse = FloatHistogram::buckets_from_spans(&spans, &[1.0, 2.0, 3.0]).unwrap();

    // then
    assert_eq!(
        sparse.iter().map(|b| b.index).collect::<Vec<_>>(),
        vec![-2, -1, 3]
    );
    assert_eq!(to_spans(&sparse), (spans.to_vec(), vec![1.0, 2.0, 3.0]));
}

#[test]
fn should_decode_delta_encoded_counts() {
    let spans = [Span {
        offset: 0,
        length: 3,
    }];
    let sparse = FloatHistogram::buckets_from_delta_spans(&spans, &[2, 1, -3]).unwrap();
    assert_eq!(
        sparse.iter().map(|b| b.count).collect::<Vec<_>>(),
        vec![2.0, 3.0, 0.0]
    );
}

#[test]
fn should_add_histograms_with_different_schemas_at_lower_resolution() {
    // given: schema 1 buckets (1,√2] (√2,2] and schema 0 bucket (1,2]
    let mut a = exponential(1, 3.0, 4.0, buckets(1, &[1.0, 2.0]));
    let b = exponential(0, 5.0, 6.0, buckets(1, &[5.0]));

    // when
    a.add(&b).unwrap();

    // then
    assert_eq!(a.schema, 0);
    assert_eq!(a.positive, buckets(1, &[8.0]));
    assert_eq!((a.count, a.sum), (8.0, 10.0));
}

#[test]
fn should_widen_zero_bucket_when_adding() {
    // given: a has zero threshold 1 (so buckets <= 1 are inside it); b has a
    // tiny threshold and a bucket (0.5,1] that must fold into a's zero bucket
    let mut a = FloatHistogram {
        zero_threshold: 1.0,
        zero_count: 2.0,
        ..exponential(0, 3.0, 3.0, buckets(1, &[1.0]))
    };
    let b = FloatHistogram {
        zero_threshold: 0.001,
        zero_count: 1.0,
        ..exponential(0, 4.0, 2.0, buckets(0, &[2.0, 1.0]))
    };

    // when
    a.add(&b).unwrap();

    // then: b's zero count and its (0.5,1] bucket join a's zero bucket
    assert_eq!(a.zero_threshold, 1.0);
    assert_eq!(a.zero_count, 5.0);
    assert_eq!(a.positive, buckets(1, &[2.0]));
}

#[test]
fn should_reject_mixing_custom_and_exponential() {
    let mut a = exponential(0, 1.0, 1.0, buckets(0, &[1.0]));
    let b = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        custom_values: Arc::from(vec![1.0]),
        ..exponential(0, 1.0, 1.0, buckets(0, &[1.0]))
    };
    assert_eq!(a.add(&b), Err(HistogramError::IncompatibleSchema));
}

#[test]
fn should_intersect_mismatched_custom_bounds() {
    // given: bounds [1,2,4] vs [2,4]
    let custom = |bounds: Vec<f64>, counts: &[f64]| FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        custom_values: Arc::from(bounds),
        count: counts.iter().sum(),
        positive: buckets(0, counts),
        ..FloatHistogram::default()
    };
    let mut a = custom(vec![1.0, 2.0, 4.0], &[1.0, 1.0, 1.0, 1.0]);
    let b = custom(vec![2.0, 4.0], &[2.0, 2.0, 2.0]);

    // when
    let outcome = a.add(&b).unwrap();

    // then: a's first two buckets merge into (-Inf,2]
    assert!(outcome.custom_bounds_reconciled);
    assert_eq!(&*a.custom_values, &[2.0, 4.0]);
    assert_eq!(a.positive, buckets(0, &[4.0, 3.0, 3.0]));
}

#[test]
fn should_detect_counter_resets() {
    let prev = exponential(0, 5.0, 5.0, buckets(0, &[2.0, 3.0]));

    let grown = exponential(0, 7.0, 9.0, buckets(0, &[3.0, 4.0]));
    assert!(!grown.detect_reset(&prev));

    let bucket_dropped = exponential(0, 6.0, 9.0, buckets(0, &[6.0]));
    assert!(bucket_dropped.detect_reset(&prev));

    let higher_schema = exponential(1, 6.0, 9.0, buckets(0, &[3.0, 3.0]));
    assert!(higher_schema.detect_reset(&prev));

    let hinted = FloatHistogram {
        counter_reset_hint: CounterResetHint::NotCounterReset,
        ..bucket_dropped
    };
    assert!(!hinted.detect_reset(&prev));
}

#[test]
fn should_interpolate_quantile_exponentially() {
    // given: all 4 observations in (1,2]
    let h = exponential(0, 4.0, 6.0, buckets(1, &[4.0]));

    // when / then: the median sits at the geometric midpoint √2
    assert!((quantile(0.5, &h) - std::f64::consts::SQRT_2).abs() < 1e-12);
    assert_eq!(quantile(1.5, &h), f64::INFINITY);
    assert!(quantile(0.5, &FloatHistogram::default()).is_nan());
}

#[test]
fn should_estimate_fraction_and_variance() {
    // given: 2 observations in (1,2], 2 in (2,4]
    let h = exponential(0, 4.0, 10.0, buckets(1, &[2.0, 2.0]));

    // then
    assert_eq!(fraction(f64::NEG_INFINITY, 2.0, &h), 0.5);
    assert_eq!(fraction(0.0, f64::INFINITY, &h), 1.0);
    let expected_variance = {
        let mean = 2.5;
        let (m1, m2) = (2f64.sqrt(), 8f64.sqrt());
        (2.0 * (m1 - mean) * (m1 - mean) + 2.0 * (m2 - mean) * (m2 - mean)) / 4.0
    };
    assert!((variance(&h) - expected_variance).abs() < 1e-12);
}

#[test]
fn should_reduce_oversized_schema_on_normalize() {
    let mut h = exponential(10, 2.0, 2.0, buckets(1, &[1.0, 1.0]));
    h.normalize();
    assert_eq!(h.schema, MAX_EXPONENTIAL_SCHEMA);
    assert_eq!(h.positive, buckets(1, &[2.0]));
    assert!(h.validate().is_ok());
}
