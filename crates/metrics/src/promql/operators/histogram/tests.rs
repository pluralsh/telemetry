use super::*;

fn buckets(pairs: &[(f64, f64)]) -> Vec<Bucket> {
    pairs
        .iter()
        .map(|&(upper_bound, count)| Bucket { upper_bound, count })
        .collect()
}

fn standard() -> Vec<Bucket> {
    buckets(&[
        (0.1, 10.0),
        (0.5, 50.0),
        (1.0, 90.0),
        (f64::INFINITY, 100.0),
    ])
}

#[test]
fn should_interpolate_quantile_within_bucket() {
    // rank 50 lands exactly on the 0.5 bucket boundary
    assert_eq!(bucket_quantile(0.5, &mut standard()), 0.5);
    // rank 70 is halfway through (0.5, 1.0]
    assert_eq!(bucket_quantile(0.7, &mut standard()), 0.75);
    // rank 5 is halfway through the lowest bucket, lower bound 0
    assert_eq!(bucket_quantile(0.05, &mut standard()), 0.05);
}

#[test]
fn should_return_second_highest_bound_for_inf_bucket() {
    assert_eq!(bucket_quantile(0.99, &mut standard()), 1.0);
}

#[test]
fn should_handle_out_of_range_quantiles() {
    assert_eq!(bucket_quantile(-0.1, &mut standard()), f64::NEG_INFINITY);
    assert_eq!(bucket_quantile(1.1, &mut standard()), f64::INFINITY);
    assert!(bucket_quantile(f64::NAN, &mut standard()).is_nan());
}

#[test]
fn should_return_nan_without_inf_bucket_or_observations() {
    assert!(bucket_quantile(0.5, &mut buckets(&[(0.1, 1.0), (1.0, 2.0)])).is_nan());
    assert!(bucket_quantile(0.5, &mut buckets(&[(f64::INFINITY, 3.0)])).is_nan());
    assert!(bucket_quantile(0.5, &mut buckets(&[(1.0, 0.0), (f64::INFINITY, 0.0)])).is_nan());
}

#[test]
fn should_sort_coalesce_and_force_monotonic_buckets() {
    // unsorted, a duplicate `le`, and a decreasing count at 1.0
    let mut input = buckets(&[
        (f64::INFINITY, 100.0),
        (0.5, 25.0),
        (1.0, 40.0),
        (0.5, 25.0),
        (0.1, 10.0),
    ]);
    // after coalescing 0.5 → 50 and flattening 1.0 → 50, rank 50 sits on
    // the 0.5 boundary
    assert_eq!(bucket_quantile(0.5, &mut input), 0.5);
}

#[test]
fn should_return_upper_bound_of_non_positive_lowest_bucket() {
    let mut input = buckets(&[(-1.0, 10.0), (1.0, 20.0), (f64::INFINITY, 20.0)]);
    assert_eq!(bucket_quantile(0.1, &mut input), -1.0);
}

#[test]
fn should_compute_fraction_between_bounds() {
    // (0, 0.5] holds 50 of 100 observations
    assert_eq!(bucket_fraction(0.0, 0.5, &mut standard()), 0.5);
    // (0.5, 0.75] is half of the (0.5, 1.0] bucket's 40
    assert_eq!(bucket_fraction(0.5, 0.75, &mut standard()), 0.2);
    // everything above 1.0 lives in the +Inf bucket
    assert_eq!(bucket_fraction(1.0, f64::INFINITY, &mut standard()), 0.1);
    assert_eq!(
        bucket_fraction(f64::NEG_INFINITY, f64::INFINITY, &mut standard()),
        1.0
    );
}

#[test]
fn should_handle_degenerate_fraction_bounds() {
    assert_eq!(bucket_fraction(1.0, 0.5, &mut standard()), 0.0);
    assert!(bucket_fraction(f64::NAN, 0.5, &mut standard()).is_nan());
    assert!(bucket_fraction(0.0, 0.5, &mut buckets(&[(1.0, 5.0)])).is_nan());
}
