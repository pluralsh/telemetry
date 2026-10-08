use crate::histogram::{Bucket, FloatHistogram};
use crate::model::RangeSample;

/// Compare actual results against expected results.
///
/// Labelsets must match exactly, as in Prometheus' promqltest: an
/// expectation without a metric name asserts that the result has none.
pub(super) fn assert_results(
    results: &[RangeSample],
    expected: &[RangeSample],
    expect_ordered: bool,
    test_name: &str,
    eval_num: usize,
    query: &str,
) -> Result<(), String> {
    if results.len() != expected.len() {
        return Err(format!(
            "{} eval #{} (query: {}): Expected {} samples, got {}",
            test_name,
            eval_num,
            query,
            expected.len(),
            results.len()
        ));
    }

    // Most instant vectors are unordered in PromQL, but promqltest supports
    // `expect ordered` for order-sensitive checks (e.g. topk/bottomk).
    let sorted_labels = |s: &RangeSample| {
        let mut labels: Vec<_> = s.labels.iter().cloned().collect();
        labels.sort();
        labels
    };
    let mut results_sorted = results.to_vec();
    let mut expected_sorted = expected.to_vec();
    if !expect_ordered {
        results_sorted.sort_by_cached_key(sorted_labels);
        expected_sorted.sort_by_cached_key(sorted_labels);
    }

    for (i, exp) in expected_sorted.iter().enumerate() {
        let result = &results_sorted[i];

        if sorted_labels(result) != sorted_labels(exp) {
            return Err(format!(
                "{} eval #{} (query: {}): Labels mismatch: expected {:?}, got {:?}",
                test_name, eval_num, query, exp.labels, result.labels
            ));
        }

        let mismatch = match (exp.histograms.first(), result.histograms.first()) {
            (Some((_, exp_h)), Some((_, got_h))) => (!histograms_match(exp_h, got_h))
                .then(|| format!("expected {exp_h:?}, got {got_h:?}")),
            (Some((_, exp_h)), None) => Some(format!(
                "expected histogram {exp_h:?}, got float {:?}",
                result.samples.first().map(|s| s.1)
            )),
            (None, Some((_, got_h))) => Some(format!(
                "expected float {:?}, got histogram {got_h:?}",
                exp.samples.first().map(|s| s.1)
            )),
            (None, None) => {
                let exp_value = exp.samples[0].1;
                let result_value = result.samples[0].1;
                (!floats_match(exp_value, result_value))
                    .then(|| format!("expected {exp_value}, got {result_value}"))
            }
        };
        if let Some(detail) = mismatch {
            return Err(format!(
                "{} eval #{} (query: {}): Value mismatch for {:?}: {}",
                test_name, eval_num, query, result.labels, detail
            ));
        }
    }

    Ok(())
}

fn floats_match(expected: f64, actual: f64) -> bool {
    if expected.is_nan() || actual.is_nan() {
        return expected.is_nan() && actual.is_nan();
    }
    if expected == actual {
        return true;
    }
    let diff = (expected - actual).abs();
    diff <= 1e-6 || diff <= 1e-6 * expected.abs().max(actual.abs())
}

/// Compare after compacting both sides; counter-reset hints are ignored.
fn histograms_match(expected: &FloatHistogram, actual: &FloatHistogram) -> bool {
    let mut expected = expected.clone();
    let mut actual = actual.clone();
    expected.compact();
    actual.compact();
    let buckets_match = |a: &[Bucket], b: &[Bucket]| {
        a.len() == b.len()
            && a.iter()
                .zip(b)
                .all(|(a, b)| a.index == b.index && floats_match(a.count, b.count))
    };
    expected.schema == actual.schema
        && floats_match(expected.zero_threshold, actual.zero_threshold)
        && floats_match(expected.zero_count, actual.zero_count)
        && floats_match(expected.count, actual.count)
        && floats_match(expected.sum, actual.sum)
        && buckets_match(&expected.positive, &actual.positive)
        && buckets_match(&expected.negative, &actual.negative)
        && expected.custom_values == actual.custom_values
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Labels};

    fn labels_from(pairs: &[(&str, &str)]) -> Labels {
        let mut labels: Vec<Label> = pairs.iter().map(|(k, v)| Label::new(*k, *v)).collect();
        labels.sort();
        Labels::new(labels)
    }

    fn range_sample(labels: Labels, value: f64) -> RangeSample {
        RangeSample {
            labels,
            samples: vec![(0, value)],
            histograms: Vec::new(),
        }
    }

    #[test]
    fn should_match_expected_results() {
        // given
        let results = vec![range_sample(labels_from(&[("job", "test")]), 42.0)];
        let expected = vec![range_sample(labels_from(&[("job", "test")]), 42.0)];

        // when
        let result = assert_results(&results, &expected, false, "test", 1, "metric");

        // then
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_unexpected_metric_name() {
        // given
        let results = vec![range_sample(
            labels_from(&[("__name__", "metric"), ("job", "test")]),
            42.0,
        )];
        let expected = vec![range_sample(labels_from(&[("job", "test")]), 42.0)];

        // when
        let result = assert_results(&results, &expected, false, "test", 1, "metric");

        // then
        assert!(result.unwrap_err().contains("Labels mismatch"));
    }

    #[test]
    fn should_reject_count_mismatch() {
        // given
        let results = vec![range_sample(Labels::empty(), 42.0)];
        let expected = vec![];

        // when
        let result = assert_results(&results, &expected, false, "test", 1, "metric");

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Expected 0 samples, got 1"));
    }

    #[test]
    fn should_reject_mismatched_values() {
        // given
        let results = vec![range_sample(Labels::empty(), 42.0)];
        let expected = vec![range_sample(Labels::empty(), 99.0)];

        // when
        let result = assert_results(&results, &expected, false, "test", 1, "metric");

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Value mismatch"));
    }

    #[test]
    fn should_reject_order_mismatch_when_ordered() {
        // given
        let results = vec![
            range_sample(labels_from(&[("instance", "b")]), 2.0),
            range_sample(labels_from(&[("instance", "a")]), 1.0),
        ];
        let expected = vec![
            range_sample(labels_from(&[("instance", "a")]), 1.0),
            range_sample(labels_from(&[("instance", "b")]), 2.0),
        ];

        // when
        let result = assert_results(&results, &expected, true, "test", 1, "metric");

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Labels mismatch"));
    }

    #[test]
    fn should_allow_order_mismatch_when_not_ordered() {
        // given
        let results = vec![
            range_sample(labels_from(&[("instance", "b")]), 2.0),
            range_sample(labels_from(&[("instance", "a")]), 1.0),
        ];
        let expected = vec![
            range_sample(labels_from(&[("instance", "a")]), 1.0),
            range_sample(labels_from(&[("instance", "b")]), 2.0),
        ];

        // when
        let result = assert_results(&results, &expected, false, "test", 1, "metric");

        // then
        assert!(result.is_ok());
    }
}
