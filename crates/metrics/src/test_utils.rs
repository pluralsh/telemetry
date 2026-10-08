#[cfg(test)]
pub(crate) mod assertions {
    const EPSILON: f64 = 1e-10;

    /// Helper to check if two floats are approximately equal
    pub(crate) fn approx_eq(a: f64, b: f64) -> bool {
        (a - b).abs() < EPSILON || (a.is_nan() && b.is_nan())
    }

    /// Assert that two floats are approximately equal, with a helpful error message
    #[track_caller]
    pub(crate) fn assert_approx_eq(actual: f64, expected: f64) {
        assert!(
            approx_eq(actual, expected),
            "expected {}, got {}",
            expected,
            actual
        );
    }
}

#[cfg(test)]
pub(crate) mod strategies {
    use proptest::prelude::*;

    /// Floats weighted toward the values where IEEE 754 edge behaviour
    /// shows: NaN payloads (including the stale marker), infinities,
    /// signed zeros, subnormals and the extremes.
    pub(crate) fn edge_f64() -> impl Strategy<Value = f64> {
        let specials = vec![
            f64::NAN,
            -f64::NAN,
            f64::from_bits(crate::model::STALE_NAN),
            f64::from_bits(0x7ff8_0000_dead_beef),
            f64::from_bits(0xfff0_0000_0000_0001),
            f64::INFINITY,
            f64::NEG_INFINITY,
            0.0,
            -0.0,
            f64::from_bits(1),
            -f64::from_bits(1),
            f64::MIN_POSITIVE / 2.0,
            f64::MIN_POSITIVE,
            f64::MAX,
            f64::MIN,
            f64::EPSILON,
            0.5,
            -0.5,
            1.0,
            -1.0,
            2.5,
            -2.5,
        ];
        prop_oneof![
            3 => any::<u64>().prop_map(f64::from_bits),
            3 => -1.0e6f64..1.0e6,
            2 => prop::sample::select(specials),
        ]
    }
}
