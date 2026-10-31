//! Distance computation functions for vector similarity.
//!
//! This module provides distance and similarity functions used for scoring
//! candidates during similarity search.
//!
//! The L2 and dot-product kernels keep several independent accumulators so a
//! loop is bound by load/FMA throughput instead of the latency of one serial
//! add chain. aarch64 always uses NEON, x86_64 resolves the widest supported
//! kernel once per process, and other targets use a chunked portable loop that
//! LLVM auto-vectorizes. Kernels differ only in summation order, so they agree
//! to within f32 rounding error rather than bit-for-bit.

use crate::serde::collection_meta::DistanceMetric;
use std::cmp::Ordering;

/// L2-normalize a vector in place.
///
/// Projects the vector onto the unit hypersphere. Zero vectors are left
/// unchanged to avoid division by zero.
pub(crate) fn l2_normalize_vector(v: &mut [f32]) {
    let norm = dot_product(v, v).sqrt();
    if norm > 0.0 {
        v.iter_mut().for_each(|x| *x /= norm);
    }
}

/// Compute distance/similarity between two vectors.
///
/// # Arguments
/// * `a` - First vector
/// * `b` - Second vector
/// * `metric` - Distance metric to use
///
/// # Returns
/// Distance/similarity score. Higher scores indicate more similar vectors,
/// except for L2 distance where lower scores indicate more similar vectors.
///
/// # Panics
/// Panics if the vectors have different lengths.
pub(crate) fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> VectorDistance {
    assert_eq!(
        a.len(),
        b.len(),
        "Cannot compute distance between vectors of different lengths"
    );

    let v = match metric {
        DistanceMetric::L2 | DistanceMetric::Cosine => l2_distance(a, b),
        DistanceMetric::DotProduct => dot_product(a, b),
    };
    VectorDistance { score: v, metric }
}

/// Compute a uniform distance where lower = closer, suitable for comparing
/// across distance metrics in the boundary replication formula.
pub(crate) fn raw_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::L2 | DistanceMetric::Cosine => compute_distance(a, b, metric).score(),
        DistanceMetric::DotProduct => -compute_distance(a, b, metric).score(),
    }
}

/// Compute squared L2 distance between two vectors.
///
/// Formula: sum((a[i] - b[i])²)
///
/// Lower scores indicate more similar vectors.
fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "aarch64")]
    {
        neon::l2_distance(a, b)
    }
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: `KERNELS` only holds kernels whose target features were
        // detected on this CPU.
        unsafe { (x86::KERNELS.l2)(a, b) }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        l2_distance_scalar(a, b)
    }
}

/// Compute dot product between two vectors.
///
/// Formula: sum(a[i] * b[i])
///
/// Higher scores indicate more similar vectors (for normalized vectors).
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "aarch64")]
    {
        neon::dot_product(a, b)
    }
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: `KERNELS` only holds kernels whose target features were
        // detected on this CPU.
        unsafe { (x86::KERNELS.dot)(a, b) }
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        dot_product_scalar(a, b)
    }
}

/// Independent accumulators in the portable kernels. Sixteen lanes fill four
/// 128-bit or two 256-bit registers once LLVM vectorizes the loop.
const PORTABLE_LANES: usize = 16;

#[cfg_attr(target_arch = "aarch64", allow(dead_code))]
fn l2_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
    let (a_chunks, a_tail) = a.as_chunks::<PORTABLE_LANES>();
    let (b_chunks, b_tail) = b.as_chunks::<PORTABLE_LANES>();
    let mut acc = [0.0f32; PORTABLE_LANES];
    for (x, y) in a_chunks.iter().zip(b_chunks) {
        for ((slot, x), y) in acc.iter_mut().zip(x).zip(y) {
            let diff = x - y;
            *slot += diff * diff;
        }
    }
    let mut sum = reduce_lanes(acc);
    for (x, y) in a_tail.iter().zip(b_tail) {
        let diff = x - y;
        sum += diff * diff;
    }
    sum
}

#[cfg_attr(target_arch = "aarch64", allow(dead_code))]
fn dot_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    let (a_chunks, a_tail) = a.as_chunks::<PORTABLE_LANES>();
    let (b_chunks, b_tail) = b.as_chunks::<PORTABLE_LANES>();
    let mut acc = [0.0f32; PORTABLE_LANES];
    for (x, y) in a_chunks.iter().zip(b_chunks) {
        for ((slot, x), y) in acc.iter_mut().zip(x).zip(y) {
            *slot += x * y;
        }
    }
    let mut sum = reduce_lanes(acc);
    for (x, y) in a_tail.iter().zip(b_tail) {
        sum += x * y;
    }
    sum
}

/// Pairwise horizontal sum. `N` must be a power of two.
#[inline(always)]
fn reduce_lanes<const N: usize>(mut lanes: [f32; N]) -> f32 {
    let mut width = N;
    while width > 1 {
        width /= 2;
        for i in 0..width {
            lanes[i] += lanes[i + width];
        }
    }
    lanes[0]
}

#[cfg(target_arch = "aarch64")]
mod neon {
    //! NEON is part of the aarch64 baseline, so these need no runtime check.

    use std::arch::aarch64::{
        float32x4_t, vaddq_f32, vaddvq_f32, vdupq_n_f32, vfmaq_f32, vld1q_f32, vsubq_f32,
    };

    const LANES: usize = 4;
    const ACCUMULATORS: usize = 4;
    const BLOCK: usize = LANES * ACCUMULATORS;

    pub(super) fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
        kernel(
            a,
            b,
            // SAFETY: NEON is enabled on every aarch64 target.
            |acc, x, y| unsafe {
                let diff = vsubq_f32(x, y);
                vfmaq_f32(acc, diff, diff)
            },
            |x, y| (x - y) * (x - y),
        )
    }

    pub(super) fn dot_product(a: &[f32], b: &[f32]) -> f32 {
        kernel(
            a,
            b,
            // SAFETY: NEON is enabled on every aarch64 target.
            |acc, x, y| unsafe { vfmaq_f32(acc, x, y) },
            |x, y| x * y,
        )
    }

    #[inline(always)]
    fn kernel(
        a: &[f32],
        b: &[f32],
        step: impl Fn(float32x4_t, float32x4_t, float32x4_t) -> float32x4_t,
        tail: impl Fn(f32, f32) -> f32,
    ) -> f32 {
        let len = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        // SAFETY: NEON is enabled on every aarch64 target.
        let mut accs = [unsafe { vdupq_n_f32(0.0) }; ACCUMULATORS];
        let mut idx = 0;
        while idx + BLOCK <= len {
            for (j, slot) in accs.iter_mut().enumerate() {
                let off = idx + j * LANES;
                // SAFETY: `off + LANES <= idx + BLOCK <= len`, so both loads
                // read initialized `f32`s.
                let (x, y) = unsafe { (vld1q_f32(pa.add(off)), vld1q_f32(pb.add(off))) };
                *slot = step(*slot, x, y);
            }
            idx += BLOCK;
        }
        while idx + LANES <= len {
            // SAFETY: `idx + LANES <= len`.
            let (x, y) = unsafe { (vld1q_f32(pa.add(idx)), vld1q_f32(pb.add(idx))) };
            accs[0] = step(accs[0], x, y);
            idx += LANES;
        }
        // SAFETY: NEON is enabled on every aarch64 target.
        let mut sum = unsafe {
            vaddvq_f32(vaddq_f32(
                vaddq_f32(accs[0], accs[1]),
                vaddq_f32(accs[2], accs[3]),
            ))
        };
        for (&x, &y) in a[idx..len].iter().zip(&b[idx..len]) {
            sum += tail(x, y);
        }
        sum
    }
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::{
        _mm256_add_ps, _mm256_fmadd_ps, _mm256_loadu_ps, _mm256_mul_ps, _mm256_setzero_ps,
        _mm256_storeu_ps, _mm256_sub_ps,
    };
    #[cfg(feature = "avx512")]
    use std::arch::x86_64::{
        _mm512_add_ps, _mm512_fmadd_ps, _mm512_loadu_ps, _mm512_setzero_ps, _mm512_storeu_ps,
        _mm512_sub_ps,
    };
    use std::sync::LazyLock;

    const ACCUMULATORS: usize = 4;

    pub(super) type Kernel = unsafe fn(&[f32], &[f32]) -> f32;

    pub(super) struct Kernels {
        #[cfg_attr(not(test), allow(dead_code))]
        pub(super) name: &'static str,
        pub(super) l2: Kernel,
        pub(super) dot: Kernel,
    }

    pub(super) static KERNELS: LazyLock<Kernels> = LazyLock::new(|| {
        available()
            .into_iter()
            .next()
            .expect("portable kernels are always available")
    });

    /// Every kernel set this CPU supports, widest first.
    pub(super) fn available() -> Vec<Kernels> {
        let mut kernels = Vec::new();
        #[cfg(feature = "avx512")]
        if std::is_x86_feature_detected!("avx512f") {
            kernels.push(Kernels {
                name: "avx512",
                l2: l2_avx512,
                dot: dot_avx512,
            });
        }
        if std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma") {
            kernels.push(Kernels {
                name: "avx+fma",
                l2: l2_avx_fma,
                dot: dot_avx_fma,
            });
        }
        if std::is_x86_feature_detected!("avx") {
            kernels.push(Kernels {
                name: "avx",
                l2: l2_avx,
                dot: dot_avx,
            });
        }
        kernels.push(Kernels {
            name: "portable",
            l2: super::l2_distance_scalar,
            dot: super::dot_product_scalar,
        });
        kernels
    }

    /// Stamps out a `#[target_feature]` kernel that runs `ACCUMULATORS`
    /// independent vector accumulators, then a single-vector loop, then a
    /// scalar tail.
    macro_rules! kernel {
        (
            $(#[$attr:meta])*
            fn $name:ident[$features:literal, $lanes:literal]
                ($zero:ident, $load:ident, $add:ident, $store:ident)
                |$acc:ident, $x:ident, $y:ident| $step:expr;
                |$sx:ident, $sy:ident| $tail:expr
        ) => {
            $(#[$attr])*
            #[target_feature(enable = $features)]
            unsafe fn $name(a: &[f32], b: &[f32]) -> f32 {
                const LANES: usize = $lanes;
                const BLOCK: usize = LANES * ACCUMULATORS;
                let len = a.len().min(b.len());
                let (pa, pb) = (a.as_ptr(), b.as_ptr());
                let mut accs = [$zero(); ACCUMULATORS];
                let mut idx = 0;
                while idx + BLOCK <= len {
                    for (j, slot) in accs.iter_mut().enumerate() {
                        let off = idx + j * LANES;
                        // SAFETY: `off + LANES <= idx + BLOCK <= len`, so both
                        // loads read initialized `f32`s.
                        let ($x, $y) = unsafe { ($load(pa.add(off)), $load(pb.add(off))) };
                        let $acc = *slot;
                        *slot = $step;
                    }
                    idx += BLOCK;
                }
                while idx + LANES <= len {
                    // SAFETY: `idx + LANES <= len`.
                    let ($x, $y) = unsafe { ($load(pa.add(idx)), $load(pb.add(idx))) };
                    let $acc = accs[0];
                    accs[0] = $step;
                    idx += LANES;
                }
                let total = $add($add(accs[0], accs[1]), $add(accs[2], accs[3]));
                let mut lanes = [0.0f32; LANES];
                // SAFETY: `lanes` holds exactly one vector of `f32`s.
                unsafe { $store(lanes.as_mut_ptr(), total) };
                let mut sum = super::reduce_lanes(lanes);
                for (&$sx, &$sy) in a[idx..len].iter().zip(&b[idx..len]) {
                    sum += $tail;
                }
                sum
            }
        };
    }

    kernel! {
        #[cfg(feature = "avx512")]
        fn l2_avx512["avx512f", 16](_mm512_setzero_ps, _mm512_loadu_ps, _mm512_add_ps, _mm512_storeu_ps)
            |acc, x, y| {
                let diff = _mm512_sub_ps(x, y);
                _mm512_fmadd_ps(diff, diff, acc)
            };
            |x, y| (x - y) * (x - y)
    }

    kernel! {
        #[cfg(feature = "avx512")]
        fn dot_avx512["avx512f", 16](_mm512_setzero_ps, _mm512_loadu_ps, _mm512_add_ps, _mm512_storeu_ps)
            |acc, x, y| _mm512_fmadd_ps(x, y, acc);
            |x, y| x * y
    }

    kernel! {
        fn l2_avx_fma["avx,fma", 8](_mm256_setzero_ps, _mm256_loadu_ps, _mm256_add_ps, _mm256_storeu_ps)
            |acc, x, y| {
                let diff = _mm256_sub_ps(x, y);
                _mm256_fmadd_ps(diff, diff, acc)
            };
            |x, y| (x - y) * (x - y)
    }

    kernel! {
        fn dot_avx_fma["avx,fma", 8](_mm256_setzero_ps, _mm256_loadu_ps, _mm256_add_ps, _mm256_storeu_ps)
            |acc, x, y| _mm256_fmadd_ps(x, y, acc);
            |x, y| x * y
    }

    kernel! {
        fn l2_avx["avx", 8](_mm256_setzero_ps, _mm256_loadu_ps, _mm256_add_ps, _mm256_storeu_ps)
            |acc, x, y| {
                let diff = _mm256_sub_ps(x, y);
                _mm256_add_ps(acc, _mm256_mul_ps(diff, diff))
            };
            |x, y| (x - y) * (x - y)
    }

    kernel! {
        fn dot_avx["avx", 8](_mm256_setzero_ps, _mm256_loadu_ps, _mm256_add_ps, _mm256_storeu_ps)
            |acc, x, y| _mm256_add_ps(acc, _mm256_mul_ps(x, y));
            |x, y| x * y
    }
}

#[cfg(feature = "bench-internals")]
fn assert_same_len(a: &[f32], b: &[f32]) {
    assert_eq!(
        a.len(),
        b.len(),
        "Cannot compute distance between vectors of different lengths"
    );
}

/// The portable chunked kernel (what non-SIMD targets run).
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn bench_l2_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
    assert_same_len(a, b);
    l2_distance_scalar(a, b)
}

/// A single serial accumulator, kept as the baseline the kernels are measured
/// against.
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn bench_l2_distance_naive(a: &[f32], b: &[f32]) -> f32 {
    assert_same_len(a, b);
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// The kernel `compute_distance` dispatches to on this CPU.
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn bench_l2_distance_runtime(a: &[f32], b: &[f32]) -> f32 {
    assert_same_len(a, b);
    l2_distance(a, b)
}

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn bench_dot_product_runtime(a: &[f32], b: &[f32]) -> f32 {
    assert_same_len(a, b);
    dot_product(a, b)
}

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn bench_dot_product_naive(a: &[f32], b: &[f32]) -> f32 {
    assert_same_len(a, b);
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// A distance/similarity score between two vectors, with metric-aware ordering.
///
/// Ordering is defined so that `a < b` means `a` is **more similar** than `b`.
/// This abstracts over the direction of each metric:
/// - L2: lower raw value = more similar (natural order)
/// - DotProduct: higher raw value = more similar (reversed order)
#[derive(Copy, Clone, Debug)]
pub(crate) struct VectorDistance {
    score: f32,
    metric: DistanceMetric,
}

impl VectorDistance {
    /// Returns the raw distance/similarity value.
    pub(crate) fn score(&self) -> f32 {
        self.score
    }
}

impl PartialEq for VectorDistance {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for VectorDistance {}

impl PartialOrd for VectorDistance {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VectorDistance {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.metric {
            // L2/Cosine: lower value = more similar, so natural order
            DistanceMetric::L2 | DistanceMetric::Cosine => self.score.total_cmp(&other.score),
            // DotProduct: higher value = more similar, so reverse order
            DistanceMetric::DotProduct => other.score.total_cmp(&self.score),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rstest::rstest;

    type KernelFn = Box<dyn Fn(&[f32], &[f32]) -> f32>;

    /// Every (l2, dot) kernel pair this host can run, plus the dispatched one.
    fn kernels() -> Vec<(&'static str, KernelFn, KernelFn)> {
        let mut kernels: Vec<(&'static str, KernelFn, KernelFn)> = vec![
            ("dispatched", Box::new(l2_distance), Box::new(dot_product)),
            (
                "portable",
                Box::new(l2_distance_scalar),
                Box::new(dot_product_scalar),
            ),
        ];
        #[cfg(target_arch = "aarch64")]
        kernels.push((
            "neon",
            Box::new(neon::l2_distance),
            Box::new(neon::dot_product),
        ));
        #[cfg(target_arch = "x86_64")]
        for k in x86::available() {
            let (l2, dot) = (k.l2, k.dot);
            kernels.push((
                k.name,
                // SAFETY: `available` only returns kernels this CPU supports.
                Box::new(move |a, b| unsafe { l2(a, b) }),
                // SAFETY: as above.
                Box::new(move |a, b| unsafe { dot(a, b) }),
            ));
        }
        kernels
    }

    /// Asserts `actual` is within the worst-case f32 summation error of the
    /// f64 reference: any evaluation order of `n` rounded terms is off by at
    /// most `(n + 2) * eps * sum(|term|)`.
    fn assert_close(actual: f32, terms: impl Iterator<Item = f64>, what: &str) {
        let (mut exact, mut magnitude, mut n) = (0.0f64, 0.0f64, 0usize);
        for t in terms {
            exact += t;
            magnitude += t.abs();
            n += 1;
        }
        let bound =
            (n as f64 + 2.0) * f32::EPSILON as f64 * magnitude + f64::from(f32::MIN_POSITIVE);
        assert!(
            (actual as f64 - exact).abs() <= bound,
            "{what}: got {actual}, exact {exact}, bound {bound}"
        );
    }

    fn l2_terms<'a>(a: &'a [f32], b: &'a [f32]) -> impl Iterator<Item = f64> + 'a {
        a.iter()
            .zip(b)
            .map(|(&x, &y)| (x as f64 - y as f64).powi(2))
    }

    fn dot_terms<'a>(a: &'a [f32], b: &'a [f32]) -> impl Iterator<Item = f64> + 'a {
        a.iter().zip(b).map(|(&x, &y)| x as f64 * y as f64)
    }

    fn pair(len: usize) -> (Vec<f32>, Vec<f32>) {
        let a = (0..len).map(|i| (i as f32 * 0.37).sin() * 3.0).collect();
        let b = (0..len).map(|i| (i as f32 * 0.11).cos() - 0.5).collect();
        (a, b)
    }

    /// Two equal-length vectors with mixed magnitudes, so lane boundaries,
    /// remainders and cancellation all get exercised.
    fn vector_pair() -> impl Strategy<Value = (Vec<f32>, Vec<f32>)> {
        let value = prop_oneof![
            4 => -1.0f32..1.0,
            2 => -1000.0f32..1000.0,
            1 => Just(0.0f32),
            1 => -1e-20f32..1e-20,
        ];
        prop::collection::vec((value.clone(), value), 0..300)
            .prop_map(|pairs| pairs.into_iter().unzip())
    }

    proptest! {
        #[test]
        fn should_match_exact_l2_distance_for_every_kernel((a, b) in vector_pair()) {
            for (name, l2, _) in kernels() {
                assert_close(l2(&a, &b), l2_terms(&a, &b), name);
            }
        }

        #[test]
        fn should_match_exact_dot_product_for_every_kernel((a, b) in vector_pair()) {
            for (name, _, dot) in kernels() {
                assert_close(dot(&a, &b), dot_terms(&a, &b), name);
            }
        }

        #[test]
        fn should_be_exactly_symmetric_for_every_kernel((a, b) in vector_pair()) {
            for (name, l2, dot) in kernels() {
                prop_assert_eq!(l2(&a, &b).to_bits(), l2(&b, &a).to_bits(), "{} l2", name);
                prop_assert_eq!(dot(&a, &b).to_bits(), dot(&b, &a).to_bits(), "{} dot", name);
            }
        }

        #[test]
        fn should_be_zero_distance_from_itself_for_every_kernel((a, _) in vector_pair()) {
            for (name, l2, _) in kernels() {
                prop_assert_eq!(l2(&a, &a), 0.0, "{}", name);
            }
        }

        #[test]
        fn should_normalize_to_unit_length((a, _) in vector_pair()) {
            let mut v = a.clone();
            l2_normalize_vector(&mut v);
            let norm: f64 = v.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt();
            let input_norm: f64 = a.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt();
            if input_norm == 0.0 {
                prop_assert_eq!(v, a);
            } else if input_norm > 1e-10 {
                prop_assert!((norm - 1.0).abs() < 1e-5, "norm {}", norm);
            }
        }
    }

    #[test]
    fn should_handle_every_remainder_around_lane_boundaries() {
        // given - lengths straddling the 4/8/16/64-lane block sizes
        for len in (0..=70).chain([127, 128, 129, 255, 256, 257, 1023, 1024, 1025, 1536]) {
            let (a, b) = pair(len);
            for (name, l2, dot) in kernels() {
                // when/then
                assert_close(
                    l2(&a, &b),
                    l2_terms(&a, &b),
                    &format!("{name} l2 len {len}"),
                );
                assert_close(
                    dot(&a, &b),
                    dot_terms(&a, &b),
                    &format!("{name} dot len {len}"),
                );
            }
        }
    }

    // Parameterized tests for distance functions
    #[rstest]
    #[case(vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0], 27.0, "different vectors")]
    #[case(vec![1.0, 2.0, 3.0], vec![1.0, 2.0, 3.0], 0.0, "identical vectors")]
    fn should_compute_l2_distance(
        #[case] a: Vec<f32>,
        #[case] b: Vec<f32>,
        #[case] expected: f32,
        #[case] _desc: &str,
    ) {
        // when
        let distance = l2_distance(&a, &b);

        // then
        assert!((distance - expected).abs() < 0.01);
    }

    #[rstest]
    #[case(vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0], 32.0, "normal vectors")]
    #[case(vec![1.0, 0.0], vec![0.0, 1.0], 0.0, "orthogonal vectors")]
    fn should_compute_dot_product(
        #[case] a: Vec<f32>,
        #[case] b: Vec<f32>,
        #[case] expected: f32,
        #[case] _desc: &str,
    ) {
        // when
        let dot = dot_product(&a, &b);

        // then
        assert_eq!(dot, expected);
    }

    #[rstest]
    #[case(DistanceMetric::L2, "L2")]
    #[case(DistanceMetric::Cosine, "Cosine")]
    #[case(DistanceMetric::DotProduct, "DotProduct")]
    fn should_use_correct_metric(#[case] metric: DistanceMetric, #[case] _desc: &str) {
        // given
        let a = vec![1.0, 2.0];
        let b = vec![3.0, 4.0];

        // when
        let result = compute_distance(&a, &b, metric);

        // then - verify result matches direct function call
        let expected = match metric {
            DistanceMetric::L2 | DistanceMetric::Cosine => l2_distance(&a, &b),
            DistanceMetric::DotProduct => dot_product(&a, &b),
        };
        assert_eq!(result.score(), expected);
    }

    #[test]
    #[should_panic(expected = "Cannot compute distance between vectors of different lengths")]
    fn should_panic_on_mismatched_dimensions() {
        // given - vectors with different lengths
        let a = vec![1.0, 2.0];
        let b = vec![1.0, 2.0, 3.0];

        // when - attempt to compute distance
        compute_distance(&a, &b, DistanceMetric::L2);

        // then - should panic
    }

    // ---- VectorDistance ordering ----

    #[test]
    fn should_order_l2_by_lower_is_more_similar() {
        // given
        let closer = compute_distance(&[0.0, 0.0], &[1.0, 0.0], DistanceMetric::L2);
        let farther = compute_distance(&[0.0, 0.0], &[3.0, 0.0], DistanceMetric::L2);

        // then - closer (lower L2) should be "less than" farther
        assert!(closer < farther);
        assert!(farther > closer);
        assert_ne!(closer, farther);
    }

    #[test]
    fn should_order_dot_product_by_higher_is_more_similar() {
        // given
        let more_similar = compute_distance(&[3.0, 0.0], &[2.0, 0.0], DistanceMetric::DotProduct);
        let less_similar = compute_distance(&[3.0, 0.0], &[0.0, 2.0], DistanceMetric::DotProduct);

        // then - higher dot product should be "less than" (more similar)
        assert!(more_similar < less_similar);
    }

    #[test]
    fn should_consider_equal_distances_equal() {
        // given
        let d1 = compute_distance(&[1.0, 0.0], &[0.0, 1.0], DistanceMetric::L2);
        let d2 = compute_distance(&[0.0, 1.0], &[1.0, 0.0], DistanceMetric::L2);

        // then
        assert_eq!(d1, d2);
    }

    #[test]
    fn should_sort_vector_distances_most_similar_first() {
        // given - three L2 distances
        let d_far = compute_distance(&[0.0], &[10.0], DistanceMetric::L2);
        let d_mid = compute_distance(&[0.0], &[5.0], DistanceMetric::L2);
        let d_near = compute_distance(&[0.0], &[1.0], DistanceMetric::L2);
        let mut distances = [d_far, d_mid, d_near];

        // when
        distances.sort();

        // then - most similar (nearest) first
        assert_eq!(distances[0].score(), d_near.score());
        assert_eq!(distances[1].score(), d_mid.score());
        assert_eq!(distances[2].score(), d_far.score());
    }

    #[test]
    fn should_normalize_vector_to_unit_length() {
        // given
        let mut v = vec![3.0, 4.0];

        // when
        l2_normalize_vector(&mut v);

        // then
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
        assert!((v[0] - 0.6).abs() < 1e-6);
        assert!((v[1] - 0.8).abs() < 1e-6);
    }

    #[test]
    fn should_leave_zero_vector_unchanged() {
        // given
        let mut v = vec![0.0, 0.0, 0.0];

        // when
        l2_normalize_vector(&mut v);

        // then
        assert_eq!(v, vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn should_order_cosine_by_lower_is_more_similar() {
        // given - cosine uses L2 on normalized vectors
        let a = [1.0, 0.0]; // 0 degrees
        let b_close = [
            std::f32::consts::FRAC_1_SQRT_2,
            std::f32::consts::FRAC_1_SQRT_2,
        ]; // 45 degrees
        let b_far = [0.0, 1.0]; // 90 degrees

        // when
        let closer = compute_distance(&a, &b_close, DistanceMetric::Cosine);
        let farther = compute_distance(&a, &b_far, DistanceMetric::Cosine);

        // then
        assert!(closer < farther);
    }

    #[rstest]
    #[case(vec![1.0, 0.0], vec![1.0, 0.0], 0.0, "identical unit vectors")]
    #[case(vec![1.0, 0.0], vec![0.0, 1.0], 2.0, "orthogonal unit vectors")]
    #[case(vec![1.0, 0.0], vec![-1.0, 0.0], 4.0, "opposite unit vectors")]
    fn should_compute_cosine_distance(
        #[case] a: Vec<f32>,
        #[case] b: Vec<f32>,
        #[case] expected: f32,
        #[case] _desc: &str,
    ) {
        // when - cosine distance is L2 distance on normalized vectors
        let distance = compute_distance(&a, &b, DistanceMetric::Cosine);

        // then
        assert!((distance.score() - expected).abs() < 1e-6);
    }

    #[test]
    fn should_consider_equal_cosine_distances_equal() {
        // given - two pairs of unit vectors at the same angle
        let a = [1.0, 0.0];
        let d1 = compute_distance(&a, &[0.0, 1.0], DistanceMetric::Cosine);
        let d2 = compute_distance(&a, &[0.0, 1.0], DistanceMetric::Cosine);

        // then
        assert_eq!(d1, d2);
    }

    #[test]
    fn should_sort_cosine_distances_most_similar_first() {
        // given - unit vectors at increasing angles from [1, 0]
        let origin = [1.0, 0.0];
        let d_far = compute_distance(&origin, &[-1.0, 0.0], DistanceMetric::Cosine); // 180 degrees
        let d_mid = compute_distance(&origin, &[0.0, 1.0], DistanceMetric::Cosine); // 90 degrees
        let d_near = compute_distance(
            &origin,
            &[
                std::f32::consts::FRAC_1_SQRT_2,
                std::f32::consts::FRAC_1_SQRT_2,
            ],
            DistanceMetric::Cosine,
        ); // 45 degrees
        let mut distances = [d_far, d_mid, d_near];

        // when
        distances.sort();

        // then - most similar (smallest angle) first
        assert_eq!(distances[0].score(), d_near.score());
        assert_eq!(distances[1].score(), d_mid.score());
        assert_eq!(distances[2].score(), d_far.score());
    }

    #[test]
    fn should_compute_raw_cosine_distance_as_lower_is_closer() {
        // given - unit vectors at different angles
        let a = [1.0, 0.0];
        let b_close = [
            std::f32::consts::FRAC_1_SQRT_2,
            std::f32::consts::FRAC_1_SQRT_2,
        ]; // 45 degrees
        let b_far = [0.0, 1.0]; // 90 degrees

        // when
        let close_dist = raw_distance(&a, &b_close, DistanceMetric::Cosine);
        let far_dist = raw_distance(&a, &b_far, DistanceMetric::Cosine);

        // then - lower raw distance means closer
        assert!(close_dist < far_dist);
    }

    #[test]
    fn should_normalize_if_needed_for_cosine_metric() {
        // given
        let mut v = vec![3.0, 4.0];

        // when
        DistanceMetric::Cosine.normalize_if_needed(&mut v);

        // then - vector should be unit length
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
    }

    #[test]
    fn should_not_normalize_for_non_cosine_metrics() {
        // given
        let mut v_l2 = vec![3.0, 4.0];
        let mut v_dot = vec![3.0, 4.0];

        // when
        DistanceMetric::L2.normalize_if_needed(&mut v_l2);
        DistanceMetric::DotProduct.normalize_if_needed(&mut v_dot);

        // then - vectors should be unchanged
        assert_eq!(v_l2, vec![3.0, 4.0]);
        assert_eq!(v_dot, vec![3.0, 4.0]);
    }
}
