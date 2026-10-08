use super::*;
use crate::model::STALE_NAN;
use crate::test_utils::strategies::edge_f64;
use proptest::prelude::*;

fn all_options() -> Vec<Options> {
    [None]
        .into_iter()
        .chain(TimestampScheme::ALL.map(Some))
        .map(|timestamps| Options {
            timestamps,
            values: None,
        })
        .collect()
}

fn forced(scheme: ValueScheme) -> Options {
    Options {
        timestamps: None,
        values: Some(scheme),
    }
}

fn bits(values: &[f64]) -> Vec<u64> {
    values.iter().map(|v| v.to_bits()).collect()
}

fn encode_with(ts: &[i64], vs: &[f64], options: Options) -> (Vec<u8>, Layout) {
    let mut out = Vec::new();
    let layout = encode_chunk(ts, vs, options, &mut out);
    assert_eq!(layout.bytes(), out.len());
    (out, layout)
}

fn section(ts: &[i64], vs: &[f64]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_section(ts, vs, &mut out);
    out
}

#[track_caller]
fn assert_round_trip(ts: &[i64], vs: &[f64], options: Options) -> Layout {
    let (bytes, layout) = encode_with(ts, vs, options);
    let (mut got_ts, mut got_vs) = (vec![7], vec![7.0]);
    decode(&bytes, &mut got_ts, &mut got_vs).expect("decode");
    assert_eq!(&got_ts[1..], ts, "timestamps under {options:?}");
    assert_eq!(bits(&got_vs[1..]), bits(vs), "values under {layout:?}");
    layout
}

fn reference_range(ts: &[i64], vs: &[f64], start_ms: i64, end_ms: i64) -> (Vec<i64>, Vec<u64>) {
    ts.iter()
        .zip(vs)
        .filter(|&(&t, _)| t > start_ms && t <= end_ms)
        .map(|(&t, v)| (t, v.to_bits()))
        .unzip()
}

#[track_caller]
fn assert_range(bytes: &[u8], ts: &[i64], vs: &[f64], start_ms: i64, end_ms: i64) {
    let (mut got_ts, mut got_vs) = (Vec::new(), Vec::new());
    decode_range(bytes, start_ms, end_ms, &mut got_ts, &mut got_vs).expect("decode");
    let (want_ts, want_vs) = reference_range(ts, vs, start_ms, end_ms);
    assert_eq!(got_ts, want_ts, "timestamps in ({start_ms}, {end_ms}]");
    assert_eq!(bits(&got_vs), want_vs, "values in ({start_ms}, {end_ms}]");
}

/// Sorted LWW merge of sorted, deduplicated sources, oldest first.
fn reference_merge(sources: &[(Vec<i64>, Vec<f64>)]) -> (Vec<i64>, Vec<u64>) {
    let mut all: Vec<(i64, Reverse<usize>, u64)> = sources
        .iter()
        .enumerate()
        .flat_map(|(k, (ts, vs))| {
            ts.iter()
                .zip(vs)
                .map(move |(&t, v)| (t, Reverse(k), v.to_bits()))
        })
        .collect();
    all.sort_by_key(|&(t, k, _)| (t, k));
    all.dedup_by_key(|s| s.0);
    all.into_iter().map(|(t, _, v)| (t, v)).unzip()
}

fn merged(sources: &[(Vec<i64>, Vec<f64>)]) -> (Vec<u8>, Vec<i64>, Vec<u64>) {
    let encoded: Vec<Vec<u8>> = sources.iter().map(|(ts, vs)| section(ts, vs)).collect();
    let refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
    let mut out = Vec::new();
    merge(&refs, &mut out).expect("merge");
    let (mut ts, mut vs) = (Vec::new(), Vec::new());
    decode(&out, &mut ts, &mut vs).expect("decode merged");
    (out, ts, bits(&vs))
}

fn decimal() -> impl Strategy<Value = f64> {
    (-1_000_000_000i64..1_000_000_000, 0u32..=6)
        .prop_map(|(mantissa, digits)| format!("{mantissa}e-{digits}").parse().expect("float"))
}

fn near_2_53() -> impl Strategy<Value = f64> {
    (-1000i64..1000).prop_map(|k| ((1i64 << 53) + 2 * k) as f64)
}

fn value() -> impl Strategy<Value = f64> {
    prop_oneof![
        3 => edge_f64(),
        3 => decimal(),
        1 => near_2_53(),
        1 => (-1000i64..1000).prop_map(|i| i as f64),
        1 => Just(f64::from_bits(STALE_NAN)),
    ]
}

/// Mostly one shape per chunk, the way a series looks, with a mix too.
fn values(len: usize) -> impl Strategy<Value = Vec<f64>> {
    prop_oneof![
        prop::collection::vec(value(), len),
        prop::collection::vec(decimal(), len),
        prop::collection::vec(edge_f64(), len),
        (value(), prop::collection::vec(0usize..len.max(1), 0..4)).prop_map(move |(v, holes)| {
            let mut out = vec![v; len];
            for hole in holes {
                if let Some(slot) = out.get_mut(hole) {
                    *slot = f64::from_bits(STALE_NAN);
                }
            }
            out
        }),
        (0u64..1000, prop::collection::vec(0u64..50, len)).prop_map(|(start, steps)| {
            steps
                .iter()
                .scan(start, |acc, step| {
                    *acc += step;
                    Some(*acc as f64)
                })
                .collect()
        }),
    ]
}

/// Sorted scrape timestamps: jittered, with gaps; strictly increasing
/// unless `repeats`.
fn sorted_timestamps(len: usize, repeats: bool) -> impl Strategy<Value = Vec<i64>> {
    let delta = prop_oneof![
        6 => 14_990i64..15_010,
        1 => Just(if repeats { 0i64 } else { 1 }),
        1 => 1i64..1_000_000,
        1 => 1i64..(1 << 40),
    ];
    (
        any::<i64>().prop_map(|t| t >> 2),
        prop::collection::vec(delta, len),
    )
        .prop_map(move |(start, deltas)| {
            let mut out: Vec<i64> = deltas
                .iter()
                .scan(start, |acc, delta| {
                    let ts = *acc;
                    *acc = acc.saturating_add(*delta);
                    Some(ts)
                })
                .collect();
            if !repeats {
                out.dedup();
            }
            out
        })
}

fn arbitrary_timestamps(len: usize) -> impl Strategy<Value = Vec<i64>> {
    prop_oneof![
        prop::collection::vec(any::<i64>(), len),
        prop::collection::vec(
            prop::sample::select(vec![i64::MIN, i64::MAX, 0, -1, 1]),
            len
        ),
    ]
}

fn columns(
    lens: Range<usize>,
    timestamps: impl Fn(usize) -> BoxedStrategy<Vec<i64>>,
) -> impl Strategy<Value = (Vec<i64>, Vec<f64>)> {
    lens.prop_flat_map(timestamps)
        .prop_flat_map(|ts| {
            let len = ts.len();
            (Just(ts), values(len))
        })
        .prop_filter("non-empty", |(ts, _)| !ts.is_empty())
}

fn chunk(
    timestamps: impl Fn(usize) -> BoxedStrategy<Vec<i64>>,
) -> impl Strategy<Value = (Vec<i64>, Vec<f64>)> {
    columns(1..300, timestamps)
}

/// Operands over a narrow window, so they overlap and collide, or laid end
/// to end, so they merge by copying chunks.
fn merge_sources() -> impl Strategy<Value = Vec<(Vec<i64>, Vec<f64>)>> {
    let len = prop_oneof![3 => 1usize..5, 2 => 5usize..80, 1 => 500usize..2500];
    let source = len.prop_flat_map(|len| {
        (
            prop::collection::btree_set(0i64..4000, 1..=len),
            values(len),
        )
    });
    (
        prop::collection::vec(source, 1..12),
        any::<bool>(),
        0usize..4,
    )
        .prop_map(|(sources, disjoint, overlap_at)| {
            let mut next = 0i64;
            sources
                .into_iter()
                .enumerate()
                .map(|(k, (ts, vs))| {
                    let offset = if disjoint && k != overlap_at { next } else { 0 };
                    let ts: Vec<i64> = ts.into_iter().map(|t| (t + offset) * 15_000).collect();
                    next = offset + 4001;
                    let vs = vs.into_iter().take(ts.len()).collect();
                    (ts, vs)
                })
                .collect()
        })
}

proptest! {
    #[test]
    fn should_round_trip_sorted_chunks((ts, vs) in chunk(|len| sorted_timestamps(len, true).boxed())) {
        for options in all_options() {
            assert_round_trip(&ts, &vs, options);
        }
    }

    #[test]
    fn should_round_trip_arbitrary_timestamps(
        (ts, vs) in chunk(|len| arbitrary_timestamps(len).boxed()),
    ) {
        for options in all_options() {
            assert_round_trip(&ts, &vs, options);
        }
    }

    #[test]
    fn should_round_trip_every_value_scheme((ts, vs) in chunk(|len| sorted_timestamps(len, true).boxed())) {
        for scheme in ValueScheme::ALL {
            let layout = assert_round_trip(&ts, &vs, forced(scheme));
            if scheme != ValueScheme::Constant {
                prop_assert_eq!(layout.value_scheme, scheme);
            }
        }
    }

    #[test]
    fn should_round_trip_sections(
        (ts, vs) in columns(1..3000, |len| sorted_timestamps(len, false).boxed()),
    ) {
        let bytes = section(&ts, &vs);
        let (mut got_ts, mut got_vs) = (Vec::new(), Vec::new());
        decode(&bytes, &mut got_ts, &mut got_vs).expect("decode");
        prop_assert_eq!(got_ts, ts);
        prop_assert_eq!(bits(&got_vs), bits(&vs));
    }

    #[test]
    fn should_decode_ranges_like_a_filter(
        (ts, vs) in columns(1..2500, |len| sorted_timestamps(len, false).boxed()),
        cuts in prop::collection::vec((any::<prop::sample::Index>(), -2i64..=2), 2),
    ) {
        let pick = |(index, nudge): &(prop::sample::Index, i64)| {
            ts[index.index(ts.len())].saturating_add(*nudge)
        };
        let (start_ms, end_ms) = (pick(&cuts[0]), pick(&cuts[1]));
        let bytes = section(&ts, &vs);
        assert_range(&bytes, &ts, &vs, start_ms, end_ms);
        assert_range(&bytes, &ts, &vs, i64::MIN, end_ms);
        assert_range(&bytes, &ts, &vs, start_ms, i64::MAX);
        for options in all_options().into_iter().chain(ValueScheme::ALL.map(forced)) {
            let n = ts.len().min(MAX_CHUNK_SAMPLES);
            let (bytes, _) = encode_with(&ts[..n], &vs[..n], options);
            assert_range(&bytes, &ts[..n], &vs[..n], start_ms, end_ms);
        }
    }

    #[test]
    fn should_merge_like_sorted_last_write_wins(sources in merge_sources()) {
        let (_, ts, vs) = merged(&sources);
        let (want_ts, want_vs) = reference_merge(&sources);
        prop_assert_eq!(ts, want_ts);
        prop_assert_eq!(vs, want_vs);
    }

    #[test]
    fn should_merge_repeatedly_like_one_merge(sources in merge_sources()) {
        let mut acc = section(&sources[0].0, &sources[0].1);
        for (ts, vs) in &sources[1..] {
            let operand = section(ts, vs);
            let mut out = Vec::new();
            merge(&[&acc, &operand], &mut out).expect("merge");
            acc = out;
        }
        let (mut ts, mut vs) = (Vec::new(), Vec::new());
        decode(&acc, &mut ts, &mut vs).expect("decode");
        let (want_ts, want_vs) = reference_merge(&sources);
        prop_assert_eq!(ts, want_ts);
        prop_assert_eq!(bits(&vs), want_vs);
        prop_assert!(Chunks(&acc).all(|c| c.is_ok_and(|c| c.n <= MAX_CHUNK_SAMPLES)));
    }

    #[test]
    fn should_reject_truncated_chunks_without_panicking(
        (ts, vs) in chunk(|len| sorted_timestamps(len, true).boxed()),
        scheme in prop::sample::select(ValueScheme::ALL.to_vec()),
    ) {
        let (bytes, _) = encode_with(&ts, &vs, forced(scheme));
        for len in 1..bytes.len() {
            let (mut got_ts, mut got_vs) = (Vec::new(), Vec::new());
            prop_assert!(decode(&bytes[..len], &mut got_ts, &mut got_vs).is_err());
            prop_assert!(got_ts.is_empty() && got_vs.is_empty());
            prop_assert!(merge(&[&bytes[..len], &bytes], &mut Vec::new()).is_err());
        }
    }

    #[test]
    fn should_survive_bit_flips(
        (ts, vs) in columns(1..1500, |len| sorted_timestamps(len, false).boxed()),
        flips in prop::collection::vec((any::<prop::sample::Index>(), 0u8..8), 1..4),
    ) {
        let mut bytes = section(&ts, &vs);
        for (index, bit) in flips {
            let at = index.index(bytes.len());
            bytes[at] ^= 1 << bit;
        }
        let (mut got_ts, mut got_vs) = (Vec::new(), Vec::new());
        let _ = decode(&bytes, &mut got_ts, &mut got_vs);
        let _ = decode_range(&bytes, ts[0], ts[ts.len() / 2], &mut got_ts, &mut got_vs);
        let operand = section(&ts[ts.len() / 2..], &vs[vs.len() / 2..]);
        let _ = merge(&[&bytes, &operand], &mut Vec::new());
        let _ = merge(&[&operand, &bytes], &mut Vec::new());
    }

    #[test]
    fn should_survive_arbitrary_bytes(bytes in prop::collection::vec(any::<u8>(), 0..400)) {
        let (mut ts, mut vs) = (Vec::new(), Vec::new());
        let _ = decode(&bytes, &mut ts, &mut vs);
        let _ = decode_range(&bytes, 0, i64::MAX, &mut ts, &mut vs);
        let _ = merge(&[&bytes, &bytes], &mut Vec::new());
    }

    #[test]
    fn should_convert_bounded_ints_exactly(d in -ALP_BOUND..ALP_BOUND) {
        prop_assert_eq!(magic_int_to_f64(d).to_bits(), (d as f64).to_bits());
    }

    #[test]
    fn should_encode_decimals_without_exceptions(
        digits in 0u32..=6,
        mantissas in prop::collection::vec(-1_000_000_000i64..1_000_000_000, 17..240),
    ) {
        let vs: Vec<f64> = mantissas
            .iter()
            .map(|m| format!("{m}e-{digits}").parse().expect("float"))
            .collect();
        let ts: Vec<i64> = (0..vs.len() as i64).map(|i| i * 15_000).collect();
        let layout = assert_round_trip(&ts, &vs, Options::default());
        prop_assert!(!layout.value_scheme.is_fallback());
        prop_assert_eq!(layout.exceptions, 0, "{:?}", layout);
    }
}

#[test]
fn should_convert_ints_at_the_bounds_exactly() {
    for d in [-ALP_BOUND, -ALP_BOUND + 1, -1, 0, 1, ALP_BOUND - 1] {
        assert_eq!(magic_int_to_f64(d).to_bits(), (d as f64).to_bits(), "{d}");
    }
}

#[test]
fn should_encode_single_sample_chunks() {
    for value in [1.5, -0.0, f64::from_bits(STALE_NAN), f64::INFINITY] {
        for options in all_options() {
            assert_round_trip(&[i64::MIN], &[value], options);
            assert_round_trip(&[1_790_000_000_000], &[value], options);
        }
    }
    let (bytes, layout) = encode_with(&[1_790_000_000_000], &[1.0], Options::default());
    assert_eq!(layout.value_scheme, ValueScheme::ByteXor);
    assert_eq!(bytes.len(), 18, "{layout:?}");
}

#[test]
fn should_keep_tiny_operands_within_gorilla_sizes() {
    let start = 1_790_000_000_000i64;
    for (n, gorilla_bytes) in [(1usize, 23), (2, 29), (4, 35), (8, 46)] {
        let ts: Vec<i64> = (0..n as i64).map(|i| start + i * 15_000).collect();
        let counter: Vec<f64> = (0..n).map(|i| (1_000_000 + 37 * i) as f64).collect();
        let (bytes, layout) = encode_with(&ts, &counter, Options::default());
        assert!(bytes.len() < gorilla_bytes, "{n} samples: {layout:?}");
    }
}

#[test]
fn should_make_exceptions_of_values_alp_cannot_reproduce() {
    let specials = [
        f64::from_bits(STALE_NAN),
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -0.0,
        f64::from_bits(1),
        f64::MAX,
        f64::MIN,
        1.0 / 3.0,
    ];
    let mut vs: Vec<f64> = (0..120).map(|i| f64::from(i) * 0.25).collect();
    for (k, special) in specials.iter().enumerate() {
        vs[k * 13] = *special;
    }
    let ts: Vec<i64> = (0..120).map(|i| i * 15_000).collect();
    let layout = assert_round_trip(&ts, &vs, Options::default());
    assert!(!layout.value_scheme.is_fallback(), "{layout:?}");
    assert_eq!(layout.exceptions, specials.len());
}

#[test]
fn should_pack_counters_as_deltas() {
    let vs: Vec<f64> = (0..240).map(|i| f64::from(1_000_000 + i * 37)).collect();
    let ts: Vec<i64> = (0..240).map(|i| i * 15_000).collect();
    let layout = assert_round_trip(&ts, &vs, Options::default());
    assert_eq!(layout.value_scheme, ValueScheme::AlpDelta);
    assert!(layout.value_bytes < 20, "{layout:?}");
}

#[test]
fn should_shift_out_shared_trailing_zero_bits() {
    let ts: Vec<i64> = (0..240).map(|i| i * 60_000).collect();
    let pages = [3, -7, 12, 0, 5, -2, 9, -11];
    let vs: Vec<f64> = (0..240)
        .scan(2_000_000i64, |level, i| {
            *level += pages[i % pages.len()];
            Some((*level * 4096) as f64)
        })
        .collect();
    let layout = assert_round_trip(&ts, &vs, Options::default());
    assert!(layout.value_bytes * 8 <= 240 * 5 + 120, "{layout:?}");
    assert!(layout.timestamp_bytes < 16, "{layout:?}");
}

#[test]
fn should_fall_back_for_full_precision_values() {
    let ts: Vec<i64> = (0..240).map(|i| i * 15_000).collect();
    let smooth: Vec<f64> = (0..240).map(|i| (f64::from(i) / 10.0).sin()).collect();
    let layout = assert_round_trip(&ts, &smooth, Options::default());
    assert!(layout.value_scheme.is_fallback(), "{layout:?}");
    assert!(layout.alp_exceptions * 2 > smooth.len());

    let third = vec![1.0 / 3.0; 240];
    let layout = assert_round_trip(&ts, &third, Options::default());
    assert_eq!(layout.value_scheme, ValueScheme::Constant);
    assert_eq!(layout.value_bytes, 8);
}

#[test]
fn should_pack_jittered_scrapes_into_few_bits() {
    let jitter = [0, 2, -2, 5, 0, -1, 3, 0];
    let ts: Vec<i64> = (0..240)
        .map(|i| 1_790_000_000_000 + i * 15_000 + jitter[i as usize % jitter.len()])
        .collect();
    let vs = vec![1.0; 240];
    let (_, layout) = encode_with(&ts, &vs, Options::default());
    assert_eq!(layout.timestamp_scheme, TimestampScheme::Grid);
    assert!(layout.timestamp_bytes * 8 <= 240 * 4, "{layout:?}");
    for scheme in TimestampScheme::ALL {
        let options = Options {
            timestamps: Some(scheme),
            ..Options::default()
        };
        assert_round_trip(&ts, &vs, options);
    }
}

#[test]
fn should_split_long_runs_into_balanced_chunks() {
    let ts: Vec<i64> = (0..2500).map(|i| i * 15_000).collect();
    let vs: Vec<f64> = (0..2500).map(f64::from).collect();
    let sizes: Vec<usize> = Chunks(&section(&ts, &vs))
        .map(|c| c.expect("chunk").n)
        .collect();
    assert_eq!(sizes, [834, 834, 832]);
}

#[test]
fn should_merge_disjoint_operands_by_copying_chunks() {
    let ts: Vec<i64> = (0..600).map(|i| i * 15_000).collect();
    let vs: Vec<f64> = (0..600).map(|i| f64::from(i) * 0.5).collect();
    let existing = section(&ts, &vs);
    let operand = section(&[600 * 15_000], &[300.0]);
    let mut out = Vec::new();
    merge(&[&existing, &operand], &mut out).expect("merge");
    assert_eq!(out, [existing.as_slice(), operand.as_slice()].concat());

    let late: Vec<Vec<u8>> = (601..605)
        .map(|i| section(&[i * 15_000], &[f64::from(i as i32)]))
        .collect();
    let mut sources: Vec<&[u8]> = vec![&out];
    sources.extend(late.iter().map(Vec::as_slice));
    let mut coalesced = Vec::new();
    merge(&sources, &mut coalesced).expect("merge");
    let sizes: Vec<usize> = Chunks(&coalesced).map(|c| c.expect("chunk").n).collect();
    assert_eq!(sizes, [600, 5]);
    assert!(coalesced.starts_with(&existing));
}

#[test]
fn should_let_the_newest_operand_win_collisions() {
    let sources = vec![
        (vec![1000, 2000, 3000], vec![1.0, 2.0, 3.0]),
        (vec![2000, 3000], vec![20.0, -0.0]),
        (vec![3000, 4000], vec![f64::from_bits(STALE_NAN), 40.0]),
    ];
    let (_, ts, vs) = merged(&sources);
    assert_eq!(ts, [1000, 2000, 3000, 4000]);
    assert_eq!(vs, bits(&[1.0, 20.0, f64::from_bits(STALE_NAN), 40.0]));
}

#[test]
fn should_round_trip_every_width_across_block_boundaries() {
    for len in [1, 63, 64, 65, 127, 128, 129, 240] {
        for width in 0..=64u32 {
            let lanes: Vec<u64> = (0..len as u64)
                .map(|i| i.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> (64 - width.max(1)))
                .map(|lane| if width == 0 { 0 } else { lane })
                .collect();
            let mut bytes = Vec::new();
            pack_with(len, width, |i| lanes[i], &mut bytes);
            assert_eq!(bytes.len(), packed_len(len, width));
            let clean = bytes.clone();
            bytes.extend_from_slice(&[0xff; 600]);
            for bytes in [&clean, &bytes] {
                let packed = Packed {
                    bytes,
                    len,
                    frame: Frame::raw(width),
                };
                let mut block = [0u64; BLOCK];
                for (b, want) in lanes.chunks(BLOCK).enumerate() {
                    packed.block(b, &mut block);
                    assert_eq!(&block[..want.len()], want, "len {len} width {width}");
                }
                for (i, &want) in lanes.iter().enumerate() {
                    assert_eq!(packed.lane(i), want, "lane {i} len {len} width {width}");
                }
            }
        }
    }
}

#[test]
fn should_reproduce_xor_streams() {
    let vs: Vec<f64> = (0..200)
        .map(|i| match i % 5 {
            0 => f64::from_bits(STALE_NAN),
            1 => f64::from(i).sqrt(),
            2 => f64::from(i).sqrt(),
            3 => -0.0,
            _ => f64::from_bits(u64::MAX - i as u64),
        })
        .collect();
    let mut bytes = Vec::new();
    write_xor(&vs, &mut bytes);
    let mut out = vec![0.0; vs.len()];
    decode_xor(&bytes, 0..vs.len(), &mut out).expect("decode");
    assert_eq!(bits(&out), bits(&vs));
}
