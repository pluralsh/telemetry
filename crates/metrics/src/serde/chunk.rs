//! Columnar chunks: the storage encoding of a series' float samples, with
//! bit-packed timestamps and ALP-encoded values (Afroozeh, Kuffó, Boncz,
//! "ALP: Adaptive Lossless floating-Point Compression", SIGMOD 2023).
//!
//! A float section is a sequence of chunks with strictly increasing,
//! non-overlapping timestamps. A chunk is a header (sample count, first
//! timestamp, span to the last, body length) and a body: a layout byte
//! naming the timestamp and value schemes, the timestamp column, then the
//! value column. The header alone bounds the chunk, so a ranged decode
//! skips chunks outside the range and the merge operator concatenates
//! disjoint ones without decoding them.
//!
//! A packed column holds one `width`-bit lane per sample, LSB-first in
//! little-endian `u64` words truncated to whole bytes, so each block of 64
//! lanes starts on a byte boundary and unpacks on its own. With `min` the
//! frame of reference, `ts[i]` is
//! - varint: `ts[i - 1]` plus a varint delta (the last is `first + span`);
//! - grid: `first + i * interval + min + lane[i]`;
//! - delta: the running sum of `min + lane[i]` from `first - min`;
//! - delta-of-delta: the running sum of a running sum.
//!
//! Values are ALP ints `d` with `d * 10^f / 10^e` reproducing the value's
//! bits, packed against their minimum or as deltas, with the values that
//! do not round-trip stored raw and patched in after. A chunk where ALP
//! misses more than half the values (or most of four sampled ones) takes
//! the smallest of ALP, ALP-RD and a Gorilla XOR stream, or a single value
//! when all are bit-identical. Chunks of up to [`SMALL_VALUES`] samples may
//! instead hold the first value raw and each later one as a byte-aligned
//! XOR with its predecessor.
//!
//! Timestamp and int arithmetic wraps, so any `i64` sequence round-trips
//! through a chunk; ranged decodes and sections additionally need
//! timestamps sorted.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::Range;

use super::EncodingError;

/// Samples per chunk at most; longer runs split into balanced chunks.
pub const MAX_CHUNK_SAMPLES: usize = 1024;
const BLOCK: usize = 64;
/// Values sampled when choosing ALP's exponent / factor or ALP-RD's cut.
const SAMPLE: usize = 32;
/// Chunks this short also try the byte-XOR value scheme.
const SMALL_VALUES: usize = 16;

/// Room reserved for a chunk's body length while its body is written; a
/// full chunk's body stays well under the 2 MiB three varint bytes hold.
const BODY_LEN_BYTES: usize = 3;

/// Chunks shorter than this are fresh merge operands, coalesced by the
/// merge once [`COALESCE_RUN`] of them sit next to each other.
const SMALL_CHUNK: usize = 32;
const COALESCE_RUN: usize = 4;
/// Chunks at least this long are kept by the merge as they are.
const FULL_CHUNK: usize = MAX_CHUNK_SAMPLES / 2;
/// A merge leaving more chunks between [`SMALL_CHUNK`] and [`FULL_CHUNK`]
/// samples than this re-encodes the whole section into balanced chunks.
const MAX_PARTIAL_CHUNKS: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampScheme {
    /// Varint deltas between the first and the last; no column.
    Varint,
    /// `first + i * interval` plus a packed deviation; no prefix sum.
    Grid,
    /// Packed deltas, one prefix sum.
    Delta,
    /// Packed delta-of-deltas, two prefix sums.
    DeltaOfDelta,
}

impl TimestampScheme {
    pub const ALL: [Self; 4] = [Self::Varint, Self::Grid, Self::Delta, Self::DeltaOfDelta];

    fn from_tag(tag: u8) -> Option<Self> {
        Self::ALL.get(usize::from(tag)).copied()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueScheme {
    /// The first value raw, then byte-aligned XORs with the previous one.
    ByteXor,
    /// ALP ints packed against their minimum.
    AlpFor,
    /// ALP ints packed as deltas against the minimum delta.
    AlpDelta,
    /// ALP-RD: the low bits packed, the high bits through a dictionary of
    /// up to eight entries.
    AlpRd,
    /// Gorilla XOR stream.
    Xor,
    /// Every value has the same bits.
    Constant,
}

impl ValueScheme {
    pub const ALL: [Self; 6] = [
        Self::ByteXor,
        Self::AlpFor,
        Self::AlpDelta,
        Self::AlpRd,
        Self::Xor,
        Self::Constant,
    ];

    fn from_tag(tag: u8) -> Option<Self> {
        Self::ALL.get(usize::from(tag)).copied()
    }

    /// Chosen because ALP missed too many values.
    pub fn is_fallback(self) -> bool {
        matches!(self, Self::AlpRd | Self::Xor | Self::Constant)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Options {
    /// `None` picks the smallest.
    pub timestamps: Option<TimestampScheme>,
    /// `None` follows the fallback rule above. A forced `Constant` is
    /// ignored unless every value has the same bits.
    pub values: Option<ValueScheme>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    pub samples: usize,
    /// The chunk header and layout byte.
    pub header_bytes: usize,
    pub timestamp_bytes: usize,
    pub value_bytes: usize,
    pub timestamp_scheme: TimestampScheme,
    pub value_scheme: ValueScheme,
    /// Values the chosen ALP exponent and factor do not reproduce; all of
    /// them when ALP was not tried.
    pub alp_exceptions: usize,
    /// Exceptions stored by `value_scheme`.
    pub exceptions: usize,
}

impl Layout {
    pub fn bytes(&self) -> usize {
        self.header_bytes + self.timestamp_bytes + self.value_bytes
    }
}

/// Appends one chunk for `timestamps` / `values` to `out`.
///
/// # Panics
///
/// If the columns differ in length or hold no samples or more than
/// [`MAX_CHUNK_SAMPLES`].
pub fn encode_chunk(
    timestamps: &[i64],
    values: &[f64],
    options: Options,
    out: &mut Vec<u8>,
) -> Layout {
    assert_eq!(timestamps.len(), values.len(), "column lengths differ");
    let n = timestamps.len();
    assert!(
        (1..=MAX_CHUNK_SAMPLES).contains(&n),
        "chunks hold 1..={MAX_CHUNK_SAMPLES} samples"
    );
    let start = out.len();
    let first = timestamps[0];
    put_varint(out, n as u64);
    put_ivarint(out, first);
    put_varint(out, timestamps[n - 1].wrapping_sub(first) as u64);
    let len_at = out.len();
    out.reserve(BODY_LEN_BYTES + 8 + 3 * n);
    out.extend_from_slice(&[0; BODY_LEN_BYTES]);
    let body_start = out.len();
    out.push(0);
    let plan = match options.timestamps {
        Some(scheme) => TimestampPlan::new(timestamps, scheme),
        None if n <= 2 => TimestampPlan::new(timestamps, TimestampScheme::Varint),
        None => TimestampScheme::ALL
            .map(|scheme| TimestampPlan::new(timestamps, scheme))
            .into_iter()
            .min_by_key(|plan| plan.encoded_len(timestamps))
            .expect("non-empty"),
    };
    plan.write(timestamps, out);
    let timestamp_bytes = out.len() - body_start - 1;
    let stats = encode_values(values, options.values, out);
    out[body_start] = plan.scheme as u8 | (stats.scheme as u8) << 4;
    let body_len = out.len() - body_start;

    let (len, len_bytes) = varint_array(body_len as u64);
    assert!(
        len_bytes <= BODY_LEN_BYTES,
        "chunk body of {body_len} bytes"
    );
    out[len_at..len_at + len_bytes].copy_from_slice(&len[..len_bytes]);
    out.copy_within(body_start.., len_at + len_bytes);
    out.truncate(out.len() - (BODY_LEN_BYTES - len_bytes));
    Layout {
        samples: n,
        header_bytes: len_at + len_bytes - start + 1,
        timestamp_bytes,
        value_bytes: body_len - 1 - timestamp_bytes,
        timestamp_scheme: plan.scheme,
        value_scheme: stats.scheme,
        alp_exceptions: stats.alp_exceptions,
        exceptions: stats.exceptions,
    }
}

/// Appends a section for sorted, deduplicated samples: balanced chunks of
/// at most [`MAX_CHUNK_SAMPLES`].
pub(crate) fn encode_section(timestamps: &[i64], values: &[f64], out: &mut Vec<u8>) {
    let n = timestamps.len();
    if n == 0 {
        return;
    }
    let size = n.div_ceil(n.div_ceil(MAX_CHUNK_SAMPLES));
    for (ts, vs) in timestamps.chunks(size).zip(values.chunks(size)) {
        encode_chunk(ts, vs, Options::default(), out);
    }
}

/// Appends every sample of the section `bytes` to the columns; on error
/// the columns are left as they were.
pub fn decode(
    bytes: &[u8],
    timestamps: &mut Vec<i64>,
    values: &mut Vec<f64>,
) -> Result<(), EncodingError> {
    decode_section(bytes, None, timestamps, values)
}

/// Appends the samples with `start_ms < timestamp <= end_ms`. Chunks
/// outside the range are skipped by their headers; within a grid chunk
/// only the blocks overlapping the range are unpacked.
pub fn decode_range(
    bytes: &[u8],
    start_ms: i64,
    end_ms: i64,
    timestamps: &mut Vec<i64>,
    values: &mut Vec<f64>,
) -> Result<(), EncodingError> {
    decode_section(bytes, Some((start_ms, end_ms)), timestamps, values)
}

fn decode_section(
    bytes: &[u8],
    range: Option<(i64, i64)>,
    timestamps: &mut Vec<i64>,
    values: &mut Vec<f64>,
) -> Result<(), EncodingError> {
    let (ts_start, vs_start) = (timestamps.len(), values.len());
    let result = Chunks(bytes).try_for_each(|chunk| {
        let chunk = chunk?;
        match range {
            Some((start_ms, end_ms)) if chunk.last <= start_ms || chunk.first > end_ms => Ok(()),
            Some((start_ms, end_ms)) if chunk.first > start_ms && chunk.last <= end_ms => {
                chunk.decode(None, timestamps, values)
            }
            _ => chunk.decode(range, timestamps, values),
        }
    });
    if result.is_err() {
        timestamps.truncate(ts_start);
        values.truncate(vs_start);
    }
    result
}

/// One chunk of a section, bounded by its header.
#[derive(Clone, Copy)]
struct Chunk<'a> {
    n: usize,
    first: i64,
    last: i64,
    /// The whole chunk, header included.
    bytes: &'a [u8],
    body: &'a [u8],
}

/// The chunks of a section; stops after the first error.
struct Chunks<'a>(&'a [u8]);

impl<'a> Iterator for Chunks<'a> {
    type Item = Result<Chunk<'a>, EncodingError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        let result = Chunk::read(self.0);
        self.0 = match &result {
            Ok(chunk) => &self.0[chunk.bytes.len()..],
            Err(_) => &[],
        };
        Some(result)
    }
}

impl<'a> Chunk<'a> {
    fn read(bytes: &'a [u8]) -> Result<Self, EncodingError> {
        let mut cursor = Cursor(bytes);
        let n = usize::try_from(cursor.varint()?).unwrap_or(usize::MAX);
        if !(1..=MAX_CHUNK_SAMPLES).contains(&n) {
            return Err(corrupt("sample count"));
        }
        let first = cursor.ivarint()?;
        let last = first.wrapping_add(cursor.varint()? as i64);
        if n == 1 && last != first {
            return Err(corrupt("span"));
        }
        let len = usize::try_from(cursor.varint()?).unwrap_or(usize::MAX);
        let body = cursor.take(len)?;
        Ok(Self {
            n,
            first,
            last,
            bytes: &bytes[..bytes.len() - cursor.0.len()],
            body,
        })
    }

    /// Appends the samples with `start_ms < timestamp <= end_ms`, or all
    /// of them without `range`.
    fn decode(
        &self,
        range: Option<(i64, i64)>,
        timestamps: &mut Vec<i64>,
        values: &mut Vec<f64>,
    ) -> Result<(), EncodingError> {
        let mut cursor = Cursor(self.body);
        let layout = cursor.u8()?;
        let ts_scheme =
            TimestampScheme::from_tag(layout & 0xf).ok_or_else(|| corrupt("timestamp scheme"))?;
        let value_scheme =
            ValueScheme::from_tag(layout >> 4).ok_or_else(|| corrupt("value scheme"))?;
        if self.n == 1 && ts_scheme == TimestampScheme::Varint {
            if range.is_some_and(|(start_ms, end_ms)| self.first <= start_ms || self.first > end_ms)
            {
                return Ok(());
            }
            let mut value = [0.0];
            decode_values(&mut cursor, value_scheme, 1, 0..1, &mut value)?;
            timestamps.push(self.first);
            values.push(value[0]);
            return Ok(());
        }
        let base = timestamps.len();
        let decoded = decode_timestamps(&mut cursor, ts_scheme, self, range, timestamps)?;
        let wanted = match range {
            None => decoded,
            Some((start_ms, end_ms)) => {
                let slice = &timestamps[base..];
                let lo = slice.partition_point(|&ts| ts <= start_ms);
                let hi = slice.partition_point(|&ts| ts <= end_ms).max(lo);
                timestamps.copy_within(base + lo..base + hi, base);
                timestamps.truncate(base + hi - lo);
                decoded.start + lo..decoded.start + hi
            }
        };
        let base = values.len();
        values.resize(base + wanted.len(), 0.0);
        decode_values(
            &mut cursor,
            value_scheme,
            self.n,
            wanted,
            &mut values[base..],
        )
    }
}

struct TimestampPlan {
    scheme: TimestampScheme,
    /// The grid interval, or the first delta for delta-of-delta.
    param: i64,
    frame: Frame,
}

impl TimestampPlan {
    fn new(ts: &[i64], scheme: TimestampScheme) -> Self {
        let n = ts.len();
        let (param, frame) = match scheme {
            TimestampScheme::Varint => (0, Frame::raw(0)),
            TimestampScheme::Grid => {
                let interval = grid_interval(ts);
                let deviations = (0..n).map(|i| grid_deviation(ts, interval, i));
                (interval, Frame::of(deviations))
            }
            TimestampScheme::Delta => (0, Frame::of((1..n).map(|i| delta(ts, i)))),
            TimestampScheme::DeltaOfDelta => {
                let first_delta = if n > 1 { delta(ts, 1) } else { 0 };
                (
                    first_delta,
                    Frame::of((2..n).map(|i| delta_of_delta(ts, i))),
                )
            }
        };
        Self {
            scheme,
            param,
            frame,
        }
    }

    /// Zero for the leading samples the scheme's parameters already fix.
    fn lane(&self, ts: &[i64], i: usize) -> u64 {
        let value = match self.scheme {
            TimestampScheme::Varint => return 0,
            TimestampScheme::Grid => grid_deviation(ts, self.param, i),
            TimestampScheme::Delta if i == 0 => return 0,
            TimestampScheme::Delta => delta(ts, i),
            TimestampScheme::DeltaOfDelta if i < 2 => return 0,
            TimestampScheme::DeltaOfDelta => delta_of_delta(ts, i),
        };
        self.frame.lane(value)
    }

    fn has_param(&self) -> bool {
        matches!(
            self.scheme,
            TimestampScheme::Grid | TimestampScheme::DeltaOfDelta
        )
    }

    fn encoded_len(&self, ts: &[i64]) -> usize {
        if self.scheme == TimestampScheme::Varint {
            return (1..ts.len().saturating_sub(1))
                .map(|i| varint_len(delta(ts, i) as u64))
                .sum();
        }
        let param = if self.has_param() {
            ivarint_len(self.param)
        } else {
            0
        };
        param + self.frame.encoded_len(ts.len())
    }

    fn write(&self, ts: &[i64], out: &mut Vec<u8>) {
        if self.scheme == TimestampScheme::Varint {
            for i in 1..ts.len().saturating_sub(1) {
                put_varint(out, delta(ts, i) as u64);
            }
            return;
        }
        if self.has_param() {
            put_ivarint(out, self.param);
        }
        self.frame.write(ts.len(), |i| self.lane(ts, i), out);
    }
}

/// Frame of reference: lanes are `(value - min) >> shift`, `shift` the
/// trailing zero bits every `value - min` shares (page-sized byte counts,
/// second-aligned timestamps).
#[derive(Debug, Clone, Copy)]
struct Frame {
    min: i64,
    shift: u32,
    width: u32,
}

/// Set in the width byte when a shift byte follows.
const SHIFT_FLAG: u8 = 0x80;

impl Frame {
    fn of(values: impl Iterator<Item = i64> + Clone) -> Self {
        let (min, max) = min_max(values.clone());
        let common = values.fold(0u64, |acc, v| acc | v.wrapping_sub(min) as u64);
        let shift = if common == 0 {
            0
        } else {
            common.trailing_zeros()
        };
        Self {
            min,
            shift,
            width: width_of((max.wrapping_sub(min) as u64) >> shift),
        }
    }

    fn raw(width: u32) -> Self {
        Self {
            min: 0,
            shift: 0,
            width,
        }
    }

    fn lane(&self, value: i64) -> u64 {
        (value.wrapping_sub(self.min) as u64) >> self.shift
    }

    #[inline(always)]
    fn value(&self, lane: u64) -> i64 {
        self.min.wrapping_add((lane << self.shift) as i64)
    }

    fn encoded_len(&self, n: usize) -> usize {
        ivarint_len(self.min) + 1 + usize::from(self.shift > 0) + packed_len(n, self.width)
    }

    fn write(&self, len: usize, lane: impl Fn(usize) -> u64, out: &mut Vec<u8>) {
        put_ivarint(out, self.min);
        if self.shift > 0 {
            out.extend_from_slice(&[self.width as u8 | SHIFT_FLAG, self.shift as u8]);
        } else {
            out.push(self.width as u8);
        }
        pack_with(len, self.width, lane, out);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, EncodingError> {
        let min = cursor.ivarint()?;
        let byte = cursor.u8()?;
        let width = u32::from(byte & !SHIFT_FLAG);
        let shift = if byte & SHIFT_FLAG == 0 {
            0
        } else {
            u32::from(cursor.u8()?)
        };
        if width > 64 || shift > 63 {
            return Err(corrupt("frame"));
        }
        Ok(Self { min, shift, width })
    }
}

/// The slope from the first to the last timestamp, rounded.
fn grid_interval(ts: &[i64]) -> i64 {
    let (Some(&first), Some(&last)) = (ts.first(), ts.last()) else {
        return 0;
    };
    if ts.len() < 2 {
        return 0;
    }
    let span = i128::from(last) - i128::from(first);
    let steps = ts.len() as i128 - 1;
    (2 * span + steps).div_euclid(2 * steps) as i64
}

fn grid_deviation(ts: &[i64], interval: i64, i: usize) -> i64 {
    ts[i].wrapping_sub(ts[0].wrapping_add((i as i64).wrapping_mul(interval)))
}

fn delta(ts: &[i64], i: usize) -> i64 {
    ts[i].wrapping_sub(ts[i - 1])
}

fn delta_of_delta(ts: &[i64], i: usize) -> i64 {
    delta(ts, i).wrapping_sub(delta(ts, i - 1))
}

/// Appends the timestamps of the blocks that can hold samples in `range`
/// (every block without one), returning their sample indices.
fn decode_timestamps(
    cursor: &mut Cursor<'_>,
    scheme: TimestampScheme,
    chunk: &Chunk<'_>,
    range: Option<(i64, i64)>,
    out: &mut Vec<i64>,
) -> Result<Range<usize>, EncodingError> {
    let (n, first) = (chunk.n, chunk.first);
    let base = out.len();
    if scheme == TimestampScheme::Varint {
        out.reserve(n);
        out.push(first);
        let mut ts = first;
        for _ in 1..n.saturating_sub(1) {
            ts = ts.wrapping_add(cursor.varint()? as i64);
            out.push(ts);
        }
        if n > 1 {
            out.push(chunk.last);
        }
        return Ok(0..n);
    }
    let param = if scheme == TimestampScheme::Delta {
        0
    } else {
        cursor.ivarint()?
    };
    let frame = Frame::read(cursor)?;
    let packed = Packed::read(cursor, n, frame)?;
    let blocks = n.div_ceil(BLOCK);
    let mut lanes = [0u64; BLOCK];
    match scheme {
        TimestampScheme::Grid => {
            let wanted = match range {
                None => 0..blocks,
                Some((start_ms, end_ms)) => {
                    let block_start = |block: usize| {
                        let i = block * BLOCK;
                        first
                            .wrapping_add((i as i64).wrapping_mul(param))
                            .wrapping_add(frame.value(packed.lane(i)))
                    };
                    let lo = partition_blocks(blocks, |b| block_start(b) <= start_ms);
                    let hi = partition_blocks(blocks, |b| block_start(b) <= end_ms);
                    lo.saturating_sub(1)..hi.max(lo.saturating_sub(1))
                }
            };
            let indices = wanted.start * BLOCK..(wanted.end * BLOCK).min(n);
            out.resize(base + indices.len(), 0);
            let mut ramp = [0i64; BLOCK];
            for (j, step) in ramp.iter_mut().enumerate() {
                *step = (j as i64).wrapping_mul(param);
            }
            let block_step = (BLOCK as i64).wrapping_mul(param);
            let mut start = first.wrapping_add((indices.start as i64).wrapping_mul(param));
            for (block, chunk) in wanted.zip(out[base..].chunks_mut(BLOCK)) {
                packed.block(block, &mut lanes);
                for ((ts, &lane), &step) in chunk.iter_mut().zip(&lanes).zip(&ramp) {
                    *ts = start.wrapping_add(step).wrapping_add(frame.value(lane));
                }
                start = start.wrapping_add(block_step);
            }
            Ok(indices)
        }
        TimestampScheme::Delta => {
            out.resize(base + n, 0);
            let mut acc = first.wrapping_sub(frame.min);
            for (block, chunk) in out[base..].chunks_mut(BLOCK).enumerate() {
                packed.block(block, &mut lanes);
                for (ts, &lane) in chunk.iter_mut().zip(&lanes) {
                    acc = acc.wrapping_add(frame.value(lane));
                    *ts = acc;
                }
            }
            Ok(0..n)
        }
        _ => {
            out.resize(base + n, 0);
            let first_delta = param.wrapping_sub(frame.min);
            let mut delta = first_delta.wrapping_sub(frame.min);
            let mut acc = first.wrapping_sub(first_delta);
            for (block, chunk) in out[base..].chunks_mut(BLOCK).enumerate() {
                packed.block(block, &mut lanes);
                for (ts, &lane) in chunk.iter_mut().zip(&lanes) {
                    delta = delta.wrapping_add(frame.value(lane));
                    acc = acc.wrapping_add(delta);
                    *ts = acc;
                }
            }
            Ok(0..n)
        }
    }
}

/// The number of leading blocks `0..blocks` for which `pred` holds, `pred`
/// holding for a prefix.
fn partition_blocks(blocks: usize, pred: impl Fn(usize) -> bool) -> usize {
    let (mut lo, mut hi) = (0, blocks);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if pred(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

const F10: [f64; 19] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18,
];
const IF10: [f64; 19] = [
    1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14,
    1e-15, 1e-16, 1e-17, 1e-18,
];
const MAX_EXPONENT: usize = F10.len() - 1;
/// ALP ints stay strictly inside `±2^51`, where [`magic_int_to_f64`] is
/// exact.
const ALP_BOUND: i64 = 1 << 51;
/// `2^52 + 2^51`: adding it to an `|x| < 2^51` rounds `x` to an integer
/// (ties to even) in the mantissa.
const MAGIC: f64 = 6_755_399_441_055_744.0;

/// Exact for `-2^51 <= d < 2^51`, and built from integer add and float
/// subtract only, so it vectorizes where packed `i64 -> f64` conversion
/// does not exist (x86 below AVX-512).
#[inline(always)]
pub(crate) fn magic_int_to_f64(d: i64) -> f64 {
    f64::from_bits(MAGIC.to_bits().wrapping_add(d as u64)) - MAGIC
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn int_to_f64(d: i64) -> f64 {
    magic_int_to_f64(d)
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn int_to_f64(d: i64) -> f64 {
    d as f64
}

/// `d * 10^f / 10^e`: correctly rounded, so every decimal of up to 15
/// significant digits round-trips (the paper's `* 10^-e` misses some by
/// an ulp).
#[inline(always)]
fn alp_decode(d: i64, factor: f64, exponent: f64) -> f64 {
    int_to_f64(d) * factor / exponent
}

/// `None` unless decoding the int reproduces `value`'s bits.
#[inline(always)]
fn alp_encode(value: f64, e: usize, f: usize) -> Option<i64> {
    let scaled = value * F10[e] * IF10[f];
    if scaled.is_nan() || scaled.abs() >= ALP_BOUND as f64 {
        return None;
    }
    let d = ((scaled + MAGIC) - MAGIC) as i64;
    if d.unsigned_abs() >= ALP_BOUND as u64 {
        return None;
    }
    (alp_decode(d, F10[f], F10[e]).to_bits() == value.to_bits()).then_some(d)
}

/// Whether some exponent makes `value` an ALP int.
fn is_decimal(value: f64) -> bool {
    (0..=MAX_EXPONENT).any(|e| alp_encode(value, e, 0).is_some())
}

/// Evenly spaced picks of at most [`SAMPLE`] items.
fn sample<T: Copy + Default>(items: &[T]) -> ([T; SAMPLE], usize) {
    let mut out = [T::default(); SAMPLE];
    let m = items.len().min(SAMPLE);
    for (k, slot) in out[..m].iter_mut().enumerate() {
        *slot = items[k * items.len() / m];
    }
    (out, m)
}

/// The smallest exponent that encodes the whole sample (or the cheapest
/// if none does), then the factor that best trims trailing zeros. Cost is
/// exceptions at 80 bits each plus the frame-of-reference width.
fn alp_search(values: &[f64]) -> (usize, usize) {
    let (picks, m) = sample(values);
    let picks = &picks[..m];
    let cost = |e: usize, f: usize| {
        let mut exceptions = 0;
        let (mut min, mut max) = (i64::MAX, i64::MIN);
        for &value in picks {
            match alp_encode(value, e, f) {
                Some(d) => {
                    min = min.min(d);
                    max = max.max(d);
                }
                None => exceptions += 1,
            }
        }
        let width = if exceptions == m {
            0
        } else {
            width_of(max.wrapping_sub(min) as u64) as usize
        };
        (exceptions, exceptions * 80 + m * width)
    };
    let (mut best_e, mut best_f) = (0, 0);
    let (mut best_exceptions, mut best_cost) = (usize::MAX, usize::MAX);
    for e in 0..=MAX_EXPONENT {
        let (exceptions, c) = cost(e, 0);
        if c < best_cost {
            (best_e, best_exceptions, best_cost) = (e, exceptions, c);
        }
        if exceptions == 0 {
            break;
        }
    }
    for f in 1..=best_e {
        let (exceptions, c) = cost(best_e, f);
        if c < best_cost {
            (best_f, best_exceptions, best_cost) = (f, exceptions, c);
        } else if exceptions > best_exceptions {
            break;
        }
    }
    (best_e, best_f)
}

struct Alp {
    e: usize,
    f: usize,
    /// One per value; an exception holds the int before it (or the first
    /// encoded int) so it widens neither frame.
    ints: Vec<i64>,
    exceptions: Vec<u16>,
}

impl Alp {
    fn new(values: &[f64]) -> Self {
        let (e, f) = alp_search(values);
        let mut ints = Vec::with_capacity(values.len());
        let mut exceptions = Vec::new();
        let mut last = None;
        for (i, &value) in values.iter().enumerate() {
            match alp_encode(value, e, f) {
                Some(d) => {
                    ints.push(d);
                    last = Some(d);
                }
                None => {
                    exceptions.push(i as u16);
                    ints.push(last.unwrap_or(0));
                }
            }
        }
        let leading = exceptions
            .iter()
            .enumerate()
            .take_while(|&(k, &position)| k == usize::from(position))
            .count();
        if let Some(&fill) = ints.get(leading) {
            ints[..leading].fill(fill);
        }
        Self {
            e,
            f,
            ints,
            exceptions,
        }
    }

    /// `forced` is `AlpFor`, `AlpDelta` or `None` for the narrower.
    fn write(&self, values: &[f64], forced: Option<ValueScheme>, out: &mut Vec<u8>) -> ValueScheme {
        let ints = &self.ints;
        let n = ints.len();
        let direct = Frame::of(ints.iter().copied());
        let deltas = Frame::of(ints.windows(2).map(|w| w[1].wrapping_sub(w[0])));
        let scheme = forced.unwrap_or(
            if deltas.encoded_len(n) + ivarint_len(ints[0]) < direct.encoded_len(n) {
                ValueScheme::AlpDelta
            } else {
                ValueScheme::AlpFor
            },
        );
        out.extend_from_slice(&[self.e as u8, self.f as u8]);
        if scheme == ValueScheme::AlpDelta {
            put_ivarint(out, ints[0]);
            let lane = |i: usize| match i {
                0 => 0,
                _ => deltas.lane(ints[i].wrapping_sub(ints[i - 1])),
            };
            deltas.write(n, lane, out);
        } else {
            direct.write(n, |i| direct.lane(ints[i]), out);
        }
        put_varint(out, self.exceptions.len() as u64);
        for &position in &self.exceptions {
            out.extend_from_slice(&position.to_le_bytes());
        }
        for &position in &self.exceptions {
            out.extend_from_slice(&values[usize::from(position)].to_bits().to_le_bytes());
        }
        scheme
    }
}

struct ValueStats {
    scheme: ValueScheme,
    alp_exceptions: usize,
    exceptions: usize,
}

fn encode_values(values: &[f64], forced: Option<ValueScheme>, out: &mut Vec<u8>) -> ValueStats {
    let byte_xor = ValueStats {
        scheme: ValueScheme::ByteXor,
        alp_exceptions: 0,
        exceptions: 0,
    };
    match forced {
        Some(ValueScheme::ByteXor) => {
            write_byte_xor(values, out);
            byte_xor
        }
        None if values.len() == 1 || values.len() <= SMALL_VALUES && !is_decimal(values[0]) => {
            write_byte_xor(values, out);
            byte_xor
        }
        None if values.len() <= SMALL_VALUES => {
            let start = out.len();
            let stats = encode_columnar_values(values, None, out);
            if byte_xor_len(values) <= out.len() - start {
                out.truncate(start);
                write_byte_xor(values, out);
                return byte_xor;
            }
            stats
        }
        _ => encode_columnar_values(values, forced, out),
    }
}

fn encode_columnar_values(
    values: &[f64],
    forced: Option<ValueScheme>,
    out: &mut Vec<u8>,
) -> ValueStats {
    let first = values[0].to_bits();
    let constant = values.iter().all(|v| v.to_bits() == first);
    if forced.is_none() && !mostly_decimal(values) {
        if constant {
            out.extend_from_slice(&first.to_le_bytes());
            return ValueStats {
                scheme: ValueScheme::Constant,
                alp_exceptions: values.len(),
                exceptions: 0,
            };
        }
        return encode_fallback(values, None, out);
    }
    let alp = Alp::new(values);
    let alp_exceptions = alp.exceptions.len();
    let stats = |scheme, exceptions| ValueStats {
        scheme,
        alp_exceptions,
        exceptions,
    };
    match forced {
        Some(scheme @ (ValueScheme::AlpFor | ValueScheme::AlpDelta)) => {
            stats(alp.write(values, Some(scheme), out), alp_exceptions)
        }
        Some(ValueScheme::AlpRd) => stats(ValueScheme::AlpRd, write_rd(values, out)),
        Some(ValueScheme::Xor) => {
            write_xor(values, out);
            stats(ValueScheme::Xor, 0)
        }
        Some(ValueScheme::Constant) if constant => {
            out.extend_from_slice(&first.to_le_bytes());
            stats(ValueScheme::Constant, 0)
        }
        _ if alp_exceptions * 2 <= values.len() => {
            stats(alp.write(values, None, out), alp_exceptions)
        }
        _ if constant => {
            out.extend_from_slice(&first.to_le_bytes());
            stats(ValueScheme::Constant, 0)
        }
        _ => encode_fallback(values, Some(&alp), out),
    }
}

/// Whether at least two of four evenly spaced values are ALP ints at some
/// exponent; when not, ALP would miss most values and is not tried.
fn mostly_decimal(values: &[f64]) -> bool {
    let n = values.len();
    (0..4)
        .map(|k| values[k * n / 4])
        .filter(|&v| is_decimal(v))
        .nth(1)
        .is_some()
}

/// The smallest of ALP (when given), ALP-RD (past one block, below which
/// its dictionary rarely pays) and the XOR stream.
fn encode_fallback(values: &[f64], alp: Option<&Alp>, out: &mut Vec<u8>) -> ValueStats {
    let alp_exceptions = alp.map_or(values.len(), |alp| alp.exceptions.len());
    let stats = |scheme, exceptions| ValueStats {
        scheme,
        alp_exceptions,
        exceptions,
    };
    let mut candidates = Vec::with_capacity(3);
    if let Some(alp) = alp {
        let mut bytes = Vec::new();
        let scheme = alp.write(values, None, &mut bytes);
        candidates.push((bytes, stats(scheme, alp_exceptions)));
    }
    if values.len() > BLOCK {
        let mut bytes = Vec::new();
        let exceptions = write_rd(values, &mut bytes);
        candidates.push((bytes, stats(ValueScheme::AlpRd, exceptions)));
    }
    let mut bytes = Vec::new();
    write_xor(values, &mut bytes);
    candidates.push((bytes, stats(ValueScheme::Xor, 0)));
    let (bytes, chosen) = candidates
        .into_iter()
        .min_by_key(|(bytes, stats)| match stats.scheme {
            ValueScheme::Xor => bytes.len() * XOR_PENALTY_PERCENT / 100,
            _ => bytes.len(),
        })
        .expect("non-empty");
    out.extend_from_slice(&bytes);
    chosen
}

/// The XOR stream decodes a bit field at a time, several times slower than
/// the packed schemes, so it must be about a tenth smaller to be chosen.
const XOR_PENALTY_PERCENT: usize = 111;

/// Unchanged bits are the single byte `0x80`; otherwise a header byte of
/// leading (high nibble) and trailing (low nibble) zero bytes, then the
/// bytes between big-endian.
fn put_byte_xor(out: &mut Vec<u8>, xor: u64) {
    if xor == 0 {
        out.push(0x80);
        return;
    }
    let leading = xor.leading_zeros() / 8;
    let trailing = xor.trailing_zeros() / 8;
    out.push(((leading << 4) | trailing) as u8);
    let bytes = (xor >> (trailing * 8)).to_be_bytes();
    out.extend_from_slice(&bytes[(leading + trailing) as usize..]);
}

fn byte_xor_len(values: &[f64]) -> usize {
    8 + values
        .windows(2)
        .map(|w| match w[0].to_bits() ^ w[1].to_bits() {
            0 => 1,
            xor => 9 - (xor.leading_zeros() / 8 + xor.trailing_zeros() / 8) as usize,
        })
        .sum::<usize>()
}

fn write_byte_xor(values: &[f64], out: &mut Vec<u8>) {
    out.extend_from_slice(&values[0].to_bits().to_le_bytes());
    for w in values.windows(2) {
        put_byte_xor(out, w[0].to_bits() ^ w[1].to_bits());
    }
}

/// The cut leaving at most 16 left bits whose dictionary of eight covers
/// the sample most cheaply; cost is lanes plus 32 bits per exception.
fn rd_plan(values: &[f64]) -> (u32, Vec<u16>) {
    let (picks, m) = sample(values);
    let mut best = (usize::MAX, 0, Vec::new());
    for left_width in 1..=16u32 {
        let right = 64 - left_width;
        let mut lefts: Vec<u16> = picks[..m]
            .iter()
            .map(|v| (v.to_bits() >> right) as u16)
            .collect();
        lefts.sort_unstable();
        let mut runs: Vec<(usize, u16)> = lefts
            .chunk_by(|a, b| a == b)
            .map(|run| (run.len(), run[0]))
            .collect();
        runs.sort_by_key(|run| std::cmp::Reverse(run.0));
        runs.truncate(8);
        let covered: usize = runs.iter().map(|run| run.0).sum();
        let code_width = width_of(runs.len() as u64 - 1) as usize;
        let cost = m * (right as usize + code_width) + (m - covered) * 32;
        if cost < best.0 {
            best = (cost, right, runs.iter().map(|run| run.1).collect());
        }
    }
    (best.1, best.2)
}

/// Returns the exception count.
fn write_rd(values: &[f64], out: &mut Vec<u8>) -> usize {
    let (right, dict) = rd_plan(values);
    let code_width = width_of(dict.len() as u64 - 1);
    let right_mask = u64::MAX >> (64 - right);
    let mut codes = Vec::with_capacity(values.len());
    let mut exceptions = Vec::new();
    for (i, value) in values.iter().enumerate() {
        let left = (value.to_bits() >> right) as u16;
        match dict.iter().position(|&entry| entry == left) {
            Some(code) => codes.push(code as u64),
            None => {
                codes.push(0);
                exceptions.push((i as u16, left));
            }
        }
    }
    out.extend_from_slice(&[right as u8, dict.len() as u8]);
    for entry in &dict {
        out.extend_from_slice(&entry.to_le_bytes());
    }
    pack_with(values.len(), code_width, |i| codes[i], out);
    pack_with(
        values.len(),
        right,
        |i| values[i].to_bits() & right_mask,
        out,
    );
    put_varint(out, exceptions.len() as u64);
    for (position, _) in &exceptions {
        out.extend_from_slice(&position.to_le_bytes());
    }
    for (_, left) in &exceptions {
        out.extend_from_slice(&left.to_le_bytes());
    }
    exceptions.len()
}

/// The first value raw, then per value `0` (unchanged), `10` and the
/// XOR's bits in the previous window, or `11`, 5 bits of leading zeros,
/// 6 bits of length minus one and the bits.
fn write_xor(values: &[f64], out: &mut Vec<u8>) {
    let mut writer = BitWriter::new(out);
    let mut prev = values[0].to_bits();
    writer.write(prev, 64);
    let mut window: Option<(u32, u32)> = None;
    for value in &values[1..] {
        let bits = value.to_bits();
        let xor = bits ^ prev;
        prev = bits;
        if xor == 0 {
            writer.write(0, 1);
            continue;
        }
        let leading = xor.leading_zeros().min(31);
        let trailing = xor.trailing_zeros();
        match window {
            Some((lead, trail)) if leading >= lead && trailing >= trail => {
                writer.write(0b10, 2);
                writer.write(xor >> trail, 64 - lead - trail);
            }
            _ => {
                let significant = 64 - leading - trailing;
                writer.write(0b11, 2);
                writer.write(u64::from(leading), 5);
                writer.write(u64::from(significant - 1), 6);
                writer.write(xor >> trailing, significant);
                window = Some((leading, trailing));
            }
        }
    }
    writer.finish();
}

fn decode_values(
    cursor: &mut Cursor<'_>,
    scheme: ValueScheme,
    n: usize,
    wanted: Range<usize>,
    out: &mut [f64],
) -> Result<(), EncodingError> {
    if wanted.is_empty() {
        return Ok(());
    }
    match scheme {
        ValueScheme::ByteXor => decode_byte_xor(cursor, wanted, out),
        ValueScheme::AlpFor | ValueScheme::AlpDelta => decode_alp(cursor, scheme, n, wanted, out),
        ValueScheme::AlpRd => decode_rd(cursor, n, wanted, out),
        ValueScheme::Xor => decode_xor(cursor.0, wanted, out),
        ValueScheme::Constant => {
            out.fill(f64::from_bits(u64::from_le_bytes(cursor.array()?)));
            Ok(())
        }
    }
}

fn decode_byte_xor(
    cursor: &mut Cursor<'_>,
    wanted: Range<usize>,
    out: &mut [f64],
) -> Result<(), EncodingError> {
    let mut bits = u64::from_le_bytes(cursor.array()?);
    for i in 0..wanted.end {
        if i > 0 {
            let header = cursor.u8()?;
            if header != 0x80 {
                let (leading, trailing) = (usize::from(header >> 4), usize::from(header & 0xf));
                if leading + trailing >= 8 {
                    return Err(corrupt("byte XOR header"));
                }
                let mut word = [0u8; 8];
                word[leading..8 - trailing].copy_from_slice(cursor.take(8 - leading - trailing)?);
                bits ^= u64::from_be_bytes(word);
            }
        }
        if i >= wanted.start {
            out[i - wanted.start] = f64::from_bits(bits);
        }
    }
    Ok(())
}

fn decode_alp(
    cursor: &mut Cursor<'_>,
    scheme: ValueScheme,
    n: usize,
    wanted: Range<usize>,
    out: &mut [f64],
) -> Result<(), EncodingError> {
    let e = usize::from(cursor.u8()?);
    let f = usize::from(cursor.u8()?);
    if e > MAX_EXPONENT || f > MAX_EXPONENT {
        return Err(corrupt("ALP exponent"));
    }
    let (factor, exponent) = (F10[f], F10[e]);
    let mut lanes = [0u64; BLOCK];
    if scheme == ValueScheme::AlpDelta {
        let first = cursor.ivarint()?;
        let frame = Frame::read(cursor)?;
        let packed = Packed::read(cursor, n, frame)?;
        let mut ints = [0i64; BLOCK];
        let mut acc = first.wrapping_sub(frame.min);
        for block in 0..wanted.end.div_ceil(BLOCK) {
            packed.block(block, &mut lanes);
            let count = (n - block * BLOCK).min(BLOCK);
            for (int, &lane) in ints.iter_mut().zip(&lanes[..count]) {
                acc = acc.wrapping_add(frame.value(lane));
                *int = acc;
            }
            if let Some((in_block, at)) = overlap(block, &wanted) {
                let dst = &mut out[at..at + in_block.len()];
                for (value, &int) in dst.iter_mut().zip(&ints[in_block]) {
                    *value = alp_decode(int, factor, exponent);
                }
            }
        }
    } else {
        let frame = Frame::read(cursor)?;
        let packed = Packed::read(cursor, n, frame)?;
        for (block, in_block, at) in spans(&wanted) {
            packed.block(block, &mut lanes);
            let dst = &mut out[at..at + in_block.len()];
            for (value, &lane) in dst.iter_mut().zip(&lanes[in_block]) {
                *value = alp_decode(frame.value(lane), factor, exponent);
            }
        }
    }
    let exceptions = Exceptions::read(cursor, n, 8)?;
    for (position, raw) in exceptions.iter() {
        let position = position?;
        if wanted.contains(&position) {
            let raw = u64::from_le_bytes(raw.try_into().unwrap_or_default());
            out[position - wanted.start] = f64::from_bits(raw);
        }
    }
    Ok(())
}

fn decode_rd(
    cursor: &mut Cursor<'_>,
    n: usize,
    wanted: Range<usize>,
    out: &mut [f64],
) -> Result<(), EncodingError> {
    let right = u32::from(cursor.u8()?);
    let dict_len = usize::from(cursor.u8()?);
    if !(48..64).contains(&right) || !(1..=8).contains(&dict_len) {
        return Err(corrupt("ALP-RD header"));
    }
    let mut dict = [0u64; 8];
    for entry in &mut dict[..dict_len] {
        *entry = u64::from(u16::from_le_bytes(cursor.array()?)) << right;
    }
    let codes = Packed::read(cursor, n, Frame::raw(width_of(dict_len as u64 - 1)))?;
    let rights = Packed::read(cursor, n, Frame::raw(right))?;
    let (mut code_lanes, mut right_lanes) = ([0u64; BLOCK], [0u64; BLOCK]);
    for (block, in_block, at) in spans(&wanted) {
        codes.block(block, &mut code_lanes);
        rights.block(block, &mut right_lanes);
        let dst = &mut out[at..at + in_block.len()];
        let src = code_lanes[in_block.clone()]
            .iter()
            .zip(&right_lanes[in_block]);
        for (value, (&code, &low)) in dst.iter_mut().zip(src) {
            *value = f64::from_bits(dict[(code & 7) as usize] | low);
        }
    }
    let right_mask = u64::MAX >> (64 - right);
    let exceptions = Exceptions::read(cursor, n, 2)?;
    for (position, left) in exceptions.iter() {
        let position = position?;
        if wanted.contains(&position) {
            let left = u64::from(u16::from_le_bytes(left.try_into().unwrap_or_default()));
            let value = &mut out[position - wanted.start];
            *value = f64::from_bits((left << right) | (value.to_bits() & right_mask));
        }
    }
    Ok(())
}

fn decode_xor(bytes: &[u8], wanted: Range<usize>, out: &mut [f64]) -> Result<(), EncodingError> {
    let mut reader = BitReader::new(bytes);
    let mut bits = reader.read(64)?;
    let (mut leading, mut trailing) = (0u32, 0u32);
    for i in 0..wanted.end {
        if i > 0 && reader.read(1)? == 1 {
            if reader.read(1)? == 1 {
                leading = reader.read(5)? as u32;
                let significant = reader.read(6)? as u32 + 1;
                trailing = 64u32
                    .checked_sub(leading + significant)
                    .ok_or_else(|| corrupt("XOR window"))?;
            }
            bits ^= reader.read(64 - leading - trailing)? << trailing;
        }
        if i >= wanted.start {
            out[i - wanted.start] = f64::from_bits(bits);
        }
    }
    Ok(())
}

/// The lanes of `block` inside `wanted`, and where they land in the
/// output.
fn overlap(block: usize, wanted: &Range<usize>) -> Option<(Range<usize>, usize)> {
    let base = block * BLOCK;
    let start = wanted.start.max(base);
    let end = wanted.end.min(base + BLOCK);
    (start < end).then(|| (start - base..end - base, start - wanted.start))
}

fn spans(wanted: &Range<usize>) -> impl Iterator<Item = (usize, Range<usize>, usize)> + '_ {
    let blocks = wanted.start / BLOCK..wanted.end.div_ceil(BLOCK);
    blocks
        .filter_map(move |block| overlap(block, wanted).map(|(in_block, at)| (block, in_block, at)))
}

/// Positions (`u16` each), then fixed-size payloads.
struct Exceptions<'a> {
    positions: &'a [u8],
    payloads: &'a [u8],
    payload_len: usize,
    n: usize,
}

impl<'a> Exceptions<'a> {
    fn read(cursor: &mut Cursor<'a>, n: usize, payload_len: usize) -> Result<Self, EncodingError> {
        let count = usize::try_from(cursor.varint()?).unwrap_or(usize::MAX);
        if count > n {
            return Err(corrupt("exception count"));
        }
        Ok(Self {
            positions: cursor.take(2 * count)?,
            payloads: cursor.take(payload_len * count)?,
            payload_len,
            n,
        })
    }

    fn iter(&self) -> impl Iterator<Item = (Result<usize, EncodingError>, &'a [u8])> + '_ {
        let positions = self.positions.as_chunks::<2>().0.iter().map(|p| {
            let position = usize::from(u16::from_le_bytes(*p));
            if position < self.n {
                Ok(position)
            } else {
                Err(corrupt("exception position"))
            }
        });
        positions.zip(self.payloads.chunks_exact(self.payload_len))
    }
}

/// Merges sections, oldest first: at a timestamp held by several, the
/// newest wins. Disjoint chunks are copied as they are, runs of
/// [`COALESCE_RUN`] or more fresh operands re-encode into one chunk, and
/// overlapping sections (or more than [`MAX_PARTIAL_CHUNKS`] partly
/// filled chunks) re-encode into balanced chunks.
pub(crate) fn merge(sections: &[&[u8]], out: &mut Vec<u8>) -> Result<(), EncodingError> {
    let mut chunks = Vec::with_capacity(sections.len());
    for section in sections {
        for chunk in Chunks(section) {
            chunks.push(chunk?);
        }
    }
    let disjoint = |chunks: &[Chunk<'_>]| {
        chunks.iter().all(|c| c.first <= c.last)
            && chunks.windows(2).all(|w| w[0].last < w[1].first)
    };
    if !disjoint(&chunks) {
        chunks.sort_by_key(|c| c.first);
        if !disjoint(&chunks) {
            return merge_overlapping(sections, out);
        }
    }

    let mut runs: Vec<Range<usize>> = Vec::new();
    let mut partial = 0;
    let mut i = 0;
    while i < chunks.len() {
        let end = i + chunks[i..]
            .iter()
            .take_while(|c| c.n < SMALL_CHUNK)
            .count()
            .max(1);
        let n = if end - i >= COALESCE_RUN {
            runs.push(i..end);
            chunks[i..end].iter().map(|c| c.n).sum()
        } else {
            chunks[i].n
        };
        partial += usize::from((SMALL_CHUNK..FULL_CHUNK).contains(&n));
        i = if end - i >= COALESCE_RUN { end } else { i + 1 };
    }
    let rewritten = if partial > MAX_PARTIAL_CHUNKS {
        chunks.iter().map(|c| c.n).sum()
    } else {
        runs.iter()
            .map(|run| chunks[run.clone()].iter().map(|c| c.n).sum::<usize>())
            .max()
            .unwrap_or(0)
    };
    let (mut ts, mut vs) = (Vec::with_capacity(rewritten), Vec::with_capacity(rewritten));
    if partial > MAX_PARTIAL_CHUNKS {
        for chunk in &chunks {
            chunk.decode(None, &mut ts, &mut vs)?;
        }
        encode_section(&ts, &vs, out);
        return Ok(());
    }
    let mut runs = runs.into_iter().peekable();
    let mut i = 0;
    while i < chunks.len() {
        match runs.next_if(|run| run.start == i) {
            Some(run) => {
                ts.clear();
                vs.clear();
                for chunk in &chunks[run.clone()] {
                    chunk.decode(None, &mut ts, &mut vs)?;
                }
                encode_section(&ts, &vs, out);
                i = run.end;
            }
            None => {
                out.extend_from_slice(chunks[i].bytes);
                i += 1;
            }
        }
    }
    Ok(())
}

/// The k-way last-write-wins merge of fully decoded sections.
fn merge_overlapping(sections: &[&[u8]], out: &mut Vec<u8>) -> Result<(), EncodingError> {
    let mut columns = Vec::with_capacity(sections.len());
    for section in sections {
        let (mut ts, mut vs) = (Vec::new(), Vec::new());
        decode(section, &mut ts, &mut vs)?;
        if !ts.windows(2).all(|w| w[0] < w[1]) {
            let mut samples: Vec<(i64, f64)> = ts.into_iter().zip(vs).collect();
            samples.sort_by_key(|s| s.0);
            samples.dedup_by_key(|s| s.0);
            (ts, vs) = samples.into_iter().unzip();
        }
        columns.push((ts, vs));
    }
    // One head per section ordered by (timestamp, newest first), so the
    // first pop of a timestamp is its newest sample.
    let total = columns.iter().map(|(ts, _)| ts.len()).sum();
    let (mut ts, mut vs) = (Vec::with_capacity(total), Vec::with_capacity(total));
    let mut heap: BinaryHeap<Reverse<(i64, Reverse<usize>, usize)>> = columns
        .iter()
        .enumerate()
        .filter_map(|(source, (ts, _))| Some(Reverse((*ts.first()?, Reverse(source), 0))))
        .collect();
    while let Some(Reverse((timestamp, Reverse(source), at))) = heap.pop() {
        if ts.last() != Some(&timestamp) {
            ts.push(timestamp);
            vs.push(columns[source].1[at]);
        }
        if let Some(&next) = columns[source].0.get(at + 1) {
            heap.push(Reverse((next, Reverse(source), at + 1)));
        }
    }
    encode_section(&ts, &vs, out);
    Ok(())
}

fn width_of(range: u64) -> u32 {
    64 - range.leading_zeros()
}

fn packed_len(n: usize, width: u32) -> usize {
    (n * width as usize).div_ceil(8)
}

fn min_max(values: impl Iterator<Item = i64>) -> (i64, i64) {
    values
        .fold(None, |acc, v| match acc {
            None => Some((v, v)),
            Some((min, max)) => Some((v.min(min), v.max(max))),
        })
        .unwrap_or((0, 0))
}

/// `lane(i)` must fit in `width` bits.
fn pack_with(len: usize, width: u32, lane: impl Fn(usize) -> u64, out: &mut Vec<u8>) {
    if width == 0 {
        return;
    }
    let (mut acc, mut used) = (0u64, 0u32);
    for i in 0..len {
        let value = lane(i);
        debug_assert!(width == 64 || value >> width == 0);
        acc |= value << used;
        used += width;
        if used >= 64 {
            out.extend_from_slice(&acc.to_le_bytes());
            used -= 64;
            acc = if used == 0 {
                0
            } else {
                value >> (width - used)
            };
        }
    }
    if used > 0 {
        out.extend_from_slice(&acc.to_le_bytes()[..used.div_ceil(8) as usize]);
    }
}

/// A packed column. `bytes` runs on to the end of the chunk so a block can
/// load the word past its own end; bits from beyond the column only reach
/// lanes past its length.
struct Packed<'a> {
    bytes: &'a [u8],
    len: usize,
    frame: Frame,
}

/// Tail blocks this short are extracted lane by lane.
const SCALAR_TAIL: usize = 16;

impl<'a> Packed<'a> {
    fn read(cursor: &mut Cursor<'a>, len: usize, frame: Frame) -> Result<Self, EncodingError> {
        let bytes = cursor.0;
        cursor.take(packed_len(len, frame.width))?;
        Ok(Self { bytes, len, frame })
    }

    /// Lane `i` alone.
    #[inline]
    fn lane(&self, i: usize) -> u64 {
        let width = self.frame.width as usize;
        if width == 0 {
            return 0;
        }
        lane_at(self.bytes, i * width) & (u64::MAX >> (64 - width))
    }

    /// Lanes `64 * block..`; those past the column are unspecified.
    #[inline]
    fn block(&self, block: usize, lanes: &mut [u64; BLOCK]) {
        let width = self.frame.width as usize;
        if width == 0 {
            *lanes = [0; BLOCK];
            return;
        }
        let start = block * 8 * width;
        let count = self.len.saturating_sub(block * BLOCK);
        match self.bytes.get(start..start + 8 * width + 8) {
            Some(src) => unpack(self.frame.width, src, lanes),
            None if count <= SCALAR_TAIL => {
                let tail = self.bytes.get(start..).unwrap_or_default();
                let mask = u64::MAX >> (64 - width);
                for (i, lane) in lanes[..count].iter_mut().enumerate() {
                    *lane = lane_at(tail, i * width) & mask;
                }
            }
            None => {
                let mut padded = [0u8; 8 * (BLOCK + 1)];
                let tail = self.bytes.get(start..).unwrap_or_default();
                let len = tail.len().min(8 * width);
                padded[..len].copy_from_slice(&tail[..len]);
                unpack(self.frame.width, &padded, lanes);
            }
        }
    }
}

/// The 64 bits from bit `bit` on, zero past the end of `bytes`.
#[inline]
fn lane_at(bytes: &[u8], bit: usize) -> u64 {
    if let Some(window) = bytes.get(bit / 8..bit / 8 + 16) {
        let window: [u8; 16] = window.try_into().unwrap_or_default();
        return (u128::from_le_bytes(window) >> (bit % 8)) as u64;
    }
    let mut window = [0u8; 16];
    let tail = bytes.get(bit / 8..).unwrap_or_default();
    let len = tail.len().min(window.len());
    window[..len].copy_from_slice(&tail[..len]);
    (u128::from_le_bytes(window) >> (bit % 8)) as u64
}

macro_rules! unpack_lanes {
    ($word:ident, $lanes:ident, $width:ident, $mask:ident; $($i:literal)*) => {
        $({
            let bit = $i * $width;
            let (word, shift) = (bit / 64, bit % 64);
            $lanes[$i] =
                (($word(word) >> shift) | (($word(word + 1) << 1) << (63 - shift))) & $mask;
        })*
    };
}

/// Straight-line code per width: every load offset and shift is a
/// constant. `src` holds at least `8 * W + 8` bytes.
#[inline(never)]
fn unpack_width<const W: usize>(src: &[u8], lanes: &mut [u64; BLOCK]) {
    let src = &src[..8 * W + 8];
    let word = |k: usize| u64::from_le_bytes(src[8 * k..8 * k + 8].try_into().unwrap_or_default());
    let mask = u64::MAX >> (64 - W);
    unpack_lanes!(word, lanes, W, mask;
        0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
        32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60
        61 62 63);
}

fn unpack(width: u32, src: &[u8], lanes: &mut [u64; BLOCK]) {
    macro_rules! dispatch {
        ($($w:literal)*) => {
            match width {
                $($w => unpack_width::<$w>(src, lanes),)*
                _ => *lanes = [0; BLOCK],
            }
        };
    }
    dispatch!(
        1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32
        33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61
        62 63 64
    );
}

/// MSB-first.
struct BitWriter<'a> {
    out: &'a mut Vec<u8>,
    acc: u64,
    used: u32,
}

impl<'a> BitWriter<'a> {
    fn new(out: &'a mut Vec<u8>) -> Self {
        Self {
            out,
            acc: 0,
            used: 0,
        }
    }

    /// `bits` must fit in `n <= 64` bits.
    fn write(&mut self, bits: u64, n: u32) {
        if n == 0 {
            return;
        }
        let free = 64 - self.used;
        if n < free {
            self.acc |= bits << (free - n);
            self.used += n;
            return;
        }
        self.acc |= bits >> (n - free);
        self.out.extend_from_slice(&self.acc.to_be_bytes());
        self.used = n - free;
        self.acc = if self.used == 0 {
            0
        } else {
            bits << (64 - self.used)
        };
    }

    fn finish(self) {
        let len = self.used.div_ceil(8) as usize;
        self.out.extend_from_slice(&self.acc.to_be_bytes()[..len]);
    }
}

struct BitReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> BitReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    /// `n` in `1..=64`.
    fn read(&mut self, n: u32) -> Result<u64, EncodingError> {
        if self.pos + n as usize > self.bytes.len() * 8 {
            return Err(truncated());
        }
        let byte = self.pos / 8;
        let shift = (self.pos % 8) as u32;
        let mut word = [0u8; 8];
        let head = self.bytes.get(byte..).unwrap_or_default();
        let head = &head[..head.len().min(8)];
        word[..head.len()].copy_from_slice(head);
        let next = u64::from(self.bytes.get(byte + 8).copied().unwrap_or(0));
        let window = (u64::from_be_bytes(word) << shift) | (next >> (8 - shift));
        self.pos += n as usize;
        Ok(window >> (64 - n))
    }
}

struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], EncodingError> {
        if self.0.len() < len {
            return Err(truncated());
        }
        let (head, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], EncodingError> {
        Ok(self.take(N)?.try_into().unwrap_or([0; N]))
    }

    fn u8(&mut self) -> Result<u8, EncodingError> {
        Ok(self.array::<1>()?[0])
    }

    fn varint(&mut self) -> Result<u64, EncodingError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(corrupt("varint"))
    }

    fn ivarint(&mut self) -> Result<i64, EncodingError> {
        let zigzag = self.varint()?;
        Ok((zigzag >> 1) as i64 ^ -((zigzag & 1) as i64))
    }
}

fn varint_array(mut value: u64) -> ([u8; 10], usize) {
    let mut bytes = [0; 10];
    let mut len = 0;
    while value >= 0x80 {
        bytes[len] = value as u8 | 0x80;
        value >>= 7;
        len += 1;
    }
    bytes[len] = value as u8;
    (bytes, len + 1)
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn varint_len(value: u64) -> usize {
    (64 - (value | 1).leading_zeros() as usize).div_ceil(7)
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn put_ivarint(out: &mut Vec<u8>, value: i64) {
    put_varint(out, zigzag(value));
}

fn ivarint_len(value: i64) -> usize {
    varint_len(zigzag(value))
}

#[cold]
fn truncated() -> EncodingError {
    EncodingError {
        message: "series chunk truncated".to_string(),
    }
}

#[cold]
fn corrupt(what: &str) -> EncodingError {
    EncodingError {
        message: format!("series chunk has an invalid {what}"),
    }
}

#[cfg(test)]
mod tests;
