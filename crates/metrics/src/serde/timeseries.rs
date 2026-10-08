// TimeSeries value structure: columnar float chunks plus native histograms

use crate::histogram::{Bucket, CounterResetHint, FloatHistogram};
use crate::model::{HistogramSample, Sample, SeriesData};

use super::chunk;
use super::*;
use bytes::{BufMut, Bytes};
use std::cmp::Reverse;
use std::sync::Arc;

/// TimeSeries value: the float samples of a series ([`SeriesData`] without
/// histograms).
#[derive(Debug, Clone, PartialEq)]
pub struct TimeSeriesValue {
    pub points: Vec<Sample>,
}

impl TimeSeriesValue {
    /// Sorts by timestamp; of samples sharing one, the first is kept.
    pub fn encode(&self) -> Result<Bytes, EncodingError> {
        SeriesData::new(self.points.clone(), Vec::new()).encode()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        Ok(TimeSeriesValue {
            points: SeriesData::decode(buf)?.floats().collect(),
        })
    }
}

/// Leading byte of every non-empty value; values written in any other
/// format are rejected.
const FORMAT_VERSION: u8 = 1;
/// Set in the leading byte of a value that carries native histograms.
const HISTOGRAM_FLAG: u8 = 0x80;
const HISTOGRAM_VALUE_TAG: u8 = FORMAT_VERSION | HISTOGRAM_FLAG;

fn is_histogram_value(bytes: &[u8]) -> bool {
    bytes.first() == Some(&HISTOGRAM_VALUE_TAG)
}

/// The float section of a float-only value.
fn float_section(bytes: &[u8]) -> Result<&[u8], EncodingError> {
    match bytes.split_first() {
        Some((&FORMAT_VERSION, section)) => Ok(section),
        Some((&tag, _)) => Err(EncodingError {
            message: format!("unknown series value format {tag:#x}"),
        }),
        None => Ok(&[]),
    }
}

/// Storage encoding of a series value.
///
/// An empty value is empty bytes. Otherwise a format byte leads: float-only
/// values follow it with a float section ([`chunk`]). Values with
/// histograms set [`HISTOGRAM_FLAG`] in it and follow it with the
/// length-prefixed float section, then the histogram section: the sample
/// count, then per sample a delta-of-delta timestamp and the histogram
/// encoded against the previous one ([`encode_histogram`]). A value never
/// references data outside itself, so merge operands decode (and partially
/// merge) independently. Samples are sorted by timestamp and a timestamp
/// holds at most one sample: of floats sharing one the first is kept, and a
/// histogram wins over a float.
impl SeriesData {
    pub fn encode(mut self) -> Result<Bytes, EncodingError> {
        self.sort_floats();
        if !self.histograms.is_empty() {
            self.histograms.sort_by_key(|h| h.timestamp_ms);
            let histograms = std::mem::take(&mut self.histograms);
            self.retain_floats(|ts| {
                histograms
                    .binary_search_by_key(&ts, |h| h.timestamp_ms)
                    .is_err()
            });
            self.histograms = histograms;
        }
        if self.histograms.is_empty() {
            if self.timestamps.is_empty() {
                return Ok(Bytes::new());
            }
            let mut buf = Vec::with_capacity(24 + 3 * self.timestamps.len());
            buf.push(FORMAT_VERSION);
            chunk::encode_section(&self.timestamps, &self.values, &mut buf);
            return Ok(Bytes::from(buf));
        }
        let mut floats = Vec::with_capacity(3 * self.timestamps.len());
        chunk::encode_section(&self.timestamps, &self.values, &mut floats);

        let estimate: usize = self
            .histograms
            .iter()
            .map(|s| 16 + 2 * (s.histogram.positive.len() + s.histogram.negative.len()))
            .sum();
        let mut buf = Vec::with_capacity(floats.len() + 20 + estimate);
        buf.push(HISTOGRAM_VALUE_TAG);
        put_uvarint(&mut buf, floats.len() as u64);
        buf.extend_from_slice(&floats);
        put_uvarint(&mut buf, self.histograms.len() as u64);
        let mut prev_ts = 0i64;
        let mut prev_delta = 0i64;
        let mut prev: Option<&FloatHistogram> = None;
        for sample in &self.histograms {
            let delta = sample.timestamp_ms.wrapping_sub(prev_ts);
            put_varint(&mut buf, delta.wrapping_sub(prev_delta));
            prev_ts = sample.timestamp_ms;
            prev_delta = delta;
            encode_histogram(&mut buf, &sample.histogram, prev);
            prev = Some(&sample.histogram);
        }
        Ok(Bytes::from(buf))
    }

    /// Decodes the samples with `start_ms < timestamp <= end_ms`. Float
    /// chunks outside the range are skipped without decoding them.
    pub fn decode_range(buf: &[u8], start_ms: i64, end_ms: i64) -> Result<Self, EncodingError> {
        Self::decode_bounded(buf, Some((start_ms, end_ms)))
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        Self::decode_bounded(buf, None)
    }

    fn decode_bounded(buf: &[u8], range: Option<(i64, i64)>) -> Result<Self, EncodingError> {
        let mut data = Self::default();
        if !is_histogram_value(buf) {
            data.decode_floats(float_section(buf)?, range)?;
            return Ok(data);
        }
        let mut cursor = &buf[1..];
        let float_len = get_uvarint(&mut cursor)? as usize;
        data.decode_floats(take(&mut cursor, float_len)?, range)?;
        let count = get_uvarint(&mut cursor)? as usize;
        data.histograms = decode_histograms(&mut cursor, count, range)?;
        Ok(data)
    }

    fn decode_floats(
        &mut self,
        section: &[u8],
        range: Option<(i64, i64)>,
    ) -> Result<(), EncodingError> {
        match range {
            Some((start_ms, end_ms)) => chunk::decode_range(
                section,
                start_ms,
                end_ms,
                &mut self.timestamps,
                &mut self.values,
            ),
            None => chunk::decode(section, &mut self.timestamps, &mut self.values),
        }
    }

    /// Sorts the floats by timestamp; of floats sharing one, the first is
    /// kept.
    fn sort_floats(&mut self) {
        if self.timestamps.is_sorted_by(|a, b| a < b) {
            return;
        }
        let mut order = (0..self.timestamps.len()).collect::<Vec<_>>();
        order.sort_by_key(|&index| self.timestamps[index]);
        order.dedup_by_key(|index| self.timestamps[*index]);
        self.timestamps = order.iter().map(|&index| self.timestamps[index]).collect();
        self.values = order.iter().map(|&index| self.values[index]).collect();
    }
}

/// Samples are decoded in order since each depends on the one before;
/// those before the range are decoded but only kept as the next base.
fn decode_histograms(
    cursor: &mut &[u8],
    count: usize,
    range: Option<(i64, i64)>,
) -> Result<Vec<HistogramSample>, EncodingError> {
    let mut histograms: Vec<HistogramSample> = Vec::new();
    if range.is_none() {
        histograms.reserve(count.min(cursor.len()));
    }
    let mut skipped: Option<FloatHistogram> = None;
    let mut ts = 0i64;
    let mut delta = 0i64;
    for _ in 0..count {
        delta = delta.wrapping_add(get_varint(cursor)?);
        ts = ts.wrapping_add(delta);
        if range.is_some_and(|(_, end_ms)| ts > end_ms) {
            break;
        }
        let prev = match &skipped {
            Some(h) => Some(h),
            None => histograms.last().map(|s| &s.histogram),
        };
        let histogram = decode_histogram(cursor, prev)?;
        if range.is_none_or(|(start_ms, _)| ts > start_ms) {
            skipped = None;
            histograms.push(HistogramSample {
                timestamp_ms: ts,
                histogram,
            });
        } else {
            skipped = Some(histogram);
        }
    }
    Ok(histograms)
}

const HINT_MASK: u8 = 0b11;
const FLAG_LAYOUT: u8 = 1 << 2;
const FLAG_INTEGER: u8 = 1 << 3;
const FLAG_SAME_CUSTOM: u8 = 1 << 4;
const KNOWN_FLAGS: u8 = HINT_MASK | FLAG_LAYOUT | FLAG_INTEGER | FLAG_SAME_CUSTOM;

/// Encodes `h` against `prev`, the value's previous histogram.
///
/// A flags byte carries the counter reset hint and whether the bucket
/// layout (schema, zero threshold, custom bounds, bucket indices) is
/// restated; when it is not, the layout is `prev`'s. Restated custom bounds
/// equal to `prev`'s are flagged rather than written. Then `zero_count`,
/// `count`, `sum` and the bucket counts follow, each relative to `prev`'s
/// value for the same field or bucket index (zero when absent): zigzag
/// varint deltas when every count is an exact integer, XOR of the f64 bits
/// ([`put_xor`]) otherwise. `sum` is always XOR-encoded.
fn encode_histogram(buf: &mut Vec<u8>, h: &FloatHistogram, prev: Option<&FloatHistogram>) {
    let integer = [h.zero_count, h.count]
        .into_iter()
        .chain(h.positive.iter().chain(&h.negative).map(|b| b.count))
        .all(|count| as_integer(count).is_some());
    let restate = !prev.is_some_and(|p| same_layout(p, h));
    let same_custom = restate
        && h.uses_custom_buckets()
        && prev.is_some_and(|p| {
            p.uses_custom_buckets() && same_bits(&p.custom_values, &h.custom_values)
        });

    let mut flags = match h.counter_reset_hint {
        CounterResetHint::Unknown => 0,
        CounterResetHint::CounterReset => 1,
        CounterResetHint::NotCounterReset => 2,
        CounterResetHint::Gauge => 3,
    };
    if restate {
        flags |= FLAG_LAYOUT;
    }
    if integer {
        flags |= FLAG_INTEGER;
    }
    if same_custom {
        flags |= FLAG_SAME_CUSTOM;
    }
    buf.push(flags);

    if restate {
        put_varint(buf, i64::from(h.schema));
        buf.put_f64_le(h.zero_threshold);
        if h.uses_custom_buckets() && !same_custom {
            put_uvarint(buf, h.custom_values.len() as u64);
            for &bound in h.custom_values.iter() {
                buf.put_f64_le(bound);
            }
        }
        encode_indices(buf, &h.positive);
        encode_indices(buf, &h.negative);
    }

    let empty;
    let base = match prev {
        Some(p) => p,
        None => {
            empty = FloatHistogram::default();
            &empty
        }
    };
    put_count(buf, integer, h.zero_count, base.zero_count);
    put_count(buf, integer, h.count, base.count);
    put_xor(buf, h.sum.to_bits() ^ base.sum.to_bits());
    let mut positive_base = BaseCounts::new(&base.positive);
    for bucket in &h.positive {
        put_count(buf, integer, bucket.count, positive_base.get(bucket.index));
    }
    let mut negative_base = BaseCounts::new(&base.negative);
    for bucket in &h.negative {
        put_count(buf, integer, bucket.count, negative_base.get(bucket.index));
    }
}

fn decode_histogram(
    cursor: &mut &[u8],
    prev: Option<&FloatHistogram>,
) -> Result<FloatHistogram, EncodingError> {
    let flags = get_u8(cursor)?;
    if flags & !KNOWN_FLAGS != 0 {
        return Err(EncodingError {
            message: format!("invalid histogram flags {flags:#x}"),
        });
    }
    let counter_reset_hint = match flags & HINT_MASK {
        0 => CounterResetHint::Unknown,
        1 => CounterResetHint::CounterReset,
        2 => CounterResetHint::NotCounterReset,
        _ => CounterResetHint::Gauge,
    };
    let integer = flags & FLAG_INTEGER != 0;

    let mut h = if flags & FLAG_LAYOUT != 0 {
        let schema = i32::try_from(get_varint(cursor)?).map_err(|_| EncodingError {
            message: "histogram schema out of range".to_string(),
        })?;
        let mut h = FloatHistogram {
            schema,
            zero_threshold: get_f64(cursor)?,
            ..FloatHistogram::default()
        };
        if h.uses_custom_buckets() {
            h.custom_values = if flags & FLAG_SAME_CUSTOM != 0 {
                prev.filter(|p| p.uses_custom_buckets())
                    .map(|p| p.custom_values.clone())
                    .ok_or_else(|| EncodingError {
                        message: "histogram references missing custom bounds".to_string(),
                    })?
            } else {
                let len = get_uvarint(cursor)? as usize;
                (0..len)
                    .map(|_| get_f64(cursor))
                    .collect::<Result<Arc<[f64]>, _>>()?
            };
        }
        h.positive = decode_indices(cursor)?;
        h.negative = decode_indices(cursor)?;
        h
    } else {
        let p = prev.ok_or_else(|| EncodingError {
            message: "histogram references missing bucket layout".to_string(),
        })?;
        FloatHistogram {
            schema: p.schema,
            zero_threshold: p.zero_threshold,
            custom_values: p.custom_values.clone(),
            positive: p.positive.clone(),
            negative: p.negative.clone(),
            ..FloatHistogram::default()
        }
    };
    h.counter_reset_hint = counter_reset_hint;

    let empty;
    let base = match prev {
        Some(p) => p,
        None => {
            empty = FloatHistogram::default();
            &empty
        }
    };
    h.zero_count = get_count(cursor, integer, base.zero_count)?;
    h.count = get_count(cursor, integer, base.count)?;
    h.sum = f64::from_bits(get_xor(cursor)? ^ base.sum.to_bits());
    let mut positive_base = BaseCounts::new(&base.positive);
    for bucket in &mut h.positive {
        bucket.count = get_count(cursor, integer, positive_base.get(bucket.index))?;
    }
    let mut negative_base = BaseCounts::new(&base.negative);
    for bucket in &mut h.negative {
        bucket.count = get_count(cursor, integer, negative_base.get(bucket.index))?;
    }
    Ok(h)
}

fn same_layout(a: &FloatHistogram, b: &FloatHistogram) -> bool {
    let same_indices = |x: &[Bucket], y: &[Bucket]| {
        x.len() == y.len() && x.iter().zip(y).all(|(x, y)| x.index == y.index)
    };
    a.schema == b.schema
        && a.zero_threshold.to_bits() == b.zero_threshold.to_bits()
        && (Arc::ptr_eq(&a.custom_values, &b.custom_values)
            || same_bits(&a.custom_values, &b.custom_values))
        && same_indices(&a.positive, &b.positive)
        && same_indices(&a.negative, &b.negative)
}

/// Bitwise equality, so reused bounds decode bit-identical (`-0.0`, NaN).
fn same_bits(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.to_bits() == b.to_bits())
}

/// Resolves the previous sample's count for each bucket index, visited in
/// ascending order; indices it lacks count as zero.
struct BaseCounts<'a> {
    buckets: &'a [Bucket],
    next: usize,
}

impl<'a> BaseCounts<'a> {
    fn new(buckets: &'a [Bucket]) -> Self {
        Self { buckets, next: 0 }
    }

    fn get(&mut self, index: i32) -> f64 {
        while self.buckets.get(self.next).is_some_and(|b| b.index < index) {
            self.next += 1;
        }
        match self.buckets.get(self.next) {
            Some(b) if b.index == index => b.count,
            _ => 0.0,
        }
    }
}

/// 2^53: every integer up to this magnitude is exactly representable.
const MAX_EXACT_INTEGER: f64 = 9_007_199_254_740_992.0;

/// `v` as an integer when the conversion round-trips bit-exactly.
fn as_integer(v: f64) -> Option<i64> {
    let i = v as i64;
    (i as f64 == v && v.abs() <= MAX_EXACT_INTEGER && (i != 0 || v.is_sign_positive())).then_some(i)
}

fn put_count(buf: &mut Vec<u8>, integer: bool, value: f64, base: f64) {
    if integer {
        let value = as_integer(value).expect("integer mode requires integer counts");
        put_varint(buf, value - as_integer(base).unwrap_or(0));
    } else {
        put_xor(buf, value.to_bits() ^ base.to_bits());
    }
}

fn get_count(cursor: &mut &[u8], integer: bool, base: f64) -> Result<f64, EncodingError> {
    if integer {
        let value = as_integer(base)
            .unwrap_or(0)
            .checked_add(get_varint(cursor)?)
            .ok_or_else(|| EncodingError {
                message: "histogram count delta overflow".to_string(),
            })?;
        Ok(value as f64)
    } else {
        Ok(f64::from_bits(get_xor(cursor)? ^ base.to_bits()))
    }
}

/// Byte-aligned XOR packing: a header byte holding the number of leading
/// (high nibble) and trailing (low nibble) zero bytes, then the remaining
/// bytes big-endian. An unchanged value is the single byte `0x80`.
fn put_xor(buf: &mut Vec<u8>, xor: u64) {
    if xor == 0 {
        buf.push(0x80);
        return;
    }
    let leading = xor.leading_zeros() / 8;
    let trailing = xor.trailing_zeros() / 8;
    buf.push(((leading << 4) | trailing) as u8);
    buf.put_uint(xor >> (trailing * 8), (8 - leading - trailing) as usize);
}

fn get_xor(cursor: &mut &[u8]) -> Result<u64, EncodingError> {
    let header = get_u8(cursor)?;
    if header == 0x80 {
        return Ok(0);
    }
    let leading = u32::from(header >> 4);
    let trailing = u32::from(header & 0x0f);
    if leading + trailing >= 8 {
        return Err(EncodingError {
            message: format!("invalid histogram xor header {header:#x}"),
        });
    }
    let body = take(cursor, (8 - leading - trailing) as usize)?
        .iter()
        .fold(0u64, |acc, &byte| (acc << 8) | u64::from(byte));
    Ok(body << (trailing * 8))
}

/// Bucket indices as a count, then per bucket the gap from the previous
/// index (the first is absolute).
fn encode_indices(buf: &mut Vec<u8>, buckets: &[Bucket]) {
    put_uvarint(buf, buckets.len() as u64);
    let mut next = 0i64;
    for bucket in buckets {
        put_varint(buf, i64::from(bucket.index) - next);
        next = i64::from(bucket.index) + 1;
    }
}

/// Decodes indices written by [`encode_indices`], with zero counts.
fn decode_indices(cursor: &mut &[u8]) -> Result<Vec<Bucket>, EncodingError> {
    let len = get_uvarint(cursor)? as usize;
    let mut buckets = Vec::with_capacity(len.min(cursor.len()));
    let mut next = 0i64;
    for _ in 0..len {
        let index = next + get_varint(cursor)?;
        let index = i32::try_from(index).map_err(|_| EncodingError {
            message: "histogram bucket index out of range".to_string(),
        })?;
        next = i64::from(index) + 1;
        buckets.push(Bucket { index, count: 0.0 });
    }
    Ok(buckets)
}

fn put_uvarint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push(value as u8 | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

fn put_varint(buf: &mut Vec<u8>, value: i64) {
    put_uvarint(buf, ((value << 1) ^ (value >> 63)) as u64);
}

fn truncated() -> EncodingError {
    EncodingError {
        message: "truncated histogram series value".to_string(),
    }
}

fn get_u8(cursor: &mut &[u8]) -> Result<u8, EncodingError> {
    let (&byte, rest) = cursor.split_first().ok_or_else(truncated)?;
    *cursor = rest;
    Ok(byte)
}

fn get_uvarint(cursor: &mut &[u8]) -> Result<u64, EncodingError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = get_u8(cursor)?;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok(value);
        }
    }
    Err(EncodingError {
        message: "varint overflow in histogram series value".to_string(),
    })
}

fn get_varint(cursor: &mut &[u8]) -> Result<i64, EncodingError> {
    let raw = get_uvarint(cursor)?;
    Ok((raw >> 1) as i64 ^ -((raw & 1) as i64))
}

fn get_f64(cursor: &mut &[u8]) -> Result<f64, EncodingError> {
    let bytes = take(cursor, 8)?;
    Ok(f64::from_le_bytes(bytes.try_into().expect("8 bytes")))
}

fn take<'a>(cursor: &mut &'a [u8], len: usize) -> Result<&'a [u8], EncodingError> {
    if cursor.len() < len {
        return Err(truncated());
    }
    let (head, rest) = cursor.split_at(len);
    *cursor = rest;
    Ok(head)
}

/// Last-write-wins merge of values that may carry histograms: at each
/// timestamp the newest source's sample survives, whatever its type.
fn merge_mixed_values(sources: &[&Bytes]) -> Result<Bytes, EncodingError> {
    enum Point {
        Histogram(FloatHistogram),
        Float(f64),
    }
    let mut points: Vec<(i64, Reverse<usize>, u8, Point)> = Vec::new();
    for (priority, source) in sources.iter().enumerate() {
        let value = SeriesData::decode(source)?;
        points.extend(
            value
                .floats()
                .map(|f| (f.timestamp_ms, Reverse(priority), 1, Point::Float(f.value))),
        );
        points.extend(value.histograms.into_iter().map(|h| {
            (
                h.timestamp_ms,
                Reverse(priority),
                0,
                Point::Histogram(h.histogram),
            )
        }));
    }
    points.sort_by_key(|&(ts, priority, kind, _)| (ts, priority, kind));
    points.dedup_by_key(|point| point.0);

    let mut merged = SeriesData::default();
    for (timestamp_ms, _, _, point) in points {
        match point {
            Point::Histogram(histogram) => merged.histograms.push(HistogramSample {
                timestamp_ms,
                histogram,
            }),
            Point::Float(value) => merged.push(Sample {
                timestamp_ms,
                value,
            }),
        }
    }
    merged.encode()
}

/// Merges series values, `existing` (if any) then `operands` oldest to
/// newest, into one: at a timestamp held by several the newest wins.
///
/// Float-only values merge their chunks ([`chunk::merge`]): chunks that do
/// not overlap are copied without decoding, so merging fresh operands onto
/// a compacted value costs little more than a copy. Values carrying native
/// histograms take a slower fully decoded path with the same semantics
/// across both sample types. A single non-empty source passes through
/// untouched.
pub(crate) fn merge_batch_time_series(
    existing: Option<Bytes>,
    operands: &[Bytes],
) -> Result<Bytes, EncodingError> {
    let mut sources: Vec<&Bytes> = Vec::new();
    if let Some(ref existing) = existing
        && !existing.is_empty()
    {
        sources.push(existing);
    }
    for operand in operands {
        if !operand.is_empty() {
            sources.push(operand);
        }
    }

    if sources.is_empty() {
        return Ok(Bytes::new());
    }
    if sources.len() == 1 {
        return Ok(sources[0].clone());
    }
    if sources.iter().any(|source| is_histogram_value(source)) {
        return merge_mixed_values(&sources);
    }

    let sections = sources
        .iter()
        .map(|source| float_section(source))
        .collect::<Result<Vec<_>, _>>()?;
    let total: usize = sections.iter().map(|section| section.len()).sum();
    let mut buf = Vec::with_capacity(1 + total);
    buf.push(FORMAT_VERSION);
    chunk::merge(&sections, &mut buf)?;
    if buf.len() == 1 {
        return Ok(Bytes::new());
    }
    Ok(Bytes::from(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_time_series_value() {
        // given
        let value = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 10.0,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 30.0,
                },
            ],
        };

        // when
        let encoded = value.encode().unwrap();
        let decoded = TimeSeriesValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_when_points_are_out_of_order() {
        // given
        let value = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
                Sample {
                    timestamp_ms: 1000,
                    value: 10.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 30.0,
                },
            ],
        };

        // when
        let encoded = value.encode().unwrap();
        let decoded = TimeSeriesValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(
            decoded
                .points
                .iter()
                .map(|p| p.timestamp_ms)
                .collect::<Vec<_>>(),
            vec![1000, 2000, 3000]
        );
    }

    #[test]
    fn should_encode_and_decode_empty_time_series_value() {
        // given
        let value = TimeSeriesValue { points: vec![] };

        // when
        let encoded = value.encode().unwrap();
        let decoded = TimeSeriesValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_single_point() {
        // given
        let value = TimeSeriesValue {
            points: vec![Sample {
                timestamp_ms: 1609459200,
                value: 42.5,
            }],
        };

        // when
        let encoded = value.encode().unwrap();
        let decoded = TimeSeriesValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_special_float_values() {
        // given
        let value = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: f64::INFINITY,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: f64::NEG_INFINITY,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 0.0,
                },
                Sample {
                    timestamp_ms: 4000,
                    value: -0.0,
                },
            ],
        };

        // when
        let encoded = value.encode().unwrap();
        let decoded = TimeSeriesValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded.points.len(), 4);
        assert_eq!(decoded.points[0].value, f64::INFINITY);
        assert_eq!(decoded.points[1].value, f64::NEG_INFINITY);
        assert_eq!(decoded.points[2].value, 0.0);
        assert_eq!(decoded.points[3].value, -0.0);
    }

    #[test]
    fn should_merge_time_series_with_deduplication() {
        // given: two time series with overlapping timestamps
        let base = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 10.0,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 30.0,
                },
            ],
        };
        let base_bytes = base.encode().unwrap();

        let other = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 2000,
                    value: 200.0, // Should override base's 20.0
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 300.0, // Should override base's 30.0
                },
                Sample {
                    timestamp_ms: 4000,
                    value: 40.0,
                },
            ],
        };
        let other_bytes = other.encode().unwrap();

        // when: merge the series
        let merged_bytes = merge_batch_time_series(Some(base_bytes), &[other_bytes]).unwrap();
        let merged = TimeSeriesValue::decode(merged_bytes.as_ref()).unwrap();

        // then: should have 4 points with duplicates resolved (last write wins)
        assert_eq!(merged.points.len(), 4);
        assert_eq!(merged.points[0].timestamp_ms, 1000);
        assert_eq!(merged.points[0].value, 10.0); // From base
        assert_eq!(merged.points[1].timestamp_ms, 2000);
        assert_eq!(merged.points[1].value, 200.0); // From other (overrides base)
        assert_eq!(merged.points[2].timestamp_ms, 3000);
        assert_eq!(merged.points[2].value, 300.0); // From other (overrides base)
        assert_eq!(merged.points[3].timestamp_ms, 4000);
        assert_eq!(merged.points[3].value, 40.0); // From other
    }

    #[test]
    fn should_merge_time_series_interleaved() {
        // given: two time series with interleaved timestamps
        let base = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 10.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 30.0,
                },
                Sample {
                    timestamp_ms: 5000,
                    value: 50.0,
                },
            ],
        };
        let base_bytes = base.encode().unwrap();

        let other = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
                Sample {
                    timestamp_ms: 4000,
                    value: 40.0,
                },
                Sample {
                    timestamp_ms: 6000,
                    value: 60.0,
                },
            ],
        };
        let other_bytes = other.encode().unwrap();

        // when: merge the series
        let merged_bytes = merge_batch_time_series(Some(base_bytes), &[other_bytes]).unwrap();
        let merged = TimeSeriesValue::decode(merged_bytes.as_ref()).unwrap();

        // then: should have all 6 points in sorted order
        assert_eq!(merged.points.len(), 6);
        assert_eq!(merged.points[0].timestamp_ms, 1000);
        assert_eq!(merged.points[1].timestamp_ms, 2000);
        assert_eq!(merged.points[2].timestamp_ms, 3000);
        assert_eq!(merged.points[3].timestamp_ms, 4000);
        assert_eq!(merged.points[4].timestamp_ms, 5000);
        assert_eq!(merged.points[5].timestamp_ms, 6000);
    }

    #[test]
    fn should_batch_merge_return_empty_when_no_existing_and_no_operands() {
        // given - nothing

        // when
        let merged = merge_batch_time_series(None, &[]).unwrap();

        // then
        assert!(merged.is_empty());
    }

    #[test]
    fn should_batch_merge_return_existing_when_no_operands() {
        // given
        let existing = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 10.0,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
            ],
        };
        let existing_bytes = existing.encode().unwrap();

        // when
        let merged = merge_batch_time_series(Some(existing_bytes.clone()), &[]).unwrap();

        // then
        assert_eq!(merged, existing_bytes);
    }

    #[test]
    fn should_batch_merge_return_operand_when_no_existing_and_single_operand() {
        // given
        let op = TimeSeriesValue {
            points: vec![Sample {
                timestamp_ms: 1000,
                value: 10.0,
            }],
        };
        let op_bytes = op.encode().unwrap();

        // when
        let merged = merge_batch_time_series(None, std::slice::from_ref(&op_bytes)).unwrap();

        // then
        assert_eq!(merged, op_bytes);
    }

    #[test]
    fn should_batch_merge_skip_empty_operands() {
        // given
        let existing = TimeSeriesValue {
            points: vec![Sample {
                timestamp_ms: 1000,
                value: 10.0,
            }],
        };
        let existing_bytes = existing.encode().unwrap();

        // when
        let merged =
            merge_batch_time_series(Some(existing_bytes.clone()), &[Bytes::new(), Bytes::new()])
                .unwrap();

        // then - only existing remains, single source passthrough
        assert_eq!(merged, existing_bytes);
    }

    #[test]
    fn should_batch_merge_return_empty_when_all_sources_empty() {
        // given - empty existing and empty operands

        // when
        let merged =
            merge_batch_time_series(Some(Bytes::new()), &[Bytes::new(), Bytes::new()]).unwrap();

        // then
        assert!(merged.is_empty());
    }

    #[test]
    fn should_match_sort_and_dedup_oracle_on_random_operands() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |bound: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % bound
        };
        for _ in 0..200 {
            let sources: Vec<Vec<Sample>> = (0..1 + next(5))
                .map(|_| {
                    (0..next(12))
                        .map(|_| Sample {
                            // A narrow range forces ties within and across sources.
                            timestamp_ms: next(16) as i64 * 1000,
                            value: next(1_000) as f64,
                        })
                        .collect()
                })
                .collect();
            let encoded: Vec<Bytes> = sources
                .iter()
                .map(|points| {
                    TimeSeriesValue {
                        points: points.clone(),
                    }
                    .encode()
                    .unwrap()
                })
                .collect();

            let mut expected: Vec<(usize, Sample)> = encoded
                .iter()
                .enumerate()
                .flat_map(|(priority, bytes)| {
                    TimeSeriesValue::decode(bytes)
                        .unwrap()
                        .points
                        .into_iter()
                        .map(move |sample| (priority, sample))
                })
                .collect();
            expected.sort_by(|a, b| a.1.timestamp_ms.cmp(&b.1.timestamp_ms).then(b.0.cmp(&a.0)));
            expected.dedup_by(|a, b| a.1.timestamp_ms == b.1.timestamp_ms);
            let expected: Vec<Sample> = expected.into_iter().map(|(_, sample)| sample).collect();

            let (existing, operands) = encoded.split_first().unwrap();
            let merged = merge_batch_time_series(Some(existing.clone()), operands).unwrap();
            assert_eq!(TimeSeriesValue::decode(&merged).unwrap().points, expected);
        }
    }

    #[test]
    fn should_batch_merge_multiple_operands_with_last_write_wins() {
        // given: three operands where later ones override earlier timestamps
        let op0 = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 10.0,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
            ],
        }
        .encode()
        .unwrap();
        let op1 = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 2000,
                    value: 200.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 30.0,
                },
            ],
        }
        .encode()
        .unwrap();
        let op2 = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 3000,
                    value: 300.0,
                },
                Sample {
                    timestamp_ms: 4000,
                    value: 40.0,
                },
            ],
        }
        .encode()
        .unwrap();

        // when - no existing value
        let merged = merge_batch_time_series(None, &[op0, op1, op2]).unwrap();
        let decoded = TimeSeriesValue::decode(merged.as_ref()).unwrap();

        // then - timestamp 2000 takes op1's value, timestamp 3000 takes op2's value
        let expected = vec![
            Sample {
                timestamp_ms: 1000,
                value: 10.0,
            },
            Sample {
                timestamp_ms: 2000,
                value: 200.0,
            },
            Sample {
                timestamp_ms: 3000,
                value: 300.0,
            },
            Sample {
                timestamp_ms: 4000,
                value: 40.0,
            },
        ];
        assert_eq!(decoded.points, expected);
    }

    #[test]
    fn should_batch_merge_existing_with_multiple_operands() {
        // given
        let existing = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 1.0,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: 2.0,
                },
            ],
        }
        .encode()
        .unwrap();
        let op0 = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 30.0,
                },
            ],
        }
        .encode()
        .unwrap();
        let op1 = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 3000,
                    value: 300.0,
                },
                Sample {
                    timestamp_ms: 4000,
                    value: 40.0,
                },
            ],
        }
        .encode()
        .unwrap();

        // when
        let merged = merge_batch_time_series(Some(existing), &[op0, op1]).unwrap();
        let decoded = TimeSeriesValue::decode(merged.as_ref()).unwrap();

        // then - existing ts=2000 overridden by op0, op0 ts=3000 overridden by op1
        let expected = vec![
            Sample {
                timestamp_ms: 1000,
                value: 1.0,
            },
            Sample {
                timestamp_ms: 2000,
                value: 20.0,
            },
            Sample {
                timestamp_ms: 3000,
                value: 300.0,
            },
            Sample {
                timestamp_ms: 4000,
                value: 40.0,
            },
        ];
        assert_eq!(decoded.points, expected);
    }

    fn exponential_histogram(count: f64) -> FloatHistogram {
        FloatHistogram {
            schema: 1,
            zero_threshold: 1e-128,
            zero_count: 1.0,
            count,
            sum: count * 2.5,
            positive: vec![
                Bucket {
                    index: -3,
                    count: 1.0,
                },
                Bucket {
                    index: 4,
                    count: count - 2.0,
                },
            ],
            negative: vec![Bucket {
                index: 0,
                count: 0.0,
            }],
            ..FloatHistogram::default()
        }
    }

    fn custom_histogram(bounds: &Arc<[f64]>) -> FloatHistogram {
        FloatHistogram {
            schema: crate::histogram::CUSTOM_BUCKETS_SCHEMA,
            counter_reset_hint: CounterResetHint::Gauge,
            count: 3.0,
            sum: 4.0,
            custom_values: bounds.clone(),
            positive: vec![Bucket {
                index: 1,
                count: 3.0,
            }],
            ..FloatHistogram::default()
        }
    }

    fn histogram_sample(timestamp_ms: i64, histogram: FloatHistogram) -> HistogramSample {
        HistogramSample {
            timestamp_ms,
            histogram,
        }
    }

    #[test]
    fn should_round_trip_series_value_with_histograms() {
        // given
        let bounds: Arc<[f64]> = Arc::from(vec![0.5, 1.0, 2.0]);
        let value = SeriesData::new(
            vec![Sample::new(500, 1.5), Sample::new(4000, f64::NAN)],
            vec![
                histogram_sample(1000, exponential_histogram(10.0)),
                histogram_sample(2000, custom_histogram(&bounds)),
                histogram_sample(3000, custom_histogram(&bounds)),
            ],
        );

        // when
        let encoded = value.clone().encode().unwrap();
        let decoded = SeriesData::decode(&encoded).unwrap();

        // then
        assert_eq!(encoded[0], HISTOGRAM_VALUE_TAG);
        assert_eq!(decoded.histograms, value.histograms);
        assert_eq!(decoded.timestamps, value.timestamps);
        assert_eq!(decoded.values[0], value.values[0]);
        assert!(decoded.values[1].is_nan());
        // repeated custom bounds share one allocation after decoding
        assert!(Arc::ptr_eq(
            &decoded.histograms[1].histogram.custom_values,
            &decoded.histograms[2].histogram.custom_values
        ));
    }

    #[test]
    fn should_encode_float_only_series_value_behind_format_tag() {
        let value = SeriesData::new(vec![Sample::new(1000, 1.0)], Vec::new());
        let encoded = value.clone().encode().unwrap();
        assert_eq!(encoded[0], FORMAT_VERSION);
        assert_eq!(
            TimeSeriesValue::decode(&encoded).unwrap().points,
            value.floats().collect::<Vec<_>>()
        );
        assert_eq!(SeriesData::decode(&encoded).unwrap(), value);
    }

    #[test]
    fn should_prefer_histogram_on_collision_within_value() {
        let value = SeriesData::new(
            vec![Sample::new(1000, 1.0), Sample::new(2000, 2.0)],
            vec![histogram_sample(1000, exponential_histogram(5.0))],
        );
        let decoded = SeriesData::decode(&value.encode().unwrap()).unwrap();
        assert_eq!(
            decoded.floats().collect::<Vec<_>>(),
            vec![Sample::new(2000, 2.0)]
        );
        assert_eq!(decoded.histograms.len(), 1);
    }

    #[test]
    fn should_merge_floats_and_histograms_last_write_wins() {
        // given: existing floats, then an operand overwriting ts=2000 with a
        // histogram, then a float operand overwriting the ts=3000 histogram
        let existing = SeriesData::new(
            vec![Sample::new(1000, 1.0), Sample::new(2000, 2.0)],
            Vec::new(),
        )
        .encode()
        .unwrap();
        let op0 = SeriesData::new(
            Vec::new(),
            vec![
                histogram_sample(2000, exponential_histogram(4.0)),
                histogram_sample(3000, exponential_histogram(6.0)),
            ],
        )
        .encode()
        .unwrap();
        let op1 = SeriesData::new(vec![Sample::new(3000, 30.0)], Vec::new())
            .encode()
            .unwrap();

        // when
        let merged = merge_batch_time_series(Some(existing), &[op0, op1]).unwrap();
        let decoded = SeriesData::decode(&merged).unwrap();

        // then
        assert_eq!(
            decoded.floats().collect::<Vec<_>>(),
            vec![Sample::new(1000, 1.0), Sample::new(3000, 30.0)]
        );
        assert_eq!(
            decoded.histograms,
            vec![histogram_sample(2000, exponential_histogram(4.0))]
        );
    }

    #[test]
    fn should_reject_truncated_histogram_value() {
        let encoded = SeriesData::new(
            Vec::new(),
            vec![histogram_sample(1000, exponential_histogram(4.0))],
        )
        .encode()
        .unwrap();
        assert!(SeriesData::decode(&encoded[..encoded.len() - 3]).is_err());
    }

    fn buckets(entries: &[(i32, f64)]) -> Vec<Bucket> {
        entries
            .iter()
            .map(|&(index, count)| Bucket { index, count })
            .collect()
    }

    fn assert_bit_identical(actual: &[HistogramSample], expected: &[HistogramSample]) {
        let bits = |h: &FloatHistogram| {
            let counts = |b: &[Bucket]| {
                b.iter()
                    .map(|b| (b.index, b.count.to_bits()))
                    .collect::<Vec<_>>()
            };
            (
                h.counter_reset_hint,
                h.schema,
                h.zero_threshold.to_bits(),
                h.zero_count.to_bits(),
                h.count.to_bits(),
                h.sum.to_bits(),
                counts(&h.positive),
                counts(&h.negative),
                h.custom_values
                    .iter()
                    .map(|v| v.to_bits())
                    .collect::<Vec<_>>(),
            )
        };
        assert_eq!(actual.len(), expected.len());
        for (a, e) in actual.iter().zip(expected) {
            assert_eq!(a.timestamp_ms, e.timestamp_ms);
            assert_eq!(bits(&a.histogram), bits(&e.histogram));
        }
    }

    #[test]
    fn should_round_trip_histogram_edge_cases_bit_exactly() {
        // given: layout changes (bucket added, removed, schema change),
        // a counter reset, fractional counts between integer samples, a
        // negative-zero count, NaN and stale sums, and custom bounds that
        // repeat across a layout change
        let bounds: Arc<[f64]> = Arc::from(vec![0.5, 1.0, 2.0]);
        let exp = |schema, positive: &[(i32, f64)], count, sum| FloatHistogram {
            schema,
            zero_threshold: 1e-128,
            zero_count: 1.0,
            count,
            sum,
            positive: buckets(positive),
            negative: buckets(&[(-2, 1.0)]),
            ..FloatHistogram::default()
        };
        let samples = vec![
            histogram_sample(1000, exp(1, &[(0, 3.0), (1, 5.0)], 10.0, 2.5)),
            histogram_sample(2000, exp(1, &[(0, 4.0), (1, 9.0)], 15.0, 3.75)),
            histogram_sample(3000, exp(1, &[(0, 4.0), (1, 9.0), (4, 2.0)], 17.0, 4.0)),
            histogram_sample(4000, exp(1, &[(1, 12.0), (4, 2.0)], 16.0, 4.5)),
            histogram_sample(
                5000,
                FloatHistogram {
                    counter_reset_hint: CounterResetHint::CounterReset,
                    ..exp(1, &[(1, 1.0), (4, 0.0)], 3.0, 0.1)
                },
            ),
            histogram_sample(6000, exp(1, &[(1, 1.5), (4, 0.25)], 3.75, 0.2)),
            histogram_sample(7000, exp(1, &[(1, 2.0), (4, -0.0)], 4.0, f64::NAN)),
            histogram_sample(8000, exp(0, &[(0, 2.0)], 4.0, 0.3)),
            histogram_sample(9000, FloatHistogram::stale_marker()),
            histogram_sample(10_000, custom_histogram(&bounds)),
            histogram_sample(
                11_000,
                FloatHistogram {
                    positive: buckets(&[(0, 1.0), (1, 3.0)]),
                    ..custom_histogram(&bounds)
                },
            ),
        ];
        let value = SeriesData::new(Vec::new(), samples.clone());

        // when
        let decoded = SeriesData::decode(&value.encode().unwrap()).unwrap();

        // then
        assert_bit_identical(&decoded.histograms, &samples);
        assert!(decoded.histograms[8].histogram.is_stale_marker());
        assert!(Arc::ptr_eq(
            &decoded.histograms[9].histogram.custom_values,
            &decoded.histograms[10].histogram.custom_values
        ));
    }

    #[test]
    fn should_delta_encode_steady_integer_histograms_compactly() {
        // given: 20 buckets growing by small integer increments
        let samples: Vec<_> = (0..60i64)
            .map(|i| {
                let positive: Vec<_> = (0..20)
                    .map(|b| Bucket {
                        index: b,
                        count: (i * (i64::from(b) + 1)) as f64,
                    })
                    .collect();
                histogram_sample(
                    i * 15_000,
                    FloatHistogram {
                        count: positive.iter().map(|b| b.count).sum(),
                        sum: i as f64 * 0.5,
                        positive,
                        ..FloatHistogram::default()
                    },
                )
            })
            .collect();

        // when
        let encoded = SeriesData::new(Vec::new(), samples.clone())
            .encode()
            .unwrap();

        // then: well under the 8 bytes per bucket of raw counts
        assert!(
            encoded.len() < samples.len() * 20 * 2,
            "encoded {} bytes",
            encoded.len()
        );
        assert_eq!(SeriesData::decode(&encoded).unwrap().histograms, samples);
    }

    #[test]
    fn should_decode_histogram_range_across_delta_chain() {
        // given: floats and histograms interleaved over ten timestamps
        let value = SeriesData::new(
            (0..10)
                .filter(|i| i % 2 == 1)
                .map(|i| Sample::new(i * 1000, i as f64))
                .collect(),
            (0..10)
                .filter(|i| i % 2 == 0)
                .map(|i| histogram_sample(i * 1000, exponential_histogram(10.0 + i as f64)))
                .collect(),
        );
        let encoded = value.clone().encode().unwrap();

        for (start_ms, end_ms) in [(-1, 9000), (2500, 6000), (3999, 4000), (8000, 20_000)] {
            // when
            let ranged = SeriesData::decode_range(&encoded, start_ms, end_ms).unwrap();

            // then: matches a full decode filtered to (start, end]
            let mut expected = value.clone();
            expected.retain_range(start_ms, end_ms);
            assert_eq!(ranged, expected, "range ({start_ms}, {end_ms}]");
        }
    }

    #[test]
    fn should_reject_unknown_histogram_flags() {
        let mut encoded = SeriesData::new(
            Vec::new(),
            vec![histogram_sample(0, exponential_histogram(4.0))],
        )
        .encode()
        .unwrap()
        .to_vec();
        // tag, empty float section length, sample count, timestamp
        assert_eq!(&encoded[..4], &[HISTOGRAM_VALUE_TAG, 0, 1, 0]);
        encoded[4] |= 0x80;
        assert!(SeriesData::decode(&encoded).is_err());
    }

    #[test]
    fn should_reject_unknown_value_format() {
        let mut encoded = SeriesData::new(vec![Sample::new(1000, 1.0)], Vec::new())
            .encode()
            .unwrap()
            .to_vec();
        for tag in [0, 2, 0x7f, 0x82] {
            encoded[0] = tag;
            assert!(SeriesData::decode(&encoded).is_err(), "tag {tag:#x}");
            let operand = Bytes::from(encoded.clone());
            let other = TimeSeriesValue {
                points: vec![Sample::new(2000, 2.0)],
            }
            .encode()
            .unwrap();
            assert!(merge_batch_time_series(Some(other), &[operand]).is_err());
        }
    }

    #[test]
    fn should_round_trip_multi_chunk_floats_beside_histograms() {
        // given: enough floats to span several chunks, with stale markers
        let floats: Vec<Sample> = (0..3000)
            .map(|i| match i % 97 {
                0 => Sample::new(i * 15_000 + 1, f64::from_bits(crate::model::STALE_NAN)),
                _ => Sample::new(i * 15_000 + 1, i as f64 * 0.25),
            })
            .collect();
        let value = SeriesData::new(
            floats.clone(),
            (0..20)
                .map(|i| histogram_sample(i * 15_000, exponential_histogram(10.0 + i as f64)))
                .collect(),
        );

        // when
        let encoded = value.clone().encode().unwrap();
        let decoded = SeriesData::decode(&encoded).unwrap();
        let ranged = SeriesData::decode_range(&encoded, 20_000_000, 30_000_000).unwrap();

        // then
        let bits = |samples: &[Sample]| {
            samples
                .iter()
                .map(|s| (s.timestamp_ms, s.value.to_bits()))
                .collect::<Vec<_>>()
        };
        assert_eq!(bits(&decoded.floats().collect::<Vec<_>>()), bits(&floats));
        assert_eq!(decoded.histograms, value.histograms);
        let mut expected = floats;
        expected.retain(|s| s.timestamp_ms > 20_000_000 && s.timestamp_ms <= 30_000_000);
        assert_eq!(bits(&ranged.floats().collect::<Vec<_>>()), bits(&expected));
    }

    #[test]
    fn should_reject_corrupt_operands_without_panicking() {
        let existing = TimeSeriesValue {
            points: (0..300)
                .map(|i| Sample::new(i * 15_000, i as f64))
                .collect(),
        }
        .encode()
        .unwrap();
        let operand = TimeSeriesValue {
            points: (290..310)
                .map(|i| Sample::new(i * 15_000, -(i as f64)))
                .collect(),
        }
        .encode()
        .unwrap();
        for len in 2..operand.len() {
            let truncated = operand.slice(..len);
            assert!(merge_batch_time_series(Some(existing.clone()), &[truncated]).is_err());
        }
        for at in 0..operand.len() {
            let mut flipped = operand.to_vec();
            flipped[at] ^= 0x5a;
            let _ = merge_batch_time_series(Some(existing.clone()), &[Bytes::from(flipped)]);
        }
    }
}
