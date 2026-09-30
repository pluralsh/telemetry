// TimeSeries value structure with Gorilla compression using tsz crate

use crate::histogram::{Bucket, CounterResetHint, FloatHistogram};
use crate::model::{HistogramSample, Sample, SeriesData};

use super::*;
use bytes::{BufMut, Bytes, BytesMut};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tsz::stream::{BufferedWriter, Error as TszError, Read as TszRead};
use tsz::{Bit, DataPoint, Decode, Encode, StdDecoder, StdEncoder};

/// A reader that implements `tsz::stream::Read` for byte slices without copying.
struct BytesReader<'a> {
    bytes: &'a [u8],
    byte_pos: usize,
    bit_pos: u8, // 0-7, position within current byte
}

impl<'a> BytesReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            byte_pos: 0,
            bit_pos: 0,
        }
    }
}

impl<'a> TszRead for BytesReader<'a> {
    fn read_bit(&mut self) -> std::result::Result<Bit, TszError> {
        if self.bit_pos == 8 {
            self.byte_pos += 1;
            self.bit_pos = 0;
        }

        if self.byte_pos >= self.bytes.len() {
            return Err(TszError::EOF);
        }

        let byte = self.bytes[self.byte_pos];
        let bit = if byte & 1u8.wrapping_shl(7 - self.bit_pos as u32) == 0 {
            Bit::Zero
        } else {
            Bit::One
        };

        self.bit_pos += 1;

        Ok(bit)
    }

    fn read_byte(&mut self) -> std::result::Result<u8, TszError> {
        // When bit_pos == 0, we're byte-aligned
        if self.bit_pos == 0 {
            if self.byte_pos >= self.bytes.len() {
                return Err(TszError::EOF);
            }
            let byte = self.bytes[self.byte_pos];
            // Set bit_pos to 8 to mark we've consumed this byte
            // The next read operation will increment byte_pos
            self.bit_pos = 8;
            return Ok(byte);
        }

        // When bit_pos == 8, move to next byte
        if self.bit_pos == 8 {
            self.byte_pos += 1;
            if self.byte_pos >= self.bytes.len() {
                return Err(TszError::EOF);
            }
            let byte = self.bytes[self.byte_pos];
            // Keep bit_pos at 8 since we've consumed this byte
            return Ok(byte);
        }

        // When bit_pos is between 1-7, we need to combine parts of two bytes
        if self.byte_pos >= self.bytes.len() {
            return Err(TszError::EOF);
        }

        let mut byte = 0;
        let mut b = self.bytes[self.byte_pos];
        byte |= b.wrapping_shl(self.bit_pos as u32);

        self.byte_pos += 1;
        if self.byte_pos >= self.bytes.len() {
            return Err(TszError::EOF);
        }

        b = self.bytes[self.byte_pos];
        byte |= b.wrapping_shr(8 - self.bit_pos as u32);

        Ok(byte)
    }

    fn read_bits(&mut self, mut num: u32) -> std::result::Result<u64, TszError> {
        if num > 64 {
            num = 64;
        }

        let mut bits: u64 = 0;
        while num >= 8 {
            let byte = self.read_byte().map(u64::from)?;
            bits = bits.wrapping_shl(8) | byte;
            num -= 8;
        }

        while num > 0 {
            self.read_bit()
                .map(|bit| bits = bits.wrapping_shl(1) | bit.to_u64())?;
            num -= 1;
        }

        Ok(bits)
    }

    fn peak_bits(&mut self, num: u32) -> std::result::Result<u64, TszError> {
        let saved_byte_pos = self.byte_pos;
        let saved_bit_pos = self.bit_pos;

        let bits = self.read_bits(num)?;

        self.byte_pos = saved_byte_pos;
        self.bit_pos = saved_bit_pos;

        Ok(bits)
    }
}

/// Iterator over time series samples from Gorilla-compressed data.
///
/// This iterator lazily decodes samples from the compressed format without
/// materializing the full series in memory.
pub(crate) struct TimeSeriesIterator<'a> {
    decoder: StdDecoder<BytesReader<'a>>,
}

impl<'a> TimeSeriesIterator<'a> {
    /// Creates a new iterator from compressed time series bytes.
    ///
    /// Returns None if the bytes represent an empty series.
    pub fn new(bytes: &'a [u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        let reader = BytesReader::new(bytes);
        let decoder = StdDecoder::new(reader);

        Some(TimeSeriesIterator { decoder })
    }
}

impl<'a> Iterator for TimeSeriesIterator<'a> {
    type Item = Result<Sample, EncodingError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.decoder.next() {
            Ok(dp) => Some(Ok(Sample {
                timestamp_ms: dp.get_time() as i64,
                value: dp.get_value(),
            })),
            Err(tsz::decode::Error::EndOfStream) => None,
            Err(e) => Some(Err(EncodingError {
                message: format!("Gorilla decoding failed: {}", e),
            })),
        }
    }
}

/// TimeSeries value: Gorilla-compressed stream of (timestamp_ms, value) pairs
#[derive(Debug, Clone, PartialEq)]
pub struct TimeSeriesValue {
    pub points: Vec<Sample>,
}

impl TimeSeriesValue {
    /// Encode time series points using Gorilla compression
    pub fn encode(&self) -> Result<Bytes, EncodingError> {
        // Handle empty case
        if self.points.is_empty() {
            return Ok(Bytes::new());
        }

        // Gorilla delta encoding requires monotonically non-decreasing timestamps,
        // so sort defensively to avoid a subtract-with-overflow panic on out-of-order input.
        let mut points: Vec<&Sample> = self.points.iter().collect();
        points.sort_by_key(|p| p.timestamp_ms);

        // Use Gorilla compression
        let w = BufferedWriter::new();
        let start_time = points[0].timestamp_ms as u64;
        let mut encoder = StdEncoder::new(start_time, w);

        for point in &points {
            let dp = DataPoint::new(point.timestamp_ms as u64, point.value);
            encoder.encode(dp);
        }

        let compressed = encoder.close();
        Ok(Bytes::from(compressed))
    }

    /// Decode time series points from Gorilla-compressed data
    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.is_empty() {
            return Ok(TimeSeriesValue { points: vec![] });
        }

        // Use the iterator to collect points
        let points = match TimeSeriesIterator::new(buf) {
            None => vec![], // Empty series
            Some(iter) => iter.collect::<Result<Vec<_>, _>>()?,
        };

        Ok(TimeSeriesValue { points })
    }
}

/// Leading byte of a value that carries native histograms.
///
/// A float-only value is a bare Gorilla stream, which opens with the
/// big-endian `u64` start timestamp; that only begins with `0x80` for
/// timestamps within 2^56 ms of `i64::MIN`, so the marker is unambiguous.
const HISTOGRAM_VALUE_MARKER: u8 = 0x80;

fn is_histogram_value(bytes: &[u8]) -> bool {
    bytes.first() == Some(&HISTOGRAM_VALUE_MARKER)
}

/// Storage encoding of a series value.
///
/// Float-only values encode as a bare Gorilla stream ([`TimeSeriesValue`]).
/// Values with histograms encode as the marker byte, the length-prefixed
/// Gorilla float stream, then the histogram section. Within one value a
/// timestamp holds at most one sample type; a histogram wins a collision.
impl SeriesData {
    pub fn encode(mut self) -> Result<Bytes, EncodingError> {
        if self.histograms.is_empty() {
            return TimeSeriesValue {
                points: self.floats,
            }
            .encode();
        }
        self.histograms.sort_by_key(|h| h.timestamp_ms);
        let histograms = &self.histograms;
        self.floats.retain(|f| {
            histograms
                .binary_search_by_key(&f.timestamp_ms, |h| h.timestamp_ms)
                .is_err()
        });
        let floats = TimeSeriesValue {
            points: self.floats,
        }
        .encode()?;

        let mut buf = BytesMut::new();
        buf.put_u8(HISTOGRAM_VALUE_MARKER);
        put_uvarint(&mut buf, floats.len() as u64);
        buf.put_slice(&floats);
        put_uvarint(&mut buf, self.histograms.len() as u64);
        let mut prev_ts = 0i64;
        let mut prev_custom: Option<&Arc<[f64]>> = None;
        for sample in &self.histograms {
            put_varint(&mut buf, sample.timestamp_ms.wrapping_sub(prev_ts));
            prev_ts = sample.timestamp_ms;
            encode_histogram(&mut buf, &sample.histogram, &mut prev_custom);
        }
        Ok(buf.freeze())
    }

    /// Decodes the samples with `start_ms < timestamp <= end_ms`. Stored
    /// values are sorted by timestamp (encode and merge both guarantee it),
    /// so float-only decoding stops past `end_ms`.
    pub fn decode_range(buf: &[u8], start_ms: i64, end_ms: i64) -> Result<Self, EncodingError> {
        if is_histogram_value(buf) {
            let mut data = Self::decode(buf)?;
            data.retain_range(start_ms, end_ms);
            return Ok(data);
        }
        let Some(iter) = TimeSeriesIterator::new(buf) else {
            return Ok(Self::default());
        };
        let mut floats = Vec::new();
        for sample in iter {
            let sample = sample?;
            if sample.timestamp_ms > end_ms {
                break;
            }
            if sample.timestamp_ms > start_ms {
                floats.push(sample);
            }
        }
        Ok(Self {
            floats,
            histograms: Vec::new(),
        })
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if !is_histogram_value(buf) {
            return Ok(Self {
                floats: TimeSeriesValue::decode(buf)?.points,
                histograms: Vec::new(),
            });
        }
        let mut cursor = &buf[1..];
        let float_len = get_uvarint(&mut cursor)? as usize;
        let float_bytes = take(&mut cursor, float_len)?;
        let floats = TimeSeriesValue::decode(float_bytes)?.points;
        let count = get_uvarint(&mut cursor)? as usize;
        let mut histograms = Vec::with_capacity(count.min(cursor.len()));
        let mut ts = 0i64;
        let mut prev_custom: Option<Arc<[f64]>> = None;
        for _ in 0..count {
            ts = ts.wrapping_add(get_varint(&mut cursor)?);
            histograms.push(HistogramSample {
                timestamp_ms: ts,
                histogram: decode_histogram(&mut cursor, &mut prev_custom)?,
            });
        }
        Ok(Self { floats, histograms })
    }
}

fn encode_histogram<'a>(
    buf: &mut BytesMut,
    h: &'a FloatHistogram,
    prev_custom: &mut Option<&'a Arc<[f64]>>,
) {
    buf.put_u8(match h.counter_reset_hint {
        CounterResetHint::Unknown => 0,
        CounterResetHint::CounterReset => 1,
        CounterResetHint::NotCounterReset => 2,
        CounterResetHint::Gauge => 3,
    });
    put_varint(buf, i64::from(h.schema));
    buf.put_f64_le(h.zero_threshold);
    buf.put_f64_le(h.zero_count);
    buf.put_f64_le(h.count);
    buf.put_f64_le(h.sum);
    if h.uses_custom_buckets() {
        if prev_custom.is_some_and(|prev| prev[..] == h.custom_values[..]) {
            buf.put_u8(1);
        } else {
            buf.put_u8(0);
            put_uvarint(buf, h.custom_values.len() as u64);
            for &bound in h.custom_values.iter() {
                buf.put_f64_le(bound);
            }
            *prev_custom = Some(&h.custom_values);
        }
    }
    encode_buckets(buf, &h.positive);
    encode_buckets(buf, &h.negative);
}

fn decode_histogram(
    cursor: &mut &[u8],
    prev_custom: &mut Option<Arc<[f64]>>,
) -> Result<FloatHistogram, EncodingError> {
    let counter_reset_hint = match get_u8(cursor)? {
        0 => CounterResetHint::Unknown,
        1 => CounterResetHint::CounterReset,
        2 => CounterResetHint::NotCounterReset,
        3 => CounterResetHint::Gauge,
        other => {
            return Err(EncodingError {
                message: format!("invalid histogram counter reset hint {other}"),
            });
        }
    };
    let schema = i32::try_from(get_varint(cursor)?).map_err(|_| EncodingError {
        message: "histogram schema out of range".to_string(),
    })?;
    let zero_threshold = get_f64(cursor)?;
    let zero_count = get_f64(cursor)?;
    let count = get_f64(cursor)?;
    let sum = get_f64(cursor)?;
    let mut h = FloatHistogram {
        counter_reset_hint,
        schema,
        zero_threshold,
        zero_count,
        count,
        sum,
        ..FloatHistogram::default()
    };
    if h.uses_custom_buckets() {
        h.custom_values = if get_u8(cursor)? == 1 {
            prev_custom.clone().ok_or_else(|| EncodingError {
                message: "histogram references missing custom bounds".to_string(),
            })?
        } else {
            let len = get_uvarint(cursor)? as usize;
            let bounds = (0..len)
                .map(|_| get_f64(cursor))
                .collect::<Result<Arc<[f64]>, _>>()?;
            *prev_custom = Some(bounds.clone());
            bounds
        };
    }
    h.positive = decode_buckets(cursor)?;
    h.negative = decode_buckets(cursor)?;
    Ok(h)
}

/// Buckets as a count, then per bucket the index gap from the previous
/// bucket (the first index is absolute) and the count.
fn encode_buckets(buf: &mut BytesMut, buckets: &[Bucket]) {
    put_uvarint(buf, buckets.len() as u64);
    let mut next = 0i64;
    for bucket in buckets {
        put_varint(buf, i64::from(bucket.index) - next);
        next = i64::from(bucket.index) + 1;
        buf.put_f64_le(bucket.count);
    }
}

fn decode_buckets(cursor: &mut &[u8]) -> Result<Vec<Bucket>, EncodingError> {
    let len = get_uvarint(cursor)? as usize;
    let mut buckets = Vec::with_capacity(len.min(cursor.len()));
    let mut next = 0i64;
    for _ in 0..len {
        let index = next + get_varint(cursor)?;
        let index = i32::try_from(index).map_err(|_| EncodingError {
            message: "histogram bucket index out of range".to_string(),
        })?;
        next = i64::from(index) + 1;
        buckets.push(Bucket {
            index,
            count: get_f64(cursor)?,
        });
    }
    Ok(buckets)
}

fn put_uvarint(buf: &mut BytesMut, mut value: u64) {
    while value >= 0x80 {
        buf.put_u8(value as u8 | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

fn put_varint(buf: &mut BytesMut, value: i64) {
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
        points.extend(value.histograms.into_iter().map(|h| {
            (
                h.timestamp_ms,
                Reverse(priority),
                0,
                Point::Histogram(h.histogram),
            )
        }));
        points.extend(
            value
                .floats
                .into_iter()
                .map(|f| (f.timestamp_ms, Reverse(priority), 1, Point::Float(f.value))),
        );
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
            Point::Float(value) => merged.floats.push(Sample {
                timestamp_ms,
                value,
            }),
        }
    }
    merged.encode()
}

/// Merges a batch of compressed time series byte values into a single compressed value.
///
/// This function performs an efficient sorted merge of Gorilla-compressed time series
/// without fully deserializing them into memory. Samples are merged in timestamp order,
/// with duplicates resolved by keeping the value from the newest operand (last write wins).
/// Values carrying native histograms take a slower fully-decoded path with the same
/// semantics across both sample types.
///
/// This is designed for use in merge operators during compaction.
///
/// # Arguments
///
/// * `existing` - The existing compressed time series value (if any)
/// * `operands` - A slice of compressed time series operands, ordered oldest to newest
///
/// # Returns
///
/// A new compressed `Bytes` value containing the merged series
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

    // Handle edge cases
    if sources.is_empty() {
        return Ok(Bytes::new());
    }
    if sources.len() == 1 {
        return Ok(sources[0].clone());
    }
    if sources.iter().any(|source| is_histogram_value(source)) {
        return merge_mixed_values(&sources);
    }

    // K-way merge over the sources' decoders; every encoded stream is sorted
    // by timestamp. Priority is the source index: higher = newer = wins a
    // timestamp tie. The heap holds one head per source ordered by
    // (timestamp, newest first), so every source holding the smallest pending
    // timestamp is at its head when that timestamp is popped. The first pop
    // wins; later samples at the same timestamp (older sources, or repeats
    // within one source) are dropped.
    let mut iters = sources
        .iter()
        .map(|source| TimeSeriesIterator::new(source.as_ref()).expect("Series should not be empty"))
        .collect::<Vec<_>>();
    let mut values = vec![0.0; iters.len()];
    let mut heap = BinaryHeap::with_capacity(iters.len());
    for (priority, iter) in iters.iter_mut().enumerate() {
        if let Some(sample) = iter.next().transpose()? {
            values[priority] = sample.value;
            heap.push(Reverse((sample.timestamp_ms, Reverse(priority))));
        }
    }

    let mut encoder: Option<StdEncoder<BufferedWriter>> = None;
    let mut last = None;
    while let Some(Reverse((timestamp, Reverse(priority)))) = heap.pop() {
        if last != Some(timestamp) {
            last = Some(timestamp);
            encoder
                .get_or_insert_with(|| StdEncoder::new(timestamp as u64, BufferedWriter::new()))
                .encode(DataPoint::new(timestamp as u64, values[priority]));
        }
        if let Some(sample) = iters[priority].next().transpose()? {
            values[priority] = sample.value;
            heap.push(Reverse((sample.timestamp_ms, Reverse(priority))));
        }
    }
    // If all iterators returned None immediately, treat as empty.
    Ok(encoder.map_or_else(Bytes::new, |encoder| Bytes::from(encoder.close())))
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
            // A single non-empty source is passed through untouched.
            if encoded.iter().filter(|bytes| !bytes.is_empty()).count() > 1 {
                expected.dedup_by(|a, b| a.1.timestamp_ms == b.1.timestamp_ms);
            }
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
        let value = SeriesData {
            floats: vec![Sample::new(500, 1.5), Sample::new(4000, f64::NAN)],
            histograms: vec![
                histogram_sample(1000, exponential_histogram(10.0)),
                histogram_sample(2000, custom_histogram(&bounds)),
                histogram_sample(3000, custom_histogram(&bounds)),
            ],
        };

        // when
        let encoded = value.clone().encode().unwrap();
        let decoded = SeriesData::decode(&encoded).unwrap();

        // then
        assert_eq!(encoded[0], HISTOGRAM_VALUE_MARKER);
        assert_eq!(decoded.histograms, value.histograms);
        assert_eq!(decoded.floats[0], value.floats[0]);
        assert!(decoded.floats[1].value.is_nan());
        // repeated custom bounds share one allocation after decoding
        assert!(Arc::ptr_eq(
            &decoded.histograms[1].histogram.custom_values,
            &decoded.histograms[2].histogram.custom_values
        ));
    }

    #[test]
    fn should_encode_float_only_series_value_as_gorilla() {
        let value = SeriesData {
            floats: vec![Sample::new(1000, 1.0)],
            histograms: Vec::new(),
        };
        let encoded = value.clone().encode().unwrap();
        assert_eq!(
            TimeSeriesValue::decode(&encoded).unwrap().points,
            value.floats
        );
        assert_eq!(SeriesData::decode(&encoded).unwrap(), value);
    }

    #[test]
    fn should_prefer_histogram_on_collision_within_value() {
        let value = SeriesData {
            floats: vec![Sample::new(1000, 1.0), Sample::new(2000, 2.0)],
            histograms: vec![histogram_sample(1000, exponential_histogram(5.0))],
        };
        let decoded = SeriesData::decode(&value.encode().unwrap()).unwrap();
        assert_eq!(decoded.floats, vec![Sample::new(2000, 2.0)]);
        assert_eq!(decoded.histograms.len(), 1);
    }

    #[test]
    fn should_merge_floats_and_histograms_last_write_wins() {
        // given: existing floats, then an operand overwriting ts=2000 with a
        // histogram, then a float operand overwriting the ts=3000 histogram
        let existing = SeriesData {
            floats: vec![Sample::new(1000, 1.0), Sample::new(2000, 2.0)],
            histograms: Vec::new(),
        }
        .encode()
        .unwrap();
        let op0 = SeriesData {
            floats: Vec::new(),
            histograms: vec![
                histogram_sample(2000, exponential_histogram(4.0)),
                histogram_sample(3000, exponential_histogram(6.0)),
            ],
        }
        .encode()
        .unwrap();
        let op1 = SeriesData {
            floats: vec![Sample::new(3000, 30.0)],
            histograms: Vec::new(),
        }
        .encode()
        .unwrap();

        // when
        let merged = merge_batch_time_series(Some(existing), &[op0, op1]).unwrap();
        let decoded = SeriesData::decode(&merged).unwrap();

        // then
        assert_eq!(
            decoded.floats,
            vec![Sample::new(1000, 1.0), Sample::new(3000, 30.0)]
        );
        assert_eq!(
            decoded.histograms,
            vec![histogram_sample(2000, exponential_histogram(4.0))]
        );
    }

    #[test]
    fn should_reject_truncated_histogram_value() {
        let encoded = SeriesData {
            floats: Vec::new(),
            histograms: vec![histogram_sample(1000, exponential_histogram(4.0))],
        }
        .encode()
        .unwrap();
        assert!(SeriesData::decode(&encoded[..encoded.len() - 3]).is_err());
    }
}
