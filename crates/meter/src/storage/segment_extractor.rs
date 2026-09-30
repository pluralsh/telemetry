//! SlateDB segment extractor for the timeseries subsystem.
//!
//! Implements [`slatedb::PrefixExtractor`] over the timeseries key layout,
//! mapping every record to the routing prefix
//! `[subsystem, version, namespace\0, time_bucket(4 BE), bucket_size]`. When configured on
//! the `slatedb::DbBuilder`, the extractor causes SlateDB to route writes for
//! each `(namespace, time_bucket, bucket_size)` scope into its own SlateDB
//! segment, so per-bucket LSM state can be compacted (and eventually drained)
//! as a unit.
//!
//! The `record_type` byte that follows deliberately sits *outside* the
//! extracted prefix so all record types for a given bucket (series
//! dictionary, forward index, inverted index, time series samples) share one
//! routing prefix and land in the same SlateDB segment.
//!
//! See the parent `storage` module for how the segments list is consumed by
//! the bucket-discovery path.
//!
//! # Naming
//!
//! The extractor's [`name`](slatedb::PrefixExtractor::name) is persisted in
//! the SlateDB manifest and validated on open. Changes to the routing logic
//! (including any future bump of the timeseries `KEY_VERSION`) must change
//! the name as well, so a database created under different routing rules is
//! rejected with `SlateDBError::SegmentExtractorMismatch` rather than
//! silently mis-routed.

use std::sync::Arc;

use slatedb::{PrefixExtractor, PrefixTarget};

use crate::Namespace;
use crate::model::{BucketSize, BucketStart, TimeBucket};
use crate::serde::{KEY_VERSION, SUBSYSTEM};

/// Shortest routing prefix: `[subsystem(1), version(1), namespace
/// terminator(1), time_bucket(4 BE), bucket_size(1)]` with an empty namespace.
const MIN_ROUTING_PREFIX_LEN: usize = 8;

/// Stable, persisted identifier for this extractor's routing rules. Bump
/// whenever the routing logic changes in a way that would re-route existing
/// keys (e.g. a new `KEY_VERSION` with a different prefix shape).
pub(crate) const EXTRACTOR_NAME: &str = "meter-timeseries/v3";

fn routing_prefix_len(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < MIN_ROUTING_PREFIX_LEN || bytes[0] != SUBSYSTEM || bytes[1] != KEY_VERSION {
        return None;
    }
    let namespace_end = bytes[2..].iter().position(|byte| *byte == 0)? + 2;
    let length = namespace_end + 1 + 4 + 1;
    (bytes.len() >= length).then_some(length)
}

/// Prefix extractor routing timeseries records to per-bucket SlateDB segments.
///
/// Every well-formed timeseries key has the shape `[SUBSYSTEM, KEY_VERSION,
/// namespace\0, time_bucket(4 BE), bucket_size, record_type, ...]` by
/// construction, so this extractor returns the length through `bucket_size`
/// for any well-formed `PrefixTarget::Point`, and for any `PrefixTarget::Prefix`
/// that covers at least that much.
///
/// **Point inputs that don't match are a bug.** SlateDB rejects writes whose
/// segment extractor returns `None` (or `Some(0)`) with
/// `SlateDBError::EmptySegmentPrefix` at write time, so a malformed key would
/// surface as a confusing downstream error rather than a clear failure at
/// the origin. We panic on mismatched `Point` inputs to make the bug
/// obvious. Short or non-conforming `Prefix` scan inputs are permitted to
/// return `None` — the trait contract allows it and slatedb simply skips
/// prefix-based filtering in that case.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct TimeseriesSegmentExtractor;

impl TimeseriesSegmentExtractor {
    /// Returns a shared `Arc<dyn PrefixExtractor>` for handing to
    /// `slatedb::DbBuilder::with_segment_extractor`.
    pub(crate) fn shared() -> Arc<dyn PrefixExtractor> {
        Arc::new(Self)
    }
}

impl PrefixExtractor for TimeseriesSegmentExtractor {
    fn name(&self) -> &str {
        EXTRACTOR_NAME
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        match target {
            // Stored keys and point-read targets must be well-formed
            // timeseries keys. A mismatch is a writer bug; panic with the
            // offending bytes rather than letting slatedb reject the write
            // later with an opaque EmptySegmentPrefix error.
            PrefixTarget::Point(b) => {
                let bytes = b.as_ref();
                let length = routing_prefix_len(bytes);
                assert!(
                    length.is_some(),
                    "TimeseriesSegmentExtractor: malformed timeseries key (subsystem/version \
                     mismatch or under {} bytes): {:02x?}",
                    MIN_ROUTING_PREFIX_LEN,
                    bytes
                );
                length
            }
            // Scan prefixes are caller-supplied; a too-short or non-matching
            // prefix is benign — return `None` so any prefix-based filter is
            // skipped (no false negatives, per the trait contract).
            PrefixTarget::Prefix(b) => {
                let bytes = b.as_ref();
                routing_prefix_len(bytes)
            }
        }
    }
}

/// Decodes a routing prefix back into its namespace and `TimeBucket`.
///
/// Used by the bucket-discovery path to project the segments list returned by
/// `StorageRead::list_segments` into the set of buckets visible to a query.
/// Returns `None` for any prefix that isn't a well-formed timeseries routing
/// prefix, including the reserved `bucket_size == 0`.
pub(crate) fn parse_bucket(prefix: &[u8]) -> Option<(Namespace, TimeBucket)> {
    let length = routing_prefix_len(prefix)?;
    let namespace_end = prefix[2..].iter().position(|byte| *byte == 0)? + 2;
    let namespace =
        Namespace::new(String::from_utf8(prefix[2..namespace_end].to_vec()).ok()?).ok()?;
    let bucket_offset = namespace_end + 1;
    let start = BucketStart::from_be_bytes([
        prefix[bucket_offset],
        prefix[bucket_offset + 1],
        prefix[bucket_offset + 2],
        prefix[bucket_offset + 3],
    ]);
    let size: BucketSize = prefix[bucket_offset + 4];
    if size == 0 {
        return None;
    }
    debug_assert_eq!(length, bucket_offset + 5);
    Some((namespace, TimeBucket { start, size }))
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use super::*;
    use crate::serde::{RecordType, write_record_prefix};

    fn namespace(name: &str) -> Namespace {
        Namespace::new(name).unwrap()
    }

    fn key(namespace: &Namespace, start: u32, size: u8, record_type: RecordType) -> Bytes {
        let mut buf = BytesMut::new();
        write_record_prefix(
            &mut buf,
            namespace,
            &TimeBucket { start, size },
            record_type,
        );
        buf.extend_from_slice(b"tail");
        buf.freeze()
    }

    /// Hand-built routing prefix, for inputs `write_record_prefix` refuses to
    /// produce (such as the reserved bucket size 0).
    fn raw_prefix(namespace: &str, start: u32, size: u8) -> Vec<u8> {
        let mut bytes = vec![SUBSYSTEM, KEY_VERSION];
        bytes.extend_from_slice(namespace.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&start.to_be_bytes());
        bytes.push(size);
        bytes
    }

    fn point(bytes: &[u8]) -> PrefixTarget {
        PrefixTarget::Point(Bytes::copy_from_slice(bytes))
    }

    fn prefix(bytes: &[u8]) -> PrefixTarget {
        PrefixTarget::Prefix(Bytes::copy_from_slice(bytes))
    }

    fn extracted(key: &[u8]) -> &[u8] {
        &key[..TimeseriesSegmentExtractor.prefix_len(&point(key)).unwrap()]
    }

    #[test]
    fn should_return_stable_name() {
        assert_eq!(TimeseriesSegmentExtractor.name(), "meter-timeseries/v3");
    }

    #[test]
    fn should_extract_scope_through_bucket_size() {
        let key = key(&namespace("tenant"), 12345, 1, RecordType::TimeSeries);

        assert_eq!(extracted(&key), raw_prefix("tenant", 12345, 1));
    }

    #[test]
    fn should_route_all_record_types_of_a_bucket_together() {
        let tenant = namespace("tenant");
        let dictionary = key(&tenant, 100, 1, RecordType::SeriesDictionary);
        let forward = key(&tenant, 100, 1, RecordType::ForwardIndex);
        let inverted = key(&tenant, 100, 1, RecordType::InvertedIndex);
        let samples = key(&tenant, 100, 1, RecordType::TimeSeries);

        assert_eq!(extracted(&dictionary), extracted(&samples));
        assert_eq!(extracted(&forward), extracted(&samples));
        assert_eq!(extracted(&inverted), extracted(&samples));
    }

    #[test]
    fn should_route_distinct_scopes_apart() {
        let tenant = namespace("tenant");
        let base = key(&tenant, 100, 1, RecordType::TimeSeries);
        for other in [
            key(&tenant, 200, 1, RecordType::TimeSeries),
            key(&tenant, 100, 2, RecordType::TimeSeries),
            key(&namespace("other"), 100, 1, RecordType::TimeSeries),
            key(&namespace("tenant-b"), 100, 1, RecordType::TimeSeries),
        ] {
            assert_ne!(extracted(&base), extracted(&other));
        }
    }

    #[test]
    fn should_extend_scan_prefix_to_the_same_point_prefix() {
        let scan = raw_prefix("tenant", 42, 1);
        let length = TimeseriesSegmentExtractor.prefix_len(&prefix(&scan));
        assert_eq!(length, Some(scan.len()));

        let key = key(&namespace("tenant"), 42, 1, RecordType::InvertedIndex);
        assert!(key.starts_with(&scan));
        assert_eq!(extracted(&key), scan.as_slice());
    }

    #[test]
    fn should_return_none_for_incomplete_or_foreign_scan_prefixes() {
        let scope = raw_prefix("tenant", 42, 1);
        let mut wrong_subsystem = scope.clone();
        wrong_subsystem[0] = 0xFF;
        let mut wrong_version = scope.clone();
        wrong_version[1] = 0xFF;
        let namespace_only = raw_prefix("tenant", 0, 0)[..9].to_vec();

        for bytes in [
            &[][..],
            &[SUBSYSTEM, KEY_VERSION][..],
            &scope[..scope.len() - 1],
            &namespace_only,
            &wrong_subsystem,
            &wrong_version,
        ] {
            assert_eq!(
                TimeseriesSegmentExtractor.prefix_len(&prefix(bytes)),
                None,
                "{bytes:02x?}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "malformed timeseries key")]
    fn should_panic_on_truncated_point() {
        let scope = raw_prefix("tenant", 42, 1);
        let _ = TimeseriesSegmentExtractor.prefix_len(&point(&scope[..scope.len() - 1]));
    }

    #[test]
    #[should_panic(expected = "malformed timeseries key")]
    fn should_panic_on_point_with_wrong_subsystem() {
        let mut key = key(&namespace("tenant"), 42, 1, RecordType::TimeSeries).to_vec();
        key[0] = 0xFF;
        let _ = TimeseriesSegmentExtractor.prefix_len(&point(&key));
    }

    #[test]
    #[should_panic(expected = "malformed timeseries key")]
    fn should_panic_on_point_with_wrong_version() {
        let mut key = key(&namespace("tenant"), 42, 1, RecordType::TimeSeries).to_vec();
        key[1] = 0xFF;
        let _ = TimeseriesSegmentExtractor.prefix_len(&point(&key));
    }

    #[test]
    #[should_panic(expected = "malformed timeseries key")]
    fn should_panic_on_empty_point() {
        let _ = TimeseriesSegmentExtractor.prefix_len(&point(&[]));
    }

    #[test]
    fn parse_bucket_decodes_extracted_prefix() {
        for size in [1, 2] {
            let key = key(&namespace("tenant"), 7777, size, RecordType::TimeSeries);

            assert_eq!(
                parse_bucket(extracted(&key)),
                Some((namespace("tenant"), TimeBucket { start: 7777, size }))
            );
        }
    }

    #[test]
    fn parse_bucket_rejects_malformed_prefixes() {
        let mut wrong_subsystem = raw_prefix("tenant", 7777, 1);
        wrong_subsystem[0] = 0xFF;
        let mut wrong_version = raw_prefix("tenant", 7777, 1);
        wrong_version[1] = 0xFF;
        let truncated = raw_prefix("tenant", 7777, 1);

        for bytes in [
            raw_prefix("tenant", 7777, 0),
            raw_prefix("", 7777, 1),
            raw_prefix("bad\nname", 7777, 1),
            truncated[..truncated.len() - 1].to_vec(),
            wrong_subsystem,
            wrong_version,
        ] {
            assert_eq!(parse_bucket(&bytes), None, "{bytes:02x?}");
        }
    }
}
