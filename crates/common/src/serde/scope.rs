//! Namespace- and segment-scoped record keys.
//!
//! Stores that partition data by tenant and time segment prefix every key
//! with the same scope:
//!
//! ```text
//! ┌───────────┬─────────┬─────────────────┬────────────┬──────────────┬──────────────┬─────┐
//! │ subsystem │ version │ namespace bytes │ 0x00       │ start ns i64 │ width ns u64 │ ... │
//! │  1 byte   │ 1 byte  │ (no NUL bytes)  │ terminator │ sortable BE  │ BE           │     │
//! └───────────┴─────────┴─────────────────┴────────────┴──────────────┴──────────────┴─────┘
//! ```
//!
//! The width makes each scope self-describing, so segments of different
//! widths (such as hours rolled up into a day) can coexist. A zero width marks
//! a segment that isn't bounded in time.
//!
//! [`ScopedSegmentExtractor`] routes every record sharing a scope into one
//! SlateDB segment.

use std::fmt;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use slatedb::{PrefixExtractor, PrefixTarget};

use super::DeserializeError;
use super::sortable::{decode_i64_sortable, encode_i64_sortable};
use super::terminated_bytes;
use crate::namespace::Namespace;

const SEGMENT_LEN: usize = 16;
/// Smallest valid scope: the 2-byte key prefix, a one-byte namespace, its
/// terminator, and the segment.
const MIN_SCOPE_LEN: usize = 2 + 1 + 1 + SEGMENT_LEN;

/// A time segment: `[start_ns, start_ns + width_ns)`, or unbounded when
/// `width_ns` is zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Segment {
    pub start_ns: i64,
    pub width_ns: i64,
}

impl Segment {
    pub const fn new(start_ns: i64, width_ns: i64) -> Self {
        Self { start_ns, width_ns }
    }

    /// An unbounded segment, for records that aren't partitioned by time.
    pub const fn unbounded(start_ns: i64) -> Self {
        Self::new(start_ns, 0)
    }

    /// The `width_ns` segment containing `timestamp_ns`.
    pub fn containing(timestamp_ns: i64, width_ns: i64) -> Self {
        Self::new(timestamp_ns.div_euclid(width_ns) * width_ns, width_ns)
    }

    pub fn is_bounded(&self) -> bool {
        self.width_ns > 0
    }

    /// Exclusive end, or `None` for an unbounded segment.
    pub fn end_ns(&self) -> Option<i64> {
        self.is_bounded()
            .then(|| self.start_ns.saturating_add(self.width_ns))
    }

    /// The segment of the same width that follows this one.
    pub fn next(&self) -> Self {
        Self::new(self.start_ns.saturating_add(self.width_ns), self.width_ns)
    }
}

impl fmt::Display for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}+{}", self.start_ns, self.width_ns)
    }
}

/// Parses the [`Display`](fmt::Display) form, `start_ns+width_ns`.
impl std::str::FromStr for Segment {
    type Err = DeserializeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let malformed = || error(format!("malformed segment {value:?}"));
        let (start, width) = value.split_once('+').ok_or_else(malformed)?;
        let start_ns = start.parse().map_err(|_| malformed())?;
        let width_ns: i64 = width.parse().map_err(|_| malformed())?;
        if width_ns < 0 {
            return Err(malformed());
        }
        Ok(Self::new(start_ns, width_ns))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyScope {
    subsystem: u8,
    version: u8,
}

impl KeyScope {
    pub const fn new(subsystem: u8, version: u8) -> Self {
        Self { subsystem, version }
    }

    pub fn write(&self, bytes: &mut BytesMut, namespace: &Namespace, segment: Segment) {
        bytes.put_u8(self.subsystem);
        bytes.put_u8(self.version);
        terminated_bytes::serialize(namespace.as_bytes(), bytes);
        bytes.put_u64(encode_i64_sortable(segment.start_ns));
        bytes.put_u64(segment.width_ns.max(0) as u64);
    }

    /// Parses the scope, returning the namespace, segment, and the scope's
    /// length in bytes.
    pub fn parse(&self, bytes: &[u8]) -> Result<(Namespace, Segment, usize), DeserializeError> {
        if !self.has_prefix(bytes) {
            return Err(error("invalid key prefix".to_owned()));
        }
        let mut suffix = &bytes[2..];
        let namespace = terminated_bytes::deserialize(&mut suffix)?;
        if suffix.len() < SEGMENT_LEN {
            return Err(error("truncated key scope".to_owned()));
        }
        let namespace = String::from_utf8(namespace.to_vec())
            .map_err(|error| self::error(format!("namespace is not UTF-8: {error}")))?;
        let namespace =
            Namespace::new(namespace).map_err(|error| self::error(error.to_string()))?;
        let start_ns = decode_i64_sortable(u64::from_be_bytes(suffix[..8].try_into().unwrap()));
        let width_ns = i64::try_from(u64::from_be_bytes(suffix[8..16].try_into().unwrap()))
            .map_err(|_| error("segment width overflows i64".to_owned()))?;
        Ok((
            namespace,
            Segment::new(start_ns, width_ns),
            bytes.len() - suffix.len() + SEGMENT_LEN,
        ))
    }

    /// Length of the scope at the start of `bytes`, without validating the
    /// namespace. Used on the hot compaction and routing paths.
    pub fn prefix_len(&self, bytes: &[u8]) -> Option<usize> {
        if !self.has_prefix(bytes) {
            return None;
        }
        let namespace_end = bytes[2..].iter().position(|byte| *byte == 0)? + 2;
        let length = namespace_end + 1 + SEGMENT_LEN;
        (bytes.len() >= length).then_some(length)
    }

    fn has_prefix(&self, bytes: &[u8]) -> bool {
        bytes.len() >= MIN_SCOPE_LEN && bytes[0] == self.subsystem && bytes[1] == self.version
    }
}

fn error(message: String) -> DeserializeError {
    DeserializeError { message }
}

/// Routes every record for one `(namespace, segment)` scope into one SlateDB
/// segment.
///
/// SlateDB persists the extractor name and refuses to open a database whose
/// extractor name changed, so `name` is part of the storage format.
#[derive(Clone, Copy, Debug)]
pub struct ScopedSegmentExtractor {
    name: &'static str,
    scope: KeyScope,
}

impl ScopedSegmentExtractor {
    pub const fn new(name: &'static str, scope: KeyScope) -> Self {
        Self { name, scope }
    }

    pub fn shared(self) -> Arc<dyn PrefixExtractor> {
        Arc::new(self)
    }
}

impl PrefixExtractor for ScopedSegmentExtractor {
    fn name(&self) -> &str {
        self.name
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        match target {
            PrefixTarget::Point(bytes) => {
                let result = self.scope.prefix_len(bytes);
                assert!(
                    result.is_some(),
                    "{} received malformed key: {:02x?}",
                    self.name,
                    bytes
                );
                result
            }
            PrefixTarget::Prefix(bytes) => self.scope.prefix_len(bytes),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    const SCOPE: KeyScope = KeyScope::new(0x7f, 3);

    fn scope_bytes(namespace: &str, start_ns: i64) -> Bytes {
        let mut bytes = BytesMut::new();
        SCOPE.write(
            &mut bytes,
            &Namespace::new(namespace).unwrap(),
            Segment::new(start_ns, 3_600),
        );
        bytes.freeze()
    }

    #[test]
    fn round_trips_scope() {
        let mut bytes = BytesMut::from(scope_bytes("tenant", -1_000).as_ref());
        let scope_len = bytes.len();
        bytes.put_u8(9);
        let (namespace, segment, length) = SCOPE.parse(&bytes).unwrap();
        assert_eq!(namespace.as_str(), "tenant");
        assert_eq!(segment, Segment::new(-1_000, 3_600));
        assert_eq!(length, scope_len);
        assert_eq!(SCOPE.prefix_len(&bytes), Some(scope_len));
    }

    #[test]
    fn rejects_foreign_and_truncated_keys() {
        let bytes = scope_bytes("tenant", 5);
        assert!(KeyScope::new(0x7f, 4).parse(&bytes).is_err());
        assert_eq!(KeyScope::new(0x7e, 3).prefix_len(&bytes), None);
        assert!(SCOPE.parse(&bytes[..bytes.len() - 1]).is_err());
        assert_eq!(SCOPE.prefix_len(&bytes[..bytes.len() - 1]), None);
    }

    #[test]
    fn segments_sort_numerically() {
        assert!(scope_bytes("a", -1) < scope_bytes("a", 0));
        assert!(scope_bytes("a", i64::MAX) < scope_bytes("b", i64::MIN));
    }

    #[test]
    fn widths_distinguish_segments_with_one_start() {
        let mut hour = BytesMut::new();
        let mut day = BytesMut::new();
        let namespace = Namespace::new("a").unwrap();
        SCOPE.write(&mut hour, &namespace, Segment::new(0, 3_600));
        SCOPE.write(&mut day, &namespace, Segment::new(0, 86_400));
        assert_ne!(hour, day);
        assert!(hour < day);
    }

    #[test]
    fn segment_arithmetic() {
        let segment = Segment::containing(-1, 10);
        assert_eq!(segment, Segment::new(-10, 10));
        assert_eq!(segment.end_ns(), Some(0));
        assert_eq!(segment.next(), Segment::new(0, 10));
        assert_eq!(Segment::unbounded(i64::MIN).end_ns(), None);
    }

    #[test]
    fn extractor_routes_scope_prefix() {
        let prefix = scope_bytes("tenant", -1_000);
        let extractor = ScopedSegmentExtractor::new("test/v1", SCOPE);
        assert_eq!(extractor.name(), "test/v1");
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Prefix(prefix.clone())),
            Some(prefix.len())
        );
    }
}
