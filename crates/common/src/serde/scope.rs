//! Namespace- and segment-scoped record keys.
//!
//! Stores that partition data by tenant and time segment prefix every key
//! with the same scope:
//!
//! ```text
//! ┌───────────┬─────────┬──────────────────┬──────────────────────┬─────────────┬─────┐
//! │ subsystem │ version │ namespace bytes  │ 0x00 terminator      │ segment i64 │ ... │
//! │  1 byte   │ 1 byte  │ (no NUL bytes)   │                      │ sortable BE │     │
//! └───────────┴─────────┴──────────────────┴──────────────────────┴─────────────┴─────┘
//! ```
//!
//! [`ScopedSegmentExtractor`] routes every record sharing a scope into one
//! SlateDB segment.

use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use slatedb::{PrefixExtractor, PrefixTarget};

use super::DeserializeError;
use super::sortable::{decode_i64_sortable, encode_i64_sortable};
use super::terminated_bytes;
use crate::namespace::Namespace;

const SEGMENT_LEN: usize = 8;
/// Smallest valid scope: the 2-byte key prefix, a one-byte namespace, its
/// terminator, and the segment.
const MIN_SCOPE_LEN: usize = 2 + 1 + 1 + SEGMENT_LEN;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyScope {
    subsystem: u8,
    version: u8,
}

impl KeyScope {
    pub const fn new(subsystem: u8, version: u8) -> Self {
        Self { subsystem, version }
    }

    pub fn write(&self, bytes: &mut BytesMut, namespace: &Namespace, segment: i64) {
        bytes.put_u8(self.subsystem);
        bytes.put_u8(self.version);
        terminated_bytes::serialize(namespace.as_bytes(), bytes);
        bytes.put_u64(encode_i64_sortable(segment));
    }

    /// Parses the scope, returning the namespace, segment, and the scope's
    /// length in bytes.
    pub fn parse(&self, bytes: &[u8]) -> Result<(Namespace, i64, usize), DeserializeError> {
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
        let segment = decode_i64_sortable(u64::from_be_bytes(
            suffix[..SEGMENT_LEN].try_into().unwrap(),
        ));
        Ok((namespace, segment, bytes.len() - suffix.len() + SEGMENT_LEN))
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

    fn scope_bytes(namespace: &str, segment: i64) -> Bytes {
        let mut bytes = BytesMut::new();
        SCOPE.write(&mut bytes, &Namespace::new(namespace).unwrap(), segment);
        bytes.freeze()
    }

    #[test]
    fn round_trips_scope() {
        let mut bytes = BytesMut::from(scope_bytes("tenant", -1_000).as_ref());
        let scope_len = bytes.len();
        bytes.put_u8(9);
        let (namespace, segment, length) = SCOPE.parse(&bytes).unwrap();
        assert_eq!(namespace.as_str(), "tenant");
        assert_eq!(segment, -1_000);
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
