//! Serialization utilities for OpenData.

pub mod encoding;
pub mod key_prefix;
pub mod record_tag;
pub mod scope;
pub mod seq_block;
pub mod sortable;
pub mod subsystem;
pub mod terminated_bytes;
pub mod varint;

/// Error type for deserialization failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeserializeError {
    pub message: String,
}

impl std::error::Error for DeserializeError {}

impl std::fmt::Display for DeserializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// Fails when a decoder left bytes unread in `buf`.
pub fn ensure_consumed(buf: &[u8], what: &str) -> Result<(), DeserializeError> {
    if buf.is_empty() {
        Ok(())
    } else {
        Err(DeserializeError {
            message: format!("trailing bytes in {what}"),
        })
    }
}

fn fixed_at<const N: usize>(bytes: &[u8], offset: usize) -> Option<[u8; N]> {
    bytes.get(offset..offset.checked_add(N)?)?.try_into().ok()
}

/// Big-endian `u32` at `offset`, or `None` when `bytes` is too short.
pub fn be_u32_at(bytes: &[u8], offset: usize) -> Option<u32> {
    fixed_at(bytes, offset).map(u32::from_be_bytes)
}

/// Big-endian `u64` at `offset`, or `None` when `bytes` is too short.
pub fn be_u64_at(bytes: &[u8], offset: usize) -> Option<u64> {
    fixed_at(bytes, offset).map(u64::from_be_bytes)
}

/// Big-endian `i64` at `offset`, or `None` when `bytes` is too short.
pub fn be_i64_at(bytes: &[u8], offset: usize) -> Option<i64> {
    fixed_at(bytes, offset).map(i64::from_be_bytes)
}

#[cfg(test)]
mod fixed_width_tests {
    use super::*;

    #[test]
    fn reads_big_endian_values_within_bounds() {
        let bytes = [0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 9];
        assert_eq!(be_u32_at(&bytes, 0), Some(7));
        assert_eq!(be_u64_at(&bytes, 4), Some(9));
        assert_eq!(be_i64_at(&bytes, 4), Some(9));
        assert_eq!(be_u32_at(&bytes, 9), None);
        assert_eq!(be_u64_at(&bytes, usize::MAX), None);
    }

    #[test]
    fn reports_trailing_bytes() {
        assert!(ensure_consumed(&[], "page").is_ok());
        assert_eq!(
            ensure_consumed(&[1], "page").unwrap_err().message,
            "trailing bytes in page"
        );
    }
}
