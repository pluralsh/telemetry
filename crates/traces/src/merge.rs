// Copyright 2026 Plural, Inc.
//
// SPDX-License-Identifier: AGPL-3.0-only

//! Merge operator for trace heads, which the writer adds to without reading.

use bytes::Bytes;
use common::storage::{MergeOperator, decode_merge_operands, default_merge_batch};

use crate::codec::{decode_head, encode_head, is_head_key, merge_heads};

pub(crate) struct TracesMergeOperator;

impl MergeOperator for TracesMergeOperator {
    fn merge_batch(&self, key: &Bytes, existing_value: Option<Bytes>, operands: &[Bytes]) -> Bytes {
        if !is_head_key(key) {
            return default_merge_batch(key, existing_value, operands, |_, _, value| value);
        }
        let merged = decode_merge_operands(
            existing_value.iter().chain(operands),
            decode_head,
            "traces_corrupt_merge_operands_total",
        )
        .reduce(merge_heads);
        match merged {
            Some(head) => encode_head(&head).expect("trace heads encode into memory"),
            None => operands
                .last()
                .or(existing_value.as_ref())
                .cloned()
                .unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{TraceHead, TraceLocator, continuation_key, head_key};
    use crate::{Namespace, TraceId};

    fn head(sequence: u64, pages: u32) -> TraceHead {
        TraceHead {
            first: TraceLocator {
                segment: crate::SegmentId::new(0, 3_600_000_000_000),
                page_sequence: sequence,
                trace_index: 0,
                expires_at_unix_ms: Some(sequence),
            },
            continued: pages > 1,
            pages,
            replaces: false,
        }
    }

    fn encoded(sequence: u64, pages: u32) -> Bytes {
        encode_head(&head(sequence, pages)).unwrap()
    }

    #[test]
    fn heads_merge_in_any_grouping() {
        let key = head_key(&Namespace::default(), TraceId::new([1; 16]).unwrap());
        let operator = TracesMergeOperator;
        let all = operator.merge_batch(&key, Some(encoded(1, 1)), &[encoded(2, 2), encoded(3, 1)]);
        let partial = operator.merge_batch(&key, None, &[encoded(2, 2), encoded(3, 1)]);
        let grouped = operator.merge_batch(&key, Some(encoded(1, 1)), &[partial]);
        assert_eq!(all, grouped);
        let merged = decode_head(&all).unwrap();
        assert_eq!(merged.first.page_sequence, 1);
        assert_eq!(merged.first.expires_at_unix_ms, Some(3));
        assert_eq!(merged.pages, 4);
        assert!(merged.continued);
    }

    #[test]
    fn a_replacing_head_discards_older_operands_in_any_grouping() {
        let key = head_key(&Namespace::default(), TraceId::new([1; 16]).unwrap());
        let operator = TracesMergeOperator;
        let replacing = encode_head(&TraceHead {
            replaces: true,
            ..head(7, 1)
        })
        .unwrap();
        let operands = [encoded(2, 2), replacing, encoded(3, 1)];
        let all = operator.merge_batch(&key, Some(encoded(1, 1)), &operands);
        let older = operator.merge_batch(&key, Some(encoded(1, 1)), &operands[..1]);
        let newer = operator.merge_batch(&key, None, &operands[1..]);
        assert_eq!(all, operator.merge_batch(&key, Some(older), &[newer]));
        let merged = decode_head(&all).unwrap();
        assert_eq!(merged.first.page_sequence, 7);
        assert_eq!(merged.pages, 2);
        assert!(merged.continued);
        assert!(merged.replaces);
    }

    #[test]
    fn a_single_operand_is_unchanged() {
        let key = head_key(&Namespace::default(), TraceId::new([1; 16]).unwrap());
        let merged = TracesMergeOperator.merge_batch(&key, None, &[encoded(4, 1)]);
        assert_eq!(decode_head(&merged).unwrap(), head(4, 1));
    }

    #[test]
    fn corrupt_operands_are_dropped() {
        let key = head_key(&Namespace::default(), TraceId::new([1; 16]).unwrap());
        let merged = TracesMergeOperator.merge_batch(
            &key,
            Some(Bytes::from_static(b"junk")),
            &[encoded(5, 1)],
        );
        assert_eq!(decode_head(&merged).unwrap(), head(5, 1));
    }

    #[test]
    fn other_records_keep_the_newest_value() {
        let key = continuation_key(
            &Namespace::default(),
            TraceId::new([1; 16]).unwrap(),
            crate::SegmentId::new(0, 3_600_000_000_000),
            1,
        );
        let merged = TracesMergeOperator.merge_batch(
            &key,
            Some(Bytes::from_static(b"old")),
            &[Bytes::from_static(b"new")],
        );
        assert_eq!(merged, Bytes::from_static(b"new"));
    }
}
