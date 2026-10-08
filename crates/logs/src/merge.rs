// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Merge operator for the records the writer updates without reading them.

use bytes::Bytes;
use common::storage::{MergeOperator, default_merge_batch};
use roaring::RoaringBitmap;

use crate::codec::{MergeKind, decode_postings, encode_postings, merge_kind};
use crate::error::Result;
use crate::search::{
    FieldStats, TermStats, decode_field_stats, decode_term_stats, encode_field_stats,
    encode_term_stats,
};

pub(crate) struct LogsMergeOperator;

impl MergeOperator for LogsMergeOperator {
    fn merge_batch(&self, key: &Bytes, existing_value: Option<Bytes>, operands: &[Bytes]) -> Bytes {
        let Some(kind) = merge_kind(key) else {
            return default_merge_batch(key, existing_value, operands, |_, _, value| value);
        };
        let values = existing_value.iter().chain(operands);
        match kind {
            MergeKind::StreamPostings => {
                let mut merged = RoaringBitmap::new();
                for bitmap in decoded(values, decode_postings) {
                    merged |= bitmap;
                }
                encode_postings(&merged).expect("roaring bitmaps serialize into memory")
            }
            MergeKind::FieldStats => {
                let mut merged = FieldStats::default();
                for stats in decoded(values, decode_field_stats) {
                    merged.documents = merged.documents.saturating_add(stats.documents);
                    merged.total_terms = merged.total_terms.saturating_add(stats.total_terms);
                }
                encode_field_stats(merged)
            }
            MergeKind::TermStats => {
                let mut merged = TermStats::default();
                for stats in decoded(values, decode_term_stats) {
                    merged.documents = merged.documents.saturating_add(stats.documents);
                    merged.blocks = merged.blocks.saturating_add(stats.blocks);
                }
                encode_term_stats(merged)
            }
        }
    }
}

/// Decodes every value, dropping and reporting those that are corrupt: a
/// merge cannot fail, and panicking would take down SlateDB compaction.
fn decoded<'a, T>(
    values: impl Iterator<Item = &'a Bytes>,
    decode: impl Fn(&[u8]) -> Result<T>,
) -> impl Iterator<Item = T> {
    values.filter_map(move |value| match decode(value) {
        Ok(decoded) => Some(decoded),
        Err(error) => {
            tracing::error!(%error, "dropping corrupt logs merge operand");
            ::metrics::counter!("logs_corrupt_merge_operands_total").increment(1);
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Namespace;
    use crate::codec::{field_stats_key, forward_key, posting_key, term_stats_key};
    use crate::model::Label;

    fn bitmap(ids: &[u32]) -> Bytes {
        encode_postings(&ids.iter().copied().collect()).unwrap()
    }

    #[test]
    fn unions_stream_postings_in_any_grouping() {
        let key = posting_key(&Namespace::default(), 0, &Label::new("app", "api"));
        let operator = LogsMergeOperator;
        let all = operator.merge_batch(&key, Some(bitmap(&[1])), &[bitmap(&[2]), bitmap(&[1, 3])]);
        let partial = operator.merge_batch(&key, None, &[bitmap(&[2]), bitmap(&[1, 3])]);
        let grouped = operator.merge_batch(&key, Some(bitmap(&[1])), &[partial]);
        let expected: RoaringBitmap = [1, 2, 3].into_iter().collect();
        assert_eq!(decode_postings(&all).unwrap(), expected);
        assert_eq!(decode_postings(&grouped).unwrap(), expected);
    }

    #[test]
    fn sums_search_statistics() {
        let namespace = Namespace::default();
        let operator = LogsMergeOperator;
        let field = |documents, total_terms| {
            encode_field_stats(FieldStats {
                documents,
                total_terms,
            })
        };
        let merged = operator.merge_batch(
            &field_stats_key(&namespace, 0),
            Some(field(2, 10)),
            &[field(3, 7)],
        );
        assert_eq!(
            decode_field_stats(&merged).unwrap(),
            FieldStats {
                documents: 5,
                total_terms: 17
            }
        );
        let term = |documents, blocks| encode_term_stats(TermStats { documents, blocks });
        let merged = operator.merge_batch(
            &term_stats_key(&namespace, 0, "error"),
            None,
            &[term(4, 1), term(130, 2)],
        );
        assert_eq!(
            decode_term_stats(&merged).unwrap(),
            TermStats {
                documents: 134,
                blocks: 3
            }
        );
    }

    #[test]
    fn corrupt_operands_are_dropped() {
        let key = posting_key(&Namespace::default(), 0, &Label::new("app", "api"));
        let merged =
            LogsMergeOperator.merge_batch(&key, Some(Bytes::from_static(b"junk")), &[bitmap(&[4])]);
        assert_eq!(
            decode_postings(&merged).unwrap(),
            [4].into_iter().collect::<RoaringBitmap>()
        );
    }

    #[test]
    fn other_records_keep_the_newest_value() {
        let key = forward_key(&Namespace::default(), 0, 1);
        let merged = LogsMergeOperator.merge_batch(
            &key,
            Some(Bytes::from_static(b"old")),
            &[Bytes::from_static(b"new")],
        );
        assert_eq!(merged, Bytes::from_static(b"new"));
    }
}
