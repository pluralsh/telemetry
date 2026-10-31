//! Prepared query filter (`PreparedFilter`).
//!
//! Lowers a [`Filter`] AST into precomputed metadata bitmaps so the query
//! operators can answer per-item membership with a cheap
//! [`PreparedFilter::matches`] check. Negations are kept as exclusion bitmaps
//! rather than complemented positive sets, so building a filter never has to
//! enumerate the entire corpus universe.

use std::future::Future;
use std::pin::Pin;

use std::ops::Bound::{Excluded, Included};

use bytes::BytesMut;
use common::BytesRange;
use common::storage::StorageRead;
use roaring::RoaringTreemap;

use crate::error::{Error, Result};
use crate::model::{AttributeValue, Filter};
use crate::serde::id_dictionary::IdDictionaryValue;
use crate::serde::key::MetadataIndexKey;
use crate::serde::metadata_index::MetadataIndexValue;
use crate::serde::{FieldType, FieldValue, RecordType};
use crate::storage::VectorDbStorageReadExt;

/// Executable form of a query [`Filter`].
///
/// An item matches when it is present in `positive` (or `positive` is
/// `None`, meaning "no positive constraint") and absent from every bitmap in
/// `negative`. Equivalently, the matching set is `positive ∩ ¬(⋃ negative)`.
pub(crate) struct PreparedFilter {
    /// Items allowed by the positive (union/intersection) part of the
    /// filter. `None` imposes no positive constraint (matches all ids).
    positive: Option<RoaringTreemap>,
    /// Items excluded by negations. An item matches only if it is
    /// absent from every bitmap here.
    negative: Vec<RoaringTreemap>,
}

/// A subtree collapsed to a single concrete bitmap while combining `Or`
/// branches. `Pos(s)` matches ids in `s`; `Neg(s)` matches ids *not* in `s`.
enum Single {
    Pos(RoaringTreemap),
    Neg(RoaringTreemap),
}

impl PreparedFilter {
    /// Build a prepared filter from an optional filter expression. `None`
    /// matches every item.
    pub(crate) async fn build(expr: Option<&Filter>, storage: &dyn StorageRead) -> Result<Self> {
        match expr {
            None => Ok(Self::match_all()),
            Some(filter) => Self::build_expr(filter, storage).await,
        }
    }

    /// Returns `true` if `doc_id` satisfies the filter.
    pub(crate) fn matches(&self, doc_id: u64) -> bool {
        if let Some(positive) = &self.positive
            && !positive.contains(doc_id)
        {
            return false;
        }
        self.negative
            .iter()
            .all(|excluded| !excluded.contains(doc_id))
    }

    /// No positive constraint and no exclusions: matches everything.
    fn match_all() -> Self {
        Self {
            positive: None,
            negative: Vec::new(),
        }
    }

    /// Empty positive set: matches nothing.
    fn match_none() -> Self {
        Self {
            positive: Some(RoaringTreemap::new()),
            negative: Vec::new(),
        }
    }

    fn positive(set: RoaringTreemap) -> Result<Self> {
        Ok(Self {
            positive: Some(set),
            negative: Vec::new(),
        })
    }

    /// The matching ids as a concrete set, if the filter has a positive
    /// constraint; `None` when it is a pure negation (or matches all), which
    /// needs [`Self::resolve`] against the corpus universe instead.
    pub(crate) fn candidates(&self) -> Option<RoaringTreemap> {
        let mut positive = self.positive.clone()?;
        for excluded in &self.negative {
            positive -= excluded;
        }
        Some(positive)
    }

    /// The matching ids within `universe`.
    pub(crate) fn resolve(&self, universe: &RoaringTreemap) -> RoaringTreemap {
        let mut set = match &self.positive {
            Some(positive) => positive & universe,
            None => universe.clone(),
        };
        for excluded in &self.negative {
            set -= excluded;
        }
        set
    }

    fn build_expr<'a>(
        filter: &'a Filter,
        storage: &'a dyn StorageRead,
    ) -> Pin<Box<dyn Future<Output = Result<Self>> + Send + 'a>> {
        Box::pin(async move {
            match filter {
                Filter::Eq(field, value) => Ok(Self {
                    positive: Some(load_eq(field, value, storage).await?),
                    negative: Vec::new(),
                }),
                Filter::In(field, values) => {
                    let mut set = RoaringTreemap::new();
                    for value in values {
                        set |= &load_eq(field, value, storage).await?;
                    }
                    Ok(Self {
                        positive: Some(set),
                        negative: Vec::new(),
                    })
                }
                Filter::Neq(field, value) => Ok(Self {
                    positive: None,
                    negative: vec![load_eq(field, value, storage).await?],
                }),
                Filter::NotIn(field, values) => {
                    let mut set = RoaringTreemap::new();
                    for value in values {
                        set |= &load_eq(field, value, storage).await?;
                    }
                    Ok(Self {
                        positive: None,
                        negative: vec![set],
                    })
                }
                Filter::Lt(field, value) => {
                    Self::positive(load_range(field, value, RangeOp::Lt, storage).await?)
                }
                Filter::Lte(field, value) => {
                    Self::positive(load_range(field, value, RangeOp::Lte, storage).await?)
                }
                Filter::Gt(field, value) => {
                    Self::positive(load_range(field, value, RangeOp::Gt, storage).await?)
                }
                Filter::Gte(field, value) => {
                    Self::positive(load_range(field, value, RangeOp::Gte, storage).await?)
                }
                Filter::Glob(field, pattern) => {
                    Self::positive(load_glob(field, pattern, false, storage).await?)
                }
                Filter::IGlob(field, pattern) => {
                    Self::positive(load_glob(field, pattern, true, storage).await?)
                }
                Filter::Exists(field) => Self::positive(load_exists(field, storage).await?),
                Filter::Not(inner) => Ok(
                    match Self::build_expr(inner, storage).await?.into_single() {
                        Single::Pos(set) => Self {
                            positive: None,
                            negative: vec![set],
                        },
                        Single::Neg(set) => Self {
                            positive: Some(set),
                            negative: Vec::new(),
                        },
                    },
                ),
                Filter::And(filters) => {
                    let mut acc = Self::match_all();
                    for child in filters {
                        acc = acc.and(Self::build_expr(child, storage).await?);
                    }
                    Ok(acc)
                }
                Filter::Or(filters) => {
                    let mut iter = filters.iter();
                    let Some(first) = iter.next() else {
                        // An empty `Or` matches nothing.
                        return Ok(Self::match_none());
                    };
                    let mut acc = Self::build_expr(first, storage).await?;
                    for child in iter {
                        acc = acc.or(Self::build_expr(child, storage).await?);
                    }
                    Ok(acc)
                }
            }
        })
    }

    /// Intersect two prepared filters (`self AND other`).
    ///
    /// `(P1 ∩ ¬N1) ∩ (P2 ∩ ¬N2) = (P1 ∩ P2) ∩ ¬(N1 ∪ N2)`, so positives
    /// intersect and negatives concatenate.
    fn and(self, other: Self) -> Self {
        let Self {
            positive: p1,
            mut negative,
        } = self;
        let Self {
            positive: p2,
            negative: n2,
        } = other;
        let positive = match (p1, p2) {
            (Some(mut a), Some(b)) => {
                a &= &b;
                Some(a)
            }
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };
        negative.extend(n2);
        Self { positive, negative }
    }

    /// Union two prepared filters (`self OR other`).
    ///
    /// Each side is first collapsed to a single concrete bitmap so the union
    /// can be computed with set operations alone — no corpus universe needed.
    fn or(self, other: Self) -> Self {
        match (self.into_single(), other.into_single()) {
            (Single::Pos(mut a), Single::Pos(b)) => {
                a |= &b;
                Self {
                    positive: Some(a),
                    negative: Vec::new(),
                }
            }
            // a ∨ ¬t = ¬(t − a)
            (Single::Pos(a), Single::Neg(mut t)) | (Single::Neg(mut t), Single::Pos(a)) => {
                t -= &a;
                Self {
                    positive: None,
                    negative: vec![t],
                }
            }
            // ¬a ∨ ¬b = ¬(a ∩ b)
            (Single::Neg(mut a), Single::Neg(b)) => {
                a &= &b;
                Self {
                    positive: None,
                    negative: vec![a],
                }
            }
        }
    }

    /// Collapse to a single concrete bitmap by folding the negatives into the
    /// positive: `Some(p)` → `Pos(p − ⋃neg)`, `None` → `Neg(⋃neg)`.
    fn into_single(self) -> Single {
        let mut excluded = RoaringTreemap::new();
        for bitmap in &self.negative {
            excluded |= bitmap;
        }
        match self.positive {
            Some(mut positive) => {
                positive -= &excluded;
                Single::Pos(positive)
            }
            None => Single::Neg(excluded),
        }
    }
}

/// Load the metadata-index bitmap for a single `field = value` pair.
async fn load_eq(
    field: &str,
    value: &AttributeValue,
    storage: &dyn StorageRead,
) -> Result<RoaringTreemap> {
    let field_value: FieldValue = value.clone().into();
    let mut set = RoaringTreemap::new();
    // Array operands match any element, like the indexed array attributes.
    for term in field_value.index_terms() {
        set |= storage
            .get_metadata_index(field, term)
            .await?
            .effective_vector_ids();
    }
    Ok(set)
}

#[derive(Clone, Copy)]
enum RangeOp {
    Lt,
    Lte,
    Gt,
    Gte,
}

/// Union of the index bitmaps in `range`, keeping only entries whose decoded
/// key passes `keep`.
async fn union_index_range(
    range: BytesRange,
    storage: &dyn StorageRead,
    keep: impl Fn(&MetadataIndexKey) -> bool,
) -> Result<RoaringTreemap> {
    let mut iter = storage.scan_iter(range).await?;
    let mut set = RoaringTreemap::new();
    while let Some(record) = iter.next().await? {
        let key = MetadataIndexKey::decode(&record.key)?;
        if !keep(&key) {
            continue;
        }
        set |= MetadataIndexValue::decode_from_bytes(&record.value)?.effective_vector_ids();
    }
    Ok(set)
}

/// Load the ids whose `field` value compares to `value` per `op`, by a
/// bounded scan over the field's index entries of the value's type.
async fn load_range(
    field: &str,
    value: &AttributeValue,
    op: RangeOp,
    storage: &dyn StorageRead,
) -> Result<RoaringTreemap> {
    let value: FieldValue = value.clone().into();
    if value.index_terms().len() != 1 || value.index_terms()[0] != value {
        return Err(Error::InvalidInput(format!(
            "range filter on '{field}' needs a scalar value"
        )));
    }
    let type_range = BytesRange::prefix(MetadataIndexKey::field_type_prefix(
        field,
        value.field_type(),
    ));
    let key = MetadataIndexKey::new(field, value).encode();
    let range = match op {
        RangeOp::Lt => BytesRange::new(type_range.start, Excluded(key)),
        RangeOp::Lte => BytesRange::new(type_range.start, Included(key)),
        RangeOp::Gt => BytesRange::new(Excluded(key), type_range.end),
        RangeOp::Gte => BytesRange::new(Included(key), type_range.end),
    };
    union_index_range(range, storage, |_| true).await
}

/// Load the ids whose string `field` matches `pattern`, scanning only the
/// index entries under the pattern's literal prefix.
async fn load_glob(
    field: &str,
    pattern: &str,
    case_insensitive: bool,
    storage: &dyn StorageRead,
) -> Result<RoaringTreemap> {
    let range = if case_insensitive {
        BytesRange::prefix(MetadataIndexKey::field_type_prefix(
            field,
            FieldType::String,
        ))
    } else {
        // The sortable string encoding escapes bytes but preserves order, so
        // every match sorts at or after the literal prefix's encoding and
        // before the end of the field's string entries.
        let prefix = crate::utils::glob_literal_prefix(pattern);
        let start = MetadataIndexKey::new(field, FieldValue::String(prefix)).encode();
        // Drop the terminator so longer strings with this prefix sort after.
        let start = start.slice(..start.len() - 1);
        let type_range = BytesRange::prefix(MetadataIndexKey::field_type_prefix(
            field,
            FieldType::String,
        ));
        BytesRange::new(Included(start), type_range.end)
    };
    let literal = crate::utils::glob_literal_prefix(pattern);
    let mut iter = storage.scan_iter(range).await?;
    let mut set = RoaringTreemap::new();
    while let Some(record) = iter.next().await? {
        let key = MetadataIndexKey::decode(&record.key)?;
        let FieldValue::String(value) = &key.value else {
            continue;
        };
        if !case_insensitive && !value.starts_with(&literal) {
            // Past the literal prefix: no later entry can match.
            break;
        }
        if crate::utils::glob_match(pattern, value, case_insensitive) {
            set |= MetadataIndexValue::decode_from_bytes(&record.value)?.effective_vector_ids();
        }
    }
    Ok(set)
}

/// Load every id that has any indexed value for `field`.
async fn load_exists(field: &str, storage: &dyn StorageRead) -> Result<RoaringTreemap> {
    union_index_range(MetadataIndexKey::field_range(field), storage, |_| true).await
}

/// Every live internal id, from the id dictionary. Needed to enumerate the
/// matches of a pure negation.
pub(crate) async fn load_universe(storage: &dyn StorageRead) -> Result<RoaringTreemap> {
    let mut prefix = BytesMut::with_capacity(crate::serde::PREFIX_AND_TAG_LEN);
    RecordType::IdDictionary.write_prefix(&mut prefix);
    let mut iter = storage
        .scan_iter(BytesRange::prefix(prefix.freeze()))
        .await?;
    let mut set = RoaringTreemap::new();
    while let Some(record) = iter.next().await? {
        let value = IdDictionaryValue::decode_from_bytes(&record.value)?;
        set.insert(value.vector_id.id());
    }
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A positive leaf (`Eq`/`In`-like): matches exactly `ids`.
    fn pos(ids: &[u64]) -> PreparedFilter {
        PreparedFilter {
            positive: Some(ids.iter().copied().collect()),
            negative: Vec::new(),
        }
    }

    /// A negated leaf (`Neq`-like): matches everything except `ids`.
    fn neg(ids: &[u64]) -> PreparedFilter {
        PreparedFilter {
            positive: None,
            negative: vec![ids.iter().copied().collect()],
        }
    }

    /// Collect which ids in `0..=universe` the filter matches.
    fn matching(filter: &PreparedFilter, universe: u64) -> Vec<u64> {
        (0..=universe).filter(|&id| filter.matches(id)).collect()
    }

    #[test]
    fn should_match_all_when_no_filter() {
        let filter = PreparedFilter::match_all();
        assert_eq!(matching(&filter, 4), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn should_match_nothing_for_empty_positive() {
        let filter = PreparedFilter::match_none();
        assert!(matching(&filter, 4).is_empty());
    }

    #[test]
    fn should_intersect_positives_on_and() {
        // {1,2,3} AND {2,3,4} -> {2,3}
        let filter = pos(&[1, 2, 3]).and(pos(&[2, 3, 4]));
        assert_eq!(matching(&filter, 5), vec![2, 3]);
    }

    #[test]
    fn should_exclude_on_and_with_neg() {
        // {1,2,3} AND NOT {2} -> {1,3}
        let filter = pos(&[1, 2, 3]).and(neg(&[2]));
        assert_eq!(matching(&filter, 5), vec![1, 3]);
    }

    #[test]
    fn should_union_positives_on_or() {
        // {1,2} OR {3,4} -> {1,2,3,4}
        let filter = pos(&[1, 2]).or(pos(&[3, 4]));
        assert_eq!(matching(&filter, 5), vec![1, 2, 3, 4]);
    }

    #[test]
    fn should_handle_neg_under_or() {
        // {1} OR NOT {1,2} == NOT {2}: everything except 2.
        let filter = pos(&[1]).or(neg(&[1, 2]));
        assert_eq!(matching(&filter, 4), vec![0, 1, 3, 4]);
    }

    #[test]
    fn should_match_all_when_pos_covers_neg_under_or() {
        // {1,2} OR NOT {2} == everything (id 2 is rescued by the positive).
        let filter = pos(&[1, 2]).or(neg(&[2]));
        assert_eq!(matching(&filter, 4), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn should_intersect_negs_under_or() {
        // NOT {1} OR NOT {2} == NOT ({1} ∩ {2}) == NOT {} == everything.
        let filter = neg(&[1]).or(neg(&[2]));
        assert_eq!(matching(&filter, 4), vec![0, 1, 2, 3, 4]);

        // NOT {1,2} OR NOT {2,3} == NOT {2}: only id 2 is excluded.
        let filter = neg(&[1, 2]).or(neg(&[2, 3]));
        assert_eq!(matching(&filter, 4), vec![0, 1, 3, 4]);
    }

    #[test]
    fn should_handle_nested_and_or() {
        // ({1,2,3} AND NOT {3}) OR {5} -> {1,2,5}
        let filter = pos(&[1, 2, 3]).and(neg(&[3])).or(pos(&[5]));
        assert_eq!(matching(&filter, 6), vec![1, 2, 5]);
    }
}
