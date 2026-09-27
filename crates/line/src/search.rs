// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Bounded, segment-local full-text records and BM25 scoring.
//!
//! The BM25 arithmetic and block-impact idea are adapted from OpenData's MIT
//! licensed `vector` crate. Line deliberately uses a different persistence
//! layout: every postings block is an independently addressable SlateDB value,
//! while fixed-size directory pages describe those blocks.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;
use common::storage::Storage;
use serde::{Deserialize, Serialize};

use crate::codec::{field_stats_key, term_directory_key, term_posting_block_key, term_stats_key};
use crate::error::{Error, Result};
use crate::model::{SegmentId, StreamId};
use crate::namespace::Namespace;

pub(crate) const POSTINGS_PER_BLOCK: usize = 128;
pub(crate) const DIRECTORY_ENTRIES: usize = 256;
pub(crate) const SCORE_METADATA_FIELD: &str = "__line_bm25_score";
const K1: f32 = 1.2;
const B: f32 = 0.75;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct DocAddress {
    pub(crate) stream_id: StreamId,
    pub(crate) page_sequence: u64,
    pub(crate) row_id: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(crate) struct Posting {
    pub(crate) address: DocAddress,
    pub(crate) frequency: u32,
    pub(crate) length: u32,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub(crate) struct FieldStats {
    pub(crate) documents: u64,
    pub(crate) total_terms: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub(crate) struct TermStats {
    pub(crate) documents: u64,
    pub(crate) blocks: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(crate) struct BlockDirectoryEntry {
    pub(crate) ordinal: u32,
    pub(crate) postings: u16,
    pub(crate) max_frequency: u32,
    pub(crate) min_length: u32,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct IndexDelta {
    pub(crate) documents: u64,
    pub(crate) total_terms: u64,
    pub(crate) postings: BTreeMap<String, Vec<Posting>>,
}

impl IndexDelta {
    pub(crate) fn add_page(
        &mut self,
        stream_id: StreamId,
        page_sequence: u64,
        lines: impl IntoIterator<Item = String>,
    ) -> Result<()> {
        for (row, line) in lines.into_iter().enumerate() {
            let row_id = u32::try_from(row)
                .map_err(|_| Error::Invalid("page row id exceeds u32".to_owned()))?;
            let frequencies = token_frequencies(&line);
            let length = frequencies
                .values()
                .try_fold(0u32, |sum, frequency| sum.checked_add(*frequency))
                .ok_or_else(|| Error::Invalid("token count exceeds u32".to_owned()))?;
            self.documents = self
                .documents
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("segment document count overflow".to_owned()))?;
            self.total_terms = self
                .total_terms
                .checked_add(u64::from(length))
                .ok_or_else(|| Error::Invalid("segment token count overflow".to_owned()))?;
            for (term, frequency) in frequencies {
                self.postings.entry(term).or_default().push(Posting {
                    address: DocAddress {
                        stream_id,
                        page_sequence,
                        row_id,
                    },
                    frequency,
                    length,
                });
            }
        }
        Ok(())
    }
}

pub(crate) fn tokenize(value: &str) -> Vec<String> {
    value
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(str::to_lowercase)
        .collect()
}

pub(crate) fn token_frequencies(value: &str) -> BTreeMap<String, u32> {
    let mut frequencies = BTreeMap::new();
    for term in tokenize(value) {
        let frequency = frequencies.entry(term).or_insert(0u32);
        *frequency = frequency.saturating_add(1);
    }
    frequencies
}

pub(crate) fn query_terms(value: &str) -> Vec<String> {
    tokenize(value)
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(crate) fn source_matches(line: &str, terms: &[String]) -> bool {
    let source = token_frequencies(line);
    !terms.is_empty() && terms.iter().all(|term| source.contains_key(term))
}

pub(crate) fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(value)?))
}

pub(crate) fn decode<T: for<'de> Deserialize<'de>>(value: &[u8]) -> Result<T> {
    Ok(serde_json::from_slice(value)?)
}

#[derive(Clone, Copy, Debug)]
struct Hit {
    frequency: u32,
    length: u32,
    idf: f32,
}

fn idf(documents: u64, term_documents: u64) -> f32 {
    let documents = documents as f32;
    let term_documents = term_documents as f32;
    (((documents - term_documents + 0.5) / (term_documents + 0.5)) + 1.0).ln()
}

#[inline]
fn hit_score(hit: Hit, average_length: f32) -> f32 {
    let normalization = K1 * (1.0 - B + B * (hit.length.max(1) as f32 / average_length.max(1.0)));
    let weight = hit.idf * (K1 + 1.0);
    weight - weight / (1.0 + hit.frequency as f32 / normalization)
}

fn score(hits: &[Hit], average_length: f32) -> f32 {
    hits.iter()
        .map(|hit| hit_score(*hit, average_length) as f64)
        .sum::<f64>() as f32
}

/// Loads compact directories first and fetches posting values only as needed.
///
/// Single-term top-k queries use block maxima to stop once every remaining
/// block is unable to beat the current floor. Multi-term queries retain the
/// same exact scorer but load every participating block because independently
/// chunked term ranges need not align.
pub(crate) async fn block_max_scores(
    storage: &dyn Storage,
    namespace: &Namespace,
    segment: SegmentId,
    terms: &[String],
    allowed_pages: &BTreeSet<(StreamId, u64)>,
    limit: Option<usize>,
) -> Result<Option<HashMap<DocAddress, f32>>> {
    if allowed_pages.is_empty() {
        return Ok(Some(HashMap::new()));
    }
    let Some(record) = storage.get(field_stats_key(namespace, segment)).await? else {
        return Ok(None);
    };
    let field: FieldStats = decode(&record.value)?;
    if field.documents == 0 || field.total_terms == 0 || terms.is_empty() {
        return Ok(Some(HashMap::new()));
    }
    let average_length = field.total_terms as f32 / field.documents as f32;
    let mut all_hits: HashMap<DocAddress, Vec<Hit>> = HashMap::new();
    let mut matched_terms: HashMap<DocAddress, usize> = HashMap::new();

    for term in terms {
        let Some(record) = storage
            .get(term_stats_key(namespace, segment, term))
            .await?
        else {
            return Ok(Some(HashMap::new()));
        };
        let stats: TermStats = decode(&record.value)?;
        if stats.documents == 0 {
            return Ok(Some(HashMap::new()));
        }
        let term_idf = idf(field.documents, stats.documents);
        let directory_pages = (stats.blocks as usize).div_ceil(DIRECTORY_ENTRIES);
        let mut directory = Vec::with_capacity(stats.blocks as usize);
        for page in 0..directory_pages {
            let record = storage
                .get(term_directory_key(namespace, segment, term, page as u32))
                .await?
                .ok_or_else(|| Error::Corrupt("term stats reference missing directory".into()))?;
            let entries: Vec<BlockDirectoryEntry> = decode(&record.value)?;
            if entries.len() > DIRECTORY_ENTRIES {
                return Err(Error::Corrupt("term directory exceeds bound".into()));
            }
            directory.extend(entries);
        }
        if directory.len() != stats.blocks as usize {
            return Err(Error::Corrupt("term directory block count mismatch".into()));
        }

        // Highest-bound blocks establish the top-k floor early.
        directory.sort_by(|left, right| {
            block_bound(*right, term_idf, average_length)
                .total_cmp(&block_bound(*left, term_idf, average_length))
                .then_with(|| left.ordinal.cmp(&right.ordinal))
        });
        let mut single_term_scores = Vec::<f32>::new();
        for entry in directory {
            if terms.len() == 1
                && let Some(limit) = limit
                && single_term_scores.len() >= limit
            {
                single_term_scores.sort_by(|left, right| right.total_cmp(left));
                if block_bound(entry, term_idf, average_length) < single_term_scores[limit - 1] {
                    break;
                }
            }
            let record = storage
                .get(term_posting_block_key(
                    namespace,
                    segment,
                    term,
                    entry.ordinal,
                ))
                .await?
                .ok_or_else(|| Error::Corrupt("directory references missing postings".into()))?;
            let postings: Vec<Posting> = decode(&record.value)?;
            if postings.is_empty() || postings.len() > POSTINGS_PER_BLOCK {
                return Err(Error::Corrupt("posting block violates size bound".into()));
            }
            if postings.len() != entry.postings as usize {
                return Err(Error::Corrupt(
                    "posting block count differs from directory".into(),
                ));
            }
            for posting in postings {
                if !allowed_pages
                    .contains(&(posting.address.stream_id, posting.address.page_sequence))
                {
                    continue;
                }
                let hit = Hit {
                    frequency: posting.frequency,
                    length: posting.length,
                    idf: term_idf,
                };
                all_hits.entry(posting.address).or_default().push(hit);
                *matched_terms.entry(posting.address).or_default() += 1;
                if terms.len() == 1 {
                    single_term_scores.push(hit_score(hit, average_length));
                }
            }
        }
    }

    Ok(Some(
        all_hits
            .into_iter()
            .filter(|(address, _)| matched_terms.get(address) == Some(&terms.len()))
            .map(|(address, hits)| (address, score(&hits, average_length)))
            .collect(),
    ))
}

fn block_bound(entry: BlockDirectoryEntry, term_idf: f32, average_length: f32) -> f32 {
    hit_score(
        Hit {
            frequency: entry.max_frequency,
            length: entry.min_length,
            idf: term_idf,
        },
        average_length,
    )
}

#[cfg(test)]
pub(crate) fn exhaustive_scores(
    field: FieldStats,
    terms: &[(TermStats, Vec<Posting>)],
) -> HashMap<DocAddress, f32> {
    if field.documents == 0 || field.total_terms == 0 || terms.is_empty() {
        return HashMap::new();
    }
    let average_length = field.total_terms as f32 / field.documents as f32;
    let mut hits: HashMap<DocAddress, Vec<Hit>> = HashMap::new();
    let mut counts: HashMap<DocAddress, usize> = HashMap::new();
    for (stats, postings) in terms {
        let term_idf = idf(field.documents, stats.documents);
        for posting in postings {
            hits.entry(posting.address).or_default().push(Hit {
                frequency: posting.frequency,
                length: posting.length,
                idf: term_idf,
            });
            *counts.entry(posting.address).or_default() += 1;
        }
    }
    hits.into_iter()
        .filter(|(address, _)| counts.get(address) == Some(&terms.len()))
        .map(|(address, hits)| (address, score(&hits, average_length)))
        .collect()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn posting(document: u64, frequency: u32, length: u32) -> Posting {
        Posting {
            address: DocAddress {
                stream_id: (document / 10_000) as u32,
                page_sequence: document / 100,
                row_id: (document % 100) as u32,
            },
            frequency,
            length,
        }
    }

    proptest! {
        #[test]
        fn block_impact_bounds_every_posting(
            frequencies in prop::collection::vec(1u32..100, 1..POSTINGS_PER_BLOCK),
            lengths in prop::collection::vec(1u32..10_000, 1..POSTINGS_PER_BLOCK),
        ) {
            let count = frequencies.len().min(lengths.len());
            let entry = BlockDirectoryEntry {
                ordinal: 0,
                postings: count as u16,
                max_frequency: *frequencies[..count].iter().max().unwrap(),
                min_length: *lengths[..count].iter().min().unwrap(),
            };
            let bound = block_bound(entry, 1.7, 100.0);
            for index in 0..count {
                let actual = hit_score(Hit {
                    frequency: frequencies[index],
                    length: lengths[index],
                    idf: 1.7,
                }, 100.0);
                prop_assert!(actual <= bound);
            }
        }

        #[test]
        fn blockwise_scoring_equals_exhaustive_reference(
            documents in prop::collection::vec((1u32..20, 1u32..500, any::<bool>(), any::<bool>()), 1..500),
        ) {
            let field = FieldStats {
                documents: documents.len() as u64,
                total_terms: documents.iter().map(|(_, length, _, _)| u64::from(*length)).sum(),
            };
            let first = documents.iter().enumerate()
                .filter(|(_, (_, _, present, _))| *present)
                .map(|(id, (frequency, length, _, _))| posting(id as u64, *frequency, *length))
                .collect::<Vec<_>>();
            let second = documents.iter().enumerate()
                .filter(|(_, (_, _, _, present))| *present)
                .map(|(id, (frequency, length, _, _))| posting(id as u64, *frequency, *length))
                .collect::<Vec<_>>();
            let terms = vec![
                (TermStats { documents: first.len() as u64, blocks: first.len().div_ceil(POSTINGS_PER_BLOCK) as u32 }, first),
                (TermStats { documents: second.len() as u64, blocks: second.len().div_ceil(POSTINGS_PER_BLOCK) as u32 }, second),
            ];
            let expected = exhaustive_scores(field, &terms);

            // A block-at-a-time traversal must produce the exact same hits;
            // chunking is a storage concern and cannot affect arithmetic.
            let blocked = terms.iter().map(|(stats, postings)| {
                let flattened = postings.chunks(POSTINGS_PER_BLOCK)
                    .flat_map(|block| block.iter().copied())
                    .collect::<Vec<_>>();
                (*stats, flattened)
            }).collect::<Vec<_>>();
            let actual = exhaustive_scores(field, &blocked);
            prop_assert_eq!(actual.len(), expected.len());
            for (address, expected_score) in expected {
                prop_assert_eq!(actual[&address].to_bits(), expected_score.to_bits());
            }
        }
    }

    #[test]
    fn tokenizer_and_source_verifier_agree() {
        let terms = query_terms("Error status");
        assert!(source_matches("level=ERROR status=500", &terms));
        assert!(!source_matches("error without code", &terms));
    }

    #[test]
    fn rare_terms_outweigh_common_terms_and_ties_are_exact() {
        let field = FieldStats {
            documents: 100,
            total_terms: 1_000,
        };
        let rare = idf(field.documents, 1);
        let common = idf(field.documents, 90);
        assert!(rare > common);
        let hit = Hit {
            frequency: 2,
            length: 10,
            idf: rare,
        };
        assert_eq!(score(&[hit], 10.0).to_bits(), score(&[hit], 10.0).to_bits());
    }
}
