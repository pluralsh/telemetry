// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Bounded, segment-local full-text records and BM25 scoring.
//!
//! The BM25 arithmetic and block-impact idea are adapted from OpenData's MIT
//! licensed `vector` crate. Logs deliberately uses a different persistence
//! layout: every postings block is an independently addressable SlateDB value,
//! described by bounded directory fragments that each flush writes alongside
//! its blocks, so writes never read the index they extend.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};

use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use common::serde::ensure_consumed;
use common::serde::varint::{var_u32, var_u64};
use common::storage::{RecordOp, StorageRead, Ttl};
use futures::{StreamExt, TryStreamExt};

use crate::Namespace;
use crate::analyzer::Analyzer;
use crate::codec::{
    field_stats_key, term_directory_key, term_directory_prefix, term_posting_block_key,
    term_stats_key,
};
use crate::error::{Error, Result};
use crate::model::{SegmentId, StreamId};

pub(crate) const POSTINGS_PER_BLOCK: usize = 128;
pub(crate) const DIRECTORY_ENTRIES: usize = 256;
/// Low bits of a posting block ID, holding the block's index among one
/// flush's blocks of a term; the high bits are the flush's first object ID.
const BLOCK_INDEX_BITS: u32 = 24;
pub(crate) const SCORE_METADATA_FIELD: &str = "__line_bm25_score";
const K1: f32 = 1.2;
const B: f32 = 0.75;
/// Leading byte of search index values.
const VALUE_FORMAT: u8 = 1;
/// Posting blocks fetched ahead of the scorer. Top-k traversal may discard up
/// to this many reads once the block-max bound terminates it.
const BLOCK_PREFETCH: usize = 16;

/// A row of the `leaf` object's run of `stream_id`, which stays valid when
/// merges move the row into another object.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct DocAddress {
    pub(crate) stream_id: StreamId,
    pub(crate) leaf: u64,
    pub(crate) row_id: u32,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Posting {
    pub(crate) address: DocAddress,
    pub(crate) frequency: u32,
    pub(crate) length: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct FieldStats {
    pub(crate) documents: u64,
    pub(crate) total_terms: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct TermStats {
    pub(crate) documents: u64,
    pub(crate) blocks: u32,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct BlockDirectoryEntry {
    pub(crate) block: u64,
    pub(crate) postings: u16,
    pub(crate) max_frequency: u32,
    pub(crate) min_length: u32,
}

impl BlockDirectoryEntry {
    fn describe(block: u64, postings: &[Posting]) -> Result<Self> {
        Ok(Self {
            block,
            postings: u16::try_from(postings.len())
                .map_err(|_| Error::Invalid("posting block exceeds u16".into()))?,
            max_frequency: postings
                .iter()
                .map(|posting| posting.frequency)
                .max()
                .unwrap_or(0),
            min_length: postings
                .iter()
                .map(|posting| posting.length)
                .min()
                .unwrap_or(0),
        })
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct IndexDelta {
    pub(crate) documents: u64,
    pub(crate) total_terms: u64,
    pub(crate) postings: BTreeMap<String, Vec<Posting>>,
}

impl IndexDelta {
    /// Indexes the rows of `stream_id`'s run in the written object `leaf`.
    pub(crate) fn add_run<'a>(
        &mut self,
        analyzer: &dyn Analyzer,
        stream_id: StreamId,
        leaf: u64,
        lines: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        for (row, line) in lines.into_iter().enumerate() {
            let row_id = u32::try_from(row)
                .map_err(|_| Error::Invalid("run row id exceeds u32".to_owned()))?;
            let frequencies = token_frequencies(analyzer, line);
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
                        leaf,
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

pub(crate) fn token_frequencies(analyzer: &dyn Analyzer, value: &str) -> HashMap<String, u32> {
    let mut frequencies = HashMap::new();
    analyzer.for_each_term(value, &mut |term| {
        let frequency = frequencies.entry(term.to_owned()).or_insert(0u32);
        *frequency = frequency.saturating_add(1);
    });
    frequencies
}

pub(crate) fn query_terms(analyzer: &dyn Analyzer, value: &str) -> Vec<String> {
    let mut terms = BTreeSet::new();
    analyzer.for_each_term(value, &mut |term| {
        terms.insert(term.to_owned());
    });
    terms.into_iter().collect()
}

/// Terms of every line containing `needle`: those of each chunk the needle
/// bounds by ASCII whitespace on both sides. Analyzed together with that
/// whitespace, a chunk yields the same terms as it does inside any line,
/// since neither link detection nor word boundaries reach across ASCII
/// whitespace. The first and last chunks may be parts of longer words, and
/// other Unicode spaces (U+202F) can join words, so neither bounds a chunk.
pub(crate) fn interior_terms(analyzer: &dyn Analyzer, needle: &str) -> Vec<String> {
    let mut terms = BTreeSet::new();
    let mut previous = None;
    for (index, byte) in needle.bytes().enumerate() {
        if !byte.is_ascii_whitespace() {
            continue;
        }
        if let Some(start) = previous
            && index > start + 1
        {
            analyzer.for_each_term(&needle[start..=index], &mut |term| {
                terms.insert(term.to_owned());
            });
        }
        previous = Some(index);
    }
    terms.into_iter().collect()
}

/// `terms` must be analyzed, as produced by [`query_terms`].
pub(crate) fn source_matches(analyzer: &dyn Analyzer, line: &str, terms: &[String]) -> bool {
    if terms.is_empty() {
        return false;
    }
    let mut found = vec![false; terms.len()];
    let mut remaining = terms.len();
    analyzer.for_each_term(line, &mut |term| {
        if remaining == 0 {
            return;
        }
        for (index, expected) in terms.iter().enumerate() {
            if !found[index] && expected == term {
                found[index] = true;
                remaining -= 1;
                break;
            }
        }
    });
    remaining == 0
}

/// A segment's field statistics; `None` when the segment predates the index.
pub(crate) async fn field_stats(
    storage: &dyn StorageRead,
    namespace: &Namespace,
    segment: SegmentId,
) -> Result<Option<FieldStats>> {
    storage
        .get(field_stats_key(namespace, segment))
        .await?
        .map(|record| decode_field_stats(&record.value))
        .transpose()
}

pub(crate) async fn term_stats(
    storage: &dyn StorageRead,
    namespace: &Namespace,
    segment: SegmentId,
    term: &str,
) -> Result<Option<TermStats>> {
    storage
        .get(term_stats_key(namespace, segment, term))
        .await?
        .map(|record| decode_term_stats(&record.value))
        .transpose()
}

/// Every posting of `term`, unscored and in no particular order. `stats`
/// must have been read before the directory, as the term may gain blocks.
pub(crate) async fn term_postings(
    storage: &dyn StorageRead,
    namespace: &Namespace,
    segment: SegmentId,
    term: &str,
    stats: TermStats,
) -> Result<Vec<Posting>> {
    let directory = load_directory(storage, namespace, segment, term, stats).await?;
    let blocks: Vec<Vec<Posting>> = futures::stream::iter(directory)
        .map(|entry| load_block(storage, namespace, segment, term, entry))
        .buffer_unordered(BLOCK_PREFETCH)
        .try_collect()
        .await?;
    Ok(blocks.into_iter().flatten().collect())
}

pub(crate) fn encode_field_stats(stats: FieldStats) -> Bytes {
    let mut buf = value_buffer(18);
    var_u64::serialize(stats.documents, &mut buf);
    var_u64::serialize(stats.total_terms, &mut buf);
    buf.freeze()
}

pub(crate) fn decode_field_stats(value: &[u8]) -> Result<FieldStats> {
    let mut buf = binary_body(value)?;
    let stats = FieldStats {
        documents: var_u64::deserialize(&mut buf)?,
        total_terms: var_u64::deserialize(&mut buf)?,
    };
    ensure_consumed(buf, "search value")?;
    Ok(stats)
}

pub(crate) fn encode_term_stats(stats: TermStats) -> Bytes {
    let mut buf = value_buffer(14);
    var_u64::serialize(stats.documents, &mut buf);
    var_u32::serialize(stats.blocks, &mut buf);
    buf.freeze()
}

pub(crate) fn decode_term_stats(value: &[u8]) -> Result<TermStats> {
    let mut buf = binary_body(value)?;
    let stats = TermStats {
        documents: var_u64::deserialize(&mut buf)?,
        blocks: var_u32::deserialize(&mut buf)?,
    };
    ensure_consumed(buf, "search value")?;
    Ok(stats)
}

/// Entries must have strictly increasing block IDs; they are delta-encoded.
pub(crate) fn encode_directory(entries: &[BlockDirectoryEntry]) -> Result<Bytes> {
    let mut buf = value_buffer(5 + entries.len() * 12);
    var_u32::serialize(count_u32(entries.len())?, &mut buf);
    let mut previous: Option<u64> = None;
    for entry in entries {
        let delta = match previous {
            None => entry.block,
            Some(previous) if entry.block > previous => entry.block - previous,
            Some(_) => return Err(Error::Invalid("directory block IDs must increase".into())),
        };
        previous = Some(entry.block);
        var_u64::serialize(delta, &mut buf);
        var_u32::serialize(u32::from(entry.postings), &mut buf);
        var_u32::serialize(entry.max_frequency, &mut buf);
        var_u32::serialize(entry.min_length, &mut buf);
    }
    Ok(buf.freeze())
}

pub(crate) fn decode_directory(value: &[u8]) -> Result<Vec<BlockDirectoryEntry>> {
    let mut buf = binary_body(value)?;
    let count = read_count(&mut buf, DIRECTORY_ENTRIES, "term directory")?;
    let mut entries = Vec::with_capacity(count);
    let mut block = 0u64;
    for index in 0..count {
        let delta = var_u64::deserialize(&mut buf)?;
        block = if index == 0 {
            delta
        } else {
            block
                .checked_add(delta)
                .filter(|_| delta > 0)
                .ok_or_else(|| Error::Corrupt("directory block IDs out of order".into()))?
        };
        entries.push(BlockDirectoryEntry {
            block,
            postings: u16::try_from(var_u32::deserialize(&mut buf)?)
                .map_err(|_| Error::Corrupt("directory posting count exceeds u16".into()))?,
            max_frequency: var_u32::deserialize(&mut buf)?,
            min_length: var_u32::deserialize(&mut buf)?,
        });
    }
    ensure_consumed(buf, "search value")?;
    Ok(entries)
}

/// Postings must be strictly ordered by address. Each address is encoded
/// relative to its predecessor: a stream delta, then either an absolute leaf
/// and row (new stream) or a leaf delta, then either an absolute row (new
/// leaf) or a row delta.
pub(crate) fn encode_postings(postings: &[Posting]) -> Result<Bytes> {
    let mut buf = value_buffer(5 + postings.len() * 6);
    var_u32::serialize(count_u32(postings.len())?, &mut buf);
    let mut previous = DocAddress {
        stream_id: 0,
        leaf: 0,
        row_id: 0,
    };
    for (index, posting) in postings.iter().enumerate() {
        let address = posting.address;
        if index > 0 && address <= previous {
            return Err(Error::Invalid(
                "posting block must be strictly ordered by address".into(),
            ));
        }
        let stream_delta = address.stream_id - previous.stream_id;
        var_u32::serialize(stream_delta, &mut buf);
        if stream_delta != 0 {
            var_u64::serialize(address.leaf, &mut buf);
            var_u32::serialize(address.row_id, &mut buf);
        } else {
            let leaf_delta = address.leaf - previous.leaf;
            var_u64::serialize(leaf_delta, &mut buf);
            let row = if leaf_delta == 0 {
                address.row_id - previous.row_id
            } else {
                address.row_id
            };
            var_u32::serialize(row, &mut buf);
        }
        var_u32::serialize(posting.frequency, &mut buf);
        var_u32::serialize(posting.length, &mut buf);
        previous = address;
    }
    Ok(buf.freeze())
}

pub(crate) fn decode_postings(value: &[u8]) -> Result<Vec<Posting>> {
    let mut buf = binary_body(value)?;
    let count = read_count(&mut buf, POSTINGS_PER_BLOCK, "posting block")?;
    let mut postings = Vec::with_capacity(count);
    let mut address = DocAddress {
        stream_id: 0,
        leaf: 0,
        row_id: 0,
    };
    let overflow = || Error::Corrupt("posting address overflow".into());
    for _ in 0..count {
        let stream_delta = var_u32::deserialize(&mut buf)?;
        if stream_delta != 0 {
            address.stream_id = address
                .stream_id
                .checked_add(stream_delta)
                .ok_or_else(overflow)?;
            address.leaf = var_u64::deserialize(&mut buf)?;
            address.row_id = var_u32::deserialize(&mut buf)?;
        } else {
            let leaf_delta = var_u64::deserialize(&mut buf)?;
            let row = var_u32::deserialize(&mut buf)?;
            if leaf_delta != 0 {
                address.leaf = address.leaf.checked_add(leaf_delta).ok_or_else(overflow)?;
                address.row_id = row;
            } else {
                address.row_id = address.row_id.checked_add(row).ok_or_else(overflow)?;
            }
        }
        postings.push(Posting {
            address,
            frequency: var_u32::deserialize(&mut buf)?,
            length: var_u32::deserialize(&mut buf)?,
        });
    }
    ensure_consumed(buf, "search value")?;
    Ok(postings)
}

fn value_buffer(capacity: usize) -> BytesMut {
    let mut buf = BytesMut::with_capacity(1 + capacity);
    buf.put_u8(VALUE_FORMAT);
    buf
}

/// Returns the payload after the format byte.
fn binary_body(value: &[u8]) -> Result<&[u8]> {
    match value.first() {
        Some(&VALUE_FORMAT) => Ok(&value[1..]),
        Some(format) => Err(Error::Corrupt(format!(
            "unknown search value format {format}"
        ))),
        None => Err(Error::Corrupt("empty search value".into())),
    }
}

fn read_count(buf: &mut &[u8], bound: usize, what: &str) -> Result<usize> {
    let count = var_u32::deserialize(buf)? as usize;
    if count > bound {
        return Err(Error::Corrupt(format!("{what} exceeds bound")));
    }
    Ok(count)
}

fn count_u32(count: usize) -> Result<u32> {
    u32::try_from(count).map_err(|_| Error::Invalid("search value count exceeds u32".into()))
}

/// Appends the records that add one flush's `postings` of `term` to the
/// segment's index, without reading it.
///
/// Every block is new: its ID combines `flush_object`, the first object ID
/// the flush allocated in the segment, with its index among the flush's
/// blocks of the term, so IDs never collide and increase across flushes.
/// The flush's directory entries form new fragments keyed by their first
/// block, and the term's statistics grow by a merge operand.
pub(crate) fn term_index_ops(
    ops: &mut Vec<RecordOp>,
    (namespace, segment): (&Namespace, SegmentId),
    term: &str,
    mut postings: Vec<Posting>,
    flush_object: u64,
    ttl: Ttl,
) -> Result<()> {
    postings.sort_unstable_by_key(|posting| posting.address);
    let first_block = flush_object
        .checked_mul(1 << BLOCK_INDEX_BITS)
        .ok_or_else(|| Error::Invalid("object ID exceeds the block ID space".into()))?;
    let mut entries = Vec::with_capacity(postings.len().div_ceil(POSTINGS_PER_BLOCK));
    for (index, chunk) in postings.chunks(POSTINGS_PER_BLOCK).enumerate() {
        if index >> BLOCK_INDEX_BITS != 0 {
            return Err(Error::Invalid("flush exceeds a term's block IDs".into()));
        }
        let block = first_block | index as u64;
        entries.push(BlockDirectoryEntry::describe(block, chunk)?);
        ops.push(RecordOp::put_with_ttl(
            term_posting_block_key(namespace, segment, term, block),
            encode_postings(chunk)?,
            ttl,
        ));
    }
    for fragment in entries.chunks(DIRECTORY_ENTRIES) {
        ops.push(RecordOp::put_with_ttl(
            term_directory_key(namespace, segment, term, fragment[0].block),
            encode_directory(fragment)?,
            ttl,
        ));
    }
    ops.push(RecordOp::merge_with_ttl(
        term_stats_key(namespace, segment, term),
        encode_term_stats(TermStats {
            documents: postings.len() as u64,
            blocks: count_u32(entries.len())?,
        }),
        ttl,
    ));
    Ok(())
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

#[derive(Clone, Copy, PartialEq)]
struct Score(f32);

impl Eq for Score {}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

struct TermQuery<'a> {
    term: &'a str,
    stats: TermStats,
    idf: f32,
}

/// Loads compact directories first and fetches posting values only as needed.
///
/// Single-term top-k queries use block maxima to stop once every remaining
/// block is unable to beat the current floor. Multi-term queries retain the
/// same exact scorer but load every participating block because independently
/// chunked term ranges need not align; terms are visited rarest first so later
/// terms only extend documents that already matched every earlier term.
pub(crate) async fn block_max_scores(
    storage: &dyn StorageRead,
    namespace: &Namespace,
    segment: SegmentId,
    terms: &[String],
    allowed_leaves: &HashSet<(StreamId, u64)>,
    limit: Option<usize>,
) -> Result<Option<HashMap<DocAddress, f32>>> {
    if allowed_leaves.is_empty() {
        return Ok(Some(HashMap::new()));
    }
    let Some(field) = field_stats(storage, namespace, segment).await? else {
        return Ok(None);
    };
    if field.documents == 0 || field.total_terms == 0 || terms.is_empty() {
        return Ok(Some(HashMap::new()));
    }
    let average_length = field.total_terms as f32 / field.documents as f32;

    let stats = futures::future::try_join_all(
        terms
            .iter()
            .map(|term| term_stats(storage, namespace, segment, term)),
    )
    .await?;
    let mut queries = Vec::with_capacity(terms.len());
    for (term, stats) in terms.iter().zip(stats) {
        match stats {
            Some(stats) if stats.documents > 0 => queries.push(TermQuery {
                term,
                stats,
                idf: idf(field.documents, stats.documents),
            }),
            _ => return Ok(Some(HashMap::new())),
        }
    }

    if let ([query], Some(limit)) = (queries.as_slice(), limit) {
        return single_term_top_k(
            storage,
            SearchSegment { namespace, segment },
            query,
            allowed_leaves,
            limit,
            average_length,
        )
        .await
        .map(Some);
    }

    queries.sort_by_key(|query| query.stats.documents);
    let mut candidates: HashMap<DocAddress, Vec<Hit>> = HashMap::new();
    for (index, query) in queries.iter().enumerate() {
        let directory =
            load_directory(storage, namespace, segment, query.term, query.stats).await?;
        let mut blocks = std::pin::pin!(
            futures::stream::iter(directory)
                .map(|entry| load_block(storage, namespace, segment, query.term, entry))
                .buffer_unordered(BLOCK_PREFETCH)
        );
        while let Some(postings) = blocks.try_next().await? {
            for posting in postings {
                let leaf = (posting.address.stream_id, posting.address.leaf);
                if allowed_leaves.contains(&leaf) {
                    let hit = Hit {
                        frequency: posting.frequency,
                        length: posting.length,
                        idf: query.idf,
                    };
                    add_hit(&mut candidates, index, posting.address, hit);
                }
            }
        }
        if index > 0 {
            candidates.retain(|_, hits| hits.len() == index + 1);
        }
        if candidates.is_empty() {
            break;
        }
    }

    Ok(Some(
        candidates
            .into_iter()
            .map(|(address, hits)| (address, score(&hits, average_length)))
            .collect(),
    ))
}

/// Records `hit` for the `index`-th term in rarest-first order. The first
/// term seeds candidates; later terms only extend documents that matched
/// every earlier term.
fn add_hit(
    candidates: &mut HashMap<DocAddress, Vec<Hit>>,
    index: usize,
    address: DocAddress,
    hit: Hit,
) {
    if index == 0 {
        candidates.entry(address).or_default().push(hit);
    } else if let Some(hits) = candidates.get_mut(&address)
        && hits.len() == index
    {
        hits.push(hit);
    }
}

#[derive(Clone, Copy)]
struct SearchSegment<'a> {
    namespace: &'a Namespace,
    segment: SegmentId,
}

async fn single_term_top_k(
    storage: &dyn StorageRead,
    scope: SearchSegment<'_>,
    query: &TermQuery<'_>,
    allowed_leaves: &HashSet<(StreamId, u64)>,
    limit: usize,
    average_length: f32,
) -> Result<HashMap<DocAddress, f32>> {
    let mut directory = load_directory(
        storage,
        scope.namespace,
        scope.segment,
        query.term,
        query.stats,
    )
    .await?;
    // Highest-bound blocks establish the top-k floor early.
    directory.sort_by(|left, right| {
        block_bound(*right, query.idf, average_length)
            .total_cmp(&block_bound(*left, query.idf, average_length))
            .then_with(|| left.block.cmp(&right.block))
    });

    let mut scores = HashMap::new();
    let mut floor: BinaryHeap<Reverse<Score>> = BinaryHeap::with_capacity(limit + 1);
    let mut blocks = std::pin::pin!(
        futures::stream::iter(directory)
            .map(|entry| async move {
                let postings =
                    load_block(storage, scope.namespace, scope.segment, query.term, entry).await?;
                Ok::<_, Error>((entry, postings))
            })
            .buffered(BLOCK_PREFETCH)
    );
    while let Some((entry, postings)) = blocks.try_next().await? {
        if floor.len() >= limit
            && let Some(Reverse(Score(kth))) = floor.peek()
            && block_bound(entry, query.idf, average_length) < *kth
        {
            break;
        }
        for posting in postings {
            if !allowed_leaves.contains(&(posting.address.stream_id, posting.address.leaf)) {
                continue;
            }
            let score = hit_score(
                Hit {
                    frequency: posting.frequency,
                    length: posting.length,
                    idf: query.idf,
                },
                average_length,
            );
            scores.insert(posting.address, score);
            floor.push(Reverse(Score(score)));
            if floor.len() > limit {
                floor.pop();
            }
        }
    }
    Ok(scores)
}

async fn load_directory(
    storage: &dyn StorageRead,
    namespace: &Namespace,
    segment: SegmentId,
    term: &str,
    stats: TermStats,
) -> Result<Vec<BlockDirectoryEntry>> {
    let mut fragments = storage
        .scan_prefix_iter(
            term_directory_prefix(namespace, segment, term),
            BytesRange::unbounded(),
            None,
        )
        .await?;
    let mut directory = Vec::with_capacity(stats.blocks as usize);
    while let Some(record) = fragments.next().await? {
        directory.extend(decode_directory(&record.value)?);
    }
    // Flushes after `stats` was read may have added fragments, never removed.
    if directory.len() < stats.blocks as usize {
        return Err(Error::Corrupt(
            "term stats reference missing directory".into(),
        ));
    }
    Ok(directory)
}

async fn load_block(
    storage: &dyn StorageRead,
    namespace: &Namespace,
    segment: SegmentId,
    term: &str,
    entry: BlockDirectoryEntry,
) -> Result<Vec<Posting>> {
    let record = storage
        .get(term_posting_block_key(
            namespace,
            segment,
            term,
            entry.block,
        ))
        .await?
        .ok_or_else(|| Error::Corrupt("directory references missing postings".into()))?;
    let postings = decode_postings(&record.value)?;
    if postings.is_empty() || postings.len() > POSTINGS_PER_BLOCK {
        return Err(Error::Corrupt("posting block violates size bound".into()));
    }
    if postings.len() != entry.postings as usize {
        return Err(Error::Corrupt(
            "posting block count differs from directory".into(),
        ));
    }
    Ok(postings)
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
    use crate::analyzer::DEFAULT_ANALYZER;

    fn posting(document: u64, frequency: u32, length: u32) -> Posting {
        Posting {
            address: DocAddress {
                stream_id: (document / 10_000) as u32,
                leaf: document / 100,
                row_id: (document % 100) as u32,
            },
            frequency,
            length,
        }
    }

    #[test]
    fn term_index_ops_name_blocks_and_fragments_by_flush() {
        let namespace = Namespace::default();
        let postings: Vec<_> = (0..(POSTINGS_PER_BLOCK * DIRECTORY_ENTRIES + 1) as u64)
            .map(|document| posting(document, 1, 1))
            .collect();
        let mut ops = Vec::new();
        term_index_ops(
            &mut ops,
            (&namespace, 0),
            "error",
            postings,
            3,
            Ttl::NoExpiry,
        )
        .unwrap();

        let first = 3 << BLOCK_INDEX_BITS;
        let mut blocks = 0;
        let mut fragments = Vec::new();
        let mut stats = None;
        for op in ops {
            match op {
                RecordOp::Put(put)
                    if put
                        .record
                        .key
                        .starts_with(&term_directory_prefix(&namespace, 0, "error")) =>
                {
                    fragments.push((put.record.key, decode_directory(&put.record.value).unwrap()));
                }
                RecordOp::Put(_) => blocks += 1,
                RecordOp::Merge(merge) => {
                    assert_eq!(merge.record.key, term_stats_key(&namespace, 0, "error"));
                    stats = Some(decode_term_stats(&merge.record.value).unwrap());
                }
                RecordOp::Delete(_) => unreachable!(),
            }
        }
        assert_eq!(blocks, DIRECTORY_ENTRIES + 1);
        assert_eq!(
            fragments
                .iter()
                .map(|(key, entries)| (key.clone(), entries.len()))
                .collect::<Vec<_>>(),
            vec![
                (
                    term_directory_key(&namespace, 0, "error", first),
                    DIRECTORY_ENTRIES
                ),
                (
                    term_directory_key(&namespace, 0, "error", first | DIRECTORY_ENTRIES as u64),
                    1
                ),
            ]
        );
        assert_eq!(fragments[1].1[0].block, first | DIRECTORY_ENTRIES as u64);
        let stats = stats.unwrap();
        assert_eq!(stats.blocks as usize, DIRECTORY_ENTRIES + 1);
        assert_eq!(
            stats.documents as usize,
            POSTINGS_PER_BLOCK * DIRECTORY_ENTRIES + 1
        );
    }

    fn address_strategy() -> impl Strategy<Value = DocAddress> {
        (0u32..8, prop_oneof![0u64..4, any::<u64>()], any::<u32>()).prop_map(
            |(stream_id, leaf, row_id)| DocAddress {
                stream_id,
                leaf,
                row_id,
            },
        )
    }

    proptest! {
        #[test]
        fn block_impact_bounds_every_posting(
            frequencies in prop::collection::vec(1u32..100, 1..POSTINGS_PER_BLOCK),
            lengths in prop::collection::vec(1u32..10_000, 1..POSTINGS_PER_BLOCK),
        ) {
            let count = frequencies.len().min(lengths.len());
            let entry = BlockDirectoryEntry {
                block: 0,
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

        #[test]
        fn posting_blocks_roundtrip(
            addresses in prop::collection::btree_set(address_strategy(), 0..POSTINGS_PER_BLOCK),
            frequency in any::<u32>(),
            length in any::<u32>(),
        ) {
            let postings = addresses
                .into_iter()
                .map(|address| Posting { address, frequency, length })
                .collect::<Vec<_>>();
            let encoded = encode_postings(&postings).unwrap();
            prop_assert_eq!(decode_postings(&encoded).unwrap(), postings);
        }

        #[test]
        fn directories_roundtrip(
            blocks in prop::collection::btree_set(any::<u64>(), 0..DIRECTORY_ENTRIES),
            postings in 1u16..=POSTINGS_PER_BLOCK as u16,
            max_frequency in any::<u32>(),
            min_length in any::<u32>(),
        ) {
            let entries = blocks
                .into_iter()
                .map(|block| BlockDirectoryEntry { block, postings, max_frequency, min_length })
                .collect::<Vec<_>>();
            let encoded = encode_directory(&entries).unwrap();
            prop_assert_eq!(decode_directory(&encoded).unwrap(), entries);
        }

        #[test]
        fn source_matches_agrees_with_token_frequencies(
            line in "[a-zA-Z0-9 =_ÄÖÜäöü-]{0,64}",
            query in "[a-zA-Z0-9 ÄÖÜäöü]{0,16}",
        ) {
            let terms = query_terms(&DEFAULT_ANALYZER, &query);
            let tokens = token_frequencies(&DEFAULT_ANALYZER, &line);
            let expected = !terms.is_empty() && terms.iter().all(|term| tokens.contains_key(term));
            prop_assert_eq!(source_matches(&DEFAULT_ANALYZER, &line, &terms), expected);
        }
    }

    proptest! {
        #[test]
        fn lines_containing_a_needle_hold_its_interior_terms(
            prefix in "(\\PC|[ \t\n.@:/_'\"-]|https?://|\u{301}|\u{200D}|\u{202F}|\u{3000}){0,12}",
            needle in "(\\PC|[ \t\n.@:/_'\"-]|https?://|a@b\\.c|\u{301}|\u{200D}|\u{202F}|\u{3000}|[a-z]{1,4}){0,24}",
            suffix in "(\\PC|[ \t\n.@:/_'\"-]|https?://|\u{301}|\u{200D}|\u{202F}|\u{3000}){0,12}",
        ) {
            let line = format!("{prefix}{needle}{suffix}");
            let tokens = token_frequencies(&DEFAULT_ANALYZER, &line);
            for term in interior_terms(&DEFAULT_ANALYZER, &needle) {
                prop_assert!(tokens.contains_key(&term), "{term:?} of {needle:?} not in {line:?}");
            }
        }
    }

    #[test]
    fn interior_terms_skip_the_edge_chunks() {
        let terms = |needle| interior_terms(&DEFAULT_ANALYZER, needle);
        assert!(terms("error").is_empty());
        assert!(terms("level=error status").is_empty());
        assert!(terms(" error").is_empty());
        assert_eq!(terms("ab GET /Api/v1 cd"), ["api", "get", "v1"]);
        assert_eq!(terms("x  user@Example.com\ty"), ["user@example.com"]);
        assert_eq!(terms(" a\u{202F}b "), ["a\u{202F}b"]);
    }

    #[test]
    fn stats_roundtrip() {
        let field = FieldStats {
            documents: 12,
            total_terms: u64::MAX,
        };
        assert_eq!(
            decode_field_stats(&encode_field_stats(field)).unwrap(),
            field
        );
        let term = TermStats {
            documents: 3,
            blocks: 7,
        };
        assert_eq!(decode_term_stats(&encode_term_stats(term)).unwrap(), term);
    }

    #[test]
    fn rejects_values_without_the_format_byte() {
        assert!(decode_field_stats(br#"{"documents":2,"total_terms":9}"#).is_err());
        assert!(decode_directory(b"[]").is_err());
        assert!(decode_postings(&[]).is_err());
    }

    #[test]
    fn binary_posting_blocks_are_compact() {
        let postings = (0..POSTINGS_PER_BLOCK as u64)
            .map(|document| posting(document * 3, 1, 12))
            .collect::<Vec<_>>();
        let encoded = encode_postings(&postings).unwrap();
        assert!(
            encoded.len() <= 1 + 2 + postings.len() * 5,
            "{} bytes",
            encoded.len()
        );
    }

    #[test]
    fn unordered_postings_are_rejected() {
        let postings = [posting(5, 1, 1), posting(4, 1, 1)];
        assert!(encode_postings(&postings).is_err());
    }

    #[test]
    fn analyzer_and_source_verifier_agree() {
        let terms = query_terms(&DEFAULT_ANALYZER, "Error status");
        assert!(source_matches(
            &DEFAULT_ANALYZER,
            "level=ERROR status=500",
            &terms
        ));
        assert!(!source_matches(
            &DEFAULT_ANALYZER,
            "error without code",
            &terms
        ));
    }

    #[test]
    fn analyzer_preserves_urls_and_email_addresses() {
        let terms = query_terms(
            &DEFAULT_ANALYZER,
            "Visit https://Example.com/a?x=1, or email User@Example.com.",
        );
        assert_eq!(
            terms,
            [
                "email",
                "https://example.com/a?x=1",
                "or",
                "user@example.com",
                "visit",
            ]
        );
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
