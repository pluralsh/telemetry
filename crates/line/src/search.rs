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

use std::borrow::Cow;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};

use bytes::{BufMut, Bytes, BytesMut};
use common::serde::varint::{var_u32, var_u64};
use common::storage::Storage;
use futures::{StreamExt, TryStreamExt};

use crate::Namespace;
use crate::codec::{field_stats_key, term_directory_key, term_posting_block_key, term_stats_key};
use crate::error::{Error, Result};
use crate::model::{SegmentId, StreamId};

pub(crate) const POSTINGS_PER_BLOCK: usize = 128;
pub(crate) const DIRECTORY_ENTRIES: usize = 256;
pub(crate) const SCORE_METADATA_FIELD: &str = "__line_bm25_score";
const K1: f32 = 1.2;
const B: f32 = 0.75;
/// Leading byte of search index values.
const VALUE_FORMAT: u8 = 1;
/// Posting blocks fetched ahead of the scorer. Top-k traversal may discard up
/// to this many reads once the block-max bound terminates it.
const BLOCK_PREFETCH: usize = 16;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct DocAddress {
    pub(crate) stream_id: StreamId,
    pub(crate) page_sequence: u64,
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
    pub(crate) ordinal: u32,
    pub(crate) postings: u16,
    pub(crate) max_frequency: u32,
    pub(crate) min_length: u32,
}

impl BlockDirectoryEntry {
    fn describe(ordinal: u32, postings: &[Posting]) -> Result<Self> {
        Ok(Self {
            ordinal,
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
    pub(crate) fn add_page<'a>(
        &mut self,
        stream_id: StreamId,
        page_sequence: u64,
        lines: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        for (row, line) in lines.into_iter().enumerate() {
            let row_id = u32::try_from(row)
                .map_err(|_| Error::Invalid("page row id exceeds u32".to_owned()))?;
            let frequencies = token_frequencies(line);
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

fn tokens(value: &str) -> impl Iterator<Item = &str> {
    value
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
}

/// Equivalent to `str::to_lowercase`, borrowing when the term is already
/// lowercase ASCII.
fn normalize(term: &str) -> Cow<'_, str> {
    if term.is_ascii() && !term.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Cow::Borrowed(term)
    } else {
        Cow::Owned(term.to_lowercase())
    }
}

pub(crate) fn token_frequencies(value: &str) -> HashMap<String, u32> {
    let mut frequencies = HashMap::new();
    for term in tokens(value) {
        let frequency = frequencies
            .entry(normalize(term).into_owned())
            .or_insert(0u32);
        *frequency = frequency.saturating_add(1);
    }
    frequencies
}

pub(crate) fn query_terms(value: &str) -> Vec<String> {
    tokens(value)
        .map(|term| normalize(term).into_owned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// `terms` must be normalized, as produced by [`query_terms`].
pub(crate) fn source_matches(line: &str, terms: &[String]) -> bool {
    if terms.is_empty() {
        return false;
    }
    let mut found = vec![false; terms.len()];
    let mut remaining = terms.len();
    for token in tokens(line) {
        let token = normalize(token);
        for (index, term) in terms.iter().enumerate() {
            if !found[index] && *term == token {
                found[index] = true;
                remaining -= 1;
                if remaining == 0 {
                    return true;
                }
            }
        }
    }
    false
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
        documents: read_u64(&mut buf)?,
        total_terms: read_u64(&mut buf)?,
    };
    expect_consumed(buf)?;
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
        documents: read_u64(&mut buf)?,
        blocks: read_u32(&mut buf)?,
    };
    expect_consumed(buf)?;
    Ok(stats)
}

/// Entries must have strictly increasing ordinals; they are delta-encoded.
pub(crate) fn encode_directory(entries: &[BlockDirectoryEntry]) -> Result<Bytes> {
    let mut buf = value_buffer(5 + entries.len() * 8);
    var_u32::serialize(count_u32(entries.len())?, &mut buf);
    let mut previous: Option<u32> = None;
    for entry in entries {
        let delta = match previous {
            None => entry.ordinal,
            Some(previous) if entry.ordinal > previous => entry.ordinal - previous,
            Some(_) => return Err(Error::Invalid("directory ordinals must increase".into())),
        };
        previous = Some(entry.ordinal);
        var_u32::serialize(delta, &mut buf);
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
    let mut ordinal = 0u32;
    for index in 0..count {
        let delta = read_u32(&mut buf)?;
        ordinal = if index == 0 {
            delta
        } else {
            ordinal
                .checked_add(delta)
                .ok_or_else(|| Error::Corrupt("directory ordinal overflow".into()))?
        };
        entries.push(BlockDirectoryEntry {
            ordinal,
            postings: u16::try_from(read_u32(&mut buf)?)
                .map_err(|_| Error::Corrupt("directory posting count exceeds u16".into()))?,
            max_frequency: read_u32(&mut buf)?,
            min_length: read_u32(&mut buf)?,
        });
    }
    expect_consumed(buf)?;
    Ok(entries)
}

/// Postings must be strictly ordered by address. Each address is encoded
/// relative to its predecessor: a stream delta, then either an absolute page
/// sequence and row (new stream) or a page delta, then either an absolute row
/// (new page) or a row delta.
pub(crate) fn encode_postings(postings: &[Posting]) -> Result<Bytes> {
    let mut buf = value_buffer(5 + postings.len() * 6);
    var_u32::serialize(count_u32(postings.len())?, &mut buf);
    let mut previous = DocAddress {
        stream_id: 0,
        page_sequence: 0,
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
            var_u64::serialize(address.page_sequence, &mut buf);
            var_u32::serialize(address.row_id, &mut buf);
        } else {
            let page_delta = address.page_sequence - previous.page_sequence;
            var_u64::serialize(page_delta, &mut buf);
            let row = if page_delta == 0 {
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
        page_sequence: 0,
        row_id: 0,
    };
    let overflow = || Error::Corrupt("posting address overflow".into());
    for _ in 0..count {
        let stream_delta = read_u32(&mut buf)?;
        if stream_delta != 0 {
            address.stream_id = address
                .stream_id
                .checked_add(stream_delta)
                .ok_or_else(overflow)?;
            address.page_sequence = read_u64(&mut buf)?;
            address.row_id = read_u32(&mut buf)?;
        } else {
            let page_delta = read_u64(&mut buf)?;
            let row = read_u32(&mut buf)?;
            if page_delta != 0 {
                address.page_sequence = address
                    .page_sequence
                    .checked_add(page_delta)
                    .ok_or_else(overflow)?;
                address.row_id = row;
            } else {
                address.row_id = address.row_id.checked_add(row).ok_or_else(overflow)?;
            }
        }
        postings.push(Posting {
            address,
            frequency: read_u32(&mut buf)?,
            length: read_u32(&mut buf)?,
        });
    }
    expect_consumed(buf)?;
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

fn read_u32(buf: &mut &[u8]) -> Result<u32> {
    var_u32::deserialize(buf).map_err(|error| Error::Corrupt(error.message))
}

fn read_u64(buf: &mut &[u8]) -> Result<u64> {
    var_u64::deserialize(buf).map_err(|error| Error::Corrupt(error.message))
}

fn read_count(buf: &mut &[u8], bound: usize, what: &str) -> Result<usize> {
    let count = read_u32(buf)? as usize;
    if count > bound {
        return Err(Error::Corrupt(format!("{what} exceeds bound")));
    }
    Ok(count)
}

fn count_u32(count: usize) -> Result<u32> {
    u32::try_from(count).map_err(|_| Error::Invalid("search value count exceeds u32".into()))
}

fn expect_consumed(buf: &[u8]) -> Result<()> {
    if buf.is_empty() {
        Ok(())
    } else {
        Err(Error::Corrupt("trailing bytes in search value".into()))
    }
}

/// Produces the key/value writes that append `postings` to one term's index.
///
/// A partially filled trailing block is topped up before new blocks are
/// allocated, so a stream of small writes does not leave one tiny block (and
/// one query-time read) per write.
pub(crate) async fn term_index_writes(
    storage: &dyn Storage,
    namespace: &Namespace,
    segment: SegmentId,
    term: String,
    mut postings: Vec<Posting>,
) -> Result<Vec<(Bytes, Bytes)>> {
    postings.sort_unstable_by_key(|posting| posting.address);
    let stats_key = term_stats_key(namespace, segment, &term);
    let mut stats = storage
        .get(stats_key.clone())
        .await?
        .map(|record| decode_term_stats(&record.value))
        .transpose()?
        .unwrap_or_default();
    stats.documents = stats
        .documents
        .checked_add(postings.len() as u64)
        .ok_or_else(|| Error::Invalid("term document count overflow".into()))?;

    let mut writes = Vec::new();
    let mut directories: BTreeMap<u32, Vec<BlockDirectoryEntry>> = BTreeMap::new();
    let mut remaining = postings.as_slice();

    if let Some(tail_ordinal) = stats.blocks.checked_sub(1) {
        let page = directory_page(tail_ordinal);
        let mut entries = storage
            .get(term_directory_key(namespace, segment, &term, page))
            .await?
            .map(|record| decode_directory(&record.value))
            .transpose()?
            .unwrap_or_default();
        if entries.len() > DIRECTORY_ENTRIES {
            return Err(Error::Corrupt("term directory exceeds bound".into()));
        }
        if let Some(tail) = entries.last_mut()
            && tail.ordinal == tail_ordinal
            && usize::from(tail.postings) < POSTINGS_PER_BLOCK
        {
            let block_key = term_posting_block_key(namespace, segment, &term, tail_ordinal);
            let mut block = storage
                .get(block_key.clone())
                .await?
                .map(|record| decode_postings(&record.value))
                .transpose()?
                .ok_or_else(|| Error::Corrupt("directory references missing postings".into()))?;
            let take = (POSTINGS_PER_BLOCK - block.len()).min(remaining.len());
            block.extend_from_slice(&remaining[..take]);
            block.sort_unstable_by_key(|posting| posting.address);
            remaining = &remaining[take..];
            *tail = BlockDirectoryEntry::describe(tail_ordinal, &block)?;
            writes.push((block_key, encode_postings(&block)?));
        }
        directories.insert(page, entries);
    }

    for chunk in remaining.chunks(POSTINGS_PER_BLOCK) {
        let ordinal = stats.blocks;
        stats.blocks = stats
            .blocks
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("term block ordinal overflow".into()))?;
        // Directory pages fill in ordinal order, so any page other than the
        // tail's (loaded above) is new.
        let entries = directories.entry(directory_page(ordinal)).or_default();
        if entries.len() >= DIRECTORY_ENTRIES {
            return Err(Error::Corrupt("term directory ordinal is full".into()));
        }
        entries.push(BlockDirectoryEntry::describe(ordinal, chunk)?);
        writes.push((
            term_posting_block_key(namespace, segment, &term, ordinal),
            encode_postings(chunk)?,
        ));
    }
    for (page, entries) in directories {
        writes.push((
            term_directory_key(namespace, segment, &term, page),
            encode_directory(&entries)?,
        ));
    }
    writes.push((stats_key, encode_term_stats(stats)));
    Ok(writes)
}

fn directory_page(ordinal: u32) -> u32 {
    ordinal / DIRECTORY_ENTRIES as u32
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
    storage: &dyn Storage,
    namespace: &Namespace,
    segment: SegmentId,
    terms: &[String],
    allowed_pages: &HashSet<(StreamId, u64)>,
    limit: Option<usize>,
) -> Result<Option<HashMap<DocAddress, f32>>> {
    if allowed_pages.is_empty() {
        return Ok(Some(HashMap::new()));
    }
    let Some(record) = storage.get(field_stats_key(namespace, segment)).await? else {
        return Ok(None);
    };
    let field = decode_field_stats(&record.value)?;
    if field.documents == 0 || field.total_terms == 0 || terms.is_empty() {
        return Ok(Some(HashMap::new()));
    }
    let average_length = field.total_terms as f32 / field.documents as f32;

    let stats = futures::future::try_join_all(terms.iter().map(|term| async move {
        storage
            .get(term_stats_key(namespace, segment, term))
            .await?
            .map(|record| decode_term_stats(&record.value))
            .transpose()
    }))
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
            namespace,
            segment,
            query,
            allowed_pages,
            limit,
            average_length,
        )
        .await
        .map(Some);
    }

    queries.sort_by_key(|query| query.stats.documents);
    let mut candidates: HashMap<DocAddress, Vec<Hit>> = HashMap::new();
    for (index, query) in queries.iter().enumerate() {
        let directory = load_directory(storage, namespace, segment, query).await?;
        let mut blocks = std::pin::pin!(
            futures::stream::iter(directory)
                .map(|entry| load_block(storage, namespace, segment, query.term, entry))
                .buffer_unordered(BLOCK_PREFETCH)
        );
        while let Some(postings) = blocks.try_next().await? {
            for posting in postings {
                if !allowed_pages
                    .contains(&(posting.address.stream_id, posting.address.page_sequence))
                {
                    continue;
                }
                let hit = Hit {
                    frequency: posting.frequency,
                    length: posting.length,
                    idf: query.idf,
                };
                if index == 0 {
                    candidates.entry(posting.address).or_default().push(hit);
                } else if let Some(hits) = candidates.get_mut(&posting.address)
                    && hits.len() == index
                {
                    hits.push(hit);
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

async fn single_term_top_k(
    storage: &dyn Storage,
    namespace: &Namespace,
    segment: SegmentId,
    query: &TermQuery<'_>,
    allowed_pages: &HashSet<(StreamId, u64)>,
    limit: usize,
    average_length: f32,
) -> Result<HashMap<DocAddress, f32>> {
    let mut directory = load_directory(storage, namespace, segment, query).await?;
    // Highest-bound blocks establish the top-k floor early.
    directory.sort_by(|left, right| {
        block_bound(*right, query.idf, average_length)
            .total_cmp(&block_bound(*left, query.idf, average_length))
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    });

    let mut scores = HashMap::new();
    let mut floor: BinaryHeap<Reverse<Score>> = BinaryHeap::with_capacity(limit + 1);
    let mut blocks = std::pin::pin!(
        futures::stream::iter(directory)
            .map(|entry| async move {
                let postings = load_block(storage, namespace, segment, query.term, entry).await?;
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
            if !allowed_pages.contains(&(posting.address.stream_id, posting.address.page_sequence))
            {
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
    storage: &dyn Storage,
    namespace: &Namespace,
    segment: SegmentId,
    query: &TermQuery<'_>,
) -> Result<Vec<BlockDirectoryEntry>> {
    let pages = (query.stats.blocks as usize).div_ceil(DIRECTORY_ENTRIES);
    let pages = futures::future::try_join_all((0..pages).map(|page| async move {
        let record = storage
            .get(term_directory_key(
                namespace,
                segment,
                query.term,
                page as u32,
            ))
            .await?
            .ok_or_else(|| Error::Corrupt("term stats reference missing directory".into()))?;
        let entries = decode_directory(&record.value)?;
        if entries.len() > DIRECTORY_ENTRIES {
            return Err(Error::Corrupt("term directory exceeds bound".into()));
        }
        Ok(entries)
    }))
    .await?;
    let directory: Vec<_> = pages.into_iter().flatten().collect();
    if directory.len() != query.stats.blocks as usize {
        return Err(Error::Corrupt("term directory block count mismatch".into()));
    }
    Ok(directory)
}

async fn load_block(
    storage: &dyn Storage,
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
            entry.ordinal,
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

    fn address_strategy() -> impl Strategy<Value = DocAddress> {
        (0u32..8, prop_oneof![0u64..4, any::<u64>()], any::<u32>()).prop_map(
            |(stream_id, page_sequence, row_id)| DocAddress {
                stream_id,
                page_sequence,
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
            ordinals in prop::collection::btree_set(any::<u32>(), 0..DIRECTORY_ENTRIES),
            postings in 1u16..=POSTINGS_PER_BLOCK as u16,
            max_frequency in any::<u32>(),
            min_length in any::<u32>(),
        ) {
            let entries = ordinals
                .into_iter()
                .map(|ordinal| BlockDirectoryEntry { ordinal, postings, max_frequency, min_length })
                .collect::<Vec<_>>();
            let encoded = encode_directory(&entries).unwrap();
            prop_assert_eq!(decode_directory(&encoded).unwrap(), entries);
        }

        #[test]
        fn source_matches_agrees_with_token_frequencies(
            line in "[a-zA-Z0-9 =_ÄÖÜäöü-]{0,64}",
            query in "[a-zA-Z0-9 ÄÖÜäöü]{0,16}",
        ) {
            let terms = query_terms(&query);
            let tokens = token_frequencies(&line);
            let expected = !terms.is_empty() && terms.iter().all(|term| tokens.contains_key(term));
            prop_assert_eq!(source_matches(&line, &terms), expected);
        }
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
