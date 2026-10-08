//! Internals exposed to the crate's benchmarks; not a stable API.

use crate::logql::LineFilter;
use crate::object::Block;
use crate::{LogEntry, Result};

/// A block's stored meta and lines values.
#[derive(Clone, Debug)]
pub struct EncodedBlock(Block);

impl EncodedBlock {
    /// Stored size of both values.
    pub fn len(&self) -> usize {
        self.0.encoded_len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stored size of the meta value alone, what a lineless read fetches.
    pub fn meta_len(&self) -> usize {
        self.0.meta.len()
    }
}

pub fn encode_block(entries: &[LogEntry]) -> Result<EncodedBlock> {
    crate::object::encode_block(entries).map(EncodedBlock)
}

pub fn decode_block(block: &EncodedBlock) -> Result<Vec<LogEntry>> {
    crate::object::decode_block(&block.0)
}

/// Timestamps and line lengths of a block's rows, read from its meta value
/// alone, as a lineless metric query reads them.
pub fn decode_samples(block: &EncodedBlock) -> Result<Vec<(i64, u32)>> {
    let mut samples = Vec::new();
    crate::object::decode_samples_where(
        &block.0.meta,
        (i64::MIN, i64::MAX),
        |_| true,
        false,
        |_, sample| samples.push((sample.timestamp_ns, sample.line_len)),
    )?;
    Ok(samples)
}

/// Rows of a run's blocks in `[start_ns, end_ns]`, as a log query reads them.
pub fn run_entries(blocks: &[EncodedBlock], start_ns: i64, end_ns: i64) -> Result<Vec<LogEntry>> {
    let blocks = blocks
        .iter()
        .map(|block| block.0.clone())
        .collect::<Vec<_>>();
    crate::db::run_entries(&blocks, (start_ns, end_ns))
}

/// Lines of `lines` that pass `filter`, evaluated as a query pipeline does.
pub fn count_line_matches<'a>(
    filter: &LineFilter,
    lines: impl IntoIterator<Item = &'a str>,
) -> Result<usize> {
    crate::query::count_line_matches(filter, lines)
}
