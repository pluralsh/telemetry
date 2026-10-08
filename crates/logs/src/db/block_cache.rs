// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Cross-query cache of object blocks, which keep their bodies once a query
//! decompresses them.
//!
//! A block is written once and never changes: `(id, level)` never names two
//! objects of a segment, so an entry can only go unused, never stale. Blocks
//! of merged-away or expired objects are no longer reachable from any run
//! and age out.

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::Namespace;
use crate::codec::ObjectRef;
use crate::model::SegmentId;
use crate::object::Block;

/// Bounds how long a block of a dropped object can hold memory.
const BLOCK_CACHE_TTL: Duration = Duration::from_secs(30 * 60);

/// Shared by every database of a process; clones share entries.
#[derive(Clone)]
pub(crate) struct BlockCache {
    blocks: Option<moka::sync::Cache<BlockKey, Block>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BlockKey {
    /// Shards of a process reuse segment and object IDs.
    database: u64,
    namespace: Namespace,
    segment: SegmentId,
    object: ObjectRef,
    block: u32,
}

/// Identifies a database within the process's [`BlockCache`].
pub(crate) fn next_database_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

impl BlockCache {
    /// A cache holding up to `capacity_bytes` of blocks; zero disables it.
    pub(crate) fn new(capacity_bytes: u64) -> Self {
        Self {
            blocks: (capacity_bytes > 0).then(|| {
                moka::sync::Cache::builder()
                    .max_capacity(capacity_bytes)
                    .time_to_live(BLOCK_CACHE_TTL)
                    .weigher(|_, block: &Block| {
                        let bytes = block.resident_len().unwrap_or(block.encoded_len());
                        u32::try_from(bytes + 128).unwrap_or(u32::MAX)
                    })
                    .build()
            }),
        }
    }

    /// Blocks `range` of `object`, when all are cached with their lines or
    /// lines are not needed.
    pub(crate) fn get(
        &self,
        database: u64,
        namespace: &Namespace,
        segment: SegmentId,
        object: ObjectRef,
        range: Range<u32>,
        lines: bool,
    ) -> Option<Vec<Block>> {
        let cache = self.blocks.as_ref()?;
        let mut key = BlockKey {
            database,
            namespace: namespace.clone(),
            segment,
            object,
            block: range.start,
        };
        let mut blocks = Vec::with_capacity(range.len());
        for block in range {
            key.block = block;
            let found = cache.get(&key)?;
            if lines && found.lines.is_none() {
                return None;
            }
            blocks.push(found);
        }
        Some(blocks)
    }

    /// Caches `blocks`, the blocks of `object` from `first`, keeping cached
    /// blocks that hold lines over ones read without.
    pub(crate) fn insert(
        &self,
        database: u64,
        namespace: &Namespace,
        segment: SegmentId,
        object: ObjectRef,
        first: u32,
        blocks: &[Block],
    ) {
        let Some(cache) = &self.blocks else {
            return;
        };
        let mut key = BlockKey {
            database,
            namespace: namespace.clone(),
            segment,
            object,
            block: first,
        };
        for (block, index) in blocks.iter().zip(first..) {
            key.block = index;
            if block.lines.is_none() && cache.get(&key).is_some_and(|cached| cached.lines.is_some())
            {
                continue;
            }
            cache.insert(key.clone(), block.clone());
        }
    }
}
