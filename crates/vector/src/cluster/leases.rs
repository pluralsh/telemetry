//! Exclusive ownership of shards by writers.
//!
//! The topology says which writer *should* own a shard; a lease says which
//! one *does*. A writer opens a shard read-write only while it holds the
//! shard's lease, so a shard moving between writers is closed (and flushed)
//! by the old owner before the new one opens it. SlateDB's writer fencing
//! backs this up should a lease ever be lost while a shard is open.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait ShardLeases: Send + Sync {
    /// Takes the lease of `shard` for `owner`, if free, expired, or already
    /// held by `owner`.
    async fn acquire(&self, owner: &str, shard: u32) -> Result<bool>;

    /// Extends a lease `owner` holds; `false` when it has lost it.
    async fn renew(&self, owner: &str, shard: u32) -> Result<bool>;

    async fn release(&self, owner: &str, shard: u32) -> Result<()>;
}

/// Leases for a single process, which owns every shard.
pub struct NoLeases;

#[async_trait]
impl ShardLeases for NoLeases {
    async fn acquire(&self, _owner: &str, _shard: u32) -> Result<bool> {
        Ok(true)
    }

    async fn renew(&self, _owner: &str, _shard: u32) -> Result<bool> {
        Ok(true)
    }

    async fn release(&self, _owner: &str, _shard: u32) -> Result<()> {
        Ok(())
    }
}

/// Process-local leases without expiry, for tests of several writers.
#[derive(Default)]
pub struct MemoryLeases {
    holders: Mutex<HashMap<u32, String>>,
}

impl MemoryLeases {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl ShardLeases for MemoryLeases {
    async fn acquire(&self, owner: &str, shard: u32) -> Result<bool> {
        let mut holders = self.holders.lock().expect("lock poisoned");
        let holder = holders.entry(shard).or_insert_with(|| owner.to_string());
        Ok(holder == owner)
    }

    async fn renew(&self, owner: &str, shard: u32) -> Result<bool> {
        let holders = self.holders.lock().expect("lock poisoned");
        Ok(holders.get(&shard).is_some_and(|h| h == owner))
    }

    async fn release(&self, owner: &str, shard: u32) -> Result<()> {
        let mut holders = self.holders.lock().expect("lock poisoned");
        if holders.get(&shard).is_some_and(|h| h == owner) {
            holders.remove(&shard);
        }
        Ok(())
    }
}
