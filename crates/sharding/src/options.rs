use crate::{DEFAULT_IO_CONCURRENCY_LIMIT, DEFAULT_SHARDS, ModelError, ShardId};

/// Process-level storage shard settings shared by every product facade.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardingOptions {
    shard_count: u32,
    io_concurrency_limit: u32,
}

impl Default for ShardingOptions {
    fn default() -> Self {
        Self {
            shard_count: DEFAULT_SHARDS,
            io_concurrency_limit: DEFAULT_IO_CONCURRENCY_LIMIT,
        }
    }
}

impl ShardingOptions {
    pub fn new(shard_count: u32, io_concurrency_limit: u32) -> Result<Self, ModelError> {
        if shard_count == 0 {
            return Err(ModelError::ZeroShards);
        }
        if io_concurrency_limit == 0 {
            return Err(ModelError::ZeroIoConcurrencyLimit);
        }
        Ok(Self {
            shard_count,
            io_concurrency_limit,
        })
    }

    pub const fn shard_count(self) -> u32 {
        self.shard_count
    }

    pub const fn io_concurrency_limit(self) -> u32 {
        self.io_concurrency_limit
    }

    /// Storage path component for `shard`, e.g. `shard-0003`.
    pub fn shard_suffix(shard: ShardId) -> String {
        format!("shard-{:04}", shard.get())
    }

    pub fn shard_path(base: &str, shard: ShardId) -> String {
        format!(
            "{}/{}",
            base.trim_end_matches('/'),
            Self::shard_suffix(shard)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_limits() {
        assert_eq!(ShardingOptions::new(0, 1), Err(ModelError::ZeroShards));
        assert_eq!(
            ShardingOptions::new(1, 0),
            Err(ModelError::ZeroIoConcurrencyLimit)
        );
    }

    #[test]
    fn shard_paths_are_zero_padded() {
        assert_eq!(
            ShardingOptions::shard_path("data/", ShardId::new(3)),
            "data/shard-0003"
        );
    }
}
