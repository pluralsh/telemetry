// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use common::storage::config::{ObjectStoreConfig, StorageConfig};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// SlateDB metastore holding inodes, directory entries, generations, and
    /// inline content.
    pub storage: StorageConfig,
    /// Local directory for the metastore write-ahead log. When set, the WAL is
    /// written to this fsynced local directory instead of the storage object
    /// store, so `WRITTEN` acknowledgements wait on local disk rather than
    /// remote object storage.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metastore_wal_path: Option<String>,
    pub blobs: BlobConfig,
    /// Size of chunks streamed to readers.
    pub chunk_size_bytes: usize,
    /// Files at or below this size are stored inline in the metastore rather
    /// than as blobs.
    pub inline_threshold_bytes: usize,
    pub max_file_size_bytes: u64,
    pub max_append_generations: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            metastore_wal_path: None,
            blobs: BlobConfig::default(),
            chunk_size_bytes: 1024 * 1024,
            inline_threshold_bytes: 64 * 1024,
            max_file_size_bytes: 1024 * 1024 * 1024,
            max_append_generations: 64,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlobConfig {
    /// Remote blob store. Defaults to the metastore's object store.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_store: Option<ObjectStoreConfig>,
    /// Remote key prefix. Defaults to `<storage.path>-blobs`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Local directory holding staged uploads, blobs awaiting upload, and the
    /// blob read cache. Must be persistent: blobs are acknowledged before they
    /// reach the remote store.
    pub local_path: String,
    /// Local disk budget for blobs. Uploaded blobs are evicted least recently
    /// used first; blobs awaiting upload are never evicted, and new large
    /// writes are rejected while they alone exceed this budget.
    pub cache_capacity_bytes: u64,
    pub upload_concurrency: usize,
    /// How long unreferenced blobs are retained before deletion, so streams
    /// that began before an overwrite or unlink can finish.
    pub gc_grace_seconds: u64,
}

impl Default for BlobConfig {
    fn default() -> Self {
        Self {
            object_store: None,
            path: None,
            local_path: ".data/pseudofs-blobs".to_owned(),
            cache_capacity_bytes: 4 * 1024 * 1024 * 1024,
            upload_concurrency: 4,
            gc_grace_seconds: 300,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if self.chunk_size_bytes == 0 {
            return Err("chunk_size_bytes must be greater than zero".to_owned());
        }
        if self.max_file_size_bytes == 0 {
            return Err("max_file_size_bytes must be greater than zero".to_owned());
        }
        if self.chunk_size_bytes as u64 > self.max_file_size_bytes {
            return Err("chunk_size_bytes must not exceed max_file_size_bytes".to_owned());
        }
        if self.inline_threshold_bytes > 16 * 1024 * 1024 {
            return Err("inline_threshold_bytes must not exceed 16 MiB".to_owned());
        }
        if self.max_append_generations < 2 {
            return Err("max_append_generations must be at least two".to_owned());
        }
        if self.blobs.local_path.is_empty() {
            return Err("blobs.local_path must be set".to_owned());
        }
        if self.blobs.upload_concurrency == 0 {
            return Err("blobs.upload_concurrency must be greater than zero".to_owned());
        }
        if self.blobs.cache_capacity_bytes == 0 {
            return Err("blobs.cache_capacity_bytes must be greater than zero".to_owned());
        }
        Ok(())
    }
}
