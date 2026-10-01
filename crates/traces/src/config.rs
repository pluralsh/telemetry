// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::time::Duration;

use common::coordinator::WriteCoordinatorConfig;
use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, StorageConfig};

#[derive(Clone, Debug)]
pub struct Config {
    pub storage: StorageConfig,
    pub segment_duration: Duration,
    pub retention: Option<Duration>,
    pub page: PageConfig,
    pub write_buffer: WriteCoordinatorConfig,
}

#[derive(Clone, Debug)]
pub struct PageConfig {
    /// Soft target used to cut a page before adding another trace.
    pub target_size_bytes: usize,
    /// Hard encoded page bound. A single trace exceeding it is rejected.
    pub max_size_bytes: usize,
    pub max_traces: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::SlateDb(common::storage::config::SlateDbStorageConfig {
                path: "traces".to_owned(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: ".data".to_owned(),
                }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            }),
            segment_duration: Duration::from_secs(60 * 60),
            retention: None,
            page: PageConfig::default(),
            write_buffer: WriteCoordinatorConfig::default(),
        }
    }
}

impl Default for PageConfig {
    fn default() -> Self {
        Self {
            target_size_bytes: 1024 * 1024,
            max_size_bytes: 4 * 1024 * 1024,
            max_traces: 1024,
        }
    }
}

impl Config {
    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.segment_duration.is_zero() {
            return Err(crate::Error::Invalid(
                "segment_duration must be positive".to_owned(),
            ));
        }
        if self.page.target_size_bytes == 0
            || self.page.max_size_bytes == 0
            || self.page.max_traces == 0
            || self.write_buffer.queue_capacity == 0
            || self.write_buffer.flush_interval.is_zero()
            || self.write_buffer.flush_size_threshold == 0
        {
            return Err(crate::Error::Invalid(
                "page size and trace limits must be positive".to_owned(),
            ));
        }
        if self.page.target_size_bytes > self.page.max_size_bytes {
            return Err(crate::Error::Invalid(
                "page target_size_bytes cannot exceed max_size_bytes".to_owned(),
            ));
        }
        Ok(())
    }
}
