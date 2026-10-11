// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![forbid(unsafe_code)]

mod blob;
mod codec;
pub mod config;
mod db;
pub mod error;
mod model;
mod search;

pub use config::{BlobConfig, Config};
pub use db::{FileUpload, PseudoFs, normalize_path, validate_tenant};
pub use error::{Error, Result};
pub use model::{DirectoryEntry, Durability, FileHandle, FileKind, FileStat};
pub use search::{GlobOptions, GrepLine, GrepMatch, GrepOptions};

/// Persisted SlateDB segment extractor identifier.
pub const SEGMENT_EXTRACTOR_NAME: &str = codec::SEGMENT_EXTRACTOR_NAME;
