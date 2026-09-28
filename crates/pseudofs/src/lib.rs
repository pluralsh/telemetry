// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![forbid(unsafe_code)]

mod codec;
pub mod config;
mod db;
pub mod error;
mod model;

pub use config::Config;
pub use db::{FileUpload, PseudoFs, normalize_path, validate_tenant};
pub use error::{Error, Result};
pub use model::{DirectoryEntry, Durability, FileHandle, FileKind, FileStat};
