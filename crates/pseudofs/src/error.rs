// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use common::storage::StorageError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no such file or directory: '{0}'")]
    NotFound(String),
    #[error("file exists: '{0}'")]
    AlreadyExists(String),
    #[error("not a directory: '{0}'")]
    NotDirectory(String),
    #[error("is a directory: '{0}'")]
    IsDirectory(String),
    #[error("directory not empty: '{0}'")]
    DirectoryNotEmpty(String),
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("invalid open mode: '{0}'")]
    InvalidMode(String),
    #[error("invalid UTF-8 in file '{0}'")]
    InvalidUtf8(String),
    #[error("file changed while the operation was in progress: '{0}'")]
    Conflict(String),
    #[error("operation is not permitted on the root directory")]
    RootOperation,
    #[error("corrupt filesystem: {0}")]
    Corrupt(String),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}
