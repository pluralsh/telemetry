// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Inode {
    pub id: Uuid,
    pub tenant: String,
    pub kind: FileKind,
    pub mode: u32,
    pub size: u64,
    pub atime: f64,
    pub mtime: f64,
    pub ctime: f64,
    pub head: Option<Uuid>,
    #[serde(default = "default_generation_count")]
    pub generation_count: u64,
}

impl Inode {
    pub(crate) fn directory(id: Uuid, tenant: String, now: f64) -> Self {
        Self {
            id,
            tenant,
            kind: FileKind::Directory,
            mode: 0o040_755,
            size: 4096,
            atime: now,
            mtime: now,
            ctime: now,
            head: None,
            generation_count: 0,
        }
    }

    pub(crate) fn file(id: Uuid, tenant: String, size: u64, head: Uuid, now: f64) -> Self {
        Self {
            id,
            tenant,
            kind: FileKind::File,
            mode: 0o100_644,
            size,
            atime: now,
            mtime: now,
            ctime: now,
            head: Some(head),
            generation_count: 1,
        }
    }
}

fn default_generation_count() -> u64 {
    1
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Generation {
    pub inode_id: Uuid,
    pub id: Uuid,
    pub previous: Option<Uuid>,
    pub chunk_count: u64,
    pub byte_len: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct UploadMarker {
    pub inode_id: Uuid,
    pub generation_id: Uuid,
    pub chunk_count: u64,
    pub started_at: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct GarbageMarker {
    pub inode_id: Uuid,
    pub head: Uuid,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FileStat {
    pub kind: FileKind,
    pub mode: u32,
    pub size: u64,
    pub atime: f64,
    pub mtime: f64,
    pub ctime: f64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryEntry {
    pub path: String,
    pub name: String,
    pub kind: FileKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileHandle {
    pub tenant: String,
    pub path: String,
    pub mode: String,
    pub position: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Durability {
    Applied,
    #[default]
    Written,
    Durable,
}
