// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::error::{Error, Result};

const INODE: u8 = b'i';
const DIRECTORY_ENTRY: u8 = b'd';
const GENERATION: u8 = b'g';
const CHUNK: u8 = b'c';
const UPLOAD: u8 = b'u';
const GARBAGE: u8 = b'x';

pub(crate) fn inode_key(id: Uuid) -> Bytes {
    tagged_uuid(INODE, id)
}

pub(crate) fn directory_prefix(parent: Uuid) -> Bytes {
    tagged_uuid(DIRECTORY_ENTRY, parent)
}

pub(crate) fn directory_entry_key(parent: Uuid, name: &str) -> Bytes {
    let mut key = BytesMut::with_capacity(17 + name.len());
    key.put_u8(DIRECTORY_ENTRY);
    key.extend_from_slice(parent.as_bytes());
    key.extend_from_slice(name.as_bytes());
    key.freeze()
}

pub(crate) fn directory_entry_name(key: &[u8]) -> Result<String> {
    let raw = key
        .get(17..)
        .ok_or_else(|| Error::Corrupt("short directory-entry key".to_owned()))?;
    String::from_utf8(raw.to_vec())
        .map_err(|_| Error::Corrupt("non-UTF-8 directory-entry key".to_owned()))
}

pub(crate) fn generation_key(inode: Uuid, generation: Uuid) -> Bytes {
    tagged_two_uuids(GENERATION, inode, generation)
}

pub(crate) fn chunk_key(inode: Uuid, generation: Uuid, index: u64) -> Bytes {
    let mut key = BytesMut::with_capacity(41);
    key.put_u8(CHUNK);
    key.extend_from_slice(inode.as_bytes());
    key.extend_from_slice(generation.as_bytes());
    key.put_u64(index);
    key.freeze()
}

pub(crate) fn upload_key(generation: Uuid) -> Bytes {
    tagged_uuid(UPLOAD, generation)
}

pub(crate) fn upload_prefix() -> Bytes {
    Bytes::from_static(&[UPLOAD])
}

pub(crate) fn garbage_key(head: Uuid) -> Bytes {
    tagged_uuid(GARBAGE, head)
}

pub(crate) fn garbage_prefix() -> Bytes {
    Bytes::from_static(&[GARBAGE])
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Bytes> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| Error::Corrupt(format!("cannot encode metadata: {error}")))
}

pub(crate) fn decode<T: DeserializeOwned>(value: &[u8]) -> Result<T> {
    serde_json::from_slice(value)
        .map_err(|error| Error::Corrupt(format!("cannot decode metadata: {error}")))
}

fn tagged_uuid(tag: u8, id: Uuid) -> Bytes {
    let mut key = BytesMut::with_capacity(17);
    key.put_u8(tag);
    key.extend_from_slice(id.as_bytes());
    key.freeze()
}

fn tagged_two_uuids(tag: u8, first: Uuid, second: Uuid) -> Bytes {
    let mut key = BytesMut::with_capacity(33);
    key.put_u8(tag);
    key.extend_from_slice(first.as_bytes());
    key.extend_from_slice(second.as_bytes());
    key.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_entry_round_trip() {
        let parent = Uuid::new_v4();
        let key = directory_entry_key(parent, "notes.txt");
        assert_eq!(directory_entry_name(&key).unwrap(), "notes.txt");
        assert!(key.starts_with(&directory_prefix(parent)));
    }

    #[test]
    fn chunk_indices_sort_lexicographically() {
        let inode = Uuid::new_v4();
        let generation = Uuid::new_v4();
        assert!(chunk_key(inode, generation, 1) < chunk_key(inode, generation, 2));
    }
}
