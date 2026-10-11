// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;
use serde::de::DeserializeOwned;
use slatedb::{PrefixExtractor, PrefixTarget};
use uuid::Uuid;

use crate::error::{Error, Result};

pub(crate) const KEY_VERSION: u8 = 3;
pub(crate) const SUBSYSTEM: u8 = common::serde::subsystem::PSEUDOFS;
pub(crate) const SEGMENT_EXTRACTOR_NAME: &str = "pseudofs/v3";
const SEGMENT_PREFIX_LEN: usize = 3;
const TENANT_PREFIX_LEN: usize = SEGMENT_PREFIX_LEN + 16;
const DIRECTORY_NAME_OFFSET: usize = TENANT_PREFIX_LEN + 16;
const BLOB_ID_OFFSET: usize = TENANT_PREFIX_LEN;

const INODE: u8 = b'i';
const DIRECTORY_ENTRY: u8 = b'd';
const GENERATION: u8 = b'g';
const CONTENT: u8 = b'c';
const PENDING_BLOB: u8 = b'p';
const DEAD_BLOB: u8 = b'r';
const GARBAGE: u8 = b'x';

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TenantScope {
    pub(crate) root_id: Uuid,
}

/// Places every record family in its own SlateDB segment, so inline content
/// and blob bookkeeping compact independently of inodes and directory entries.
pub(crate) struct PseudofsSegmentExtractor;

impl PseudofsSegmentExtractor {
    pub(crate) fn shared() -> Arc<dyn PrefixExtractor> {
        Arc::new(Self)
    }
}

impl PrefixExtractor for PseudofsSegmentExtractor {
    fn name(&self) -> &str {
        SEGMENT_EXTRACTOR_NAME
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let bytes = match target {
            PrefixTarget::Point(bytes) | PrefixTarget::Prefix(bytes) => bytes.as_ref(),
        };
        let valid =
            bytes.len() >= SEGMENT_PREFIX_LEN && bytes[0] == SUBSYSTEM && bytes[1] == KEY_VERSION;
        match target {
            PrefixTarget::Point(_) => {
                assert!(
                    valid && bytes.len() >= TENANT_PREFIX_LEN,
                    "{SEGMENT_EXTRACTOR_NAME} received malformed key: {bytes:02x?}"
                );
                Some(SEGMENT_PREFIX_LEN)
            }
            PrefixTarget::Prefix(_) => valid.then_some(SEGMENT_PREFIX_LEN),
        }
    }
}

pub(crate) fn inode_key(scope: TenantScope, id: Uuid) -> Bytes {
    tagged_uuid(scope, INODE, id)
}

pub(crate) fn directory_prefix(scope: TenantScope, parent: Uuid) -> Bytes {
    tagged_uuid(scope, DIRECTORY_ENTRY, parent)
}

pub(crate) fn directory_entry_key(scope: TenantScope, parent: Uuid, name: &str) -> Bytes {
    let mut key = record_prefix(scope, DIRECTORY_ENTRY, 16 + name.len());
    key.extend_from_slice(parent.as_bytes());
    key.extend_from_slice(name.as_bytes());
    key.freeze()
}

pub(crate) fn directory_entry_name(key: &[u8]) -> Result<String> {
    let raw = key
        .get(DIRECTORY_NAME_OFFSET..)
        .ok_or_else(|| Error::Corrupt("short directory-entry key".to_owned()))?;
    String::from_utf8(raw.to_vec())
        .map_err(|_| Error::Corrupt("non-UTF-8 directory-entry key".to_owned()))
}

pub(crate) fn generation_key(scope: TenantScope, inode: Uuid, generation: Uuid) -> Bytes {
    tagged_two_uuids(scope, GENERATION, inode, generation)
}

pub(crate) fn content_key(scope: TenantScope, inode: Uuid, generation: Uuid) -> Bytes {
    tagged_two_uuids(scope, CONTENT, inode, generation)
}

pub(crate) fn pending_blob_key(scope: TenantScope, blob: Uuid) -> Bytes {
    tagged_uuid(scope, PENDING_BLOB, blob)
}

pub(crate) fn pending_blob_prefix() -> Bytes {
    family_prefix(PENDING_BLOB)
}

pub(crate) fn dead_blob_key(scope: TenantScope, blob: Uuid) -> Bytes {
    tagged_uuid(scope, DEAD_BLOB, blob)
}

pub(crate) fn dead_blob_prefix() -> Bytes {
    family_prefix(DEAD_BLOB)
}

pub(crate) fn blob_id_from_key(key: &[u8]) -> Result<Uuid> {
    key.get(BLOB_ID_OFFSET..BLOB_ID_OFFSET + 16)
        .and_then(|raw| Uuid::from_slice(raw).ok())
        .ok_or_else(|| Error::Corrupt("malformed blob record key".to_owned()))
}

pub(crate) fn garbage_key(scope: TenantScope, head: Uuid) -> Bytes {
    tagged_uuid(scope, GARBAGE, head)
}

pub(crate) fn garbage_prefix() -> Bytes {
    family_prefix(GARBAGE)
}

pub(crate) fn tenant_scope_from_key(key: &[u8]) -> Result<TenantScope> {
    if key.len() < TENANT_PREFIX_LEN || key[0] != SUBSYSTEM || key[1] != KEY_VERSION {
        return Err(Error::Corrupt("malformed PseudoFS record key".to_owned()));
    }
    let root_id = Uuid::from_slice(&key[SEGMENT_PREFIX_LEN..TENANT_PREFIX_LEN])
        .map_err(|error| Error::Corrupt(format!("invalid tenant root ID: {error}")))?;
    Ok(TenantScope { root_id })
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

fn tagged_uuid(scope: TenantScope, tag: u8, id: Uuid) -> Bytes {
    let mut key = record_prefix(scope, tag, 16);
    key.extend_from_slice(id.as_bytes());
    key.freeze()
}

fn tagged_two_uuids(scope: TenantScope, tag: u8, first: Uuid, second: Uuid) -> Bytes {
    let mut key = record_prefix(scope, tag, 32);
    key.extend_from_slice(first.as_bytes());
    key.extend_from_slice(second.as_bytes());
    key.freeze()
}

fn record_prefix(scope: TenantScope, tag: u8, suffix_len: usize) -> BytesMut {
    let mut key = BytesMut::with_capacity(TENANT_PREFIX_LEN + suffix_len);
    key.put_u8(SUBSYSTEM);
    key.put_u8(KEY_VERSION);
    key.put_u8(tag);
    key.extend_from_slice(scope.root_id.as_bytes());
    key
}

fn family_prefix(tag: u8) -> Bytes {
    Bytes::from(vec![SUBSYSTEM, KEY_VERSION, tag])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(root: u128) -> TenantScope {
        TenantScope {
            root_id: Uuid::from_u128(root),
        }
    }

    #[test]
    fn directory_entry_round_trip() {
        let parent = Uuid::new_v4();
        let scope = scope(1);
        let key = directory_entry_key(scope, parent, "notes.txt");
        assert_eq!(directory_entry_name(&key).unwrap(), "notes.txt");
        assert!(key.starts_with(&directory_prefix(scope, parent)));
    }

    #[test]
    fn record_family_follows_version_and_scope_round_trips() {
        let scope = scope(0x0123);
        let inode = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let keys = [
            inode_key(scope, inode),
            directory_entry_key(scope, inode, "name"),
            generation_key(scope, inode, generation),
            content_key(scope, inode, generation),
            pending_blob_key(scope, generation),
            dead_blob_key(scope, generation),
            garbage_key(scope, generation),
        ];
        for key in keys {
            assert_eq!(&key[..2], &[SUBSYSTEM, KEY_VERSION]);
            assert_eq!(tenant_scope_from_key(&key).unwrap(), scope);
        }
    }

    #[test]
    fn blob_keys_round_trip_and_family_prefixes_cover_every_tenant() {
        for root in [0, 1, u128::MAX] {
            let blob = Uuid::from_u128(7);
            let pending = pending_blob_key(scope(root), blob);
            let dead = dead_blob_key(scope(root), blob);
            assert!(pending.starts_with(&pending_blob_prefix()));
            assert!(dead.starts_with(&dead_blob_prefix()));
            assert_eq!(blob_id_from_key(&pending).unwrap(), blob);
            assert_eq!(blob_id_from_key(&dead).unwrap(), blob);
            assert!(garbage_key(scope(root), blob).starts_with(&garbage_prefix()));
        }
    }

    #[test]
    fn segments_split_by_record_family() {
        let extractor = PseudofsSegmentExtractor;
        let key = inode_key(scope(1), Uuid::nil());
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Point(key.clone())),
            Some(3)
        );
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Prefix(Bytes::from(vec![
                SUBSYSTEM,
                KEY_VERSION
            ]))),
            None
        );
    }
}
