// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use common::storage::{PutRecordOp, Record, RecordOp, Storage, StorageRead, WriteOptions};
use common::{BytesRange, StorageBuilder, StorageSemantics};
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

use crate::codec::{
    PseudofsSegmentExtractor, TenantScope, chunk_key, decode, directory_entry_key,
    directory_entry_name, directory_prefix, encode, garbage_key, garbage_prefix, generation_key,
    inode_key, tenant_scope_from_key, upload_key, upload_prefix,
};
use crate::config::Config;
use crate::error::{Error, Result};
use crate::model::{
    DirectoryEntry, Durability, FileHandle, FileKind, FileStat, GarbageMarker, Generation, Inode,
    UploadMarker,
};

mod upload;

const UPLOAD_BATCH_CHUNKS: usize = 8;
const DELETE_BATCH_OPS: usize = 512;
const MAX_GENERATION_CHAIN: u64 = 1_000_000;

#[derive(Clone)]
pub struct PseudoFs {
    inner: Arc<Inner>,
}

struct Inner {
    storage: Arc<dyn Storage>,
    config: Config,
    mutation_lock: Mutex<()>,
    compaction_tx: mpsc::Sender<CompactionRequest>,
}

struct CompactionRequest {
    tenant: String,
    path: String,
}

pub struct FileUpload {
    fs: PseudoFs,
    tenant: String,
    scope: TenantScope,
    path: String,
    inode_id: Uuid,
    generation_id: Uuid,
    expected_head: Option<Uuid>,
    append: bool,
    chunk_count: u64,
    byte_len: u64,
    base_size: u64,
    started_at: f64,
    buffer: BytesMut,
    pending_chunks: Vec<Bytes>,
    finished: bool,
}

impl PseudoFs {
    pub async fn open(config: Config) -> Result<Self> {
        config.validate().map_err(Error::InvalidPath)?;
        let semantics =
            StorageSemantics::new().with_segment_extractor(PseudofsSegmentExtractor::shared());
        let storage = StorageBuilder::new(&config.storage)
            .await?
            .with_semantics(semantics)
            .build()
            .await?;
        let (compaction_tx, mut compaction_rx) = mpsc::channel::<CompactionRequest>(64);
        let fs = Self {
            inner: Arc::new(Inner {
                storage,
                config,
                mutation_lock: Mutex::new(()),
                compaction_tx,
            }),
        };
        fs.cleanup_staged_uploads().await?;
        fs.cleanup_garbage().await?;
        let compactor = Arc::downgrade(&fs.inner);
        tokio::spawn(async move {
            while let Some(request) = compaction_rx.recv().await {
                let Some(inner) = compactor.upgrade() else {
                    break;
                };
                let _ = PseudoFs { inner }
                    .compact_file(&request.tenant, &request.path)
                    .await;
            }
        });
        Ok(fs)
    }

    pub fn chunk_size_bytes(&self) -> usize {
        self.inner.config.chunk_size_bytes
    }

    pub async fn exists(&self, tenant: &str, path: &str) -> Result<bool> {
        match self.resolve(tenant, path).await {
            Ok(_) => Ok(true),
            Err(Error::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn is_file(&self, tenant: &str, path: &str) -> Result<bool> {
        Ok(self
            .resolve(tenant, path)
            .await
            .is_ok_and(|inode| inode.kind == FileKind::File))
    }

    pub async fn is_dir(&self, tenant: &str, path: &str) -> Result<bool> {
        Ok(self
            .resolve(tenant, path)
            .await
            .is_ok_and(|inode| inode.kind == FileKind::Directory))
    }

    pub async fn is_symlink(&self, tenant: &str, path: &str) -> Result<bool> {
        validate_tenant(tenant)?;
        normalize_path(path)?;
        Ok(false)
    }

    pub async fn read_bytes(&self, tenant: &str, path: &str) -> Result<Bytes> {
        let chunks = self.read_chunks(tenant, path).await?;
        let capacity = chunks.iter().map(Bytes::len).sum();
        let mut output = Vec::with_capacity(capacity);
        for chunk in chunks {
            output.extend_from_slice(&chunk);
        }
        Ok(Bytes::from(output))
    }

    pub async fn read_text(&self, tenant: &str, path: &str) -> Result<String> {
        let normalized = normalize_path(path)?;
        String::from_utf8(self.read_bytes(tenant, path).await?.to_vec())
            .map_err(|_| Error::InvalidUtf8(normalized))
    }

    pub async fn read_chunks(&self, tenant: &str, path: &str) -> Result<Vec<Bytes>> {
        let mut stream = self.stream_chunks(tenant, path).await?;
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.recv().await {
            chunks.push(chunk?);
        }
        Ok(chunks)
    }

    pub async fn stream_chunks(
        &self,
        tenant: &str,
        path: &str,
    ) -> Result<mpsc::Receiver<Result<Bytes>>> {
        let normalized = normalize_path(path)?;
        let scope = self.ensure_tenant(tenant).await?;
        let snapshot = self.inner.storage.snapshot().await?;
        let inode = self
            .resolve_from(snapshot.as_ref(), tenant, scope, &normalized)
            .await?;
        if inode.kind == FileKind::Directory {
            return Err(Error::IsDirectory(normalized));
        }
        let mut generations = Vec::new();
        let mut cursor = inode.head;
        let mut traversed = 0_u64;
        while let Some(id) = cursor {
            traversed += 1;
            if traversed > MAX_GENERATION_CHAIN {
                return Err(Error::Corrupt("generation chain is cyclic".to_owned()));
            }
            let generation: Generation = self
                .get_json_from(snapshot.as_ref(), generation_key(scope, inode.id, id))
                .await?
                .ok_or_else(|| Error::Corrupt(format!("missing generation {id}")))?;
            cursor = generation.previous;
            generations.push(generation);
        }
        generations.reverse();

        let (sender, receiver) = mpsc::channel(2);
        tokio::spawn(async move {
            for generation in generations {
                for index in 0..generation.chunk_count {
                    let chunk = match snapshot
                        .get(chunk_key(scope, inode.id, generation.id, index))
                        .await
                    {
                        Ok(Some(record)) => Ok(record.value),
                        Ok(None) => Err(Error::Corrupt(format!(
                            "missing chunk {index} in generation {}",
                            generation.id
                        ))),
                        Err(error) => Err(Error::Storage(error)),
                    };
                    let failed = chunk.is_err();
                    if sender.send(chunk).await.is_err() || failed {
                        return;
                    }
                }
            }
        });
        Ok(receiver)
    }

    pub async fn write_bytes(
        &self,
        tenant: &str,
        path: &str,
        content: Bytes,
        durability: Durability,
    ) -> Result<u64> {
        let mut upload = self.start_upload(tenant, path, false).await?;
        let mut offset = 0;
        while offset < content.len() {
            let end = (offset + self.inner.config.chunk_size_bytes).min(content.len());
            if let Err(error) = upload.push(content.slice(offset..end)).await {
                let _ = upload.abort().await;
                return Err(error);
            }
            offset = end;
        }
        match upload.finish(durability).await {
            Ok(size) => Ok(size),
            Err(error) => {
                let _ = upload.abort().await;
                Err(error)
            }
        }
    }

    pub async fn write_text(
        &self,
        tenant: &str,
        path: &str,
        content: String,
        durability: Durability,
    ) -> Result<u64> {
        self.write_bytes(tenant, path, Bytes::from(content), durability)
            .await
    }

    pub async fn append_bytes(
        &self,
        tenant: &str,
        path: &str,
        content: Bytes,
        durability: Durability,
    ) -> Result<u64> {
        let mut upload = self.start_upload(tenant, path, true).await?;
        let mut offset = 0;
        while offset < content.len() {
            let end = (offset + self.inner.config.chunk_size_bytes).min(content.len());
            if let Err(error) = upload.push(content.slice(offset..end)).await {
                let _ = upload.abort().await;
                return Err(error);
            }
            offset = end;
        }
        match upload.finish(durability).await {
            Ok(size) => Ok(size),
            Err(error) => {
                let _ = upload.abort().await;
                Err(error)
            }
        }
    }

    pub async fn append_text(
        &self,
        tenant: &str,
        path: &str,
        content: String,
        durability: Durability,
    ) -> Result<u64> {
        self.append_bytes(tenant, path, Bytes::from(content), durability)
            .await
    }

    pub async fn start_upload(&self, tenant: &str, path: &str, append: bool) -> Result<FileUpload> {
        let path = normalize_path(path)?;
        let scope = self.ensure_tenant(tenant).await?;
        if path == "/" {
            return Err(Error::IsDirectory(path));
        }
        let existing = match self.resolve_from_storage(tenant, scope, &path).await {
            Ok(inode) => {
                if inode.kind == FileKind::Directory {
                    return Err(Error::IsDirectory(path));
                }
                Some(inode)
            }
            Err(Error::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        let inode_id = existing
            .as_ref()
            .map_or_else(Uuid::new_v4, |inode| inode.id);
        let expected_head = existing.as_ref().and_then(|inode| inode.head);
        let base_size = if append {
            existing.as_ref().map_or(0, |inode| inode.size)
        } else {
            0
        };
        let generation_id = Uuid::new_v4();
        let started_at = now();
        let marker = UploadMarker {
            inode_id,
            generation_id,
            chunk_count: 0,
            started_at,
        };
        self.inner
            .storage
            .put(vec![put(
                upload_key(scope, generation_id),
                encode(&marker)?,
            )])
            .await?;
        Ok(FileUpload {
            fs: self.clone(),
            tenant: tenant.to_owned(),
            scope,
            path,
            inode_id,
            generation_id,
            expected_head,
            append,
            chunk_count: 0,
            byte_len: 0,
            base_size,
            started_at,
            buffer: BytesMut::with_capacity(self.inner.config.chunk_size_bytes),
            pending_chunks: Vec::with_capacity(UPLOAD_BATCH_CHUNKS),
            finished: false,
        })
    }

    pub async fn mkdir(
        &self,
        tenant: &str,
        path: &str,
        parents: bool,
        exist_ok: bool,
    ) -> Result<()> {
        let path = normalize_path(path)?;
        let scope = self.ensure_tenant(tenant).await?;
        if path == "/" {
            return if exist_ok {
                Ok(())
            } else {
                Err(Error::AlreadyExists(path))
            };
        }
        let _guard = self.inner.mutation_lock.lock().await;
        if let Ok(inode) = self.resolve_from_storage(tenant, scope, &path).await {
            return if inode.kind == FileKind::Directory && exist_ok {
                Ok(())
            } else {
                Err(Error::AlreadyExists(path))
            };
        }
        let (parent_path, name) = parent_and_name(&path)?;
        let mut ops = Vec::new();
        let parent = if parents {
            self.ensure_directory_path(tenant, scope, &parent_path, &mut ops)
                .await?
        } else {
            let inode = self
                .resolve_from_storage(tenant, scope, &parent_path)
                .await?;
            if inode.kind != FileKind::Directory {
                return Err(Error::NotDirectory(parent_path));
            }
            inode.id
        };
        let inode = Inode::directory(Uuid::new_v4(), tenant.to_owned(), now());
        ops.push(put_op(inode_key(scope, inode.id), encode(&inode)?));
        ops.push(put_op(
            directory_entry_key(scope, parent, &name),
            directory_entry_value(scope.root_id, inode.id, inode.kind),
        ));
        self.apply_records(ops, Durability::Written).await?;
        drop(_guard);
        self.finish_durability(Durability::Written).await
    }

    pub async fn unlink(&self, tenant: &str, path: &str, durability: Durability) -> Result<()> {
        let path = normalize_path(path)?;
        let scope = self.ensure_tenant(tenant).await?;
        if path == "/" {
            return Err(Error::RootOperation);
        }
        let _guard = self.inner.mutation_lock.lock().await;
        let inode = self.resolve_from_storage(tenant, scope, &path).await?;
        if inode.kind == FileKind::Directory {
            return Err(Error::IsDirectory(path));
        }
        let (parent_path, name) = parent_and_name(&path)?;
        let parent = self
            .resolve_from_storage(tenant, scope, &parent_path)
            .await?;
        let garbage = inode.head.map(|head| GarbageMarker {
            inode_id: inode.id,
            head,
        });
        let mut ops = vec![
            RecordOp::Delete(directory_entry_key(scope, parent.id, &name)),
            RecordOp::Delete(inode_key(scope, inode.id)),
        ];
        if let Some(marker) = &garbage {
            ops.push(put_op(garbage_key(scope, marker.head), encode(marker)?));
        }
        self.apply_records(ops, durability).await?;
        drop(_guard);
        self.finish_durability(durability).await?;
        if let Some(marker) = garbage {
            let _ = self.cleanup_generation_chain(scope, &marker).await;
        }
        Ok(())
    }

    pub async fn rmdir(&self, tenant: &str, path: &str, durability: Durability) -> Result<()> {
        let path = normalize_path(path)?;
        let scope = self.ensure_tenant(tenant).await?;
        if path == "/" {
            return Err(Error::RootOperation);
        }
        let _guard = self.inner.mutation_lock.lock().await;
        let inode = self.resolve_from_storage(tenant, scope, &path).await?;
        if inode.kind != FileKind::Directory {
            return Err(Error::NotDirectory(path));
        }
        if self.has_directory_entries(scope, inode.id).await? {
            return Err(Error::DirectoryNotEmpty(path));
        }
        let (parent_path, name) = parent_and_name(&path)?;
        let parent = self
            .resolve_from_storage(tenant, scope, &parent_path)
            .await?;
        self.apply_records(
            vec![
                RecordOp::Delete(directory_entry_key(scope, parent.id, &name)),
                RecordOp::Delete(inode_key(scope, inode.id)),
            ],
            durability,
        )
        .await?;
        drop(_guard);
        self.finish_durability(durability).await
    }

    pub async fn iterdir(&self, tenant: &str, path: &str) -> Result<Vec<DirectoryEntry>> {
        let mut stream = self.stream_dir(tenant, path).await?;
        let mut entries = Vec::new();
        while let Some(entry) = stream.recv().await {
            entries.push(entry?);
        }
        Ok(entries)
    }

    pub async fn stream_dir(
        &self,
        tenant: &str,
        path: &str,
    ) -> Result<mpsc::Receiver<Result<DirectoryEntry>>> {
        let path = normalize_path(path)?;
        let scope = self.ensure_tenant(tenant).await?;
        let snapshot = self.inner.storage.snapshot().await?;
        let inode = self
            .resolve_from(snapshot.as_ref(), tenant, scope, &path)
            .await?;
        if inode.kind != FileKind::Directory {
            return Err(Error::NotDirectory(path));
        }
        let mut iter = snapshot
            .scan_prefix_iter(
                directory_prefix(scope, inode.id),
                BytesRange::unbounded(),
                None,
            )
            .await?;
        let (sender, receiver) = mpsc::channel(32);
        let tenant = tenant.to_owned();
        tokio::spawn(async move {
            loop {
                let record = match iter.next().await {
                    Ok(Some(record)) => record,
                    Ok(None) => return,
                    Err(error) => {
                        let _ = sender.send(Err(Error::Storage(error))).await;
                        return;
                    }
                };
                let entry = async {
                    let name = directory_entry_name(&record.key)?;
                    let (child_id, stored_kind, entry_root) =
                        decode_directory_entry(&record.value)?;
                    let kind = match (stored_kind, entry_root) {
                        (Some(kind), Some(entry_root)) => {
                            ensure_directory_entry_root(entry_root, scope.root_id)?;
                            kind
                        }
                        (stored_kind, entry_root) => {
                            if let Some(entry_root) = entry_root {
                                ensure_directory_entry_root(entry_root, scope.root_id)?;
                            }
                            let child =
                                snapshot.get(inode_key(scope, child_id)).await?.ok_or_else(
                                    || Error::Corrupt(format!("missing inode {child_id}")),
                                )?;
                            let child = decode::<Inode>(&child.value)?;
                            ensure_inode_tenant(&child, &tenant)?;
                            stored_kind.unwrap_or(child.kind)
                        }
                    };
                    let child_path = if path == "/" {
                        format!("/{name}")
                    } else {
                        format!("{path}/{name}")
                    };
                    Ok(DirectoryEntry {
                        path: child_path,
                        name,
                        kind,
                    })
                }
                .await;
                let failed = entry.is_err();
                if sender.send(entry).await.is_err() || failed {
                    return;
                }
            }
        });
        Ok(receiver)
    }

    pub async fn stat(&self, tenant: &str, path: &str) -> Result<FileStat> {
        let inode = self.resolve(tenant, path).await?;
        Ok(FileStat {
            kind: inode.kind,
            mode: inode.mode,
            size: inode.size,
            atime: inode.atime,
            mtime: inode.mtime,
            ctime: inode.ctime,
        })
    }

    pub async fn rename(
        &self,
        tenant: &str,
        source: &str,
        target: &str,
        durability: Durability,
    ) -> Result<String> {
        let source = normalize_path(source)?;
        let target = normalize_path(target)?;
        let scope = self.ensure_tenant(tenant).await?;
        if source == "/" || target == "/" {
            return Err(Error::RootOperation);
        }
        if source == target {
            return Ok(target);
        }
        if target.starts_with(&(source.clone() + "/")) {
            return Err(Error::InvalidPath(
                "cannot move a directory inside itself".to_owned(),
            ));
        }
        let _guard = self.inner.mutation_lock.lock().await;
        let source_inode = self.resolve_from_storage(tenant, scope, &source).await?;
        let (source_parent_path, source_name) = parent_and_name(&source)?;
        let source_parent = self
            .resolve_from_storage(tenant, scope, &source_parent_path)
            .await?;
        let (target_parent_path, target_name) = parent_and_name(&target)?;
        let mut ops = Vec::new();
        let target_parent = self
            .ensure_directory_path(tenant, scope, &target_parent_path, &mut ops)
            .await?;
        let mut garbage = None;
        if let Ok(existing) = self.resolve_from_storage(tenant, scope, &target).await {
            if existing.kind == FileKind::Directory
                && self.has_directory_entries(scope, existing.id).await?
            {
                return Err(Error::DirectoryNotEmpty(target));
            }
            if existing.kind != source_inode.kind {
                return Err(if existing.kind == FileKind::Directory {
                    Error::IsDirectory(target)
                } else {
                    Error::NotDirectory(target)
                });
            }
            ops.push(RecordOp::Delete(inode_key(scope, existing.id)));
            garbage = existing.head.map(|head| GarbageMarker {
                inode_id: existing.id,
                head,
            });
            if let Some(marker) = &garbage {
                ops.push(put_op(garbage_key(scope, marker.head), encode(marker)?));
            }
        }
        ops.push(RecordOp::Delete(directory_entry_key(
            scope,
            source_parent.id,
            &source_name,
        )));
        ops.push(put_op(
            directory_entry_key(scope, target_parent, &target_name),
            directory_entry_value(scope.root_id, source_inode.id, source_inode.kind),
        ));
        self.apply_records(ops, durability).await?;
        drop(_guard);
        self.finish_durability(durability).await?;
        if let Some(marker) = garbage {
            let _ = self.cleanup_generation_chain(scope, &marker).await;
        }
        Ok(target)
    }

    pub async fn open_file(
        &self,
        tenant: &str,
        path: &str,
        mode: &str,
        durability: Durability,
    ) -> Result<FileHandle> {
        let path = normalize_path(path)?;
        match mode {
            "r" | "rb" => {
                let inode = self.resolve(tenant, &path).await?;
                if inode.kind == FileKind::Directory {
                    return Err(Error::IsDirectory(path));
                }
            }
            "w" | "wb" => {
                self.write_bytes(tenant, &path, Bytes::new(), durability)
                    .await?;
            }
            "a" | "ab" => {
                if !self.exists(tenant, &path).await? {
                    self.write_bytes(tenant, &path, Bytes::new(), durability)
                        .await?;
                } else if self.is_dir(tenant, &path).await? {
                    return Err(Error::IsDirectory(path));
                }
            }
            _ => return Err(Error::InvalidMode(mode.to_owned())),
        }
        Ok(FileHandle {
            tenant: tenant.to_owned(),
            path,
            mode: mode.to_owned(),
            position: 0,
        })
    }

    pub fn resolve_path(&self, tenant: &str, path: &str) -> Result<String> {
        validate_tenant(tenant)?;
        normalize_path(path)
    }

    pub fn absolute(&self, tenant: &str, path: &str) -> Result<String> {
        validate_tenant(tenant)?;
        normalize_path(path)
    }

    pub async fn flush(&self) -> Result<()> {
        self.inner.storage.flush().await?;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.inner.storage.close().await?;
        Ok(())
    }

    async fn compact_file(&self, tenant: &str, path: &str) -> Result<()> {
        let mut chunks = self.stream_chunks(tenant, path).await?;
        let mut upload = self.start_upload(tenant, path, false).await?;
        while let Some(chunk) = chunks.recv().await {
            if let Err(error) = upload.push(chunk?).await {
                let _ = upload.abort().await;
                return Err(error);
            }
        }
        match upload.finish(Durability::Written).await {
            Ok(_) => Ok(()),
            Err(error) => {
                let _ = upload.abort().await;
                Err(error)
            }
        }
    }

    async fn ensure_tenant(&self, tenant: &str) -> Result<TenantScope> {
        validate_tenant(tenant)?;
        let scope = tenant_scope(tenant);
        if let Some(root) = self.get_inode(scope, scope.root_id).await? {
            ensure_inode_tenant(&root, tenant)?;
            return Ok(scope);
        }

        let _guard = self.inner.mutation_lock.lock().await;
        if let Some(root) = self.get_inode(scope, scope.root_id).await? {
            ensure_inode_tenant(&root, tenant)?;
            return Ok(scope);
        }
        let root = Inode::directory(scope.root_id, tenant.to_owned(), now());
        self.inner
            .storage
            .put(vec![put(inode_key(scope, scope.root_id), encode(&root)?)])
            .await?;
        self.inner.storage.flush().await?;
        Ok(scope)
    }

    async fn cleanup_staged_uploads(&self) -> Result<()> {
        let snapshot = self.inner.storage.snapshot().await?;
        let mut ops = Vec::new();
        let mut iter = snapshot
            .scan_prefix_iter(upload_prefix(), BytesRange::unbounded(), None)
            .await?;
        while let Some(record) = iter.next().await? {
            let scope = tenant_scope_from_key(&record.key)?;
            let marker: UploadMarker = decode(&record.value)?;
            for index in 0..marker.chunk_count {
                ops.push(RecordOp::Delete(chunk_key(
                    scope,
                    marker.inode_id,
                    marker.generation_id,
                    index,
                )));
                self.flush_delete_batch(&mut ops).await?;
            }
            ops.push(RecordOp::Delete(record.key));
            self.flush_delete_batch(&mut ops).await?;
        }
        if !ops.is_empty() {
            self.inner.storage.apply(ops).await?;
            self.inner.storage.flush().await?;
        }
        Ok(())
    }

    async fn cleanup_garbage(&self) -> Result<()> {
        let snapshot = self.inner.storage.snapshot().await?;
        let mut iter = snapshot
            .scan_prefix_iter(garbage_prefix(), BytesRange::unbounded(), None)
            .await?;
        while let Some(record) = iter.next().await? {
            let scope = tenant_scope_from_key(&record.key)?;
            let marker = decode::<GarbageMarker>(&record.value)?;
            self.cleanup_generation_chain(scope, &marker).await?;
        }
        Ok(())
    }

    async fn cleanup_generation_chain(
        &self,
        scope: TenantScope,
        marker: &GarbageMarker,
    ) -> Result<()> {
        let mut cursor = Some(marker.head);
        let mut ops = Vec::new();
        let mut traversed = 0_u64;
        while let Some(id) = cursor {
            traversed += 1;
            if traversed > MAX_GENERATION_CHAIN {
                return Err(Error::Corrupt(
                    "generation chain exceeds traversal limit".to_owned(),
                ));
            }
            let key = generation_key(scope, marker.inode_id, id);
            let Some(generation) = self.get_json::<Generation>(key.clone()).await? else {
                break;
            };
            for index in 0..generation.chunk_count {
                ops.push(RecordOp::Delete(chunk_key(
                    scope,
                    marker.inode_id,
                    id,
                    index,
                )));
                self.flush_delete_batch(&mut ops).await?;
            }
            ops.push(RecordOp::Delete(key));
            self.flush_delete_batch(&mut ops).await?;
            cursor = generation.previous;
        }
        if !ops.is_empty() {
            self.inner.storage.apply(std::mem::take(&mut ops)).await?;
        }
        ops.push(RecordOp::Delete(garbage_key(scope, marker.head)));
        self.inner.storage.apply(ops).await?;
        Ok(())
    }

    async fn flush_delete_batch(&self, ops: &mut Vec<RecordOp>) -> Result<()> {
        if ops.len() >= DELETE_BATCH_OPS {
            self.inner.storage.apply(std::mem::take(ops)).await?;
        }
        Ok(())
    }

    async fn resolve(&self, tenant: &str, path: &str) -> Result<Inode> {
        let scope = self.ensure_tenant(tenant).await?;
        self.resolve_from_storage(tenant, scope, path).await
    }

    async fn resolve_from_storage(
        &self,
        tenant: &str,
        scope: TenantScope,
        path: &str,
    ) -> Result<Inode> {
        self.resolve_from(self.inner.storage.as_ref(), tenant, scope, path)
            .await
    }

    async fn resolve_from(
        &self,
        storage: &dyn StorageRead,
        tenant: &str,
        scope: TenantScope,
        path: &str,
    ) -> Result<Inode> {
        let normalized = normalize_path(path)?;
        let mut inode = self
            .get_inode_from(storage, scope, scope.root_id)
            .await?
            .ok_or_else(|| Error::Corrupt("root inode is missing".to_owned()))?;
        ensure_inode_tenant(&inode, tenant)?;
        if normalized == "/" {
            return Ok(inode);
        }
        for component in components(&normalized) {
            if inode.kind != FileKind::Directory {
                return Err(Error::NotDirectory(normalized.clone()));
            }
            let entry = storage
                .get(directory_entry_key(scope, inode.id, component))
                .await?
                .ok_or_else(|| Error::NotFound(normalized.clone()))?;
            let (child_id, _, entry_root) = decode_directory_entry(&entry.value)?;
            if let Some(entry_root) = entry_root {
                ensure_directory_entry_root(entry_root, scope.root_id)?;
            }
            inode = self
                .get_inode_from(storage, scope, child_id)
                .await?
                .ok_or_else(|| Error::Corrupt(format!("missing inode {child_id}")))?;
            ensure_inode_tenant(&inode, tenant)?;
        }
        Ok(inode)
    }

    async fn ensure_directory_path(
        &self,
        tenant: &str,
        scope: TenantScope,
        path: &str,
        ops: &mut Vec<RecordOp>,
    ) -> Result<Uuid> {
        let normalized = normalize_path(path)?;
        let mut current = self
            .get_inode(scope, scope.root_id)
            .await?
            .ok_or_else(|| Error::Corrupt("root inode is missing".to_owned()))?;
        ensure_inode_tenant(&current, tenant)?;
        for component in components(&normalized) {
            let key = directory_entry_key(scope, current.id, component);
            if let Some(record) = self.inner.storage.get(key.clone()).await? {
                let (child_id, _, entry_root) = decode_directory_entry(&record.value)?;
                if let Some(entry_root) = entry_root {
                    ensure_directory_entry_root(entry_root, scope.root_id)?;
                }
                current = self
                    .get_inode(scope, child_id)
                    .await?
                    .ok_or_else(|| Error::Corrupt(format!("missing inode {child_id}")))?;
                ensure_inode_tenant(&current, tenant)?;
                if current.kind != FileKind::Directory {
                    return Err(Error::NotDirectory(normalized.clone()));
                }
            } else {
                let child = Inode::directory(Uuid::new_v4(), tenant.to_owned(), now());
                ops.push(put_op(inode_key(scope, child.id), encode(&child)?));
                ops.push(put_op(
                    key,
                    directory_entry_value(scope.root_id, child.id, child.kind),
                ));
                current = child;
            }
        }
        Ok(current.id)
    }

    async fn get_inode(&self, scope: TenantScope, id: Uuid) -> Result<Option<Inode>> {
        self.get_inode_from(self.inner.storage.as_ref(), scope, id)
            .await
    }

    async fn get_inode_from(
        &self,
        storage: &dyn StorageRead,
        scope: TenantScope,
        id: Uuid,
    ) -> Result<Option<Inode>> {
        self.get_json_from(storage, inode_key(scope, id)).await
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, key: Bytes) -> Result<Option<T>> {
        self.get_json_from(self.inner.storage.as_ref(), key).await
    }

    async fn get_json_from<T: serde::de::DeserializeOwned>(
        &self,
        storage: &dyn StorageRead,
        key: Bytes,
    ) -> Result<Option<T>> {
        storage
            .get(key)
            .await?
            .map(|record| decode(&record.value))
            .transpose()
    }

    async fn has_directory_entries(&self, scope: TenantScope, id: Uuid) -> Result<bool> {
        let mut iter = self
            .inner
            .storage
            .scan_prefix_iter(directory_prefix(scope, id), BytesRange::unbounded(), None)
            .await?;
        Ok(iter.next().await?.is_some())
    }

    async fn apply_records(&self, ops: Vec<RecordOp>, durability: Durability) -> Result<()> {
        self.inner
            .storage
            .apply_with_options(
                ops,
                WriteOptions {
                    await_durable: durability == Durability::Durable,
                },
            )
            .await?;
        Ok(())
    }

    async fn finish_durability(&self, durability: Durability) -> Result<()> {
        if durability == Durability::Written {
            self.inner.storage.flush().await?;
        }
        Ok(())
    }
}

pub fn normalize_path(path: &str) -> Result<String> {
    if path.as_bytes().contains(&0) {
        return Err(Error::InvalidPath("paths cannot contain NUL".to_owned()));
    }
    let absolute = path.starts_with('/');
    let mut normalized: Vec<&str> = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                if normalized.pop().is_none() {
                    return Err(Error::InvalidPath(
                        "path traversal cannot escape the filesystem root".to_owned(),
                    ));
                }
            }
            value => normalized.push(value),
        }
    }
    if !absolute && path.is_empty() {
        return Ok("/".to_owned());
    }
    Ok(format!("/{}", normalized.join("/")))
}

pub fn validate_tenant(tenant: &str) -> Result<()> {
    let bytes = tenant.as_bytes();
    if bytes.is_empty() || bytes.len() > 128 {
        return Err(Error::InvalidPath(
            "tenant must contain between 1 and 128 bytes".to_owned(),
        ));
    }
    if !bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        || !bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(Error::InvalidPath(
            "tenant must start and end with an ASCII letter or digit and contain only letters, digits, '-', '_', or '.'"
                .to_owned(),
        ));
    }
    Ok(())
}

fn tenant_root_id(tenant: &str) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"plural-telemetry-pseudofs-tenant-root-v1\0");
    hasher.update(tenant.as_bytes());
    let mut id = [0_u8; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Uuid::from_bytes(id)
}

fn tenant_scope(tenant: &str) -> TenantScope {
    TenantScope {
        root_id: tenant_root_id(tenant),
    }
}

fn ensure_inode_tenant(inode: &Inode, tenant: &str) -> Result<()> {
    if inode.tenant == tenant {
        Ok(())
    } else {
        Err(Error::Corrupt(
            "inode crosses a tenant filesystem boundary".to_owned(),
        ))
    }
}

fn ensure_directory_entry_root(entry_root: Uuid, expected_root: Uuid) -> Result<()> {
    if entry_root == expected_root {
        Ok(())
    } else {
        Err(Error::Corrupt(
            "directory entry crosses a tenant filesystem boundary".to_owned(),
        ))
    }
}

fn components(path: &str) -> impl Iterator<Item = &str> {
    path.trim_start_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
}

fn parent_and_name(path: &str) -> Result<(String, String)> {
    let normalized = normalize_path(path)?;
    if normalized == "/" {
        return Err(Error::RootOperation);
    }
    let index = normalized
        .rfind('/')
        .ok_or_else(|| Error::InvalidPath(normalized.clone()))?;
    let parent = if index == 0 {
        "/".to_owned()
    } else {
        normalized[..index].to_owned()
    };
    Ok((parent, normalized[index + 1..].to_owned()))
}

fn put(key: Bytes, value: Bytes) -> PutRecordOp {
    PutRecordOp::new(Record::new(key, value))
}

fn put_op(key: Bytes, value: Bytes) -> RecordOp {
    RecordOp::Put(put(key, value))
}

fn directory_entry_value(root_id: Uuid, id: Uuid, kind: FileKind) -> Bytes {
    let mut value = Vec::with_capacity(33);
    value.push(match kind {
        FileKind::File => 1,
        FileKind::Directory => 2,
    });
    value.extend_from_slice(root_id.as_bytes());
    value.extend_from_slice(id.as_bytes());
    Bytes::from(value)
}

fn decode_directory_entry(value: &[u8]) -> Result<(Uuid, Option<FileKind>, Option<Uuid>)> {
    let (kind, root, id) = match value {
        id if id.len() == 16 => (None, None, id),
        [tag, id @ ..] if id.len() == 16 => {
            let kind = match tag {
                1 => FileKind::File,
                2 => FileKind::Directory,
                _ => {
                    return Err(Error::Corrupt(format!(
                        "invalid directory entry kind: {tag}"
                    )));
                }
            };
            (Some(kind), None, id)
        }
        [tag, root @ ..] if root.len() == 32 => {
            let kind = match tag {
                1 => FileKind::File,
                2 => FileKind::Directory,
                _ => {
                    return Err(Error::Corrupt(format!(
                        "invalid directory entry kind: {tag}"
                    )));
                }
            };
            (Some(kind), Some(&root[..16]), &root[16..])
        }
        _ => {
            return Err(Error::Corrupt(format!(
                "invalid directory entry length: {}",
                value.len()
            )));
        }
    };
    let id = Uuid::from_slice(id)
        .map_err(|error| Error::Corrupt(format!("invalid inode ID: {error}")))?;
    let root = root
        .map(Uuid::from_slice)
        .transpose()
        .map_err(|error| Error::Corrupt(format!("invalid tenant root ID: {error}")))?;
    Ok((id, kind, root))
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |duration| duration.as_secs_f64())
}

#[cfg(test)]
mod tests;
