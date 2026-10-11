// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::collections::HashSet;
use std::io::SeekFrom;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use common::storage::config::{ObjectStoreConfig, StorageConfig};
use common::storage::{PutRecordOp, Record, RecordOp, Storage, StorageRead, StorageSnapshot};
use common::{BytesRange, StorageBuilder, StorageSemantics};
use futures::StreamExt;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{Mutex, Notify, mpsc};
use uuid::Uuid;

use crate::blob::{BlobStore, StagedBlob};
use crate::codec::{
    PseudofsSegmentExtractor, TenantScope, blob_id_from_key, content_key, dead_blob_key,
    dead_blob_prefix, decode, directory_entry_key, directory_entry_name, directory_prefix, encode,
    garbage_key, garbage_prefix, generation_key, inode_key, pending_blob_key, pending_blob_prefix,
    tenant_scope_from_key,
};
use crate::config::Config;
use crate::error::{Error, Result};
use crate::model::{
    Content, DeadBlob, DirectoryEntry, Durability, FileHandle, FileKind, FileStat, GarbageMarker,
    Generation, Inode, PendingBlob,
};

mod upload;

pub use upload::FileUpload;

const DELETE_BATCH_OPS: usize = 512;
const MAX_GENERATION_CHAIN: u64 = 1_000_000;
const BLOB_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5);
const BLOB_SCAN_BATCH: usize = 1024;

#[derive(Clone)]
pub struct PseudoFs {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) blobs: BlobStore,
    pub(crate) config: Config,
    mutation_lock: Mutex<()>,
    compaction_tx: mpsc::Sender<CompactionRequest>,
    blob_notify: Arc<Notify>,
    /// Serializes maintenance passes so an upload and a deletion never run for
    /// the same blob at once.
    maintenance_lock: Mutex<()>,
    closed: AtomicBool,
}

struct CompactionRequest {
    tenant: String,
    path: String,
}

impl PseudoFs {
    pub async fn open(config: Config) -> Result<Self> {
        config.validate().map_err(Error::InvalidPath)?;
        let semantics =
            StorageSemantics::new().with_segment_extractor(PseudofsSegmentExtractor::shared());
        let mut builder = StorageBuilder::new(&config.storage)
            .await?
            .with_semantics(semantics);
        if let Some(path) = &config.metastore_wal_path {
            let wal = local_object_store(path)?;
            builder = builder.map_slatedb(move |db| db.with_wal_object_store(wal));
        }
        let storage = builder.build().await?;
        let pending = pending_blob_ids(storage.as_ref()).await?;
        let (remote, prefix) = blob_remote(&config)?;
        let blobs = BlobStore::open(
            &config.blobs.local_path,
            remote,
            &prefix,
            config.blobs.cache_capacity_bytes,
            &pending,
        )
        .await?;
        let (compaction_tx, mut compaction_rx) = mpsc::channel::<CompactionRequest>(64);
        let blob_notify = Arc::new(Notify::new());
        let fs = Self {
            inner: Arc::new(Inner {
                storage,
                blobs,
                config,
                mutation_lock: Mutex::new(()),
                compaction_tx,
                blob_notify: Arc::clone(&blob_notify),
                maintenance_lock: Mutex::new(()),
                closed: AtomicBool::new(false),
            }),
        };
        fs.cleanup_garbage().await?;

        let compactor = Arc::downgrade(&fs.inner);
        tokio::spawn(async move {
            while let Some(request) = compaction_rx.recv().await {
                let Some(inner) = compactor.upgrade() else {
                    break;
                };
                let fs = PseudoFs { inner };
                if let Err(error) = fs.compact_file(&request.tenant, &request.path).await {
                    tracing::warn!(path = %request.path, "append compaction failed: {error}");
                }
            }
        });
        spawn_blob_worker(Arc::downgrade(&fs.inner), blob_notify);
        Ok(fs)
    }

    pub fn chunk_size_bytes(&self) -> usize {
        self.inner.config.chunk_size_bytes
    }

    pub async fn exists(&self, tenant: &str, path: &str) -> Result<bool> {
        Ok(self.lookup(tenant, path).await?.is_some())
    }

    pub async fn is_file(&self, tenant: &str, path: &str) -> Result<bool> {
        Ok(self
            .lookup(tenant, path)
            .await?
            .is_some_and(|inode| inode.kind == FileKind::File))
    }

    pub async fn is_dir(&self, tenant: &str, path: &str) -> Result<bool> {
        Ok(self
            .lookup(tenant, path)
            .await?
            .is_some_and(|inode| inode.kind == FileKind::Directory))
    }

    /// Resolves `path`, treating a missing entry or a file in place of a
    /// parent directory as absent; invalid input and storage errors propagate.
    async fn lookup(&self, tenant: &str, path: &str) -> Result<Option<Inode>> {
        match self.resolve(tenant, path).await {
            Ok(inode) => Ok(Some(inode)),
            Err(Error::NotFound(_) | Error::NotDirectory(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn is_symlink(&self, tenant: &str, path: &str) -> Result<bool> {
        validate_tenant(tenant)?;
        normalize_path(path)?;
        Ok(false)
    }

    pub async fn read_bytes(&self, tenant: &str, path: &str) -> Result<Bytes> {
        self.read_bytes_within(tenant, path, u64::MAX).await
    }

    /// Reads a whole file as of one snapshot, failing with
    /// [`Error::ResourceExhausted`] if it is larger than `max_size`.
    pub async fn read_bytes_within(
        &self,
        tenant: &str,
        path: &str,
        max_size: u64,
    ) -> Result<Bytes> {
        let (size, mut stream) = self.open_range(tenant, path, 0, None).await?;
        if size > max_size {
            return Err(Error::ResourceExhausted(format!(
                "file size {size} exceeds {max_size}; stream it instead"
            )));
        }
        let mut output = Vec::with_capacity(size as usize);
        while let Some(chunk) = stream.recv().await {
            output.extend_from_slice(&chunk?);
        }
        Ok(Bytes::from(output))
    }

    pub async fn read_text(&self, tenant: &str, path: &str) -> Result<String> {
        self.read_text_within(tenant, path, u64::MAX).await
    }

    pub async fn read_text_within(
        &self,
        tenant: &str,
        path: &str,
        max_size: u64,
    ) -> Result<String> {
        let normalized = normalize_path(path)?;
        String::from_utf8(
            self.read_bytes_within(tenant, path, max_size)
                .await?
                .to_vec(),
        )
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
        self.stream_range(tenant, path, 0, None).await
    }

    /// Streams `length` bytes (or through end of file) starting at `offset`,
    /// as of a single snapshot. Offsets past the end yield an empty stream.
    pub async fn stream_range(
        &self,
        tenant: &str,
        path: &str,
        offset: u64,
        length: Option<u64>,
    ) -> Result<mpsc::Receiver<Result<Bytes>>> {
        Ok(self.open_range(tenant, path, offset, length).await?.1)
    }

    /// [`Self::stream_range`], plus the file's size in the same snapshot.
    async fn open_range(
        &self,
        tenant: &str,
        path: &str,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(u64, mpsc::Receiver<Result<Bytes>>)> {
        let normalized = normalize_path(path)?;
        let scope = self.ensure_tenant(tenant).await?;
        let snapshot = self.inner.storage.snapshot().await?;
        let inode = self
            .resolve_from(snapshot.as_ref(), tenant, scope, &normalized)
            .await?;
        if inode.kind == FileKind::Directory {
            return Err(Error::IsDirectory(normalized));
        }
        let generations = self
            .generation_chain(snapshot.as_ref(), scope, inode.id, inode.head)
            .await?;
        let end = length.map_or(u64::MAX, |length| offset.saturating_add(length));
        let (sender, receiver) = mpsc::channel(2);
        let fs = self.clone();
        tokio::spawn(async move {
            if let Err(error) = fs
                .send_content(snapshot, scope, &generations, offset..end, &sender)
                .await
            {
                let _ = sender.send(Err(error)).await;
            }
        });
        Ok((inode.size, receiver))
    }

    pub async fn write_bytes(
        &self,
        tenant: &str,
        path: &str,
        content: Bytes,
        durability: Durability,
    ) -> Result<u64> {
        let upload = self.start_upload(tenant, path, false).await?;
        upload.write_all(content, durability).await
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
        let upload = self.start_upload(tenant, path, true).await?;
        upload.write_all(content, durability).await
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
        let base_size = match self.resolve_from_storage(tenant, scope, &path).await {
            Ok(inode) if inode.kind == FileKind::Directory => {
                return Err(Error::IsDirectory(path));
            }
            Ok(inode) if append => inode.size,
            Ok(_) | Err(Error::NotFound(_)) => 0,
            Err(error) => return Err(error),
        };
        Ok(FileUpload::new(
            self.clone(),
            tenant.to_owned(),
            scope,
            path,
            append,
            base_size,
        ))
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
        self.inner.storage.apply(ops).await?;
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
        self.inner.storage.apply(ops).await?;
        drop(_guard);
        self.finish_durability(durability).await?;
        if let Some(marker) = garbage {
            self.collect_generation_chain(scope, &marker).await;
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
        self.inner
            .storage
            .apply(vec![
                RecordOp::Delete(directory_entry_key(scope, parent.id, &name)),
                RecordOp::Delete(inode_key(scope, inode.id)),
            ])
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
                let entry = decode_child(&record, scope).map(|child| DirectoryEntry {
                    path: join_path(&path, &child.name),
                    name: child.name,
                    kind: child.kind,
                });
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
            self.resolve(tenant, &source).await?;
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
        self.inner.storage.apply(ops).await?;
        drop(_guard);
        self.finish_durability(durability).await?;
        if let Some(marker) = garbage {
            self.collect_generation_chain(scope, &marker).await;
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

    /// Uploads pending blobs and deletes expired dead blobs now, rather than
    /// waiting for the background worker.
    pub async fn run_blob_maintenance(&self) -> Result<()> {
        let _guard = self.inner.maintenance_lock.lock().await;
        // One failing upload must not stop the other uploads or GC.
        let uploaded = self.upload_pending_blobs().await;
        let collected = self.collect_dead_blobs().await;
        uploaded.and(collected)
    }

    pub async fn close(&self) -> Result<()> {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.storage.close().await?;
        Ok(())
    }

    /// Rewrites a long append chain as one generation. The rewrite reads the
    /// chain from one snapshot and commits only if the file still has that
    /// head, so concurrent appends are never lost.
    async fn compact_file(&self, tenant: &str, path: &str) -> Result<()> {
        let scope = self.ensure_tenant(tenant).await?;
        let snapshot = self.inner.storage.snapshot().await?;
        let inode = match self
            .resolve_from(snapshot.as_ref(), tenant, scope, path)
            .await
        {
            Ok(inode) => inode,
            Err(Error::NotFound(_)) => return Ok(()),
            Err(error) => return Err(error),
        };
        if inode.kind != FileKind::File || inode.generation_count < 2 {
            return Ok(());
        }
        let generations = self
            .generation_chain(snapshot.as_ref(), scope, inode.id, inode.head)
            .await?;
        let mut upload = FileUpload::new(
            self.clone(),
            tenant.to_owned(),
            scope,
            path.to_owned(),
            false,
            0,
        )
        .expecting(inode.id, inode.head);
        let (sender, mut receiver) = mpsc::channel(2);
        let reader = self.clone();
        tokio::spawn(async move {
            if let Err(error) = reader
                .send_content(snapshot, scope, &generations, 0..u64::MAX, &sender)
                .await
            {
                let _ = sender.send(Err(error)).await;
            }
        });
        while let Some(chunk) = receiver.recv().await {
            upload.push(chunk?).await?;
        }
        match upload.finish(Durability::Applied).await {
            Ok(_) | Err(Error::Conflict(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn ensure_tenant(&self, tenant: &str) -> Result<TenantScope> {
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

    /// Returns a file's generations, oldest first.
    pub(crate) async fn generation_chain(
        &self,
        storage: &dyn StorageRead,
        scope: TenantScope,
        inode_id: Uuid,
        head: Option<Uuid>,
    ) -> Result<Vec<Generation>> {
        let mut generations = Vec::new();
        let mut cursor = head;
        while let Some(id) = cursor {
            if generations.len() as u64 >= MAX_GENERATION_CHAIN {
                return Err(Error::Corrupt("generation chain is cyclic".to_owned()));
            }
            let generation: Generation = self
                .get_json_from(storage, generation_key(scope, inode_id, id))
                .await?
                .ok_or_else(|| Error::Corrupt(format!("missing generation {id}")))?;
            cursor = generation.previous;
            generations.push(generation);
        }
        generations.reverse();
        Ok(generations)
    }

    /// Sends the bytes of `range` (file offsets) from `generations` in
    /// `chunk_size_bytes` pieces. Returns early if the receiver goes away.
    pub(crate) async fn send_content(
        &self,
        snapshot: Arc<dyn StorageSnapshot>,
        scope: TenantScope,
        generations: &[Generation],
        range: Range<u64>,
        sender: &mpsc::Sender<Result<Bytes>>,
    ) -> Result<()> {
        let mut position = 0_u64;
        for generation in generations {
            let generation_start = position;
            let generation_end = position + generation.byte_len;
            position = generation_end;
            let start = range.start.max(generation_start);
            let end = range.end.min(generation_end);
            if start >= end {
                continue;
            }
            let local = (start - generation_start)..(end - generation_start);
            let open = match generation.content {
                Content::Inline => {
                    let value = snapshot
                        .get(content_key(scope, generation.inode_id, generation.id))
                        .await?
                        .ok_or_else(|| {
                            Error::Corrupt(format!(
                                "missing inline content for generation {}",
                                generation.id
                            ))
                        })?
                        .value;
                    let slice = value
                        .get(local.start as usize..local.end as usize)
                        .ok_or_else(|| {
                            Error::Corrupt(format!(
                                "inline content shorter than generation {}",
                                generation.id
                            ))
                        })?;
                    self.send_pieces(Bytes::copy_from_slice(slice), sender)
                        .await
                }
                Content::Blob { id } => {
                    self.send_blob(id, generation.byte_len, local, sender)
                        .await?
                }
            };
            if !open {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn send_pieces(&self, bytes: Bytes, sender: &mpsc::Sender<Result<Bytes>>) -> bool {
        let chunk = self.inner.config.chunk_size_bytes;
        let mut offset = 0;
        while offset < bytes.len() {
            let end = (offset + chunk).min(bytes.len());
            if sender.send(Ok(bytes.slice(offset..end))).await.is_err() {
                return false;
            }
            offset = end;
        }
        true
    }

    async fn send_blob(
        &self,
        id: Uuid,
        size: u64,
        range: Range<u64>,
        sender: &mpsc::Sender<Result<Bytes>>,
    ) -> Result<bool> {
        let chunk = self.inner.config.chunk_size_bytes as u64;
        let blobs = &self.inner.blobs;
        let mut file = blobs.open_local(id).await?;
        if file.is_none() && range.start == 0 && range.end == size && size <= blobs.capacity() {
            file = Some(blobs.fetch(id).await?);
        }
        if let Some(mut file) = file {
            file.seek(SeekFrom::Start(range.start)).await?;
            let mut remaining = range.end - range.start;
            while remaining > 0 {
                let length = remaining.min(chunk) as usize;
                let mut buffer = vec![0_u8; length];
                file.read_exact(&mut buffer).await?;
                remaining -= length as u64;
                if sender.send(Ok(Bytes::from(buffer))).await.is_err() {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        let mut stream = blobs.remote_range(id, range).await?;
        let mut buffer = BytesMut::new();
        while let Some(bytes) = stream.next().await {
            buffer.extend_from_slice(&bytes?);
            while buffer.len() as u64 >= chunk {
                let piece = buffer.split_to(chunk as usize).freeze();
                if sender.send(Ok(piece)).await.is_err() {
                    return Ok(false);
                }
            }
        }
        if !buffer.is_empty() && sender.send(Ok(buffer.freeze())).await.is_err() {
            return Ok(false);
        }
        Ok(true)
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

    pub(crate) async fn collect_generation_chain(
        &self,
        scope: TenantScope,
        marker: &GarbageMarker,
    ) {
        if let Err(error) = self.cleanup_generation_chain(scope, marker).await {
            tracing::warn!("generation cleanup failed; retrying at next start: {error}");
        }
    }

    /// Deletes an unreferenced generation chain. Inline content is deleted
    /// immediately; blobs are queued for deletion after the grace period.
    async fn cleanup_generation_chain(
        &self,
        scope: TenantScope,
        marker: &GarbageMarker,
    ) -> Result<()> {
        let mut cursor = Some(marker.head);
        let mut ops = Vec::new();
        let mut traversed = 0_u64;
        let dead = encode(&DeadBlob { deleted_at: now() })?;
        let mut queued_blobs = false;
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
            match generation.content {
                Content::Inline => {
                    ops.push(RecordOp::Delete(content_key(scope, marker.inode_id, id)))
                }
                Content::Blob { id: blob } => {
                    ops.push(put_op(dead_blob_key(scope, blob), dead.clone()));
                    queued_blobs = true;
                }
            }
            ops.push(RecordOp::Delete(key));
            self.flush_delete_batch(&mut ops).await?;
            cursor = generation.previous;
        }
        ops.push(RecordOp::Delete(garbage_key(scope, marker.head)));
        self.inner.storage.apply(ops).await?;
        if queued_blobs {
            self.inner.blob_notify.notify_one();
        }
        Ok(())
    }

    async fn flush_delete_batch(&self, ops: &mut Vec<RecordOp>) -> Result<()> {
        if ops.len() >= DELETE_BATCH_OPS {
            self.inner.storage.apply(std::mem::take(ops)).await?;
        }
        Ok(())
    }

    pub(crate) fn notify_blob_worker(&self) {
        self.inner.blob_notify.notify_one();
    }

    /// Uploads pending blobs in batches until none are left, one fails, or a
    /// full batch removes no pending keys (dead blobs awaiting GC).
    async fn upload_pending_blobs(&self) -> Result<()> {
        loop {
            let pending =
                scan_blob_keys(self.inner.storage.as_ref(), pending_blob_prefix()).await?;
            let full = pending.len() >= BLOB_SCAN_BATCH;
            let results: Vec<Result<bool>> = futures::stream::iter(pending)
                .map(|(key, id)| self.upload_blob(key, id))
                .buffer_unordered(self.inner.config.blobs.upload_concurrency)
                .collect()
                .await;
            let mut progressed = false;
            for result in results {
                progressed |= result?;
            }
            if !full || !progressed {
                return Ok(());
            }
        }
    }

    /// Uploads one pending blob, returning whether its pending key was removed.
    async fn upload_blob(&self, key: Bytes, id: Uuid) -> Result<bool> {
        let storage = &self.inner.storage;
        if storage.get(key.clone()).await?.is_none() {
            return Ok(false);
        }
        let scope = tenant_scope_from_key(&key)?;
        if storage.get(dead_blob_key(scope, id)).await?.is_some() {
            return Ok(false);
        }
        // The pending key's deletion is not flushed before the blob is
        // unpinned, so after a crash it can outlive an evicted local copy.
        let blobs = &self.inner.blobs;
        if !blobs.has_local(id).await? && blobs.has_remote(id).await? {
            storage.apply(vec![RecordOp::Delete(key)]).await?;
            return Ok(true);
        }
        if let Err(error) = blobs.upload(id).await {
            tracing::error!(blob = %id, "blob upload failed: {error}");
            return Err(error);
        }
        storage.apply(vec![RecordOp::Delete(key)]).await?;
        blobs.mark_uploaded(id).await;
        Ok(true)
    }

    async fn collect_dead_blobs(&self) -> Result<()> {
        let grace = self.inner.config.blobs.gc_grace_seconds as f64;
        let cutoff = now() - grace;
        let snapshot = self.inner.storage.snapshot().await?;
        let mut iter = snapshot
            .scan_prefix_iter(dead_blob_prefix(), BytesRange::unbounded(), None)
            .await?;
        let mut expired = Vec::new();
        while let Some(record) = iter.next().await? {
            let dead: DeadBlob = decode(&record.value)?;
            if dead.deleted_at <= cutoff {
                let id = blob_id_from_key(&record.key)?;
                expired.push((record.key, id));
            }
            if expired.len() >= BLOB_SCAN_BATCH {
                break;
            }
        }
        for (key, id) in expired {
            self.inner.blobs.delete(id).await?;
            let scope = tenant_scope_from_key(&key)?;
            self.inner
                .storage
                .apply(vec![
                    RecordOp::Delete(pending_blob_key(scope, id)),
                    RecordOp::Delete(key),
                ])
                .await?;
        }
        Ok(())
    }

    /// Resolves from one snapshot: walking live storage could follow an entry
    /// to an inode a concurrent unlink or replacing rename has just deleted.
    pub(crate) async fn resolve(&self, tenant: &str, path: &str) -> Result<Inode> {
        let scope = self.ensure_tenant(tenant).await?;
        let snapshot = self.inner.storage.snapshot().await?;
        self.resolve_from(snapshot.as_ref(), tenant, scope, path)
            .await
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

    pub(crate) async fn resolve_from(
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
            let (child_id, _) = decode_directory_entry(&entry.value, scope)?;
            inode = self
                .get_inode_from(storage, scope, child_id)
                .await?
                .ok_or_else(|| Error::Corrupt(format!("missing inode {child_id}")))?;
            ensure_inode_tenant(&inode, tenant)?;
        }
        Ok(inode)
    }

    pub(crate) async fn ensure_directory_path(
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
                let (child_id, _) = decode_directory_entry(&record.value, scope)?;
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

    pub(crate) async fn finish_durability(&self, durability: Durability) -> Result<()> {
        if durability != Durability::Applied {
            self.inner.storage.flush().await?;
        }
        Ok(())
    }

    pub(crate) async fn mutation_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.inner.mutation_lock.lock().await
    }

    pub(crate) async fn resolve_for_mutation(
        &self,
        tenant: &str,
        scope: TenantScope,
        path: &str,
    ) -> Result<Option<Inode>> {
        match self.resolve_from_storage(tenant, scope, path).await {
            Ok(inode) => Ok(Some(inode)),
            Err(Error::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn request_compaction(&self, tenant: &str, path: &str) {
        let _ = self.inner.compaction_tx.try_send(CompactionRequest {
            tenant: tenant.to_owned(),
            path: path.to_owned(),
        });
    }

    pub(crate) async fn stage_blob(&self) -> Result<StagedBlob> {
        self.inner.blobs.stage().await
    }
}

fn spawn_blob_worker(inner: Weak<Inner>, notify: Arc<Notify>) {
    tokio::spawn(async move {
        loop {
            {
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                if inner.closed.load(Ordering::Acquire) {
                    return;
                }
                let fs = PseudoFs { inner };
                if let Err(error) = fs.run_blob_maintenance().await {
                    tracing::warn!("blob maintenance failed: {error}");
                }
            }
            tokio::select! {
                () = notify.notified() => {}
                () = tokio::time::sleep(BLOB_MAINTENANCE_INTERVAL) => {}
            }
        }
    });
}

async fn scan_blob_keys(storage: &dyn StorageRead, prefix: Bytes) -> Result<Vec<(Bytes, Uuid)>> {
    let mut iter = storage
        .scan_prefix_iter(prefix, BytesRange::unbounded(), None)
        .await?;
    let mut keys = Vec::new();
    while let Some(record) = iter.next().await? {
        let id = blob_id_from_key(&record.key)?;
        keys.push((record.key, id));
        if keys.len() >= BLOB_SCAN_BATCH {
            break;
        }
    }
    Ok(keys)
}

async fn pending_blob_ids(storage: &dyn StorageRead) -> Result<HashSet<Uuid>> {
    let mut iter = storage
        .scan_prefix_iter(pending_blob_prefix(), BytesRange::unbounded(), None)
        .await?;
    let mut ids = HashSet::new();
    while let Some(record) = iter.next().await? {
        decode::<PendingBlob>(&record.value)?;
        ids.insert(blob_id_from_key(&record.key)?);
    }
    Ok(ids)
}

fn local_object_store(path: &str) -> Result<Arc<dyn ObjectStore>> {
    std::fs::create_dir_all(path)?;
    Ok(Arc::new(
        LocalFileSystem::new_with_prefix(path)?.with_fsync(true),
    ))
}

fn blob_remote(config: &Config) -> Result<(Arc<dyn ObjectStore>, String)> {
    let (default_store, default_prefix) = match &config.storage {
        StorageConfig::SlateDb(slate) => (
            Some(slate.object_store.clone()),
            format!("{}-blobs", slate.path),
        ),
        StorageConfig::InMemory => (None, "blobs".to_owned()),
    };
    let prefix = config.blobs.path.clone().unwrap_or(default_prefix);
    let store = match config.blobs.object_store.clone().or(default_store) {
        Some(ObjectStoreConfig::Local(local)) => local_object_store(&local.path)?,
        Some(ObjectStoreConfig::InMemory) | None => Arc::new(InMemory::new()),
        Some(other) => common::create_object_store(&other)?,
    };
    Ok((store, prefix))
}

pub(crate) struct Child {
    pub(crate) name: String,
    pub(crate) id: Uuid,
    pub(crate) kind: FileKind,
}

pub(crate) fn decode_child(record: &Record, scope: TenantScope) -> Result<Child> {
    let name = directory_entry_name(&record.key)?;
    let (id, kind) = decode_directory_entry(&record.value, scope)?;
    Ok(Child { name, id, kind })
}

pub(crate) fn join_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
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

pub(crate) fn components(path: &str) -> impl Iterator<Item = &str> {
    path.trim_start_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
}

pub(crate) fn parent_and_name(path: &str) -> Result<(String, String)> {
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

pub(crate) fn put_op(key: Bytes, value: Bytes) -> RecordOp {
    RecordOp::Put(put(key, value))
}

pub(crate) fn directory_entry_value(root_id: Uuid, id: Uuid, kind: FileKind) -> Bytes {
    let mut value = Vec::with_capacity(33);
    value.push(match kind {
        FileKind::File => 1,
        FileKind::Directory => 2,
    });
    value.extend_from_slice(root_id.as_bytes());
    value.extend_from_slice(id.as_bytes());
    Bytes::from(value)
}

fn decode_directory_entry(value: &[u8], scope: TenantScope) -> Result<(Uuid, FileKind)> {
    let [tag, root @ ..] = value else {
        return Err(Error::Corrupt("empty directory entry".to_owned()));
    };
    if root.len() != 32 {
        return Err(Error::Corrupt(format!(
            "invalid directory entry length: {}",
            value.len()
        )));
    }
    let kind = match tag {
        1 => FileKind::File,
        2 => FileKind::Directory,
        _ => {
            return Err(Error::Corrupt(format!(
                "invalid directory entry kind: {tag}"
            )));
        }
    };
    if root[..16] != *scope.root_id.as_bytes() {
        return Err(Error::Corrupt(
            "directory entry crosses a tenant filesystem boundary".to_owned(),
        ));
    }
    let id = Uuid::from_slice(&root[16..])
        .map_err(|error| Error::Corrupt(format!("invalid inode ID: {error}")))?;
    Ok((id, kind))
}

pub(crate) fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |duration| duration.as_secs_f64())
}

#[cfg(test)]
mod tests;
