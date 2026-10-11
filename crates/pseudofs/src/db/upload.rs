// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Streaming file uploads: inline buffering, blob staging, and the commit.

use super::*;

/// A file write in progress. Content up to `inline_threshold_bytes` is
/// buffered in memory and stored in the metastore; larger content spills to a
/// staged local blob. Nothing is visible until [`FileUpload::finish`].
pub struct FileUpload {
    fs: PseudoFs,
    tenant: String,
    scope: TenantScope,
    path: String,
    append: bool,
    base_size: u64,
    byte_len: u64,
    inline: BytesMut,
    staged: Option<StagedBlob>,
    compaction: Option<TailCompaction>,
    finished: bool,
}

impl FileUpload {
    pub(crate) fn new(
        fs: PseudoFs,
        tenant: String,
        scope: TenantScope,
        path: String,
        append: bool,
        base_size: u64,
    ) -> Self {
        Self {
            fs,
            tenant,
            scope,
            path,
            append,
            base_size,
            byte_len: 0,
            inline: BytesMut::new(),
            staged: None,
            compaction: None,
            finished: false,
        }
    }

    /// Commits the uploaded bytes as the merged replacement for `tail`,
    /// leaving the file's size and timestamps unchanged.
    pub(crate) fn compacting(mut self, tail: TailCompaction) -> Self {
        self.compaction = Some(tail);
        self
    }

    pub(crate) async fn write_all(mut self, content: Bytes, durability: Durability) -> Result<u64> {
        let result = async {
            self.push(content).await?;
            self.finish(durability).await
        }
        .await;
        if result.is_err() {
            let _ = self.abort().await;
        }
        result
    }

    pub async fn push(&mut self, chunk: Bytes) -> Result<()> {
        if self.finished {
            return Err(Error::Conflict(self.path.clone()));
        }
        if chunk.is_empty() {
            return Ok(());
        }
        let config = &self.fs.inner.config;
        let new_len = self
            .byte_len
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| Error::ResourceExhausted("file size overflow".to_owned()))?;
        if self.base_size.saturating_add(new_len) > config.max_file_size_bytes {
            return Err(Error::ResourceExhausted(format!(
                "file exceeds max_file_size_bytes ({})",
                config.max_file_size_bytes
            )));
        }
        self.byte_len = new_len;
        if self.staged.is_none() {
            if self.inline.len() + chunk.len() <= config.inline_threshold_bytes {
                self.inline.extend_from_slice(&chunk);
                return Ok(());
            }
            let mut staged = self.fs.stage_blob().await?;
            let inline = std::mem::take(&mut self.inline);
            self.fs
                .inner
                .blobs
                .write_reserved(&mut staged, &inline)
                .await?;
            self.staged = Some(staged);
        }
        match self.staged.as_mut() {
            Some(staged) => self.fs.inner.blobs.write_reserved(staged, &chunk).await,
            None => Err(Error::Blob("staged blob missing".to_owned())),
        }
    }

    pub async fn finish(&mut self, durability: Durability) -> Result<u64> {
        if self.finished {
            return Err(Error::Conflict(self.path.clone()));
        }
        self.finished = true;
        let fs = self.fs.clone();
        let blobs = &fs.inner.blobs;
        let content = match self.staged.take() {
            Some(staged) => {
                let id = staged.id;
                blobs.commit(staged).await?;
                if durability == Durability::Durable {
                    if let Err(error) = blobs.upload(id).await {
                        let _ = blobs.delete(id).await;
                        return Err(error);
                    }
                    blobs.mark_uploaded(id).await;
                }
                Content::Blob { id }
            }
            None => Content::Inline,
        };
        let committed = match self.compaction {
            Some(tail) => self.commit_compaction(content, tail).await,
            None => self.commit(content, durability).await,
        };
        match committed {
            Ok(size) => Ok(size),
            Err(error) => {
                if let Content::Blob { id } = content {
                    let _ = blobs.delete(id).await;
                }
                Err(error)
            }
        }
    }

    pub async fn abort(&mut self) -> Result<()> {
        self.inline = BytesMut::new();
        self.staged = None;
        self.finished = true;
        Ok(())
    }

    async fn commit(&mut self, content: Content, durability: Durability) -> Result<u64> {
        let fs = self.fs.clone();
        let scope = self.scope;
        let guard = fs.mutation_guard(scope).await;
        let current = fs
            .resolve_for_mutation(&self.tenant, scope, &self.path)
            .await?;
        if current
            .as_ref()
            .is_some_and(|inode| inode.kind == FileKind::Directory)
        {
            return Err(Error::IsDirectory(self.path.clone()));
        }
        if self.append
            && self.byte_len == 0
            && let Some(inode) = &current
        {
            return Ok(inode.size);
        }
        let (parent_path, name) = parent_and_name(&self.path)?;
        let mut ops = Vec::new();
        let parent = fs
            .ensure_directory_path(&self.tenant, scope, &parent_path, &mut ops)
            .await?;
        let timestamp = now();
        let mut inode =
            current.unwrap_or_else(|| Inode::file(Uuid::new_v4(), self.tenant.clone(), timestamp));
        let size = if self.append {
            inode.size + self.byte_len
        } else {
            self.byte_len
        };
        let max_size = fs.inner.config.max_file_size_bytes;
        if size > max_size {
            return Err(Error::ResourceExhausted(format!(
                "file exceeds max_file_size_bytes ({max_size})"
            )));
        }
        let max_chain = append_generation_cap(fs.inner.config.max_append_generations);
        if self.append && inode.generation_count >= max_chain {
            fs.request_compaction(&self.tenant, &self.path);
            return Err(Error::ResourceExhausted(format!(
                "'{}' has {} unmerged appends; retry once compaction catches up",
                self.path, inode.generation_count
            )));
        }
        let garbage = if self.append {
            None
        } else {
            GarbageMarker::chain(&inode)
        };
        let generation = Generation {
            inode_id: inode.id,
            id: Uuid::new_v4(),
            previous: if self.append { inode.head } else { None },
            byte_len: self.byte_len,
            content,
        };
        let mut pending_upload = false;
        match content {
            Content::Inline if self.byte_len > 0 => ops.push(put_op(
                content_key(scope, inode.id, generation.id),
                std::mem::take(&mut self.inline).freeze(),
            )),
            Content::Inline => {}
            Content::Blob { id } if durability != Durability::Durable => {
                ops.push(put_op(
                    pending_blob_key(scope, id),
                    encode(&PendingBlob {
                        size: self.byte_len,
                    })?,
                ));
                pending_upload = true;
            }
            Content::Blob { .. } => {}
        }
        inode.kind = FileKind::File;
        inode.size = size;
        inode.mtime = timestamp;
        inode.ctime = timestamp;
        inode.head = Some(generation.id);
        inode.generation_count = if self.append {
            inode.generation_count + 1
        } else {
            1
        };
        ops.push(put_op(
            generation_key(scope, inode.id, generation.id),
            encode(&generation)?,
        ));
        ops.push(put_op(inode_key(scope, inode.id), encode(&inode)?));
        ops.push(put_op(
            directory_entry_key(scope, parent, &name),
            directory_entry_value(scope.root_id, inode.id, FileKind::File),
        ));
        if let Some(marker) = &garbage {
            ops.push(put_op(garbage_key(scope, marker.head), encode(marker)?));
        }
        fs.inner.storage.apply(ops).await?;
        drop(guard);
        if self.append && inode.generation_count >= fs.inner.config.max_append_generations {
            fs.request_compaction(&self.tenant, &self.path);
        }
        fs.finish_durability(durability).await?;
        if garbage.is_some() || pending_upload {
            fs.notify_blob_worker();
        }
        Ok(size)
    }

    /// Commits a merged copy of the generations after `prefix` through
    /// `old_head`. Appends committed since the merge was read stay on top: the
    /// generation that pointed at `old_head` is repointed at the merged one.
    async fn commit_compaction(&mut self, content: Content, tail: TailCompaction) -> Result<u64> {
        let fs = self.fs.clone();
        let scope = self.scope;
        let _guard = fs.mutation_guard(scope).await;
        let conflict = || Error::Conflict(self.path.clone());
        let mut inode = fs
            .resolve_for_mutation(&self.tenant, scope, &self.path)
            .await?
            .filter(|inode| inode.id == tail.inode_id && inode.kind == FileKind::File)
            .ok_or_else(conflict)?;
        let generation = Generation {
            inode_id: inode.id,
            id: Uuid::new_v4(),
            previous: tail.prefix,
            byte_len: self.byte_len,
            content,
        };
        let mut ops = Vec::new();
        if inode.head == Some(tail.old_head) {
            inode.head = Some(generation.id);
        } else {
            let mut cursor = inode.head;
            let mut steps = 0;
            let newer = loop {
                let id = cursor
                    .filter(|_| steps < inode.generation_count)
                    .ok_or_else(conflict)?;
                steps += 1;
                let key = generation_key(scope, inode.id, id);
                let newer: Generation = fs
                    .get_json(key.clone())
                    .await?
                    .ok_or_else(|| Error::Corrupt(format!("missing generation {id}")))?;
                if newer.previous == Some(tail.old_head) {
                    break (key, newer);
                }
                cursor = newer.previous;
            };
            let (key, mut newer) = newer;
            newer.previous = Some(generation.id);
            ops.push(put_op(key, encode(&newer)?));
        }
        let pending_upload = match content {
            Content::Inline if self.byte_len > 0 => {
                ops.push(put_op(
                    content_key(scope, inode.id, generation.id),
                    std::mem::take(&mut self.inline).freeze(),
                ));
                false
            }
            Content::Inline => false,
            Content::Blob { id } => {
                ops.push(put_op(
                    pending_blob_key(scope, id),
                    encode(&PendingBlob {
                        size: self.byte_len,
                    })?,
                ));
                true
            }
        };
        inode.generation_count = inode.generation_count + 1 - tail.generations;
        let marker = GarbageMarker {
            inode_id: inode.id,
            head: tail.old_head,
            stop: tail.prefix,
        };
        ops.push(put_op(
            generation_key(scope, inode.id, generation.id),
            encode(&generation)?,
        ));
        ops.push(put_op(inode_key(scope, inode.id), encode(&inode)?));
        ops.push(put_op(garbage_key(scope, marker.head), encode(&marker)?));
        fs.inner.storage.apply(ops).await?;
        fs.notify_garbage_worker();
        if pending_upload {
            fs.notify_blob_worker();
        }
        Ok(inode.size)
    }
}

/// The run of an append chain a compaction replaces: `generations`
/// generations from `old_head` back to, but not including, `prefix`.
#[derive(Clone, Copy)]
pub(crate) struct TailCompaction {
    pub(crate) inode_id: Uuid,
    pub(crate) old_head: Uuid,
    pub(crate) prefix: Option<Uuid>,
    pub(crate) generations: u64,
}

/// Appends fail past this many unmerged generations, bounding read cost when
/// compaction falls behind.
pub(crate) fn append_generation_cap(max_append_generations: u64) -> u64 {
    max_append_generations.saturating_mul(16).max(1024)
}
