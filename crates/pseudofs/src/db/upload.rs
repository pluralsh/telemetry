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
    expected: Option<(Uuid, Option<Uuid>)>,
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
            expected: None,
            finished: false,
        }
    }

    /// Commits only if the file is still `inode` at `head`, preserving its
    /// timestamps. Used to rewrite content without changing it.
    pub(crate) fn expecting(mut self, inode: Uuid, head: Option<Uuid>) -> Self {
        self.expected = Some((inode, head));
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
        match self.commit(content, durability).await {
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
        let guard = fs.mutation_guard().await;
        let current = fs
            .resolve_for_mutation(&self.tenant, scope, &self.path)
            .await?;
        if current
            .as_ref()
            .is_some_and(|inode| inode.kind == FileKind::Directory)
        {
            return Err(Error::IsDirectory(self.path.clone()));
        }
        if let Some(expected) = self.expected
            && current.as_ref().map(|inode| (inode.id, inode.head)) != Some(expected)
        {
            return Err(Error::Conflict(self.path.clone()));
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
        let garbage = if self.append {
            None
        } else {
            inode.head.map(|head| GarbageMarker {
                inode_id: inode.id,
                head,
            })
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
        if self.expected.is_none() {
            inode.mtime = timestamp;
            inode.ctime = timestamp;
        }
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
        fs.finish_durability(durability).await?;
        if let Some(marker) = garbage {
            fs.collect_generation_chain(scope, &marker).await;
        }
        if pending_upload {
            fs.notify_blob_worker();
        }
        if self.append
            && inode
                .generation_count
                .is_multiple_of(fs.inner.config.max_append_generations)
        {
            fs.request_compaction(&self.tenant, &self.path);
        }
        Ok(size)
    }
}
