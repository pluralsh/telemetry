// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Streaming file uploads: chunk batching and the final commit.

use super::*;

impl FileUpload {
    pub async fn push(&mut self, chunk: Bytes) -> Result<()> {
        if self.finished {
            return Err(Error::Conflict(self.path.clone()));
        }
        if chunk.is_empty() {
            return Ok(());
        }
        let new_len = self
            .byte_len
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| Error::InvalidPath("file size overflow".to_owned()))?;
        let total_len = self
            .base_size
            .checked_add(new_len)
            .ok_or_else(|| Error::InvalidPath("file size overflow".to_owned()))?;
        if total_len > self.fs.inner.config.max_file_size_bytes {
            return Err(Error::InvalidPath(format!(
                "file exceeds max_file_size_bytes ({})",
                self.fs.inner.config.max_file_size_bytes
            )));
        }
        self.buffer.extend_from_slice(&chunk);
        self.byte_len = new_len;
        let chunk_size = self.fs.inner.config.chunk_size_bytes;
        while self.buffer.len() >= chunk_size {
            self.pending_chunks
                .push(self.buffer.split_to(chunk_size).freeze());
            if self.pending_chunks.len() >= UPLOAD_BATCH_CHUNKS {
                self.flush_pending().await?;
            }
        }
        Ok(())
    }

    async fn flush_pending(&mut self) -> Result<()> {
        if self.pending_chunks.is_empty() {
            return Ok(());
        }
        let chunks = std::mem::take(&mut self.pending_chunks);
        let next_count = self.chunk_count + chunks.len() as u64;
        let marker = UploadMarker {
            inode_id: self.inode_id,
            generation_id: self.generation_id,
            chunk_count: next_count,
            started_at: self.started_at,
        };
        let mut ops = Vec::with_capacity(chunks.len() + 1);
        for (offset, chunk) in chunks.into_iter().enumerate() {
            ops.push(put_op(
                chunk_key(
                    self.scope,
                    self.inode_id,
                    self.generation_id,
                    self.chunk_count + offset as u64,
                ),
                chunk,
            ));
        }
        ops.push(put_op(
            upload_key(self.scope, self.generation_id),
            encode(&marker)?,
        ));
        self.fs.inner.storage.apply(ops).await?;
        self.chunk_count = next_count;
        Ok(())
    }

    pub async fn finish(&mut self, durability: Durability) -> Result<u64> {
        if !self.buffer.is_empty() {
            self.pending_chunks.push(self.buffer.split().freeze());
        }
        self.flush_pending().await?;
        let _guard = self.fs.inner.mutation_lock.lock().await;
        let current = match self
            .fs
            .resolve_from_storage(&self.tenant, self.scope, &self.path)
            .await
        {
            Ok(inode) => Some(inode),
            Err(Error::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        match (&current, self.expected_head) {
            (Some(inode), expected) if inode.id == self.inode_id && inode.head == expected => {}
            (None, None) => {}
            _ => return Err(Error::Conflict(self.path.clone())),
        }
        let (parent_path, name) = parent_and_name(&self.path)?;
        let mut ops = Vec::new();
        let parent = self
            .fs
            .ensure_directory_path(&self.tenant, self.scope, &parent_path, &mut ops)
            .await?;
        let previous = if self.append {
            self.expected_head
        } else {
            None
        };
        let generation = Generation {
            inode_id: self.inode_id,
            id: self.generation_id,
            previous,
            chunk_count: self.chunk_count,
            byte_len: self.byte_len,
        };
        let timestamp = now();
        let size = if self.append {
            current.as_ref().map_or(0, |inode| inode.size) + self.byte_len
        } else {
            self.byte_len
        };
        let generation_count = if self.append {
            current
                .as_ref()
                .map_or(1, |inode| inode.generation_count.max(1) + 1)
        } else {
            1
        };
        let mut inode = current.unwrap_or_else(|| {
            Inode::file(
                self.inode_id,
                self.tenant.clone(),
                size,
                self.generation_id,
                timestamp,
            )
        });
        inode.kind = FileKind::File;
        inode.size = size;
        inode.mtime = timestamp;
        inode.ctime = timestamp;
        inode.head = Some(self.generation_id);
        inode.generation_count = generation_count;
        ops.push(put_op(
            generation_key(self.scope, self.inode_id, self.generation_id),
            encode(&generation)?,
        ));
        ops.push(put_op(
            inode_key(self.scope, self.inode_id),
            encode(&inode)?,
        ));
        ops.push(put_op(
            directory_entry_key(self.scope, parent, &name),
            directory_entry_value(self.scope.root_id, self.inode_id, FileKind::File),
        ));
        ops.push(RecordOp::Delete(upload_key(self.scope, self.generation_id)));
        let garbage = if self.append {
            None
        } else {
            self.expected_head.map(|head| GarbageMarker {
                inode_id: self.inode_id,
                head,
            })
        };
        if let Some(marker) = &garbage {
            ops.push(put_op(
                garbage_key(self.scope, marker.head),
                encode(marker)?,
            ));
        }
        self.fs.apply_records(ops, durability).await?;
        drop(_guard);
        self.fs.finish_durability(durability).await?;
        if let Some(marker) = garbage {
            let _ = self.fs.cleanup_generation_chain(self.scope, &marker).await;
        }
        self.finished = true;
        let compact =
            self.append && generation_count % self.fs.inner.config.max_append_generations == 0;
        if compact {
            let _ = self
                .fs
                .inner
                .compaction_tx
                .send(CompactionRequest {
                    tenant: self.tenant.clone(),
                    path: self.path.clone(),
                })
                .await;
        }
        Ok(size)
    }

    pub async fn abort(&mut self) -> Result<()> {
        self.buffer.clear();
        self.pending_chunks.clear();
        let mut ops = Vec::with_capacity(DELETE_BATCH_OPS);
        for index in 0..self.chunk_count {
            ops.push(RecordOp::Delete(chunk_key(
                self.scope,
                self.inode_id,
                self.generation_id,
                index,
            )));
            self.fs.flush_delete_batch(&mut ops).await?;
        }
        ops.push(RecordOp::Delete(upload_key(self.scope, self.generation_id)));
        self.fs.inner.storage.apply(ops).await?;
        self.finished = true;
        Ok(())
    }
}
