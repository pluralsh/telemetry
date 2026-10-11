// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Immutable blob storage: a persistent local directory in front of a remote
//! object store.
//!
//! Blobs are written to `staging/`, fsynced, and renamed into `blobs/` before
//! any metadata references them. A blob stays pinned on local disk until the
//! uploader copies it to the remote store; after that its local copy is an
//! ordinary least-recently-used cache entry. A whole-blob miss downloads the
//! blob once, however many readers miss at the same time, streaming it to the
//! first reader while the cache fills. Partial reads stream the requested
//! range straight from the remote store.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::ErrorKind;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use slatedb::object_store::path::Path as ObjectPath;
use slatedb::object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::error::{Error, Result};

const SINGLE_PUT_MAX_BYTES: u64 = 8 * 1024 * 1024;
const MULTIPART_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const MULTIPART_CONCURRENCY: usize = 4;
const IO_BUFFER_BYTES: usize = 1024 * 1024;
/// How long a write waits for blob uploads to free local space.
const RESERVE_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct BlobStore {
    remote: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    blobs_dir: PathBuf,
    staging_dir: PathBuf,
    capacity: u64,
    state: Mutex<CacheState>,
    staging: Arc<Staging>,
    flights: Flights,
}

/// Bytes in staged blobs, released when a staged blob is committed or dropped.
#[derive(Default)]
struct Staging {
    bytes: AtomicU64,
    /// Signalled whenever staged or pinned bytes shrink.
    space: Notify,
}

impl Staging {
    fn release(&self, bytes: u64) {
        if bytes > 0 {
            self.bytes.fetch_sub(bytes, AtomicOrdering::AcqRel);
            self.space.notify_waiters();
        }
    }
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<Uuid, Entry>,
    lru: BTreeMap<u64, Uuid>,
    tick: u64,
    local_bytes: u64,
    pinned_bytes: u64,
}

struct Entry {
    size: u64,
    tick: u64,
    pinned: bool,
}

/// A blob being written. Dropping it without committing removes the file.
pub(crate) struct StagedBlob {
    pub(crate) id: Uuid,
    path: Option<PathBuf>,
    writer: BufWriter<tokio::fs::File>,
    len: u64,
    staging: Arc<Staging>,
    reserved: u64,
}

impl StagedBlob {
    /// Writes without reserving space; see [`BlobStore::write_reserved`].
    async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer.write_all(bytes).await?;
        self.len += bytes.len() as u64;
        Ok(())
    }
}

impl Drop for StagedBlob {
    fn drop(&mut self) {
        self.staging.release(self.reserved);
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

pub(crate) enum Fetch {
    Local(tokio::fs::File),
    Download(BlobDownload),
}

/// A cache fill in progress, finished by [`BlobStore::finish_download`].
/// Other misses for the blob wait until it is finished or dropped; dropping
/// it removes the partial file.
pub(crate) struct BlobDownload {
    path: Option<PathBuf>,
    writer: BufWriter<tokio::fs::File>,
    len: u64,
    _flight: Flight,
}

impl BlobDownload {
    pub(crate) async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer.write_all(bytes).await?;
        self.len += bytes.len() as u64;
        Ok(())
    }
}

impl Drop for BlobDownload {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

type Flights = Arc<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>>;

/// Exclusive right to download one blob. The map entry is removed once no
/// other miss is waiting on it.
struct Flight {
    id: Uuid,
    guard: tokio::sync::OwnedMutexGuard<()>,
    flights: Flights,
}

impl Drop for Flight {
    fn drop(&mut self) {
        let mut flights = lock(&self.flights);
        // The map and this guard hold one reference each; any more are waiters.
        if Arc::strong_count(tokio::sync::OwnedMutexGuard::mutex(&self.guard)) <= 2 {
            flights.remove(&self.id);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl BlobStore {
    /// Opens the local directory, discarding unfinished staged files and
    /// pinning every local blob in `pending` until it is uploaded.
    pub(crate) async fn open(
        local_path: &str,
        remote: Arc<dyn ObjectStore>,
        prefix: &str,
        capacity: u64,
        pending: &HashSet<Uuid>,
    ) -> Result<Self> {
        let root = PathBuf::from(local_path);
        let blobs_dir = root.join("blobs");
        let staging_dir = root.join("staging");
        if tokio::fs::try_exists(&staging_dir).await? {
            tokio::fs::remove_dir_all(&staging_dir).await?;
        }
        tokio::fs::create_dir_all(&blobs_dir).await?;
        tokio::fs::create_dir_all(&staging_dir).await?;
        let store = Self {
            remote,
            prefix: ObjectPath::from(prefix),
            blobs_dir,
            staging_dir,
            capacity,
            state: Mutex::new(CacheState::default()),
            staging: Arc::default(),
            flights: Flights::default(),
        };
        let mut entries = tokio::fs::read_dir(&store.blobs_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(|name| Uuid::parse_str(name).ok())
            else {
                continue;
            };
            let size = entry.metadata().await?.len();
            store.register(id, size, pending.contains(&id));
        }
        store.evict().await;
        Ok(store)
    }

    #[cfg(test)]
    pub(crate) fn pinned_bytes(&self) -> u64 {
        self.lock().pinned_bytes
    }

    pub(crate) fn capacity(&self) -> u64 {
        self.capacity
    }

    pub(crate) async fn stage(&self) -> Result<StagedBlob> {
        let id = Uuid::new_v4();
        let path = self.staging_dir.join(id.to_string());
        let file = tokio::fs::File::create(&path)
            .await
            .map_err(io_error("create staged blob", &path))?;
        Ok(StagedBlob {
            id,
            path: Some(path),
            writer: BufWriter::with_capacity(IO_BUFFER_BYTES, file),
            len: 0,
            staging: self.staging.clone(),
            reserved: 0,
        })
    }

    /// Writes to a staged blob once staged and pinned bytes leave room for it
    /// within the capacity, waiting for uploads to unpin blobs. Space is only
    /// awaited while blobs are pinned, which uploads eventually release, so
    /// concurrent staged writes can never wait on each other.
    pub(crate) async fn write_reserved(&self, staged: &mut StagedBlob, bytes: &[u8]) -> Result<()> {
        let size = bytes.len() as u64;
        let deadline = tokio::time::Instant::now() + RESERVE_TIMEOUT;
        loop {
            let space = self.staging.space.notified();
            tokio::pin!(space);
            space.as_mut().enable();
            let pinned = self.lock().pinned_bytes;
            let staging = self.staging.bytes.load(AtomicOrdering::Acquire);
            if pinned == 0 || pinned + staging + size <= self.capacity {
                break;
            }
            if tokio::time::timeout_at(deadline, space).await.is_err() {
                return Err(Error::ResourceExhausted(
                    "local blob storage is full of blobs awaiting upload".to_owned(),
                ));
            }
        }
        self.staging.bytes.fetch_add(size, AtomicOrdering::AcqRel);
        staged.reserved += size;
        staged.write(bytes).await
    }

    /// Fsyncs a staged blob, moves it into the blob directory, and pins it
    /// until [`Self::mark_uploaded`].
    pub(crate) async fn commit(&self, mut staged: StagedBlob) -> Result<u64> {
        staged.writer.flush().await?;
        staged.writer.get_ref().sync_all().await?;
        let staging_path = staged
            .path
            .take()
            .ok_or_else(|| Error::Blob("staged blob already committed".to_owned()))?;
        let target = self.local_path(staged.id);
        if let Err(error) = tokio::fs::rename(&staging_path, &target).await {
            let _ = tokio::fs::remove_file(&staging_path).await;
            return Err(io_error("commit blob", &target)(error));
        }
        sync_dir(&self.blobs_dir)
            .await
            .map_err(|error| Error::Blob(format!("sync blob directory: {error}")))?;
        self.register(staged.id, staged.len, true);
        Ok(staged.len)
    }

    /// Copies a local blob to the remote store.
    pub(crate) async fn upload(&self, id: Uuid) -> Result<()> {
        let path = self.local_path(id);
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(io_error("open blob for upload", &path))?;
        let size = file.metadata().await?.len();
        let location = self.remote_path(id);
        if size <= SINGLE_PUT_MAX_BYTES {
            let mut bytes = Vec::with_capacity(size as usize);
            file.read_to_end(&mut bytes).await?;
            self.remote
                .put(&location, PutPayload::from(Bytes::from(bytes)))
                .await?;
            return Ok(());
        }
        let upload = self.remote.put_multipart(&location).await?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, MULTIPART_CHUNK_BYTES);
        let mut buffer = vec![0_u8; IO_BUFFER_BYTES];
        let copied: Result<()> = async {
            loop {
                let read = file.read(&mut buffer).await?;
                if read == 0 {
                    return Ok(());
                }
                writer.wait_for_capacity(MULTIPART_CONCURRENCY).await?;
                writer.write(&buffer[..read]);
            }
        }
        .await;
        match copied {
            Ok(()) => {
                writer.finish().await?;
                Ok(())
            }
            Err(error) => {
                let _ = writer.abort().await;
                Err(error)
            }
        }
    }

    pub(crate) async fn mark_uploaded(&self, id: Uuid) {
        {
            let mut state = self.lock();
            let tick = state.next_tick();
            let mut unpinned = None;
            if let Some(entry) = state.entries.get_mut(&id)
                && entry.pinned
            {
                entry.pinned = false;
                entry.tick = tick;
                unpinned = Some(entry.size);
            }
            if let Some(size) = unpinned {
                state.pinned_bytes -= size;
                state.lru.insert(tick, id);
            }
        }
        self.staging.space.notify_waiters();
        self.evict().await;
    }

    pub(crate) async fn has_local(&self, id: Uuid) -> Result<bool> {
        Ok(tokio::fs::try_exists(self.local_path(id)).await?)
    }

    pub(crate) async fn has_remote(&self, id: Uuid) -> Result<bool> {
        match self.remote.head(&self.remote_path(id)).await {
            Ok(_) => Ok(true),
            Err(slatedb::object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Removes a blob locally and remotely. Missing copies are not errors.
    pub(crate) async fn delete(&self, id: Uuid) -> Result<()> {
        self.forget(id);
        match tokio::fs::remove_file(self.local_path(id)).await {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        match self.remote.delete(&self.remote_path(id)).await {
            Ok(()) | Err(slatedb::object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Opens the local copy of a blob, if one exists, and marks it recently
    /// used.
    pub(crate) async fn open_local(&self, id: Uuid) -> Result<Option<tokio::fs::File>> {
        if !self.touch(id) {
            return Ok(None);
        }
        match tokio::fs::File::open(self.local_path(id)).await {
            Ok(file) => Ok(Some(file)),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                self.forget(id);
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Opens the local copy of a blob, if one exists, without refreshing its
    /// place in the cache, so scans do not keep blobs cached.
    pub(crate) async fn peek_local(&self, id: Uuid) -> Result<Option<tokio::fs::File>> {
        if !self.lock().entries.contains_key(&id) {
            return Ok(None);
        }
        match tokio::fs::File::open(self.local_path(id)).await {
            Ok(file) => Ok(Some(file)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Waits for any download of `id` already in flight, then returns the
    /// local copy if there is one, or a download for the caller to drive.
    /// Concurrent misses for one blob therefore download it once.
    pub(crate) async fn fetch(&self, id: Uuid) -> Result<Fetch> {
        let flight = {
            let mut flights = lock(&self.flights);
            Arc::clone(flights.entry(id).or_default())
        };
        let flight = Flight {
            id,
            guard: flight.lock_owned().await,
            flights: Arc::clone(&self.flights),
        };
        if let Some(file) = self.open_local(id).await? {
            return Ok(Fetch::Local(file));
        }
        self.start_download(flight).await.map(Fetch::Download)
    }

    /// Starts a download of `id` unless it is cached or already downloading.
    pub(crate) async fn try_fetch(&self, id: Uuid) -> Result<Option<BlobDownload>> {
        if lock(&self.state).entries.contains_key(&id) {
            return Ok(None);
        }
        let guard = {
            let mut flights = lock(&self.flights);
            if flights.contains_key(&id) {
                return Ok(None);
            }
            let flight = Arc::new(tokio::sync::Mutex::new(()));
            let Ok(guard) = Arc::clone(&flight).try_lock_owned() else {
                return Ok(None);
            };
            flights.insert(id, flight);
            guard
        };
        let flight = Flight {
            id,
            guard,
            flights: Arc::clone(&self.flights),
        };
        self.start_download(flight).await.map(Some)
    }

    async fn start_download(&self, flight: Flight) -> Result<BlobDownload> {
        let path = self.staging_dir.join(format!("fetch-{}", Uuid::new_v4()));
        let file = tokio::fs::File::create(&path)
            .await
            .map_err(io_error("create fetched blob", &path))?;
        Ok(BlobDownload {
            path: Some(path),
            writer: BufWriter::with_capacity(IO_BUFFER_BYTES, file),
            len: 0,
            _flight: flight,
        })
    }

    /// Fsyncs a finished download and adds it to the cache.
    pub(crate) async fn finish_download(&self, mut download: BlobDownload) -> Result<()> {
        download.writer.flush().await?;
        download.writer.get_ref().sync_all().await?;
        let id = download._flight.id;
        let target = self.local_path(id);
        let path = download
            .path
            .take()
            .ok_or_else(|| Error::Blob("download already finished".to_owned()))?;
        if let Err(error) = tokio::fs::rename(&path, &target).await {
            let _ = tokio::fs::remove_file(&path).await;
            return Err(io_error("cache fetched blob", &target)(error));
        }
        self.register(id, download.len, false);
        self.evict().await;
        Ok(())
    }

    /// Downloads a whole blob into the cache, or waits for a download already
    /// in flight, and returns an open handle that stays readable even if the
    /// cache evicts the blob immediately.
    #[cfg(test)]
    pub(crate) async fn download(&self, id: Uuid) -> Result<tokio::fs::File> {
        let mut download = match self.fetch(id).await? {
            Fetch::Local(file) => return Ok(file),
            Fetch::Download(download) => download,
        };
        let mut stream = self.remote_stream(id).await?;
        while let Some(bytes) = stream.next().await {
            download.write(&bytes?).await?;
        }
        let reader = match &download.path {
            Some(path) => tokio::fs::File::open(path).await?,
            None => return Err(Error::Blob("download already finished".to_owned())),
        };
        self.finish_download(download).await?;
        Ok(reader)
    }

    pub(crate) async fn remote_stream(
        &self,
        id: Uuid,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let stream = self
            .remote
            .get(&self.remote_path(id))
            .await?
            .into_stream()
            .map(|bytes| bytes.map_err(Error::from));
        Ok(stream.boxed())
    }

    pub(crate) async fn remote_range(
        &self,
        id: Uuid,
        range: Range<u64>,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let options = GetOptions {
            range: Some(GetRange::Bounded(range)),
            ..GetOptions::default()
        };
        let stream = self
            .remote
            .get_opts(&self.remote_path(id), options)
            .await?
            .into_stream()
            .map(|bytes| bytes.map_err(Error::from));
        Ok(stream.boxed())
    }

    fn local_path(&self, id: Uuid) -> PathBuf {
        self.blobs_dir.join(id.to_string())
    }

    fn remote_path(&self, id: Uuid) -> ObjectPath {
        self.prefix.clone().join(id.to_string())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        lock(&self.state)
    }

    fn register(&self, id: Uuid, size: u64, pinned: bool) {
        let mut state = self.lock();
        if state.entries.contains_key(&id) {
            return;
        }
        let tick = state.next_tick();
        state.entries.insert(id, Entry { size, tick, pinned });
        state.local_bytes += size;
        if pinned {
            state.pinned_bytes += size;
        } else {
            state.lru.insert(tick, id);
        }
    }

    fn touch(&self, id: Uuid) -> bool {
        let mut state = self.lock();
        let tick = state.next_tick();
        let Some(entry) = state.entries.get_mut(&id) else {
            return false;
        };
        let previous = std::mem::replace(&mut entry.tick, tick);
        if !entry.pinned {
            state.lru.remove(&previous);
            state.lru.insert(tick, id);
        }
        true
    }

    fn forget(&self, id: Uuid) {
        let mut state = self.lock();
        if let Some(entry) = state.entries.remove(&id) {
            state.local_bytes -= entry.size;
            if entry.pinned {
                state.pinned_bytes -= entry.size;
                self.staging.space.notify_waiters();
            } else {
                state.lru.remove(&entry.tick);
            }
        }
    }

    async fn evict(&self) {
        let victims = {
            let mut state = self.lock();
            let mut victims = Vec::new();
            while state.local_bytes > self.capacity {
                let Some((_, id)) = state.lru.pop_first() else {
                    break;
                };
                if let Some(entry) = state.entries.remove(&id) {
                    state.local_bytes -= entry.size;
                    victims.push(id);
                }
            }
            victims
        };
        for id in victims {
            let _ = tokio::fs::remove_file(self.local_path(id)).await;
        }
    }
}

impl CacheState {
    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }
}

fn io_error<'a>(
    operation: &'static str,
    path: &'a Path,
) -> impl FnOnce(std::io::Error) -> Error + 'a {
    move |error| Error::Blob(format!("{operation} {}: {error}", path.display()))
}

async fn sync_dir(path: &Path) -> Result<()> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || std::fs::File::open(path)?.sync_all())
        .await
        .map_err(|error| Error::Blob(error.to_string()))??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;

    use super::*;

    async fn store(dir: &Path, capacity: u64) -> BlobStore {
        BlobStore::open(
            dir.to_str().unwrap(),
            Arc::new(InMemory::new()),
            "blobs",
            capacity,
            &HashSet::new(),
        )
        .await
        .unwrap()
    }

    async fn write(store: &BlobStore, bytes: &[u8]) -> Uuid {
        let mut staged = store.stage().await.unwrap();
        staged.write(bytes).await.unwrap();
        let id = staged.id;
        store.commit(staged).await.unwrap();
        id
    }

    #[tokio::test]
    async fn pinned_blobs_survive_eviction_until_uploaded() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), 4).await;
        let first = write(&store, b"abc").await;
        let second = write(&store, b"def").await;
        assert!(store.open_local(first).await.unwrap().is_some());
        assert!(store.open_local(second).await.unwrap().is_some());
        assert_eq!(store.pinned_bytes(), 6);

        store.upload(first).await.unwrap();
        store.mark_uploaded(first).await;
        assert!(store.open_local(first).await.unwrap().is_none());
        assert!(store.open_local(second).await.unwrap().is_some());

        let mut bytes = Vec::new();
        store
            .download(first)
            .await
            .unwrap()
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        assert_eq!(bytes, b"abc");
        assert!(store.open_local(first).await.unwrap().is_none());

        store.upload(second).await.unwrap();
        store.mark_uploaded(second).await;
        store.download(first).await.unwrap();
        assert!(store.open_local(first).await.unwrap().is_some());
        assert!(store.open_local(second).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn concurrent_misses_share_one_download() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), 1024).await;
        let id = write(&store, b"shared").await;
        store.upload(id).await.unwrap();
        store.mark_uploaded(id).await;
        store.forget(id);
        tokio::fs::remove_file(store.local_path(id)).await.unwrap();

        let Fetch::Download(abandoned) = store.fetch(id).await.unwrap() else {
            panic!("expected a download");
        };
        assert!(store.try_fetch(id).await.unwrap().is_none());
        drop(abandoned);
        assert!(lock(&store.flights).is_empty());
        let fetches = std::fs::read_dir(dir.path().join("staging"))
            .unwrap()
            .count();
        assert_eq!(fetches, 0);

        let Fetch::Download(mut download) = store.fetch(id).await.unwrap() else {
            panic!("expected a download");
        };
        let waiter = store.fetch(id);
        tokio::pin!(waiter);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut waiter)
                .await
                .is_err(),
            "a second miss must wait for the download in flight"
        );
        download.write(b"shared").await.unwrap();
        store.finish_download(download).await.unwrap();
        let Fetch::Local(mut file) = waiter.await.unwrap() else {
            panic!("the waiter should find the cached copy");
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"shared");
        assert!(lock(&store.flights).is_empty());
    }

    #[tokio::test]
    async fn multipart_upload_round_trips_large_blob_and_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), u64::MAX).await;
        let content: Vec<u8> = (0..(SINGLE_PUT_MAX_BYTES as usize + 12345))
            .map(|index| (index % 251) as u8)
            .collect();
        let id = write(&store, &content).await;
        store.upload(id).await.unwrap();
        let mut stream = store.remote_range(id, 10..1_000_010).await.unwrap();
        let mut ranged = Vec::new();
        while let Some(bytes) = stream.next().await {
            ranged.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(ranged, &content[10..1_000_010]);

        store.delete(id).await.unwrap();
        assert!(store.open_local(id).await.unwrap().is_none());
        assert!(store.download(id).await.is_err());
    }

    #[tokio::test]
    async fn reopen_discards_staging_and_repins_pending() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), 1).await;
        let id = write(&store, b"pending").await;
        let abandoned = store.stage().await.unwrap();
        std::mem::forget(abandoned);
        drop(store);

        let reopened = BlobStore::open(
            dir.path().to_str().unwrap(),
            Arc::new(InMemory::new()),
            "blobs",
            1,
            &HashSet::from([id]),
        )
        .await
        .unwrap();
        assert!(reopened.open_local(id).await.unwrap().is_some());
        assert_eq!(reopened.pinned_bytes(), 7);
        let staged = std::fs::read_dir(dir.path().join("staging"))
            .unwrap()
            .count();
        assert_eq!(staged, 0);
    }
}
