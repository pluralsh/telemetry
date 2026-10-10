//! Process-wide coalescing of identical in-flight object-store range reads.
//!
//! SlateDB reads SST indexes, filters and data blocks as bounded range GETs.
//! Concurrent queries, shards, databases and the cache warmer that miss the
//! block cache together tend to request the same ranges of the same freshly
//! written SSTs; this layer issues one GET per range and shares its bytes.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::{BoxFuture, Shared};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    self, Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions,
};

type Key = (Arc<str>, Path, Range<u64>);
type SharedFetch = Shared<BoxFuture<'static, Result<Fetched, FetchError>>>;

static IN_FLIGHT: LazyLock<Mutex<HashMap<Key, SharedFetch>>> = LazyLock::new(Mutex::default);

#[derive(Clone)]
struct Fetched {
    bytes: Bytes,
    meta: ObjectMeta,
    range: Range<u64>,
    attributes: Attributes,
}

/// `object_store::Error` is not `Clone`; waiters get its kind and message.
#[derive(Clone, Debug)]
enum FetchError {
    NotFound { path: String, message: String },
    Other(String),
}

impl FetchError {
    fn from_store(error: &object_store::Error) -> Self {
        match error {
            object_store::Error::NotFound { path, .. } => Self::NotFound {
                path: path.clone(),
                message: error.to_string(),
            },
            other => Self::Other(other.to_string()),
        }
    }

    fn into_store(self) -> object_store::Error {
        match self {
            Self::NotFound { path, message } => object_store::Error::NotFound {
                path,
                source: message.into(),
            },
            Self::Other(message) => object_store::Error::Generic {
                store: "coalesced",
                source: message.into(),
            },
        }
    }
}

/// Wraps `inner`, coalescing unconditional bounded range reads with every
/// other wrapper in the process that shares `scope`. `scope` must identify
/// the backing bucket: wrappers with equal scopes are assumed to read the
/// same objects. Everything else passes straight through.
#[derive(Debug)]
pub struct CoalescingObjectStore {
    inner: Arc<dyn ObjectStore>,
    scope: Arc<str>,
}

impl CoalescingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, scope: impl Into<Arc<str>>) -> Self {
        Self {
            inner,
            scope: scope.into(),
        }
    }

    fn coalescible(options: &GetOptions) -> Option<Range<u64>> {
        match &options.range {
            Some(GetRange::Bounded(range))
                if !options.head
                    && options.if_match.is_none()
                    && options.if_none_match.is_none()
                    && options.if_modified_since.is_none()
                    && options.if_unmodified_since.is_none()
                    && options.version.is_none() =>
            {
                Some(range.clone())
            }
            _ => None,
        }
    }

    /// Joins the in-flight read of `range`, or starts one. The read runs in
    /// its own task so a cancelled caller does not fail the others waiting.
    fn fetch(&self, location: &Path, range: Range<u64>, options: GetOptions) -> SharedFetch {
        let key: Key = (Arc::clone(&self.scope), location.clone(), range);
        let mut in_flight = IN_FLIGHT.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(fetch) = in_flight.get(&key) {
            metrics::counter!("telemetry_object_store_coalesced_gets_total", "outcome" => "shared")
                .increment(1);
            return fetch.clone();
        }
        metrics::counter!("telemetry_object_store_coalesced_gets_total", "outcome" => "issued")
            .increment(1);
        let inner = Arc::clone(&self.inner);
        let task_key = key.clone();
        let task = tokio::spawn(async move {
            let result = read(inner.as_ref(), &task_key.1, options).await;
            IN_FLIGHT
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&task_key);
            result
        });
        let fetch = async move {
            task.await
                .unwrap_or_else(|error| Err(FetchError::Other(error.to_string())))
        }
        .boxed()
        .shared();
        in_flight.insert(key, fetch.clone());
        fetch
    }
}

async fn read(
    store: &dyn ObjectStore,
    location: &Path,
    options: GetOptions,
) -> Result<Fetched, FetchError> {
    let result = store
        .get_opts(location, options)
        .await
        .map_err(|error| FetchError::from_store(&error))?;
    let meta = result.meta.clone();
    let range = result.range.clone();
    let attributes = result.attributes.clone();
    let bytes = result
        .bytes()
        .await
        .map_err(|error| FetchError::from_store(&error))?;
    Ok(Fetched {
        bytes,
        meta,
        range,
        attributes,
    })
}

impl fmt::Display for CoalescingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Coalescing({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CoalescingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let Some(range) = Self::coalescible(&options) else {
            return self.inner.get_opts(location, options).await;
        };
        let fetched = self
            .fetch(location, range, options)
            .await
            .map_err(FetchError::into_store)?;
        let bytes = fetched.bytes;
        Ok(GetResult {
            payload: GetResultPayload::Stream(futures::stream::once(async { Ok(bytes) }).boxed()),
            meta: fetched.meta,
            range: fetched.range,
            attributes: fetched.attributes,
            extensions: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use slatedb::object_store::ObjectStoreExt;
    use slatedb::object_store::memory::InMemory;

    use super::*;

    /// Counts range GETs and holds each until `release` is notified.
    #[derive(Debug)]
    struct Gated {
        inner: InMemory,
        gets: AtomicUsize,
        release: tokio::sync::Notify,
    }

    impl fmt::Display for Gated {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Gated")
        }
    }

    #[async_trait]
    impl ObjectStore for Gated {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.release.notified().await;
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    async fn gated(scope: &str) -> (Arc<Gated>, CoalescingObjectStore, CoalescingObjectStore) {
        let gated = Arc::new(Gated {
            inner: InMemory::new(),
            gets: AtomicUsize::new(0),
            release: tokio::sync::Notify::new(),
        });
        gated
            .inner
            .put(&Path::from("sst"), Bytes::from_static(b"0123456789").into())
            .await
            .unwrap();
        let first = CoalescingObjectStore::new(gated.clone(), scope);
        let second = CoalescingObjectStore::new(gated.clone(), scope);
        (gated, first, second)
    }

    #[tokio::test]
    async fn concurrent_identical_ranges_share_one_get_across_wrappers() {
        let (gated, first, second) = gated("shared-scope").await;
        let path = Path::from("sst");
        let reads = futures::future::join(first.get_range(&path, 2..5), async {
            tokio::task::yield_now().await;
            second.get_range(&path, 2..5).await
        });
        let release = async {
            while gated.gets.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            gated.release.notify_waiters();
        };
        let ((a, b), ()) = futures::future::join(reads, release).await;

        assert_eq!(a.unwrap(), Bytes::from_static(b"234"));
        assert_eq!(b.unwrap(), Bytes::from_static(b"234"));
        assert_eq!(gated.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn completed_reads_are_not_cached() {
        let (gated, first, _) = gated("uncached-scope").await;
        let path = Path::from("sst");
        for issued in 1..=2 {
            let read = first.get_range(&path, 0..3);
            let release = async {
                while gated.gets.load(Ordering::SeqCst) < issued {
                    tokio::task::yield_now().await;
                }
                gated.release.notify_waiters();
            };
            let (read, ()) = futures::future::join(read, release).await;
            assert_eq!(read.unwrap(), Bytes::from_static(b"012"));
        }
        assert_eq!(gated.gets.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn not_found_is_shared_as_not_found() {
        let store = CoalescingObjectStore::new(Arc::new(InMemory::new()), "missing-scope");
        let error = store
            .get_range(&Path::from("absent"), 0..3)
            .await
            .unwrap_err();
        assert!(matches!(error, object_store::Error::NotFound { .. }));
    }
}
