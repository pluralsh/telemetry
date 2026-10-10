//! Shared read path over a writer `Db`, snapshot, or `DbReader`, plus the
//! read-only [`StorageReader`].

use super::*;

/// The shared read side of the storage backend.
///
/// Generic over [`DbReadOps`] so the same OpenTSDB read methods serve the
/// writer `Db`, a `DbSnapshot`, and a `DbReader`. Cloning is cheap (one `Arc`).
///
/// Private to the `slate` module: callers go through the [`StorageRead`] methods on
/// [`Storage`], [`StorageSnapshot`], and [`StorageReader`], which forward here.
pub(super) struct StorageReaderInner<T: DbReadOps + Send + Sync> {
    pub(super) db: Arc<T>,
    pub(super) segments: SegmentLister,
}

impl<T: DbReadOps + Send + Sync> Clone for StorageReaderInner<T> {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            segments: Arc::clone(&self.segments),
        }
    }
}

impl<T: DbReadOps + Send + Sync> StorageReaderInner<T> {
    /// In-flight point gets for the batch index lookups.
    const BATCH_GET_CONCURRENCY: usize = 64;

    fn scan_options() -> ScanOptions {
        ScanOptions {
            durability_filter: Default::default(),
            dirty: false,
            read_ahead_bytes: 1024 * 1024,
            cache_blocks: true,
            max_fetch_tasks: 8,
            order: IterationOrder::Ascending,
            filter_context: None,
        }
    }

    /// Retrieves a single value by exact key. Returns `Ok(None)` if absent.
    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn get(&self, key: Bytes) -> StorageResult<Option<Bytes>> {
        self.db.get(key).await.map_err(StorageError::from_storage)
    }

    /// Returns an iterator over the given key range.
    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn scan(&self, range: BytesRange) -> StorageResult<DbIterator> {
        self.db
            .scan_with_options(range, &Self::scan_options())
            .await
            .map_err(StorageError::from_storage)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn scan_prefix(&self, prefix: Bytes) -> StorageResult<DbIterator> {
        self.db
            .scan_prefix_with_options(prefix, .., &Self::scan_options())
            .await
            .map_err(StorageError::from_storage)
    }

    /// Given a time range, return all the time buckets that contain data for
    /// that range sorted by start time.
    ///
    /// This method examines the actual list of buckets in storage to determine the
    /// candidate buckets (as opposed to computing theoretical buckets from the
    /// start and end times).
    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn get_buckets_in_range(
        &self,
        namespace: &Namespace,
        start_secs: Option<i64>,
        end_secs: Option<i64>,
    ) -> crate::util::Result<Vec<TimeBucket>> {
        if let (Some(start), Some(end)) = (start_secs, end_secs)
            && end < start
        {
            return Err("end must be greater than or equal to start".into());
        }

        // Convert to minutes once before filtering
        let start_min = start_secs.map(epoch_minute);
        let end_min = end_secs.map(epoch_minute);

        let mut filtered_buckets: Vec<TimeBucket> = self
            .list_buckets(namespace)
            .into_iter()
            .filter(|bucket| match (start_min, end_min) {
                (None, None) => true,
                (Some(start), None) => {
                    let start_bucket_min = start - start % bucket.size_in_mins();
                    bucket.start >= start_bucket_min
                }
                (None, Some(end)) => {
                    let end_bucket_min = end - end % bucket.size_in_mins();
                    bucket.start <= end_bucket_min
                }
                (Some(start), Some(end)) => {
                    let start_bucket_min = start - start % bucket.size_in_mins();
                    let end_bucket_min = end - end % bucket.size_in_mins();
                    bucket.start >= start_bucket_min && bucket.start <= end_bucket_min
                }
            })
            .collect();

        filtered_buckets.sort_by_key(|bucket| bucket.start);
        Ok(filtered_buckets)
    }

    /// Given a set of sorted, non-overlapping time ranges, return all buckets
    /// that overlap any range.
    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn get_buckets_for_ranges(
        &self,
        namespace: &Namespace,
        ranges: &[(i64, i64)],
    ) -> crate::util::Result<Vec<TimeBucket>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let mut filtered_buckets: Vec<TimeBucket> = self
            .list_buckets(namespace)
            .into_iter()
            .filter(|bucket| {
                let bucket_start_min = bucket.start as i64;
                let bucket_end_min = bucket_start_min + bucket.size_in_mins() as i64;
                // Convert bucket bounds to seconds for comparison
                let bucket_start_secs = bucket_start_min * 60;
                let bucket_end_secs = bucket_end_min * 60;
                // Bucket is half-open [start, end), range is closed [r_start, r_end].
                // Overlap iff bucket_end > r_start (strict: end is exclusive) and
                // bucket_start <= r_end (inclusive: start is inclusive).
                ranges.iter().any(|&(r_start, r_end)| {
                    bucket_end_secs > r_start && bucket_start_secs <= r_end
                })
            })
            .collect();

        filtered_buckets.sort_by_key(|bucket| bucket.start);
        Ok(filtered_buckets)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn get_forward_index(
        &self,
        namespace: &Namespace,
        bucket: TimeBucket,
    ) -> crate::util::Result<ForwardIndex> {
        let prefix = ForwardIndexKey::bucket_prefix(namespace, &bucket);
        let mut iter = self.scan_prefix(prefix).await?;

        let forward_index = ForwardIndex::default();
        while let Some(record) = iter.next().await.map_err(StorageError::from_storage)? {
            let Ok(key) = ForwardIndexKey::decode(record.key.as_ref()) else {
                continue;
            };
            let value = ForwardIndexValue::decode(record.value.as_ref())?;
            forward_index.series.insert(key.series_id, value.into());
        }
        Ok(forward_index)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn get_inverted_index(
        &self,
        namespace: &Namespace,
        bucket: TimeBucket,
    ) -> crate::util::Result<InvertedIndex> {
        let prefix = InvertedIndexKey::bucket_prefix(namespace, &bucket);
        let mut iter = self.scan_prefix(prefix).await?;

        let inverted_index = InvertedIndex::default();
        while let Some(record) = iter.next().await.map_err(StorageError::from_storage)? {
            let Ok(key) = InvertedIndexKey::decode(record.key.as_ref()) else {
                continue;
            };
            let value = InvertedIndexValue::decode(record.value.as_ref())?;
            let mut entry = inverted_index
                .postings
                .entry(Label {
                    name: key.attribute,
                    value: key.value,
                })
                .or_default();
            *entry.value_mut() |= value.postings;
        }

        Ok(inverted_index)
    }

    /// Load only the specified terms from the inverted index. Legacy
    /// batch path — kept for the v1 evaluator / pipeline which still
    /// calls it. New callers should use [`Self::get_inverted_index_term`]
    /// and fan out in parallel themselves so each per-term latency is
    /// independently traceable.
    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn get_inverted_index_terms(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> crate::util::Result<InvertedIndex> {
        let result = InvertedIndex::default();
        // Futures are built up front so the stream type carries no closure;
        // a closure over `&Label` trips async_trait's `Send` inference.
        let fetches: Vec<_> = terms
            .iter()
            .map(|term| async move {
                self.get_inverted_index_term(namespace, bucket, term)
                    .await
                    .map(|postings| (term, postings))
            })
            .collect();
        let mut fetches =
            futures::stream::iter(fetches).buffer_unordered(Self::BATCH_GET_CONCURRENCY);
        while let Some((term, postings)) = fetches.try_next().await? {
            if let Some(postings) = postings {
                result.postings.insert(term.clone(), postings);
            }
        }
        Ok(result)
    }

    /// Fetch a single inverted-index posting for `(bucket, term)`.
    /// Returns `None` when the term isn't present in the bucket.
    ///
    /// Per-term granularity by design: callers (e.g. the query
    /// adapter) parallelise at *their* layer so concurrency budget and
    /// caching can be managed end-to-end. The previous batched variant
    /// looped sequentially and was a silent bottleneck.
    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn get_inverted_index_term(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        term: &Label,
    ) -> crate::util::Result<Option<RoaringBitmap>> {
        let key = InvertedIndexKey {
            namespace: namespace.clone(),
            bucket: *bucket,
            attribute: term.name.clone(),
            value: term.value.clone(),
        }
        .encode();
        let Some(value) = self.get(key).await? else {
            return Ok(None);
        };
        crate::promql::trace::record_bytes(
            crate::promql::trace::IoKind::InvertedIndexFetch,
            value.len() as u64,
        );
        let postings = InvertedIndexValue::decode(value.as_ref())?.postings;
        Ok((!postings.is_empty()).then_some(postings))
    }

    /// Load only the specified series from the forward index. Legacy
    /// batch path — see [`Self::get_inverted_index_terms`] for context.
    /// New callers should use [`Self::get_forward_index_one`].
    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn get_forward_index_series(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> crate::util::Result<ForwardIndex> {
        let result = ForwardIndex::default();
        let fetches: Vec<_> = series_ids
            .iter()
            .map(|&series_id| async move {
                self.get_forward_index_one(namespace, bucket, series_id)
                    .await
                    .map(|spec| (series_id, spec))
            })
            .collect();
        let mut fetches =
            futures::stream::iter(fetches).buffer_unordered(Self::BATCH_GET_CONCURRENCY);
        while let Some((series_id, spec)) = fetches.try_next().await? {
            if let Some(spec) = spec {
                result.series.insert(series_id, spec);
            }
        }
        Ok(result)
    }

    /// Fetch a single forward-index entry for `(bucket, series_id)`.
    /// Returns `None` when the series isn't present in the bucket.
    /// See [`Self::get_inverted_index_term`] for why this is per-key.
    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn get_forward_index_one(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> crate::util::Result<Option<SeriesSpec>> {
        let key = ForwardIndexKey {
            namespace: namespace.clone(),
            bucket: *bucket,
            series_id,
        }
        .encode();

        match self.get(key).await? {
            Some(value) => {
                crate::promql::trace::record_bytes(
                    crate::promql::trace::IoKind::ForwardIndexFetch,
                    value.len() as u64,
                );
                let forward_index_value = ForwardIndexValue::decode(value.as_ref())?;
                Ok(Some(forward_index_value.into()))
            }
            None => Ok(None),
        }
    }

    /// Load the series dictionary using the provided insert function and
    /// return the next unused series ID.
    #[tracing::instrument(level = "trace", skip(self, bucket, insert))]
    pub(super) async fn load_series_dictionary<F>(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        mut insert: F,
    ) -> crate::util::Result<u32>
    where
        F: FnMut(SeriesFingerprint, SeriesId) + Send,
    {
        let prefix = SeriesDictionaryKey::bucket_prefix(namespace, bucket);
        let mut iter = self.scan_prefix(prefix).await?;

        let mut next_series_id = 0u32;
        while let Some(record) = iter.next().await.map_err(StorageError::from_storage)? {
            let key = SeriesDictionaryKey::decode(record.key.as_ref())?;
            let value = SeriesDictionaryValue::decode(record.value.as_ref())?;
            insert(key.series_fingerprint, value.series_id);
            next_series_id = next_series_id.max(value.series_id.saturating_add(1));
        }

        Ok(next_series_id)
    }

    /// Lists the timeseries buckets currently visible to this handle, by
    /// projecting the segment prefixes reported by SlateDB (manifest plus
    /// unflushed memtable segments) through the timeseries extractor.
    fn list_buckets(&self, namespace: &Namespace) -> Vec<TimeBucket> {
        (self.segments)()
            .iter()
            .filter_map(|seg| parse_bucket(&seg.prefix))
            .filter_map(|(key_namespace, bucket)| (key_namespace == *namespace).then_some(bucket))
            .collect()
    }

    /// Get all unique values for a specific label name within a bucket.
    /// This method scans only the inverted index keys for the specified label,
    /// which is more efficient than loading all inverted index entries.
    ///
    /// Note: We don't need to verify that the decoded `key.attribute` matches
    /// `label_name` after scanning. The `attribute_prefix` prefix includes a
    /// 2-byte little-endian length prefix before the attribute string (see
    /// `encode_utf8`), which guarantees that only exact attribute matches are
    /// returned. For example, searching for "hostname" (len=8, encoded as
    /// `[0x08, 0x00, ...]`) can never match a key with attribute "host"
    /// (len=4, encoded as `[0x04, 0x00, ...]`) because the length bytes differ.
    /// See `serde::name::tests::should_not_match_shorter_attribute_with_value_that_looks_like_suffix`
    /// for test coverage of this invariant.
    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn get_label_values(
        &self,
        namespace: &Namespace,
        bucket: &TimeBucket,
        label_name: &str,
    ) -> crate::util::Result<Vec<String>> {
        let mut values = HashSet::new();
        let prefix = InvertedIndexKey::attribute_prefix(namespace, bucket, label_name);
        let mut iter = self.scan_prefix(prefix).await?;
        while let Some(record) = iter.next().await.map_err(StorageError::from_storage)? {
            let key = InvertedIndexKey::decode(record.key.as_ref())?;
            values.insert(key.value);
        }
        Ok(values.into_iter().collect())
    }
}

/// Cache-warming, available only on handles backed by a SlateDB cache manager
/// (the writer `Db` and the read-only `DbReader`). A `DbSnapshot` exposes
/// neither [`DbMetadataOps`] (the manifest) nor [`DbCacheManagerOps`]
/// (`warm_sst`), so this impl deliberately does not cover it.
impl<T> StorageReaderInner<T>
where
    T: DbReadOps + DbMetadataOps + DbCacheManagerOps + Send + Sync,
{
    /// Warms the block cache for the SSTs backing `buckets`.
    ///
    /// Each timeseries bucket is its own SlateDB segment (RFC-0024). This
    /// resolves the requested buckets to the SSTs currently live in the
    /// manifest and warms, for each, the SST filters and index plus the data
    /// blocks of the bucket when `include_samples` is set. Metadata-only
    /// warming deliberately stays in SlateDB's metadata cache so application
    /// index records cannot evict sample payloads from the data cache.
    ///
    /// SSTs are warmed up to the caller-provided concurrency. Warming is a
    /// no-op for buckets with no live segment, and SlateDB itself treats
    /// `warm_sst` as a no-op when no block cache is configured. With a
    /// `tracker`, SSTs it has already warmed are skipped.
    ///
    /// If `cancel` fires the warm stops promptly, dropping (and thereby
    /// cancelling) any in-flight per-SST warms, and returns `Ok(())` with
    /// whatever was warmed so far — cancellation is an expected shutdown
    /// signal, not an error.
    #[tracing::instrument(level = "trace", skip_all)]
    pub(super) async fn warm(
        &self,
        namespace: &Namespace,
        buckets: Vec<TimeBucket>,
        include_samples: bool,
        concurrency: usize,
        cancel: &CancellationToken,
        tracker: Option<&SstWarmTracker>,
    ) -> StorageResult<()> {
        let wanted: HashSet<TimeBucket> = buckets.into_iter().collect();

        let manifest = self.db.status().current_manifest;
        let work: Vec<_> = manifest
            .segments()
            .iter()
            .filter_map(|segment| {
                let (segment_namespace, bucket) = parse_bucket(segment.prefix())?;
                if &segment_namespace != namespace {
                    return None;
                }
                if !wanted.contains(&bucket) {
                    return None;
                }
                let targets: Arc<[CacheTarget]> =
                    bucket_cache_targets(namespace, &bucket, include_samples).into();
                let ids = segment
                    .l0()
                    .iter()
                    .map(|view| view.sst.id)
                    .chain(
                        segment
                            .compacted()
                            .iter()
                            .flat_map(|run| run.sst_views.iter().map(|view| view.sst.id)),
                    )
                    .map(move |id| (id, targets.clone()))
                    .collect::<Vec<_>>();
                Some(ids)
            })
            .flatten()
            .collect();

        warm_ssts(
            self.db.as_ref(),
            "metrics",
            work,
            include_samples,
            concurrency,
            cancel,
            tracker,
        )
        .await
        .map_err(StorageError::from_storage)
    }
}

/// Builds the [`CacheTarget`]s for warming one bucket's SSTs: the SST filters
/// and index, and — only when `include_samples` — the bucket's data blocks.
/// Metadata-only warming therefore uses the dedicated metadata cache and
/// cannot displace sample payloads from the data cache.
pub(super) fn bucket_cache_targets(
    namespace: &Namespace,
    bucket: &TimeBucket,
    include_samples: bool,
) -> Vec<CacheTarget> {
    let mut targets = Vec::with_capacity(if include_samples { 3 } else { 2 });
    targets.push(CacheTarget::Filters);
    targets.push(CacheTarget::Index);
    if include_samples {
        targets.push(CacheTarget::data::<Bytes, _>(bucket_records_range(
            namespace, bucket,
        )));
    }
    targets
}

/// Read-only storage using SlateDB's `DbReader`.
///
/// Provides read-only access without fencing, so multiple readers can coexist
/// with a single writer.
#[derive(Clone)]
pub(crate) struct StorageReader {
    pub(super) reader: StorageReaderInner<DbReader>,
}

impl StorageReader {
    /// Builds a reader from configuration, wired with the OpenTSDB merge
    /// operator, the metrics recorder, and (when configured) the foyer block
    /// cache. When `checkpoint_id` is set, the reader is pinned to that
    /// checkpoint and does not advance with newer writes.
    pub(crate) async fn try_new(
        slate_config: &SlateDbStorageConfig,
        reader_options: DbReaderOptions,
        checkpoint_id: Option<Uuid>,
    ) -> crate::util::Result<Self> {
        let object_store = create_object_store(&slate_config.object_store)?;
        Self::try_new_with_object_store(slate_config, reader_options, checkpoint_id, object_store)
            .await
    }

    /// Like [`Self::try_new`] but over an explicit object store, so tests can
    /// share an in-memory store between a writer and a reader.
    pub(crate) async fn try_new_with_object_store(
        slate_config: &SlateDbStorageConfig,
        reader_options: DbReaderOptions,
        checkpoint_id: Option<Uuid>,
        object_store: Arc<dyn ObjectStore>,
    ) -> crate::util::Result<Self> {
        let cache = SharedDbCache::from_slatedb_config(slate_config).await?;
        Self::try_new_with_cache(
            slate_config,
            reader_options,
            checkpoint_id,
            object_store,
            &cache,
        )
        .await
    }

    /// Like [`Self::try_new_with_object_store`] but uses `cache` instead of
    /// building one from the config.
    pub(crate) async fn try_new_with_cache(
        slate_config: &SlateDbStorageConfig,
        reader_options: DbReaderOptions,
        checkpoint_id: Option<Uuid>,
        object_store: Arc<dyn ObjectStore>,
        cache: &SharedDbCache,
    ) -> crate::util::Result<Self> {
        let adapter = CommonSlateDbStorage::merge_operator_adapter(Arc::new(OpenTsdbMergeOperator));
        let mut builder = DbReader::builder(slate_config.path.clone(), object_store)
            .with_options(reader_options)
            .with_merge_operator(Arc::new(adapter))
            .with_segment_extractor(TimeseriesSegmentExtractor::shared())
            .with_metrics_recorder(Arc::new(MetricsRsRecorder));

        if let Some(checkpoint_id) = checkpoint_id {
            builder = builder.with_reader_mode(slatedb::DbReaderMode::Checkpoint(checkpoint_id));
        }

        if let Some(cache) = cache.cache() {
            builder = builder.with_db_cache(cache);
        }

        let reader = builder.build().await.map_err(|e| {
            StorageError::Storage(format!("Failed to create SlateDB reader: {}", e))
        })?;

        let reader = Arc::new(reader);
        Ok(StorageReader {
            reader: StorageReaderInner {
                db: reader.clone(),
                segments: Arc::new(move || reader.status().list_segments()),
            },
        })
    }

    /// Closes the underlying `DbReader`. An injected block cache stays open
    /// for its owner to close.
    pub(crate) async fn close(&self) -> StorageResult<()> {
        self.reader
            .db
            .close()
            .await
            .map_err(StorageError::from_storage)?;
        Ok(())
    }
}

impl HasReader for StorageReader {
    type Db = DbReader;
    fn reader(&self) -> &StorageReaderInner<DbReader> {
        &self.reader
    }
}

/// Epoch minute for `secs`, clamped to the `u32` bucket-minute range.
fn epoch_minute(secs: i64) -> u32 {
    u32::try_from(secs.div_euclid(60).max(0)).unwrap_or(u32::MAX)
}
