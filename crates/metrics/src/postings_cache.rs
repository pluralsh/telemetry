//! Cross-query cache of inverted-index postings for one writer's storage.
//!
//! Buckets are never sealed — late samples can add series to any hour — so
//! entries are versioned instead of assumed immutable. The flusher stamps
//! a bucket with a fresh sequence number once a flush that adds series is
//! visible to new snapshots, and each query reader carries the sequence
//! number read *before* it took its snapshot. An entry read under sequence
//! `s` is served only while its bucket's stamp is `<= s`: a stamp above it
//! means some series became visible after that reader's snapshot, so the
//! entry may be missing them.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use moka::future::Cache;
use roaring::RoaringBitmap;

use crate::model::{Label, TimeBucket};

/// Default weighted capacity of each of the term and label caches.
const DEFAULT_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;

/// Buckets expiring (per retention) within this margin are not cached, and
/// entries live at most [`ENTRY_TTL`] (shorter than the margin), so a cached
/// posting never outlives the forward-index entries it points at.
const EXPIRY_MARGIN: Duration = Duration::from_secs(60 * 60);
const ENTRY_TTL: Duration = Duration::from_secs(30 * 60);

/// Every value of one label with its postings.
type LabelPostings = Vec<(String, RoaringBitmap)>;

#[derive(Clone)]
struct Versioned<T> {
    read_at: u64,
    value: Arc<T>,
}

pub(crate) struct PostingsCache {
    seq: AtomicU64,
    stamps: DashMap<TimeBucket, u64>,
    terms: Cache<(TimeBucket, Label), Versioned<Option<RoaringBitmap>>>,
    labels: Cache<(TimeBucket, String), Versioned<LabelPostings>>,
    retention: Option<Duration>,
}

impl PostingsCache {
    pub(crate) fn new(retention: Option<Duration>) -> Self {
        let capacity_bytes = DEFAULT_CAPACITY_BYTES;
        Self {
            seq: AtomicU64::new(0),
            stamps: DashMap::new(),
            terms: Cache::builder()
                .max_capacity(capacity_bytes)
                .time_to_live(ENTRY_TTL)
                .weigher(
                    |(_, label): &(TimeBucket, Label), entry: &Versioned<Option<RoaringBitmap>>| {
                        let postings = entry
                            .value
                            .as_ref()
                            .as_ref()
                            .map_or(0, RoaringBitmap::serialized_size);
                        weight(label.name.len() + label.value.len() + postings)
                    },
                )
                .build(),
            labels: Cache::builder()
                .max_capacity(capacity_bytes)
                .time_to_live(ENTRY_TTL)
                .weigher(
                    |(_, name): &(TimeBucket, String), entry: &Versioned<LabelPostings>| {
                        let values: usize = entry
                            .value
                            .iter()
                            .map(|(value, postings)| value.len() + postings.serialized_size())
                            .sum();
                        weight(name.len() + values)
                    },
                )
                .build(),
            retention,
        }
    }

    /// The sequence a query reader is stamped with. Must be read before the
    /// reader's storage snapshot is taken.
    pub(crate) fn read_seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    /// Invalidate `bucket`'s entries. Call once a write adding series to
    /// `bucket` is visible to newly taken snapshots.
    pub(crate) fn stamp(&self, bucket: TimeBucket) {
        let stamp = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        self.stamps
            .entry(bucket)
            .and_modify(|current| *current = (*current).max(stamp))
            .or_insert(stamp);
    }

    fn fresh(&self, bucket: TimeBucket, read_at: u64) -> bool {
        self.stamps
            .get(&bucket)
            .is_none_or(|stamp| *stamp <= read_at)
    }

    fn cacheable(&self, bucket: TimeBucket) -> bool {
        let Some(retention) = self.retention else {
            return true;
        };
        let expires_secs = (i64::from(bucket.start) * 60)
            .saturating_add(i64::try_from(retention.as_secs()).unwrap_or(i64::MAX));
        let margin = i64::try_from(EXPIRY_MARGIN.as_secs()).unwrap_or(i64::MAX);
        expires_secs > common::time::now_secs().saturating_add(margin)
    }

    pub(crate) async fn term(
        &self,
        bucket: TimeBucket,
        term: &Label,
    ) -> Option<Arc<Option<RoaringBitmap>>> {
        let entry = self.terms.get(&(bucket, term.clone())).await?;
        self.fresh(bucket, entry.read_at).then_some(entry.value)
    }

    pub(crate) async fn insert_term(
        &self,
        bucket: TimeBucket,
        term: &Label,
        read_at: u64,
        postings: Option<RoaringBitmap>,
    ) {
        if self.cacheable(bucket) && self.fresh(bucket, read_at) {
            let value = Arc::new(postings);
            self.terms
                .insert((bucket, term.clone()), Versioned { read_at, value })
                .await;
        }
    }

    pub(crate) async fn label(
        &self,
        bucket: TimeBucket,
        label_name: &str,
    ) -> Option<Arc<LabelPostings>> {
        let entry = self.labels.get(&(bucket, label_name.to_owned())).await?;
        self.fresh(bucket, entry.read_at).then_some(entry.value)
    }

    pub(crate) async fn insert_label(
        &self,
        bucket: TimeBucket,
        label_name: &str,
        read_at: u64,
        postings: LabelPostings,
    ) {
        if self.cacheable(bucket) && self.fresh(bucket, read_at) {
            let value = Arc::new(postings);
            self.labels
                .insert(
                    (bucket, label_name.to_owned()),
                    Versioned { read_at, value },
                )
                .await;
        }
    }
}

fn weight(bytes: usize) -> u32 {
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bitmap(ids: &[u32]) -> RoaringBitmap {
        ids.iter().copied().collect()
    }

    #[tokio::test]
    async fn should_serve_entries_read_after_the_latest_stamp() {
        // given
        let cache = PostingsCache::new(None);
        let bucket = TimeBucket::hour(60);
        let term = Label::metric_name("up");
        cache.stamp(bucket);
        let read_at = cache.read_seq();

        // when
        cache
            .insert_term(bucket, &term, read_at, Some(bitmap(&[1, 2])))
            .await;

        // then
        let hit = cache.term(bucket, &term).await.expect("cached");
        assert_eq!(hit.as_ref().as_ref(), Some(&bitmap(&[1, 2])));
    }

    #[tokio::test]
    async fn should_drop_entries_read_before_a_stamp() {
        // given: an entry read under the current sequence
        let cache = PostingsCache::new(None);
        let bucket = TimeBucket::hour(60);
        let read_at = cache.read_seq();
        cache
            .insert_label(bucket, "job", read_at, vec![("api".into(), bitmap(&[1]))])
            .await;
        assert!(cache.label(bucket, "job").await.is_some());

        // when: a flush adding series to the bucket becomes visible
        cache.stamp(bucket);

        // then
        assert!(cache.label(bucket, "job").await.is_none());
    }

    #[tokio::test]
    async fn should_not_cache_reads_that_race_a_stamp() {
        // given: a reader stamped before a flush lands
        let cache = PostingsCache::new(None);
        let bucket = TimeBucket::hour(60);
        let term = Label::metric_name("up");
        let read_at = cache.read_seq();
        cache.stamp(bucket);

        // when: that reader finishes its (possibly stale) read
        cache.insert_term(bucket, &term, read_at, None).await;

        // then
        assert!(cache.term(bucket, &term).await.is_none());
    }

    #[tokio::test]
    async fn should_keep_other_buckets_across_a_stamp() {
        // given
        let cache = PostingsCache::new(None);
        let (old, active) = (TimeBucket::hour(60), TimeBucket::hour(120));
        let term = Label::metric_name("up");
        let read_at = cache.read_seq();
        cache
            .insert_term(old, &term, read_at, Some(bitmap(&[7])))
            .await;

        // when
        cache.stamp(active);

        // then
        assert!(cache.term(old, &term).await.is_some());
    }

    #[tokio::test]
    async fn should_not_cache_buckets_near_retention_expiry() {
        // given: retention that expires the bucket within the margin
        let bucket = TimeBucket::round_to_hour(std::time::SystemTime::now()).unwrap();
        let cache = PostingsCache::new(Some(Duration::from_secs(60)));
        let term = Label::metric_name("up");

        // when
        cache
            .insert_term(bucket, &term, cache.read_seq(), None)
            .await;

        // then
        assert!(cache.term(bucket, &term).await.is_none());
    }
}
