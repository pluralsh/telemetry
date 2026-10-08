//! Series, label-name and label-value discovery across time buckets.

use std::sync::Arc;

use super::*;

/// Discover series over a time range specified as Rust range bounds.
pub(crate) async fn find_series_in_range<E: TsdbReadEngine + ?Sized>(
    engine: &E,
    matchers: &[&str],
    range: impl RangeBounds<SystemTime>,
) -> std::result::Result<Vec<Labels>, QueryError> {
    let (start, end) = crate::util::range_bounds_to_secs(range)?;
    E::find_series(engine, matchers, start, end).await
}

/// Discover label names over a time range specified as Rust range bounds.
pub(crate) async fn find_labels_in_range<E: TsdbReadEngine + ?Sized>(
    engine: &E,
    matchers: Option<&[&str]>,
    range: impl RangeBounds<SystemTime>,
) -> std::result::Result<Vec<String>, QueryError> {
    let (start, end) = crate::util::range_bounds_to_secs(range)?;
    E::find_labels(engine, matchers, start, end).await
}

/// Discover label values over a time range specified as Rust range bounds.
pub(crate) async fn find_label_values_in_range<E: TsdbReadEngine + ?Sized>(
    engine: &E,
    label_name: &str,
    matchers: Option<&[&str]>,
    range: impl RangeBounds<SystemTime>,
) -> std::result::Result<Vec<String>, QueryError> {
    let (start, end) = crate::util::range_bounds_to_secs(range)?;
    E::find_label_values(engine, label_name, matchers, start, end).await
}

// ── Series / label discovery ────────────────────────────────────────

/// Cross-bucket readahead used by the discovery helpers below.
const DISCOVERY_BUCKET_READAHEAD: usize = 32;

/// The sorted labels of the series matching any of `selectors` within
/// `bucket`, each series once. `key` names the selector set in the reader's
/// cross-query cache, which is consulted before any index read.
async fn resolve_selectors_in_bucket<R: QueryReader>(
    reader: &R,
    index_cache: &crate::promql::index_cache::IndexCache,
    bucket: TimeBucket,
    selectors: &[VectorSelector],
    key: &Arc<str>,
) -> std::result::Result<Arc<[Labels]>, QueryError> {
    if let Some(series) = reader.cached_series_set(&bucket, key).await {
        return Ok(series);
    }
    let matched = matched_postings(reader, index_cache, bucket, selectors).await?;
    let series: Arc<[Labels]> = if matched.is_empty() {
        Arc::from([])
    } else {
        let candidates: Vec<SeriesId> = matched.iter().collect();
        index_cache
            .forward_index_many(reader, &bucket, &candidates)
            .await
            .map_err(QueryError::from)?
            .iter()
            .filter_map(|slot| slot.as_ref().as_ref())
            .map(|spec| spec.labels.clone())
            .collect()
    };
    reader.cache_series_set(&bucket, key, series.clone()).await;
    Ok(series)
}

/// The cache key of a selector set: its selectors' canonical forms, sorted.
fn series_set_key(selectors: &[VectorSelector]) -> Arc<str> {
    let mut forms: Vec<String> = selectors.iter().map(ToString::to_string).collect();
    forms.sort();
    forms.dedup();
    Arc::from(forms.join("\n"))
}

/// The series within `bucket` matching any of `selectors`.
async fn matched_postings<R: QueryReader>(
    reader: &R,
    index_cache: &crate::promql::index_cache::IndexCache,
    bucket: TimeBucket,
    selectors: &[VectorSelector],
) -> std::result::Result<roaring::RoaringBitmap, QueryError> {
    let sets = futures::future::try_join_all(selectors.iter().map(|selector| {
        crate::promql::source_adapter::selector_util::find_candidate_postings(
            reader,
            index_cache,
            &bucket,
            selector,
        )
    }))
    .await?;
    let mut matched = roaring::RoaringBitmap::new();
    for set in &sets {
        matched |= set;
    }
    Ok(matched)
}

/// The values of `label_name` held by a series matching any of `selectors`
/// within `bucket`, read from postings alone: a value is kept when its
/// postings meet the matched set, so no forward-index entry is read.
async fn matched_label_values<R: QueryReader>(
    reader: &R,
    index_cache: &crate::promql::index_cache::IndexCache,
    bucket: TimeBucket,
    selectors: &[VectorSelector],
    label_name: &str,
) -> std::result::Result<Vec<String>, QueryError> {
    let matched = matched_postings(reader, index_cache, bucket, selectors).await?;
    if matched.is_empty() {
        return Ok(Vec::new());
    }
    let postings = index_cache
        .label_postings(reader, &bucket, label_name)
        .await
        .map_err(QueryError::from)?;
    Ok(postings
        .iter()
        .filter(|(_, series)| !series.is_disjoint(&matched))
        .map(|(value, _)| value.clone())
        .collect())
}

/// Resolves every bucket concurrently, folding each matching series' sorted
/// labels into `sink` once per bucket as results arrive.
async fn resolve_selectors<R: QueryReader>(
    reader: &R,
    buckets: &[TimeBucket],
    selectors: &[VectorSelector],
    mut sink: impl FnMut(&Labels),
) -> std::result::Result<(), QueryError> {
    let index_cache = crate::promql::index_cache::IndexCache::new();
    let index_cache = &index_cache;
    let key = &series_set_key(selectors);
    let mut resolved = stream::iter(buckets.iter().copied())
        .map(|bucket| resolve_selectors_in_bucket(reader, index_cache, bucket, selectors, key))
        .buffer_unordered(DISCOVERY_BUCKET_READAHEAD);
    while let Some(found) = resolved.try_next().await? {
        for labels in found.iter() {
            sink(labels);
        }
    }
    Ok(())
}

/// Discover series matching any of the given selectors.
pub(crate) async fn discover_series<R: QueryReader>(
    reader: &R,
    matchers: &[&str],
) -> std::result::Result<Vec<Labels>, QueryError> {
    if matchers.is_empty() {
        return Err(QueryError::InvalidQuery(
            "at least one match[] required".to_string(),
        ));
    }

    let buckets = reader.list_buckets().await?;
    if buckets.is_empty() {
        return Ok(vec![]);
    }

    let selectors = parse_selectors(matchers)?;
    let mut unique_series: HashSet<Labels, foldhash::fast::RandomState> = HashSet::default();
    resolve_selectors(reader, &buckets, &selectors, |labels| {
        if !unique_series.contains(labels) {
            unique_series.insert(labels.clone());
        }
    })
    .await?;

    let mut result: Vec<Labels> = unique_series.into_iter().collect();
    result.sort();
    Ok(result)
}

/// Discover label names, optionally filtered by matchers.
pub(crate) async fn discover_labels<R: QueryReader>(
    reader: &R,
    matchers: Option<&[&str]>,
) -> std::result::Result<Vec<String>, QueryError> {
    let buckets = reader.list_buckets().await?;
    if buckets.is_empty() {
        return Ok(vec![]);
    }

    let mut label_names: HashSet<String> = HashSet::new();

    match matchers {
        Some(matches) if !matches.is_empty() => {
            let selectors = parse_selectors(matches)?;
            resolve_selectors(reader, &buckets, &selectors, |labels| {
                for attr in labels.iter() {
                    if !label_names.contains(&attr.name) {
                        label_names.insert(attr.name.clone());
                    }
                }
            })
            .await?;
        }
        _ => {
            let width = buckets.len().clamp(1, DISCOVERY_BUCKET_READAHEAD);
            let results: Vec<_> = stream::iter(buckets)
                .map(|bucket| async move { reader.all_inverted_index(&bucket).await })
                .buffer_unordered(width)
                .try_collect()
                .await?;
            for inverted_index in results {
                for attr in inverted_index.all_keys() {
                    label_names.insert(attr.name);
                }
            }
        }
    }

    let mut result: Vec<String> = label_names.into_iter().collect();
    result.sort();
    Ok(result)
}

/// Discover values for a specific label, optionally filtered by matchers.
pub(crate) async fn discover_label_values<R: QueryReader>(
    reader: &R,
    label_name: &str,
    matchers: Option<&[&str]>,
) -> std::result::Result<Vec<String>, QueryError> {
    let buckets = reader.list_buckets().await?;
    if buckets.is_empty() {
        return Ok(vec![]);
    }

    let mut values: HashSet<String> = HashSet::new();

    match matchers {
        Some(matches) if !matches.is_empty() => {
            let selectors = parse_selectors(matches)?;
            let index_cache = crate::promql::index_cache::IndexCache::new();
            let index_cache = &index_cache;
            let selectors = &selectors;
            let mut matched = stream::iter(buckets)
                .map(|bucket| async move {
                    matched_label_values(reader, index_cache, bucket, selectors, label_name).await
                })
                .buffer_unordered(DISCOVERY_BUCKET_READAHEAD);
            while let Some(found) = matched.try_next().await? {
                values.extend(found);
            }
        }
        _ => {
            let width = buckets.len().clamp(1, DISCOVERY_BUCKET_READAHEAD);
            let results: Vec<_> = stream::iter(buckets)
                .map(|bucket| async move { reader.label_values(&bucket, label_name).await })
                .buffer_unordered(width)
                .try_collect()
                .await?;
            for label_vals in results {
                values.extend(label_vals);
            }
        }
    }

    let mut result: Vec<String> = values.into_iter().collect();
    result.sort();
    Ok(result)
}
