//! Series, label-name and label-value discovery across time buckets.

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

/// Forward-index entries of the series matching `selector` within `bucket`,
/// read in one batch.
async fn resolve_selector_in_bucket<R: QueryReader>(
    reader: &R,
    index_cache: &crate::promql::index_cache::IndexCache,
    bucket: TimeBucket,
    selector: &VectorSelector,
) -> std::result::Result<Vec<crate::promql::index_cache::ForwardSeriesValue>, QueryError> {
    let candidates = crate::promql::source_adapter::selector_util::find_candidates(
        reader,
        index_cache,
        &bucket,
        selector,
    )
    .await
    .map_err(QueryError::from)?;
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    index_cache
        .forward_index_many(reader, &bucket, &candidates)
        .await
        .map_err(QueryError::from)
}

/// Resolves every (bucket, selector) pair concurrently, folding each
/// resolved series' (unsorted) labels into `sink` as results arrive.
async fn resolve_selectors<R: QueryReader>(
    reader: &R,
    buckets: &[TimeBucket],
    selectors: &[VectorSelector],
    mut sink: impl FnMut(&[Label]),
) -> std::result::Result<(), QueryError> {
    let index_cache = crate::promql::index_cache::IndexCache::new();
    let index_cache = &index_cache;
    let pairs: Vec<(TimeBucket, usize)> = buckets
        .iter()
        .flat_map(|bucket| (0..selectors.len()).map(|selector| (*bucket, selector)))
        .collect();
    let mut resolved = stream::iter(pairs)
        .map(|(bucket, selector)| {
            resolve_selector_in_bucket(reader, index_cache, bucket, &selectors[selector])
        })
        .buffer_unordered(DISCOVERY_BUCKET_READAHEAD);
    while let Some(found) = resolved.try_next().await? {
        for spec in found.iter().filter_map(|slot| slot.as_ref().as_ref()) {
            sink(&spec.labels);
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
    let mut unique_series: HashSet<Labels> = HashSet::new();
    resolve_selectors(reader, &buckets, &selectors, |labels| {
        let mut labels = labels.to_vec();
        labels.sort();
        unique_series.insert(Labels::new(labels));
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
                for attr in labels {
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
            resolve_selectors(reader, &buckets, &selectors, |labels| {
                if let Some(label) = labels.iter().find(|l| l.name == label_name)
                    && !values.contains(&label.value)
                {
                    values.insert(label.value.clone());
                }
            })
            .await?;
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
