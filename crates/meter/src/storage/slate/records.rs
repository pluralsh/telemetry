//! OpenTSDB record-op builders.
//!
//! Pure encoders from domain values to storage record operations; they touch
//! no storage state, so they are free functions rather than methods.

use super::*;

pub(crate) fn insert_series_id(
    namespace: &Namespace,
    bucket: TimeBucket,
    fingerprint: SeriesFingerprint,
    id: SeriesId,
    ttl: Ttl,
) -> crate::util::Result<RecordOp> {
    let key = SeriesDictionaryKey {
        namespace: namespace.clone(),
        bucket,
        series_fingerprint: fingerprint,
    }
    .encode();
    let value = SeriesDictionaryValue { series_id: id }.encode();
    Ok(RecordOp::Put(PutRecordOp::new_with_options(
        Record { key, value },
        PutOptions { ttl },
    )))
}

pub(crate) fn insert_forward_index(
    namespace: &Namespace,
    bucket: TimeBucket,
    series_id: SeriesId,
    series_spec: SeriesSpec,
    ttl: Ttl,
) -> crate::util::Result<RecordOp> {
    let key = ForwardIndexKey {
        namespace: namespace.clone(),
        bucket,
        series_id,
    }
    .encode();
    let value = ForwardIndexValue {
        metric_unit: series_spec.unit,
        metric_meta: series_spec.metric_type.into(),
        label_count: series_spec.labels.len() as u16,
        labels: series_spec.labels,
    }
    .encode();
    Ok(RecordOp::Put(PutRecordOp::new_with_options(
        Record { key, value },
        PutOptions { ttl },
    )))
}

pub(crate) fn merge_inverted_index(
    namespace: &Namespace,
    bucket: TimeBucket,
    label: Label,
    postings: RoaringBitmap,
    ttl: Ttl,
) -> crate::util::Result<RecordOp> {
    let key = InvertedIndexKey {
        namespace: namespace.clone(),
        bucket,
        attribute: label.name,
        value: label.value,
    }
    .encode();
    let value = InvertedIndexValue { postings }.encode()?;
    Ok(RecordOp::Merge(MergeRecordOp::new_with_ttl(
        Record { key, value },
        MergeOptions { ttl },
    )))
}

pub(crate) fn merge_samples(
    namespace: &Namespace,
    bucket: TimeBucket,
    series_id: SeriesId,
    metric_name: &str,
    samples: Vec<Sample>,
    histograms: Vec<HistogramSample>,
    ttl: Ttl,
) -> crate::util::Result<RecordOp> {
    let key = TimeSeriesKey {
        namespace: namespace.clone(),
        bucket,
        metric_name: metric_name.to_string(),
        series_id,
    }
    .encode();
    let value = SeriesData {
        floats: samples,
        histograms,
    }
    .encode()?;
    Ok(RecordOp::Merge(MergeRecordOp::new_with_ttl(
        Record { key, value },
        MergeOptions { ttl },
    )))
}
