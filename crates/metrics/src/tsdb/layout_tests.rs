//! Write layouts number series differently, but under every layout, and
//! across restarts that switch layouts as a rolling upgrade or a rollback
//! does, a bucket's IDs must stay dense, agree with its dictionary and keep
//! their samples.

use super::*;
use crate::model::{MetricType, Sample, SeriesData, SeriesFingerprint};
use crate::read_tuning::{WriteLayout, override_write_layout};
use crate::serde::TimeBucketScoped;
use crate::serde::key::TimeSeriesKey;
use crate::storage::in_memory_storage;

pub(crate) const ARRIVAL: WriteLayout = WriteLayout {
    sorted_series_ids: false,
    rollup_presort: false,
};
pub(crate) const SORTED: WriteLayout = WriteLayout {
    sorted_series_ids: true,
    rollup_presort: false,
};

const METRICS: [&str; 3] = ["m0", "m1", "m2"];

fn bucket() -> TimeBucket {
    TimeBucket::hour(60)
}

fn series(metric: &str, host: usize, timestamp_ms: i64) -> Series {
    Series {
        labels: vec![
            Label::metric_name(metric),
            Label::new("host", format!("h{host:02}")),
        ],
        metric_type: Some(MetricType::Gauge),
        unit: None,
        description: None,
        samples: vec![Sample::new(timestamp_ms, host as f64)],
        histograms: Vec::new(),
    }
}

/// `bucket`'s dictionary, after checking its IDs are exactly `0..n` and
/// each names the forward-index entry with its fingerprint.
pub(crate) async fn dense_ids(
    storage: &Storage,
    bucket: TimeBucket,
) -> HashMap<SeriesFingerprint, SeriesId> {
    let snapshot = storage.snapshot().await.unwrap();
    let namespace = Namespace::default();
    let mut dictionary = HashMap::new();
    let next = snapshot
        .load_series_dictionary(&namespace, &bucket, |fingerprint, id| {
            dictionary.insert(fingerprint, id);
        })
        .await
        .unwrap();
    let mut ids: Vec<SeriesId> = dictionary.values().copied().collect();
    ids.sort_unstable();
    assert_eq!(ids, (0..next).collect::<Vec<_>>(), "{bucket:?}");
    let forward = snapshot
        .get_forward_index(&namespace, bucket)
        .await
        .unwrap();
    assert_eq!(forward.series.len(), dictionary.len());
    for (fingerprint, id) in &dictionary {
        let spec = forward.series.get(id).unwrap();
        assert_eq!(spec.labels.series_fingerprint(), *fingerprint);
    }
    dictionary
}

/// `metric`'s series IDs in `bucket`, ascending.
pub(crate) async fn metric_ids(
    storage: &Storage,
    bucket: TimeBucket,
    metric: &str,
) -> Vec<SeriesId> {
    storage
        .snapshot()
        .await
        .unwrap()
        .get_inverted_index_term(&Namespace::default(), &bucket, &Label::metric_name(metric))
        .await
        .unwrap()
        .unwrap_or_default()
        .iter()
        .collect()
}

pub(crate) fn contiguous(ids: &[SeriesId]) -> bool {
    ids.windows(2).all(|pair| pair[1] == pair[0] + 1)
}

/// Every stored series of `bucket` as `(metric, host, samples)`, sorted.
async fn stored(storage: &Storage, bucket: TimeBucket) -> Vec<(String, String, Vec<(i64, f64)>)> {
    let snapshot = storage.snapshot().await.unwrap();
    let namespace = Namespace::default();
    let forward = snapshot
        .get_forward_index(&namespace, bucket)
        .await
        .unwrap();
    let mut records = snapshot
        .scan_prefix(TimeSeriesKey::bucket_prefix(&namespace, &bucket))
        .await
        .unwrap();
    let mut out = Vec::new();
    while let Some(record) = records.next().await.unwrap() {
        let key = TimeSeriesKey::decode(&record.key).unwrap();
        let data = SeriesData::decode(&record.value).unwrap();
        let spec = forward.series.get(&key.series_id).unwrap();
        assert_eq!(spec.labels.metric_name(), key.metric_name);
        out.push((
            key.metric_name,
            spec.labels.get("host").unwrap().to_owned(),
            data.timestamps.into_iter().zip(data.values).collect(),
        ));
    }
    out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    out
}

#[tokio::test]
async fn should_number_a_metrics_series_contiguously_only_when_sorted() {
    for (layout, sorted) in [(ARRIVAL, false), (SORTED, true)] {
        // given: three metrics written host-major
        let _layout = override_write_layout(layout).await;
        let storage = Arc::new(in_memory_storage().await);
        let tsdb = Tsdb::with_retention(storage.clone(), None);
        let batch = (0..10)
            .flat_map(|host| METRICS.map(|metric| series(metric, host, 3_600_000)))
            .collect();

        // when
        tsdb.ingest_samples(batch, None).await.unwrap();
        tsdb.flush().await.unwrap();

        // then
        assert_eq!(dense_ids(&storage, bucket()).await.len(), 30);
        for metric in METRICS {
            let ids = metric_ids(&storage, bucket(), metric).await;
            assert_eq!(ids.len(), 10);
            assert_eq!(contiguous(&ids), sorted, "{metric} {layout:?}: {ids:?}");
        }
    }
}

#[tokio::test]
async fn should_keep_ids_dense_across_restarts_that_switch_layouts() {
    for layouts in [[SORTED, ARRIVAL, SORTED], [ARRIVAL, SORTED, ARRIVAL]] {
        // given: three writers in turn, each restarted on the next layout,
        // each rewriting the earlier hosts and adding four new ones
        let held = override_write_layout(layouts[0]).await;
        let storage = Arc::new(in_memory_storage().await);
        for (step, layout) in layouts.into_iter().enumerate() {
            held.set(layout);
            let tsdb = Tsdb::with_retention(storage.clone(), None);
            let at = 3_600_000 + step as i64 * 60_000;
            let batch = (0..(step + 1) * 4)
                .flat_map(|host| METRICS.map(|metric| series(metric, host, at)))
                .collect();
            tsdb.ingest_samples(batch, None).await.unwrap();
            tsdb.flush().await.unwrap();
        }

        // then: one ID per series, each with every sample written to it
        assert_eq!(dense_ids(&storage, bucket()).await.len(), 36);
        let stored = stored(&storage, bucket()).await;
        assert_eq!(stored.len(), 36);
        for (metric, host, samples) in stored {
            let index: usize = host[1..].parse().unwrap();
            let expected: Vec<(i64, f64)> = (index / 4..3)
                .map(|step| (3_600_000 + step as i64 * 60_000, index as f64))
                .collect();
            assert_eq!(samples, expected, "{metric} {host} {layouts:?}");
        }
    }
}
