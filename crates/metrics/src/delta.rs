use std::collections::HashMap;
use std::sync::Arc;

use common::coordinator::Delta;

use crate::Namespace;
use crate::active_series::ActiveSeriesTracker;
use crate::error::{Error, Result};
use crate::index::{ForwardIndex, InvertedIndex, SeriesSpec};
use crate::model::{
    HistogramSample, Label, Labels, MetricMetadata, MetricType, Sample, Series, SeriesFingerprint,
    SeriesId, TimeBucket,
};
use crate::util::Fingerprint;

/// Samples for a single series, bundled with the metric name needed for
/// encoding the storage key.
pub(crate) struct SeriesSamples {
    pub(crate) metric_name: String,
    pub(crate) points: Vec<Sample>,
    pub(crate) histograms: Vec<HistogramSample>,
}

impl SeriesSamples {
    pub(crate) fn len(&self) -> usize {
        self.points.len() + self.histograms.len()
    }
}

/// State that persists across delta freezes.
pub(crate) struct TsdbContext {
    pub(crate) namespace: Namespace,
    pub(crate) bucket: TimeBucket,
    pub(crate) series_dict: Arc<HashMap<SeriesFingerprint, SeriesId>>,
    pub(crate) next_series_id: u32,
    pub(crate) active_series: Arc<ActiveSeriesTracker>,
}

/// Frozen (immutable) delta data sent to the flusher.
pub(crate) struct FrozenTsdbDelta {
    pub(crate) namespace: Namespace,
    pub(crate) bucket: TimeBucket,
    pub(crate) forward_index: ForwardIndex,
    pub(crate) inverted_index: InvertedIndex,
    pub(crate) metadata: HashMap<String, MetricMetadata>,
    pub(crate) series_dict_delta: HashMap<SeriesFingerprint, SeriesId>,
    pub(crate) samples: HashMap<SeriesId, SeriesSamples>,
}

impl FrozenTsdbDelta {
    pub(crate) fn is_empty(&self) -> bool {
        self.samples.is_empty() && self.series_dict_delta.is_empty()
    }
}

/// One write through a bucket's coordinator.
pub(crate) enum TsdbWrite {
    Samples(Vec<Series>),
    /// Series registered without their samples, which a rollup copies
    /// afterwards; their samples and metadata are ignored.
    Register(Vec<Series>),
}

impl From<Vec<Series>> for TsdbWrite {
    fn from(series: Vec<Series>) -> Self {
        Self::Samples(series)
    }
}

/// Delta implementation for the write coordinator.
///
/// Uses the Arc base + HashMap overlay pattern: the base series dictionary
/// is an Arc clone from context (O(1)), and new series are tracked in a
/// local HashMap overlay. Since the write coordinator is single-threaded,
/// no DashMap/AtomicU32 is needed.
pub(crate) struct TsdbWriteDelta {
    namespace: Namespace,
    bucket: TimeBucket,
    series_dict_base: Arc<HashMap<SeriesFingerprint, SeriesId>>,
    series_dict_delta: HashMap<SeriesFingerprint, SeriesId>,
    forward_index: ForwardIndex,
    inverted_index: InvertedIndex,
    metadata: HashMap<String, MetricMetadata>,
    /// Samples keyed by series_id, with metric name for key encoding.
    samples: HashMap<SeriesId, SeriesSamples>,
    /// The first ID this delta allocated: its new series hold
    /// `first_series_id..next_series_id`.
    first_series_id: u32,
    next_series_id: u32,
    active_series: Arc<ActiveSeriesTracker>,
}

impl TsdbWriteDelta {
    fn ingest(&mut self, series: Series) -> Result<()> {
        let Series {
            mut labels,
            metric_type,
            unit,
            description,
            samples,
            histograms,
        } = series;
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let fingerprint = labels.fingerprint();

        // Record the series in the active-series HLL once per ingest call.
        // HLL inserts are idempotent (fetch_max), so repeated records of the
        // same fingerprint across batches don't inflate the estimate.
        self.active_series.record(fingerprint);

        let bucket_start_ms = i64::from(self.bucket.start) * 60 * 1000;
        let bucket_end_ms =
            (i64::from(self.bucket.start) + i64::from(self.bucket.size_in_mins())) * 60 * 1000;
        // Validated before registration, so a series with a rejected sample
        // is never registered.
        let timestamps = samples
            .iter()
            .map(|s| s.timestamp_ms)
            .chain(histograms.iter().map(|h| h.timestamp_ms));
        for timestamp_ms in timestamps {
            if timestamp_ms < bucket_start_ms || timestamp_ms >= bucket_end_ms {
                return Err(Error::InvalidInput(format!(
                    "Sample timestamp {} is outside bucket range [{}, {})",
                    timestamp_ms, bucket_start_ms, bucket_end_ms
                )));
            }
        }
        if samples.is_empty() && histograms.is_empty() {
            return Ok(());
        }
        if let Some(metric_name) = labels
            .iter()
            .find(|label| label.name == "__name__")
            .map(|label| label.value.clone())
        {
            self.metadata.insert(
                metric_name.clone(),
                MetricMetadata {
                    metric_name,
                    metric_type,
                    description,
                    unit: unit.clone(),
                },
            );
        }
        let id = self.resolve_series(&labels, fingerprint, &unit, metric_type)?;
        let entry = self.samples.entry(id).or_insert_with(|| SeriesSamples {
            metric_name: labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.clone())
                .unwrap_or_default(),
            points: Vec::new(),
            histograms: Vec::new(),
        });
        entry.points.extend(samples);
        entry.histograms.extend(histograms);
        Ok(())
    }

    /// Registers `series` without its samples: they arrive later through
    /// [`Self::ingest`], which also records the metric's metadata.
    fn register(&mut self, series: Series) -> Result<()> {
        let mut labels = series.labels;
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let fingerprint = labels.fingerprint();
        self.resolve_series(&labels, fingerprint, &series.unit, series.metric_type)?;
        Ok(())
    }

    /// Looks up the series ID, registering the series and its indexes if new.
    fn resolve_series(
        &mut self,
        labels: &[Label],
        fingerprint: SeriesFingerprint,
        unit: &Option<String>,
        metric_type: Option<MetricType>,
    ) -> Result<SeriesId> {
        if let Some(&id) = self
            .series_dict_delta
            .get(&fingerprint)
            .or_else(|| self.series_dict_base.get(&fingerprint))
        {
            return Ok(id);
        }
        let id = self.next_series_id;
        self.next_series_id = id
            .checked_add(1)
            .ok_or_else(|| Error::Internal("series ID space exhausted for bucket".to_owned()))?;

        self.series_dict_delta.insert(fingerprint, id);

        let series_spec = SeriesSpec {
            unit: unit.clone(),
            metric_type,
            labels: Labels::sorted(labels.to_vec()),
        };
        self.forward_index.series.insert(id, series_spec);

        for label in labels {
            self.inverted_index
                .postings
                .entry(label.clone())
                .or_default()
                .value_mut()
                .insert(id);
        }

        Ok(id)
    }

    /// Renumbers this delta's new series within the IDs they hold, in
    /// (metric, labels) order, so a metric's records are contiguous rather
    /// than interleaved in arrival order. Nothing outside the delta has seen
    /// these IDs, and they reach storage in one batch once frozen, so IDs
    /// stay dense and become visible in order.
    fn sort_new_series(&mut self) {
        let first = self.first_series_id;
        let count = (self.next_series_id - first) as usize;
        if count < 2 || self.forward_index.series.len() != count {
            return;
        }
        let mut order: Vec<(SeriesId, Labels)> = self
            .forward_index
            .series
            .iter()
            .map(|entry| (*entry.key(), entry.value().labels.clone()))
            .collect();
        order.sort_unstable_by(|(_, a), (_, b)| {
            a.metric_name()
                .cmp(b.metric_name())
                .then_with(|| a.as_slice().cmp(b.as_slice()))
        });
        let mut renumbered = vec![0; count];
        for (rank, (id, _)) in (first..).zip(&order) {
            renumbered[(id - first) as usize] = rank;
        }
        if renumbered.iter().copied().eq(first..self.next_series_id) {
            return;
        }
        let renumber = |id: SeriesId| {
            id.checked_sub(first)
                .and_then(|offset| renumbered.get(offset as usize))
                .copied()
                .unwrap_or(id)
        };
        for (id, spec) in std::mem::take(&mut self.forward_index.series) {
            self.forward_index.series.insert(renumber(id), spec);
        }
        for mut postings in self.inverted_index.postings.iter_mut() {
            let ids: roaring::RoaringBitmap = postings.value().iter().map(renumber).collect();
            *postings.value_mut() = ids;
        }
        self.samples = std::mem::take(&mut self.samples)
            .into_iter()
            .map(|(id, samples)| (renumber(id), samples))
            .collect();
        for id in self.series_dict_delta.values_mut() {
            *id = renumber(*id);
        }
    }
}

impl Delta for TsdbWriteDelta {
    type Context = TsdbContext;
    type Write = TsdbWrite;
    type Frozen = FrozenTsdbDelta;
    type FrozenView = ();
    type ApplyResult = ();
    type DeltaView = ();
    /// Readers take a fresh storage snapshot per query instead. A SlateDB
    /// snapshot held in the coordinator's view would live as long as the
    /// bucket's `MiniTsdb` and, once the bucket goes idle, pin SlateDB's
    /// oldest-snapshot sequence: L0 flush and compaction refuse to merge any
    /// operand newer than it, across every bucket.
    type Snapshot = ();

    fn init(context: Self::Context) -> Self {
        Self {
            namespace: context.namespace,
            bucket: context.bucket,
            series_dict_base: context.series_dict.clone(),
            series_dict_delta: HashMap::new(),
            forward_index: ForwardIndex::default(),
            inverted_index: InvertedIndex::default(),
            metadata: HashMap::new(),
            samples: HashMap::new(),
            first_series_id: context.next_series_id,
            next_series_id: context.next_series_id,
            active_series: context.active_series,
        }
    }

    fn apply(&mut self, write: Self::Write) -> std::result::Result<Self::ApplyResult, String> {
        match write {
            TsdbWrite::Samples(series) => series
                .into_iter()
                .try_for_each(|series| self.ingest(series)),
            TsdbWrite::Register(series) => series
                .into_iter()
                .try_for_each(|series| self.register(series)),
        }
        .map_err(|error| error.to_string())
    }

    fn estimate_size(&self) -> usize {
        // Rough estimate: 16 bytes per float sample, 32 bytes per histogram
        // bucket + index overhead
        let sample_bytes: usize = self
            .samples
            .values()
            .map(|s| {
                s.points.len() * 16
                    + s.histograms
                        .iter()
                        .map(|h| {
                            64 + 32 * (h.histogram.positive.len() + h.histogram.negative.len())
                        })
                        .sum::<usize>()
            })
            .sum();
        sample_bytes
            + self.forward_index.series.len() * 128
            + self.inverted_index.postings.len() * 64
    }

    fn freeze(mut self) -> (Self::Frozen, Self::FrozenView, Self::Context) {
        if crate::read_tuning::write_layout().sorted_series_ids {
            self.sort_new_series();
        }
        // Merge base + delta into new Arc for the next context
        let merged_dict = if self.series_dict_delta.is_empty() {
            // O(1) when no new series
            self.series_dict_base.clone()
        } else {
            let mut merged = (*self.series_dict_base).clone();
            merged.extend(self.series_dict_delta.iter());
            Arc::new(merged)
        };

        let context = TsdbContext {
            namespace: self.namespace.clone(),
            bucket: self.bucket,
            series_dict: merged_dict,
            next_series_id: self.next_series_id,
            active_series: self.active_series,
        };

        let frozen = FrozenTsdbDelta {
            namespace: self.namespace,
            bucket: self.bucket,
            forward_index: self.forward_index,
            inverted_index: self.inverted_index,
            metadata: self.metadata,
            series_dict_delta: self.series_dict_delta,
            samples: self.samples,
        };

        (frozen, (), context)
    }

    fn reader(&self) -> Self::DeltaView {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MetricType;

    fn create_test_bucket() -> TimeBucket {
        TimeBucket::hour(1000)
    }

    fn create_test_context() -> TsdbContext {
        TsdbContext {
            namespace: crate::Namespace::default(),
            bucket: create_test_bucket(),
            series_dict: Arc::new(HashMap::new()),
            next_series_id: 0,
            active_series: Arc::new(ActiveSeriesTracker::new(0)),
        }
    }

    fn create_test_sample() -> Sample {
        // Bucket at minute 1000 covers ms 60_000_000 to 63_600_000
        Sample {
            timestamp_ms: 60_000_001,
            value: 42.5,
        }
    }

    fn create_test_series(name: &str, labels: Vec<(&str, &str)>, sample: Sample) -> Series {
        let label_vec: Vec<Label> = labels.into_iter().map(|(k, v)| Label::new(k, v)).collect();
        let mut series = Series::new(name, label_vec, vec![sample]);
        series.metric_type = Some(MetricType::Gauge);
        series
    }

    #[test]
    fn should_create_new_series_on_first_apply() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let series =
            create_test_series("http_requests", vec![("env", "prod")], create_test_sample());

        // when
        delta.apply(vec![series].into()).unwrap();

        // then
        assert_eq!(delta.next_series_id, 1);
        assert_eq!(delta.series_dict_delta.len(), 1);
        assert_eq!(delta.samples.len(), 1);
        assert_eq!(delta.samples.get(&0).unwrap().points.len(), 1);
    }

    #[test]
    fn should_allocate_dense_series_ids_from_context() {
        let mut ctx = create_test_context();
        ctx.next_series_id = 17;
        let mut delta = TsdbWriteDelta::init(ctx);

        delta
            .apply(
                vec![
                    create_test_series("a", vec![("env", "prod")], create_test_sample()),
                    create_test_series(
                        "b",
                        vec![("env", "prod")],
                        Sample {
                            timestamp_ms: 60_000_002,
                            value: 1.0,
                        },
                    ),
                ]
                .into(),
            )
            .unwrap();

        assert!(delta.samples.contains_key(&17));
        assert!(delta.samples.contains_key(&18));
        assert_eq!(delta.next_series_id, 19);
    }

    #[tokio::test]
    async fn should_number_a_flushs_new_series_by_metric_then_labels() {
        // given: a known series, then two metrics' series arriving interleaved
        let _layout = crate::read_tuning::override_write_layout(crate::read_tuning::WriteLayout {
            sorted_series_ids: true,
            rollup_presort: false,
        })
        .await;
        let mut base = HashMap::new();
        base.insert(7u128, 0u32);
        let ctx = TsdbContext {
            series_dict: Arc::new(base),
            next_series_id: 1,
            ..create_test_context()
        };
        let mut delta = TsdbWriteDelta::init(ctx);
        let arrivals = [("b", "2"), ("a", "2"), ("b", "1"), ("a", "1")];
        for (at, (name, host)) in arrivals.into_iter().enumerate() {
            let sample = Sample {
                timestamp_ms: 60_000_001,
                value: at as f64,
            };
            let series = create_test_series(name, vec![("host", host)], sample);
            delta.apply(vec![series].into()).unwrap();
        }

        // when
        let (frozen, _, context) = delta.freeze();

        // then: a{1}, a{2}, b{1}, b{2} hold 1..5, and every per-series
        // structure follows the new numbering
        let fingerprint = |name: &str, host: &str| {
            let mut labels = vec![Label::new("__name__", name), Label::new("host", host)];
            labels.sort_by(|a, b| a.name.cmp(&b.name));
            labels.fingerprint()
        };
        let sorted = [
            ("a", "1", 3.0),
            ("a", "2", 1.0),
            ("b", "1", 2.0),
            ("b", "2", 0.0),
        ];
        for ((name, host, value), id) in sorted.into_iter().zip(1u32..) {
            let fingerprint = fingerprint(name, host);
            assert_eq!(frozen.series_dict_delta[&fingerprint], id);
            assert_eq!(context.series_dict[&fingerprint], id);
            let spec = frozen.forward_index.series.get(&id).unwrap();
            assert_eq!(spec.labels.metric_name(), name);
            assert_eq!(spec.labels.get("host"), Some(host));
            assert_eq!(frozen.samples[&id].points[0].value, value);
            assert!(
                frozen
                    .inverted_index
                    .postings
                    .get(&Label::new("host", host))
                    .unwrap()
                    .contains(id)
            );
        }
        assert_eq!(context.series_dict[&7], 0);
        assert_eq!(context.next_series_id, 5);
    }

    #[test]
    fn should_reuse_series_id_for_same_fingerprint() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let sample1 = Sample {
            timestamp_ms: 60_000_001,
            value: 10.0,
        };
        let sample2 = Sample {
            timestamp_ms: 60_000_002,
            value: 20.0,
        };
        let series1 = create_test_series("http_requests", vec![("env", "prod")], sample1);
        let series2 = create_test_series("http_requests", vec![("env", "prod")], sample2);

        // when
        delta.apply(vec![series1].into()).unwrap();
        delta.apply(vec![series2].into()).unwrap();

        // then
        assert_eq!(delta.next_series_id, 1); // Only one series created
        assert_eq!(delta.series_dict_delta.len(), 1);
        assert_eq!(delta.samples.get(&0).unwrap().points.len(), 2);
    }

    #[test]
    fn should_keep_label_sets_with_equal_concatenations_distinct() {
        // given: both label sets flatten to `__name__m` + `a1b2`
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let split = create_test_series("m", vec![("a", "1"), ("b", "2")], create_test_sample());
        let joined = create_test_series("m", vec![("a", "1b2")], create_test_sample());

        // when
        delta.apply(vec![split, joined].into()).unwrap();

        // then
        assert_eq!(delta.next_series_id, 2);
        assert_eq!(delta.series_dict_delta.len(), 2);
    }

    #[test]
    fn should_carry_series_dict_across_freeze_cycles() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let series =
            create_test_series("http_requests", vec![("env", "prod")], create_test_sample());
        delta.apply(vec![series].into()).unwrap();

        // when: freeze and create new delta
        let (frozen, _, new_ctx) = delta.freeze();
        let mut delta2 = TsdbWriteDelta::init(new_ctx);

        // Apply a series with the same fingerprint
        let sample2 = Sample {
            timestamp_ms: 60_000_002,
            value: 99.0,
        };
        let series2 = create_test_series("http_requests", vec![("env", "prod")], sample2);
        delta2.apply(vec![series2].into()).unwrap();

        // then: should reuse series_id from frozen context
        assert_eq!(delta2.next_series_id, 1); // No new ID allocated
        assert_eq!(delta2.series_dict_delta.len(), 0); // Not in delta overlay
        assert_eq!(delta2.samples.get(&0).unwrap().points.len(), 1);

        // And the frozen delta has the original series
        assert_eq!(frozen.series_dict_delta.len(), 1);
        assert_eq!(frozen.samples.get(&0).unwrap().points.len(), 1);
    }

    #[test]
    fn should_reject_sample_outside_bucket_range() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let bad_sample = Sample {
            timestamp_ms: 59_999_999, // Before bucket start
            value: 42.5,
        };
        let series = create_test_series("http_requests", vec![("env", "prod")], bad_sample);

        // when
        let result = delta.apply(vec![series].into());

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("outside bucket range"));
    }

    #[test]
    fn should_accumulate_samples_across_multiple_applies() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);

        // when: apply 3 batches with same series
        for i in 0..3 {
            let sample = Sample {
                timestamp_ms: 60_000_001 + i,
                value: i as f64,
            };
            let series = create_test_series("metric", vec![("k", "v")], sample);
            delta.apply(vec![series].into()).unwrap();
        }

        // then
        assert_eq!(delta.next_series_id, 1);
        assert_eq!(delta.samples.get(&0).unwrap().points.len(), 3);
    }

    #[test]
    fn should_skip_merge_on_freeze_when_no_new_series() {
        // given: context with existing series, delta with no new series
        let mut base = HashMap::new();
        base.insert(12345u128, 0u32);
        let ctx = TsdbContext {
            namespace: crate::Namespace::default(),
            bucket: create_test_bucket(),
            series_dict: Arc::new(base),
            next_series_id: 1,
            active_series: Arc::new(ActiveSeriesTracker::new(0)),
        };
        let delta = TsdbWriteDelta::init(ctx);
        let base_ptr = Arc::as_ptr(&delta.series_dict_base);

        // when
        let (_, _, new_ctx) = delta.freeze();

        // then: Arc should be the same pointer (O(1) clone, no merge)
        assert_eq!(Arc::as_ptr(&new_ctx.series_dict), base_ptr);
    }

    #[test]
    fn should_create_different_series_id_for_different_attributes() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let series1 = create_test_series(
            "http_requests",
            vec![("service", "api")],
            create_test_sample(),
        );
        let series2 = create_test_series(
            "http_requests",
            vec![("service", "web")],
            create_test_sample(),
        );

        // when
        delta.apply(vec![series1].into()).unwrap();
        delta.apply(vec![series2].into()).unwrap();

        // then
        assert_eq!(delta.next_series_id, 2);
        assert_eq!(delta.series_dict_delta.len(), 2);
        assert_eq!(delta.samples.len(), 2);
        assert!(delta.samples.contains_key(&0));
        assert!(delta.samples.contains_key(&1));
    }

    #[test]
    fn should_sort_attributes_before_fingerprinting() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let series1 = create_test_series(
            "metric",
            vec![("z_key", "value"), ("a_key", "value")],
            create_test_sample(),
        );
        let series2 = create_test_series(
            "metric",
            vec![("a_key", "value"), ("z_key", "value")],
            Sample {
                timestamp_ms: 60_000_002,
                value: 99.0,
            },
        );

        // when
        delta.apply(vec![series1].into()).unwrap();
        delta.apply(vec![series2].into()).unwrap();

        // then
        assert_eq!(delta.next_series_id, 1); // Same series_id reused
        assert_eq!(delta.series_dict_delta.len(), 1);
        assert_eq!(delta.samples.len(), 1);
    }

    #[test]
    fn should_store_metric_unit_and_type_in_forward_index() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let label_vec: Vec<Label> = vec![("env", "prod")]
            .into_iter()
            .map(|(k, v)| Label::new(k, v))
            .collect();
        let mut series = Series::new("http_requests", label_vec, vec![create_test_sample()]);
        series.unit = Some("requests_per_second".to_string());
        series.metric_type = Some(MetricType::Sum {
            monotonic: true,
            temporality: crate::model::Temporality::Cumulative,
        });

        // when
        delta.apply(vec![series].into()).unwrap();

        // then
        let series_spec = delta.forward_index.series.get(&0).unwrap();
        assert_eq!(series_spec.unit, Some("requests_per_second".to_string()));
        match series_spec.metric_type {
            Some(MetricType::Sum {
                monotonic,
                temporality,
            }) => {
                assert!(monotonic);
                assert_eq!(temporality, crate::model::Temporality::Cumulative);
            }
            _ => panic!("Expected Sum metric type"),
        }
    }

    #[test]
    fn should_index_all_labels_in_inverted_index() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let series = create_test_series(
            "metric",
            vec![("service", "api"), ("env", "prod"), ("region", "us-east")],
            create_test_sample(),
        );

        // when
        delta.apply(vec![series].into()).unwrap();

        // then: __name__ label + 3 explicit labels = 4
        assert_eq!(delta.inverted_index.postings.len(), 4);
        for label in &[
            Label::new("__name__", "metric"),
            Label::new("service", "api"),
            Label::new("env", "prod"),
            Label::new("region", "us-east"),
        ] {
            let postings = delta.inverted_index.postings.get(label).unwrap();
            assert!(postings.value().contains(0));
            assert_eq!(postings.value().len(), 1);
        }
    }

    #[test]
    fn should_handle_empty_attributes_list() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let series = create_test_series("metric", vec![], create_test_sample());

        // when
        delta.apply(vec![series].into()).unwrap();

        // then
        assert_eq!(delta.next_series_id, 1);
        assert_eq!(delta.series_dict_delta.len(), 1);
        assert_eq!(delta.samples.len(), 1);
        // Only __name__ label in inverted index
        assert_eq!(delta.inverted_index.postings.len(), 1);
    }

    #[test]
    fn should_handle_none_metric_unit() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let label_vec: Vec<Label> = vec![("env", "prod")]
            .into_iter()
            .map(|(k, v)| Label::new(k, v))
            .collect();
        let mut series = Series::new("http_requests", label_vec, vec![create_test_sample()]);
        series.unit = None;
        series.metric_type = Some(MetricType::Gauge);

        // when
        delta.apply(vec![series].into()).unwrap();

        // then
        let series_spec = delta.forward_index.series.get(&0).unwrap();
        assert_eq!(series_spec.unit, None);
        assert!(matches!(series_spec.metric_type, Some(MetricType::Gauge)));
    }

    #[test]
    fn should_reuse_series_from_base_dict() {
        // given: context with pre-existing series
        let mut labels = vec![
            Label::new("__name__", "http_requests"),
            Label::new("env", "prod"),
        ];
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let fingerprint = labels.fingerprint();

        let mut base = HashMap::new();
        base.insert(fingerprint, 42u32);
        let ctx = TsdbContext {
            namespace: crate::Namespace::default(),
            bucket: create_test_bucket(),
            series_dict: Arc::new(base),
            next_series_id: 4138,
            active_series: Arc::new(ActiveSeriesTracker::new(0)),
        };
        let mut delta = TsdbWriteDelta::init(ctx);
        let series =
            create_test_series("http_requests", vec![("env", "prod")], create_test_sample());

        // when
        delta.apply(vec![series].into()).unwrap();

        // then
        assert_eq!(delta.next_series_id, 4138); // No new ID allocated
        assert_eq!(delta.series_dict_delta.len(), 0); // Not added to delta
        assert!(delta.samples.contains_key(&42)); // Uses existing series_id
    }
}
