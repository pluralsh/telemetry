use crate::model::{HistogramSample, Label, MetricType, Sample, Series, TimeBucket};
use crate::promql::promqltest::dsl::SeriesLoad;
use crate::tsdb::Tsdb;
use std::collections::HashMap;
use std::time::UNIX_EPOCH;

/// Load series data into TSDB
pub(super) async fn load_series(
    tsdb: &Tsdb,
    interval: std::time::Duration,
    series: &[SeriesLoad],
) -> Result<(), String> {
    for s in series {
        let mut samples = Vec::with_capacity(s.values.len());
        for (step, value) in &s.values {
            samples.push((step_timestamp_ms(interval, *step)?, *value));
        }
        let mut histograms = Vec::with_capacity(s.histograms.len());
        for (step, h) in &s.histograms {
            histograms.push((step_timestamp_ms(interval, *step)?, h.clone()));
        }

        // Sort by timestamp and deduplicate (keep last value per timestamp)
        // This matches Prometheus promqltest semantics
        samples.sort_by_key(|(ts, _)| *ts);
        samples.dedup_by_key(|(ts, _)| *ts);
        histograms.sort_by_key(|(ts, _)| *ts);
        histograms.dedup_by_key(|(ts, _)| *ts);

        // Group by bucket and ingest
        let mut buckets: HashMap<TimeBucket, (Vec<Sample>, Vec<HistogramSample>)> = HashMap::new();
        for (ts_ms, value) in samples {
            buckets
                .entry(bucket_for(ts_ms))
                .or_default()
                .0
                .push(Sample::new(ts_ms, value));
        }
        for (ts_ms, histogram) in histograms {
            buckets
                .entry(bucket_for(ts_ms))
                .or_default()
                .1
                .push(HistogramSample {
                    timestamp_ms: ts_ms,
                    histogram,
                });
        }

        // Ingest each bucket
        for (bucket, (bucket_samples, bucket_histograms)) in buckets {
            let labels: Vec<Label> = s
                .labels
                .iter()
                .map(|(k, v)| Label {
                    name: k.clone(),
                    value: v.clone(),
                })
                .collect();

            let series = Series {
                labels,
                metric_type: Some(MetricType::Gauge),
                unit: None,
                description: None,
                samples: bucket_samples,
                histograms: bucket_histograms,
            };

            let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();
            mini.ingest(&series).await.unwrap();
        }
    }
    tsdb.flush().await.unwrap();
    Ok(())
}

fn step_timestamp_ms(interval: std::time::Duration, step: i64) -> Result<i64, String> {
    if step < 0 {
        return Err(format!("Negative step index not allowed: {}", step));
    }
    let delta = interval
        .checked_mul(step as u32)
        .ok_or_else(|| format!("Timestamp overflow for step {}", step))?;
    Ok(delta.as_millis() as i64)
}

fn bucket_for(ts_ms: i64) -> TimeBucket {
    let ts = UNIX_EPOCH + std::time::Duration::from_millis(ts_ms as u64);
    TimeBucket::round_to_hour(ts).unwrap()
}
