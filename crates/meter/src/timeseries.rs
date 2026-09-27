//! Core TimeSeriesDb implementation with write API.
//!
//! This module provides the [`TimeSeriesDb`] struct, the primary entry point for
//! interacting with OpenData TimeSeries. It exposes write operations for
//! ingesting time series data.

use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::Namespace;
use crate::config::Config;
use crate::error::{QueryError, Result};
use crate::model::{Labels, MetricMetadata, QueryValue, RangeSample, Series};
use crate::storage::Storage;
use crate::tsdb::{
    Tsdb, TsdbReadEngine, find_label_values_in_range, find_labels_in_range, find_series_in_range,
};

/// Visibility guaranteed when a write call returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Visibility {
    /// Applied to the in-memory delta. This is acknowledged ingestion, but it
    /// is not yet guaranteed visible to snapshot-backed queries.
    Applied,
    /// Written into SlateDB's mutable state and visible to a fresh snapshot.
    Written,
    /// Flushed to the configured object store and safe across process restart.
    Durable,
}

/// A time series database for storing and querying metrics.
///
/// `TimeSeriesDb` provides a high-level API for ingesting Prometheus-style
/// metrics. It handles internal details like time bucketing, series
/// deduplication, and storage management automatically.
///
/// # Example
///
/// ```
/// # use meter::{TimeSeriesDb, Config, Namespace, Series};
/// # use common::storage::config::SlateDbStorageConfig;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = Config { storage: SlateDbStorageConfig::default(), ..Default::default() };
/// let ts = TimeSeriesDb::open(Namespace::default(), config).await?;
///
/// let series = Series::builder("http_requests_total")
///     .label("method", "GET")
///     .label("status", "200")
///     .sample_now(1.0)
///     .build();
///
/// ts.write(vec![series]).await?;
/// # Ok(())
/// # }
/// ```
pub struct TimeSeriesDb {
    namespace: Namespace,
    // Internal Tsdb - not exposed
    tsdb: Tsdb,
}

impl TimeSeriesDb {
    /// Opens or creates a time series database with the given configuration.
    ///
    /// This is the primary entry point for creating a `TimeSeriesDb` instance.
    /// The configuration specifies the storage backend and operational parameters.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration specifying storage backend and settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend cannot be initialized.
    ///
    /// # Example
    ///
    /// ```
    /// # use meter::{TimeSeriesDb, Config, Namespace};
    /// # use common::storage::config::SlateDbStorageConfig;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = Config { storage: SlateDbStorageConfig::default(), ..Default::default() };
    /// let ts = TimeSeriesDb::open(Namespace::default(), config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(namespace: Namespace, config: Config) -> Result<Self> {
        let storage = Arc::new(Storage::try_new(&config.storage, namespace.clone()).await?);
        let tsdb = Tsdb::with_retention_scoped(namespace.clone(), storage, config.retention);
        Ok(Self { namespace, tsdb })
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub(crate) fn read_engine(&self) -> &Tsdb {
        &self.tsdb
    }

    /// Writes one or more time series.
    ///
    /// This is the primary write method. It accepts a batch of series,
    /// each containing labels and one or more samples. The method returns
    /// when the data has been accepted for ingestion (but not necessarily
    /// flushed to durable storage).
    ///
    /// # Atomicity
    ///
    /// This operation is atomic: either all series in the batch are accepted,
    /// or none are. This matches the behavior of `LogDb::append()`.
    ///
    /// # Series Identification
    ///
    /// Each unique combination of labels identifies a distinct time series.
    /// The label set must include a `__name__` label for the metric name.
    ///
    /// # Ordering
    ///
    /// Samples within a series should be in timestamp order, but out-of-order
    /// samples are accepted. Duplicate timestamps for the same series will
    /// overwrite previous values.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meter::{TimeSeriesDb, Config, Namespace, Series};
    /// # use common::storage::config::SlateDbStorageConfig;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = Config { storage: SlateDbStorageConfig::default(), ..Default::default() };
    /// # let ts = TimeSeriesDb::open(Namespace::default(), config).await?;
    /// let series = vec![
    ///     Series::builder("cpu_usage")
    ///         .label("host", "server1")
    ///         .sample(1700000000000, 0.75)
    ///         .sample(1700000001000, 0.82)
    ///         .build(),
    ///     Series::builder("cpu_usage")
    ///         .label("host", "server2")
    ///         .sample(1700000000000, 0.45)
    ///         .build(),
    /// ];
    ///
    /// ts.write(series).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write(&self, series: Vec<Series>) -> Result<()> {
        self.write_with_visibility(series, Visibility::Applied)
            .await
    }

    pub async fn write_with_visibility(
        &self,
        series: Vec<Series>,
        visibility: Visibility,
    ) -> Result<()> {
        self.tsdb.ingest_samples(series, None).await?;
        match visibility {
            Visibility::Applied => Ok(()),
            Visibility::Written => self.tsdb.flush_written().await,
            Visibility::Durable => self.tsdb.flush().await,
        }
    }

    /// Writes one or more time series, waiting up to `timeout` for space in
    /// the write queue.
    ///
    /// Behaves identically to [`write`](Self::write) but will wait up to
    /// `timeout` when the write queue is full instead of failing immediately.
    /// If the timeout elapses before space becomes available, an error is
    /// returned and no samples are ingested.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meter::{TimeSeriesDb, Config, Namespace, Series};
    /// # use common::storage::config::SlateDbStorageConfig;
    /// # use std::time::Duration;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = Config { storage: SlateDbStorageConfig::default(), ..Default::default() };
    /// # let ts = TimeSeriesDb::open(Namespace::default(), config).await?;
    /// let series = vec![
    ///     Series::builder("cpu_usage")
    ///         .label("host", "server1")
    ///         .sample(1700000000000, 0.75)
    ///         .build(),
    /// ];
    ///
    /// ts.write_timeout(series, Duration::from_secs(30)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_timeout(&self, series: Vec<Series>, timeout: Duration) -> Result<()> {
        self.tsdb.ingest_samples(series, Some(timeout)).await
    }

    // ── Read / Query API (RFC 0003) ──────────────────────────────────

    /// Evaluates an instant PromQL query at a single point in time.
    ///
    /// If `time` is `None`, the current wall-clock time is used.
    pub async fn query(
        &self,
        query: &str,
        time: Option<SystemTime>,
    ) -> std::result::Result<QueryValue, QueryError> {
        self.tsdb
            .eval_query(query, time, &crate::model::QueryOptions::default())
            .await
    }

    /// Evaluates a range PromQL query over a time interval.
    pub async fn query_range(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
    ) -> std::result::Result<Vec<RangeSample>, QueryError> {
        <Tsdb as TsdbReadEngine>::eval_query_range(
            &self.tsdb,
            query,
            range,
            step,
            &crate::model::QueryOptions::default(),
        )
        .await
    }

    /// Returns the set of label-sets matching the given series matchers.
    pub async fn series(
        &self,
        matchers: &[&str],
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        find_series_in_range(&self.tsdb, matchers, range).await
    }

    /// Returns the set of label names matching the given matchers.
    pub async fn labels(
        &self,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        find_labels_in_range(&self.tsdb, matchers, range).await
    }

    /// Returns the set of values for a given label name.
    pub async fn label_values(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        find_label_values_in_range(&self.tsdb, label_name, matchers, range).await
    }

    /// Returns metric metadata, optionally filtered to a single metric.
    pub async fn metadata(
        &self,
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        self.tsdb.find_metadata(metric).await
    }

    /// Forces flush of all pending data to durable storage.
    ///
    /// Normally data is flushed according to the configured `flush_interval`,
    /// but this method can be used to ensure durability immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush fails due to storage issues.
    pub async fn flush(&self) -> Result<()> {
        self.tsdb.flush().await
    }

    /// Flushes pending data and creates a durable checkpoint.
    ///
    /// The returned [`common::CheckpointInfo::id`] can be passed to
    /// [`crate::TimeSeriesDbReader::open_at_checkpoint`] (or to the
    /// `checkpoint_id` field on `PrometheusConfig`) to open a reader pinned
    /// to this exact view of the database.
    ///
    /// Only supported by SlateDB-backed storage.
    pub async fn create_checkpoint(&self) -> Result<common::CheckpointInfo> {
        self.tsdb.create_checkpoint().await
    }

    /// Closes the time series database, flushing any pending data and releasing
    /// resources.
    ///
    /// All written data is flushed to durable storage before the database is
    /// closed. For SlateDB-backed storage, this also releases the database
    /// fence.
    pub async fn close(self) -> Result<()> {
        self.tsdb.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Sample, Series};
    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
    };

    #[tokio::test]
    async fn visibility_levels_have_documented_query_guarantees() {
        let config = Config {
            storage: SlateDbStorageConfig {
                path: "visibility".to_string(),
                object_store: ObjectStoreConfig::InMemory,
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            },
            ..Default::default()
        };
        let db = TimeSeriesDb::open(Namespace::new("visibility-test").unwrap(), config)
            .await
            .unwrap();

        for (offset, visibility) in [
            (0, Visibility::Applied),
            (1, Visibility::Written),
            (2, Visibility::Durable),
        ] {
            let timestamp = 1_700_000_000_000 + offset;
            db.write_with_visibility(
                vec![
                    Series::builder("visibility_metric")
                        .label("level", format!("{visibility:?}"))
                        .sample(timestamp, offset as f64)
                        .build(),
                ],
                visibility,
            )
            .await
            .unwrap();
            if visibility == Visibility::Applied {
                continue;
            }
            let result = db
                .query(
                    "visibility_metric",
                    Some(SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp as u64 + 1)),
                )
                .await
                .unwrap();
            assert!(
                matches!(result, QueryValue::Vector(ref samples) if !samples.is_empty()),
                "{visibility:?} result was {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn close_without_explicit_flush_guarantees_durability() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = SlateDbStorageConfig {
            path: "ts-data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp_dir.path().to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        };

        // Write a series and close without calling flush()
        {
            let tsdb = TimeSeriesDb::open(
                crate::Namespace::default(),
                Config {
                    storage: storage.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

            tsdb.write(vec![Series::new(
                "cpu_usage",
                vec![Label::new("host", "server1")],
                vec![Sample::new(3_900_000, 0.42)],
            )])
            .await
            .unwrap();

            tsdb.close().await.unwrap();
        }

        // Reopen and verify the series survived
        let tsdb = TimeSeriesDb::open(
            crate::Namespace::default(),
            Config {
                storage: storage.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let series = tsdb
            .series(&["{__name__=\"cpu_usage\"}"], ..)
            .await
            .unwrap();

        assert!(
            !series.is_empty(),
            "expected series to survive close without explicit flush"
        );
    }
}
