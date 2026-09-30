//! Writer/reader engine dispatch and the bucket-scoped query reader.

use super::*;

/// Wraps either a read-write [`Tsdb`] or a read-only [`crate::reader::TimeSeriesDbReader`],
/// dispatching read methods to the inner engine and rejecting writes in
/// read-only mode.
pub(crate) enum TsdbEngine {
    ReadWrite(Arc<Tsdb>),
    ReadOnly(Arc<crate::reader::TimeSeriesDbReader>),
}

impl TsdbEngine {
    /// Returns `true` when the engine is read-only.
    pub(crate) fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly(_))
    }

    /// Returns a clone of the inner `Arc<Tsdb>` if this is a read-write engine.
    pub(crate) fn as_tsdb(&self) -> Option<Arc<Tsdb>> {
        match self {
            Self::ReadWrite(tsdb) => Some(tsdb.clone()),
            Self::ReadOnly(_) => None,
        }
    }

    // ── Read methods (dispatch to inner engine) ──

    pub(crate) async fn eval_query(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: &QueryOptions,
    ) -> std::result::Result<QueryValue, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.eval_query(query, time, opts).await,
            Self::ReadOnly(reader) => reader.eval_query(query, time, opts).await,
        }
    }

    pub(crate) async fn eval_query_range(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<Vec<RangeSample>, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.eval_query_range(query, range, step, opts).await,
            Self::ReadOnly(reader) => reader.eval_query_range(query, range, step, opts).await,
        }
    }

    /// Tracing-aware instant query. Populates [`ExecuteOutcome::trace`]
    /// when `trace` is `Some`. Otherwise behaves like [`Self::eval_query`]
    /// wrapped in an [`ExecuteOutcome`].
    pub(crate) async fn eval_query_traced(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: &QueryOptions,
        trace: Option<Arc<crate::promql::trace::TraceCollector>>,
    ) -> std::result::Result<ExecuteOutcome, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.eval_query_traced(query, time, opts, trace).await,
            Self::ReadOnly(reader) => reader.eval_query_traced(query, time, opts, trace).await,
        }
    }

    /// Like [`Self::eval_query_range`] but returns the raw [`QueryValue`].
    pub(crate) async fn eval_query_range_value(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<QueryValue, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.eval_query_range_value(query, range, step, opts).await,
            Self::ReadOnly(reader) => {
                reader
                    .eval_query_range_value(query, range, step, opts)
                    .await
            }
        }
    }

    /// Tracing-aware range query. See [`Self::eval_query_traced`].
    pub(crate) async fn eval_query_range_traced(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
        opts: &QueryOptions,
        trace: Option<Arc<crate::promql::trace::TraceCollector>>,
    ) -> std::result::Result<ExecuteOutcome, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => {
                tsdb.eval_query_range_traced(query, range, step, opts, trace)
                    .await
            }
            Self::ReadOnly(reader) => {
                reader
                    .eval_query_range_traced(query, range, step, opts, trace)
                    .await
            }
        }
    }

    /// Dry-run EXPLAIN for instant queries. Parses, lowers, optimises,
    /// and describes the physical plan — does not open a reader or
    /// touch the index cache.
    pub(crate) fn explain_query(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: &QueryOptions,
    ) -> std::result::Result<crate::promql::plan::ExplainResult, QueryError> {
        let query_time = time.unwrap_or_else(SystemTime::now);
        let at_ms = system_time_to_ms(query_time);
        let ctx = crate::promql::plan::LoweringContext::for_instant(
            at_ms,
            duration_to_ms(opts.lookback_delta),
        );
        explain_query(query, &ctx)
    }

    /// Dry-run EXPLAIN for range queries.
    pub(crate) fn explain_query_range(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime>,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<crate::promql::plan::ExplainResult, QueryError> {
        let (start, end) = crate::util::range_bounds_to_system_time(range);
        let start_ms = system_time_to_ms(start);
        let end_ms = system_time_to_ms(end);
        let step_ms = duration_to_ms(step);
        if step_ms <= 0 {
            return Err(QueryError::InvalidQuery(
                "step must be greater than zero".to_string(),
            ));
        }
        let ctx = crate::promql::plan::LoweringContext::new(
            start_ms,
            end_ms,
            step_ms,
            duration_to_ms(opts.lookback_delta),
        );
        explain_query(query, &ctx)
    }

    pub(crate) async fn find_series(
        &self,
        matchers: &[&str],
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.find_series(matchers, start_secs, end_secs).await,
            Self::ReadOnly(reader) => reader.find_series(matchers, start_secs, end_secs).await,
        }
    }

    pub(crate) async fn find_labels(
        &self,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.find_labels(matchers, start_secs, end_secs).await,
            Self::ReadOnly(reader) => reader.find_labels(matchers, start_secs, end_secs).await,
        }
    }

    pub(crate) async fn find_label_values(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => {
                tsdb.find_label_values(label_name, matchers, start_secs, end_secs)
                    .await
            }
            Self::ReadOnly(reader) => {
                reader
                    .find_label_values(label_name, matchers, start_secs, end_secs)
                    .await
            }
        }
    }

    pub(crate) async fn find_metadata(
        &self,
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.find_metadata(metric).await,
            Self::ReadOnly(_) => Ok(vec![]),
        }
    }

    // ── Write methods (error in read-only mode) ──

    pub(crate) async fn ingest_samples(
        &self,
        series_list: Vec<Series>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.ingest_samples(series_list, timeout).await,
            Self::ReadOnly(_) => Err(crate::error::Error::InvalidInput(
                "write operations are not supported in read-only mode".to_string(),
            )),
        }
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.flush().await,
            Self::ReadOnly(_) => Ok(()),
        }
    }

    pub(crate) async fn create_checkpoint(&self) -> Result<common::CheckpointInfo> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.create_checkpoint().await,
            Self::ReadOnly(_) => Err(crate::error::Error::InvalidInput(
                "checkpoint creation is not supported in read-only mode".to_string(),
            )),
        }
    }

    pub(crate) async fn close(&self) -> Result<()> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.close().await,
            Self::ReadOnly(reader) => reader.close().await,
        }
    }
}

impl From<Arc<Tsdb>> for TsdbEngine {
    fn from(tsdb: Arc<Tsdb>) -> Self {
        Self::ReadWrite(tsdb)
    }
}

impl From<Arc<crate::reader::TimeSeriesDbReader>> for TsdbEngine {
    fn from(reader: Arc<crate::reader::TimeSeriesDbReader>) -> Self {
        Self::ReadOnly(reader)
    }
}

/// QueryReader implementation that properly handles bucket-scoped series IDs.
pub(crate) struct TsdbQueryReader {
    /// Map from bucket to MiniTsdb for efficient bucket queries
    mini_readers: HashMap<TimeBucket, MiniQueryReader<StorageSnapshot>>,
}

impl TsdbQueryReader {
    pub fn new(mini_tsdbs: Vec<(TimeBucket, MiniQueryReader<StorageSnapshot>)>) -> Self {
        let bucket_minis = mini_tsdbs.into_iter().collect();
        Self {
            mini_readers: bucket_minis,
        }
    }
}

#[async_trait]
impl QueryReader for TsdbQueryReader {
    async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
        Ok(self.mini_readers.keys().cloned().collect())
    }

    async fn forward_index(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.forward_index(series_ids).await
    }

    async fn inverted_index(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.inverted_index(terms).await
    }

    async fn all_inverted_index(
        &self,
        bucket: &TimeBucket,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        // TODO: this should be internal error
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.all_inverted_index().await
    }

    async fn label_values(&self, bucket: &TimeBucket, label_name: &str) -> Result<Vec<String>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.label_values(label_name).await
    }

    async fn samples(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
        metric_name: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<crate::model::SeriesData> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.samples(series_id, metric_name, start_ms, end_ms).await
    }

    async fn samples_many(
        &self,
        bucket: &TimeBucket,
        metric_name: &str,
        series_ids: &[SeriesId],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<crate::model::SeriesData>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.samples_many(metric_name, series_ids, start_ms, end_ms)
            .await
    }

    async fn forward_index_one(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> Result<Option<crate::index::SeriesSpec>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.forward_index_one(series_id).await
    }

    async fn forward_index_many(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Vec<Option<crate::index::SeriesSpec>>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.forward_index_many(series_ids).await
    }

    async fn label_postings(
        &self,
        bucket: &TimeBucket,
        label_name: &str,
    ) -> Result<Vec<(String, roaring::RoaringBitmap)>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.label_postings(label_name).await
    }

    async fn inverted_index_term(
        &self,
        bucket: &TimeBucket,
        term: &Label,
    ) -> Result<Option<roaring::RoaringBitmap>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.inverted_index_term(term).await
    }
}
