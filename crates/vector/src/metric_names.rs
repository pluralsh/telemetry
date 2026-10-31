//! Metric name constants for the vector crate.
//!
//! All instrumentation flows through the `metrics-rs` facade. Emitted counters
//! are no-ops when no global recorder is installed, so embedded users opt in by
//! installing their own recorder. The vector server binary installs a
//! `PrometheusBuilder` recorder; see [`crate::server`] for the scrape endpoint.

/// Counter for upserted vectors observed by `VectorDb::write`.
pub const VECTOR_UPSERTS_TOTAL: &str = "vector_upserts_total";

/// Counter for deleted vector ids observed by `VectorDb::delete`.
pub const VECTOR_DELETES_TOTAL: &str = "vector_deletes_total";

/// Counter for writes (upserts and deletes) seen by the indexer at the start of
/// each `WriteVectors` op.
pub const INDEXER_WRITES_TOTAL: &str = "vector_indexer_writes_total";

/// Counter for centroids split by the indexer. Labeled by tree `level`
/// (255 for the root level).
pub const INDEXER_ANN_SPLITS_TOTAL: &str = "vector_indexer_ann_splits_total";

/// Counter for centroids merged by the indexer. Labeled by tree `level`.
pub const INDEXER_ANN_MERGES_TOTAL: &str = "vector_indexer_ann_merges_total";

/// Counter for vectors reassigned by the indexer. Labeled by tree `level`.
pub const INDEXER_ANN_REASSIGNS_TOTAL: &str = "vector_indexer_ann_reassigns_total";

/// Counter for the total number of candidate vectors scored during query
/// execution (posting-list entries over loaded centroids that pass the query
/// filter; filtered-out entries are skipped before scoring).
pub const QUERY_VECTORS_SCORED_TOTAL: &str = "vector_query_vectors_scored_total";

/// Histogram of end-to-end `search_with_options` latency, in seconds.
pub const QUERY_SEARCH_DURATION_SECONDS: &str = "vector_query_search_duration_seconds";

/// Counter for texts whose embedding was found in the embedding cache.
pub const EMBEDDING_CACHE_HITS_TOTAL: &str = "vector_embedding_cache_hits_total";

/// Counter for distinct texts sent to the embedding provider.
pub const EMBEDDING_CACHE_MISSES_TOTAL: &str = "vector_embedding_cache_misses_total";

/// Histogram of embedding provider request latency, in seconds.
pub const EMBEDDING_REQUEST_DURATION_SECONDS: &str = "vector_embedding_request_duration_seconds";

/// Counter for failed embedding provider requests.
pub const EMBEDDING_REQUEST_ERRORS_TOTAL: &str = "vector_embedding_request_errors_total";

/// Gauge of each memory budget's capacity, in bytes. Labeled by `budget`
/// (`query`, `write`, `compaction`).
pub const MEMORY_BUDGET_CAPACITY_BYTES: &str = "vector_memory_budget_capacity_bytes";

/// Gauge of the bytes each memory budget holds. Labeled by `budget`.
pub const MEMORY_BUDGET_USED_BYTES: &str = "vector_memory_budget_used_bytes";

/// Counter for acquisitions that waited for room: queries and writes that
/// found their budget full, and compactions deferred to a later poll.
/// Labeled by `budget`.
pub const MEMORY_BUDGET_WAITS_TOTAL: &str = "vector_memory_budget_waits_total";

/// Counter for writes rejected because the write budget stayed full.
/// Labeled by `budget`.
pub const MEMORY_BUDGET_REJECTIONS_TOTAL: &str = "vector_memory_budget_rejections_total";
