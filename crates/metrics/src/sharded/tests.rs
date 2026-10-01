use super::*;
use crate::routing::{route, split};
use crate::{Label, Sample};
use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig};
use sharding::{DEFAULT_IO_CONCURRENCY_LIMIT, DEFAULT_SHARDS};

const TEST_TIME_MS: i64 = 1_700_000_060_000;

async fn test_databases() -> (ShardedMetrics, TimeSeriesDb) {
    let config = |path: &str| Config {
        storage: SlateDbStorageConfig {
            path: path.to_string(),
            object_store: ObjectStoreConfig::InMemory,
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        },
        ..Default::default()
    };
    let sharded = ShardedMetrics::open_writers(
        config("sharded"),
        ShardingOptions::new(2, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap(),
        [ShardId::new(0), ShardId::new(1)],
    )
    .await
    .unwrap();
    let unsharded = TimeSeriesDb::open(config("unsharded")).await.unwrap();
    (sharded, unsharded)
}

fn assignment(shards: u32) -> ShardMap {
    ShardMap::new(
        sharding::AssignmentGeneration::new(1),
        shards,
        vec![sharding::Assignment::new(
            sharding::Owner::new("metrics-0", 0),
            sharding::ShardRange::within(0, shards, shards).unwrap(),
            sharding::AssignmentState::Active,
        )],
    )
    .unwrap()
}

fn scaled(previous: &ShardMap, shards: u32, cutover_ms: i64) -> ShardMap {
    let mut epochs = previous.epochs.clone();
    epochs.push(sharding::RoutingEpoch {
        effective_from_ns: cutover_ms * 1_000_000,
        routing: sharding::HashRangeMap::bootstrap(shards).unwrap(),
    });
    ShardMap::with_epochs(
        previous.generation.next(),
        epochs,
        vec![sharding::Assignment::new(
            sharding::Owner::new("metrics-0", 0),
            sharding::ShardRange::within(0, shards, shards).unwrap(),
            sharding::AssignmentState::Active,
        )],
    )
    .unwrap()
}

#[tokio::test]
async fn reader_reconciliation_opens_new_shards() {
    let directory = tempfile::tempdir().unwrap();
    let config = Config {
        storage: SlateDbStorageConfig {
            path: "reader-reconcile".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: directory.path().to_string_lossy().into_owned(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        },
        ..Default::default()
    };
    let options = ShardingOptions::new(1, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
    let writers =
        ShardedMetrics::open_writers(config.clone(), options, [ShardId::new(0), ShardId::new(1)])
            .await
            .unwrap();
    writers.close().await.unwrap();

    let readers = ShardedMetrics::open_readers(
        config,
        options,
        [ShardId::new(0)],
        DbReaderOptions::default(),
        17,
    )
    .await
    .unwrap();
    readers
        .shards()
        .reconcile(&scaled(&assignment(1), 2, TEST_TIME_MS))
        .await
        .unwrap();
    assert_eq!(
        readers.shards().ids().await,
        vec![ShardId::new(0), ShardId::new(1)]
    );
    readers.close().await.unwrap();
}

fn series_on_shard(
    db: &ShardedMetrics,
    metric: &str,
    shard: u32,
    extra_labels: &[(&str, &str)],
    samples: Vec<Sample>,
) -> Series {
    let routing = assignment(db.shards().options().shard_count());
    for candidate in 0..10_000 {
        let mut labels = extra_labels
            .iter()
            .map(|(name, value)| Label::new(*name, *value))
            .collect::<Vec<_>>();
        labels.push(Label::new("instance", format!("instance-{candidate}")));
        let series = Series::new(metric, labels, samples.clone());
        if route(
            &routing,
            &Namespace::new("global-query-regression").unwrap(),
            &series.labels,
            TEST_TIME_MS,
        )
        .get()
            == shard
        {
            return series;
        }
    }
    panic!("failed to find labels routed to shard {shard}");
}

fn binary_join_series(db: &ShardedMetrics) -> (Series, Series) {
    let routing = assignment(db.shards().options().shard_count());
    for candidate in 0..10_000 {
        let instance = format!("join-{candidate}");
        let left = Series::new(
            "left_metric",
            vec![Label::new("instance", &instance)],
            vec![Sample::new(TEST_TIME_MS, 2.0)],
        );
        let right = Series::new(
            "right_metric",
            vec![Label::new("instance", &instance)],
            vec![Sample::new(TEST_TIME_MS, 3.0)],
        );
        let namespace = Namespace::new("global-query-regression").unwrap();
        if route(&routing, &namespace, &left.labels, TEST_TIME_MS)
            != route(&routing, &namespace, &right.labels, TEST_TIME_MS)
        {
            return (left, right);
        }
    }
    panic!("failed to find binary operands routed to different shards");
}

fn normalized(value: QueryValue) -> Vec<(String, Vec<(i64, u64)>)> {
    let mut result = value
        .into_matrix()
        .into_iter()
        .map(|sample| {
            (
                format!("{:?}", sample.labels),
                sample
                    .samples
                    .into_iter()
                    .map(|(timestamp, value)| (timestamp, value.to_bits()))
                    .collect(),
            )
        })
        .collect::<Vec<_>>();
    result.sort();
    result
}

async fn assert_matches_unsharded(sharded: &ShardedMetrics, unsharded: &TimeSeriesDb, query: &str) {
    let time = SystemTime::UNIX_EPOCH + Duration::from_millis((TEST_TIME_MS + 1_000) as u64);
    let namespace = Namespace::new("global-query-regression").unwrap();
    let actual = sharded.query(&namespace, query, Some(time)).await.unwrap();
    let expected = unsharded
        .query(&namespace, query, Some(time))
        .await
        .unwrap();
    assert_eq!(normalized(actual), normalized(expected), "query: {query}");
}

async fn write_both(sharded: &ShardedMetrics, unsharded: &TimeSeriesDb, series: Vec<Series>) {
    let routing = assignment(sharded.shards().options().shard_count());
    sharded
        .write(
            &routing,
            &Namespace::new("global-query-regression").unwrap(),
            series.clone(),
            Visibility::Written,
        )
        .await
        .unwrap();
    unsharded
        .write_with_visibility(
            &Namespace::new("global-query-regression").unwrap(),
            series,
            Visibility::Written,
        )
        .await
        .unwrap();
}

#[test]
fn routing_is_canonical_and_namespace_sensitive() {
    let options = ShardingOptions::new(64, DEFAULT_IO_CONCURRENCY_LIMIT).unwrap();
    let routing = assignment(options.shard_count());
    let a = Namespace::new("a").unwrap();
    let b = Namespace::new("b").unwrap();
    let labels = vec![Label::new("z", "1"), Label::new("a", "2")];
    let reversed = labels.iter().cloned().rev().collect::<Vec<_>>();
    assert_eq!(
        route(&routing, &a, &labels, 0),
        route(&routing, &a, &reversed, 0)
    );
    assert_ne!(
        route(&routing, &a, &labels, 0),
        route(&routing, &b, &labels, 0)
    );
}

#[test]
fn shard_paths_are_stable() {
    assert_eq!(
        ShardingOptions::shard_path("metrics", ShardId::new(3)),
        "metrics/shard-0003"
    );
    assert!(ShardingOptions::new(0, DEFAULT_IO_CONCURRENCY_LIMIT).is_err());
    assert!(ShardingOptions::new(DEFAULT_SHARDS, 0).is_err());
}

#[test]
fn split_writes_samples_straddling_a_cutover_to_both_epochs() {
    let namespace = Namespace::new("global-query-regression").unwrap();
    let cutover = TEST_TIME_MS;
    let routing = scaled(&assignment(1), 4, cutover);
    let series = (0..100)
        .map(|index| {
            Series::new(
                "requests_total",
                vec![Label::new("instance", format!("instance-{index}"))],
                vec![
                    Sample::new(cutover - 1_000, 1.0),
                    Sample::new(cutover + 1_000, 2.0),
                ],
            )
        })
        .collect::<Vec<_>>();
    let grouped = split(&routing, &namespace, series);
    let before = grouped[&ShardId::new(0)]
        .iter()
        .flat_map(|series| &series.samples)
        .filter(|sample| sample.timestamp_ms < cutover)
        .count();
    assert_eq!(before, 100);
    let after = grouped
        .values()
        .flatten()
        .flat_map(|series| &series.samples)
        .filter(|sample| sample.timestamp_ms >= cutover)
        .count();
    assert_eq!(after, 100);
    assert!(grouped.len() > 2);
}

#[tokio::test]
async fn range_query_merges_a_series_split_across_epochs() {
    let (sharded, unsharded) = test_databases().await;
    let cutover = TEST_TIME_MS - 20_000;
    let routing = scaled(&assignment(1), 2, cutover);
    let namespace = Namespace::new("global-query-regression").unwrap();
    let series = (0..20)
        .map(|index| {
            Series::new(
                "requests_total",
                vec![Label::new("instance", format!("instance-{index}"))],
                vec![
                    Sample::new(TEST_TIME_MS - 50_000, 1.0),
                    Sample::new(TEST_TIME_MS - 10_000, 5.0),
                ],
            )
        })
        .collect::<Vec<_>>();
    assert!(split(&routing, &namespace, series.clone()).len() == 2);
    sharded
        .write(&routing, &namespace, series.clone(), Visibility::Written)
        .await
        .unwrap();
    unsharded
        .write_with_visibility(&namespace, series, Visibility::Written)
        .await
        .unwrap();
    assert_matches_unsharded(&sharded, &unsharded, "sum(rate(requests_total[1m]))").await;
    assert_matches_unsharded(&sharded, &unsharded, "count(requests_total)").await;
}

#[tokio::test]
async fn global_sum_matches_unsharded_database() {
    let (sharded, unsharded) = test_databases().await;
    let series = vec![
        series_on_shard(
            &sharded,
            "requests_total",
            0,
            &[],
            vec![Sample::new(TEST_TIME_MS, 2.0)],
        ),
        series_on_shard(
            &sharded,
            "requests_total",
            1,
            &[],
            vec![Sample::new(TEST_TIME_MS, 3.0)],
        ),
    ];
    write_both(&sharded, &unsharded, series).await;
    assert_matches_unsharded(&sharded, &unsharded, "sum(requests_total)").await;
}

#[tokio::test]
async fn grouped_aggregation_matches_unsharded_database() {
    let (sharded, unsharded) = test_databases().await;
    let series = vec![
        series_on_shard(
            &sharded,
            "requests_total",
            0,
            &[("region", "east")],
            vec![Sample::new(TEST_TIME_MS, 2.0)],
        ),
        series_on_shard(
            &sharded,
            "requests_total",
            1,
            &[("region", "east")],
            vec![Sample::new(TEST_TIME_MS, 3.0)],
        ),
        series_on_shard(
            &sharded,
            "requests_total",
            1,
            &[("region", "west")],
            vec![Sample::new(TEST_TIME_MS, 7.0)],
        ),
    ];
    write_both(&sharded, &unsharded, series).await;
    let start =
        SystemTime::UNIX_EPOCH + std::time::Duration::from_millis((TEST_TIME_MS - 1_000) as u64);
    let end =
        SystemTime::UNIX_EPOCH + std::time::Duration::from_millis((TEST_TIME_MS + 1_000) as u64);
    assert_eq!(
        sharded
            .label_values(
                &Namespace::new("global-query-regression").unwrap(),
                "region",
                None,
                start..=end,
            )
            .await
            .unwrap(),
        vec!["east", "west"]
    );
    assert_matches_unsharded(&sharded, &unsharded, "sum by (region) (requests_total)").await;
}

#[tokio::test]
async fn rate_then_sum_matches_unsharded_database() {
    let (sharded, unsharded) = test_databases().await;
    let samples = |start, end| {
        vec![
            Sample::new(TEST_TIME_MS - 50_000, start),
            Sample::new(TEST_TIME_MS - 10_000, end),
        ]
    };
    let series = vec![
        series_on_shard(&sharded, "requests_total", 0, &[], samples(1.0, 5.0)),
        series_on_shard(&sharded, "requests_total", 1, &[], samples(2.0, 10.0)),
    ];
    write_both(&sharded, &unsharded, series).await;
    assert_matches_unsharded(&sharded, &unsharded, "sum(rate(requests_total[1m]))").await;
}

#[tokio::test]
async fn binary_join_across_shards_matches_unsharded_database() {
    let (sharded, unsharded) = test_databases().await;
    let (left, right) = binary_join_series(&sharded);
    write_both(&sharded, &unsharded, vec![left, right]).await;
    assert_matches_unsharded(
        &sharded,
        &unsharded,
        "left_metric * on(instance) right_metric",
    )
    .await;
}
