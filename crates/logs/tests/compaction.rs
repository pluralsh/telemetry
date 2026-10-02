use std::time::Duration;

use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig, StorageConfig};
use plural_logs::{
    CompactionConfig, Config, Direction, Label, Labels, LogBatch, LogDb, LogEntry, Namespace,
    PageConfig, QueryOptions, QueryRequest,
};

const S: i64 = 1_000_000_000;

fn config(path: &str, enabled: bool) -> Config {
    Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: path.to_owned(),
            object_store: ObjectStoreConfig::InMemory,
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        }),
        segment_duration: Duration::from_secs(10),
        discovery_rollup: Some(Duration::from_secs(20)),
        retention: None,
        write_buffer: Default::default(),
        page: PageConfig {
            target_size_bytes: 64 * 1024,
            max_rows: 1024,
            rows_per_block: 8,
        },
        compaction: CompactionConfig {
            enabled,
            ..CompactionConfig::default()
        },
    }
}

fn labels(app: &str) -> Labels {
    Labels::new(vec![Label::new("app", app), Label::new("env", "prod")]).unwrap()
}

/// Many one-row flushes across streams and three settled segments, with
/// shared timestamps and a late write to the first segment.
fn workload() -> Vec<(i64, &'static str, String)> {
    let mut writes = Vec::new();
    for index in 0..60i64 {
        let app = ["api", "worker", "db"][index as usize % 3];
        // Pairs of writes share a timestamp.
        let timestamp = (index / 2) * S / 2 + S;
        let line = if index % 4 == 0 {
            format!("level=error value={index} needle needle")
        } else {
            format!("level=info value={index} needle haystack {index}")
        };
        writes.push((timestamp, app, line));
    }
    writes.push((2 * S, "api", "level=error value=99 late needle".to_owned()));
    writes
}

async fn database(path: &str, enabled: bool) -> (LogDb, Namespace) {
    let db = LogDb::open(config(path, enabled)).await.unwrap();
    let namespace = Namespace::new("tenant").unwrap();
    for (timestamp, app, line) in workload() {
        db.write(
            &namespace,
            vec![LogBatch::new(
                labels(app),
                vec![LogEntry::new(timestamp, line)],
            )],
        )
        .await
        .unwrap();
    }
    (db, namespace)
}

fn options(limit: usize, direction: Direction) -> QueryOptions {
    QueryOptions {
        limit,
        direction,
        ..QueryOptions::default()
    }
}

#[tokio::test]
async fn compacted_pages_answer_queries_like_written_pages() {
    let (plain, namespace) = database("compaction-off", false).await;
    let (compacted, _) = database("compaction-on", true).await;

    let range = |query: &str| QueryRequest::range(query, 0, 40 * S, S);
    let cases = [
        (range(r#"{env="prod"}"#), options(1000, Direction::Forward)),
        (range(r#"{env="prod"}"#), options(7, Direction::Forward)),
        (range(r#"{app="api"}"#), options(5, Direction::Backward)),
        (
            range(r#"{env="prod"} |= "error""#),
            options(1000, Direction::Forward),
        ),
        (
            range(r#"{env="prod"} | match "needle""#),
            options(1000, Direction::Forward),
        ),
        (
            range(r#"{app=~"api|db"} | match "needle""#),
            options(4, Direction::Forward),
        ),
        (
            range(r#"sum by (app) (count_over_time({env="prod"}[5s]))"#),
            options(1000, Direction::Forward),
        ),
        (
            QueryRequest::instant(
                r#"sum(sum_over_time({env="prod"} | logfmt | unwrap value [40s]))"#,
                40 * S,
            ),
            options(1000, Direction::Forward),
        ),
    ];
    for (request, options) in cases {
        let expected = plain
            .query(&namespace, &request, options.clone())
            .await
            .unwrap();
        let actual = compacted
            .query(&namespace, &request, options)
            .await
            .unwrap();
        assert_eq!(actual, expected, "{}", request.query);
    }

    // Written pages cost one read each; compacted ones fit a small budget.
    let bounded = QueryOptions {
        max_pages: 30,
        ..options(1000, Direction::Forward)
    };
    let request = range(r#"{env="prod"}"#);
    let error = plain
        .query(&namespace, &request, bounded.clone())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("max_pages"));
    compacted
        .query(&namespace, &request, bounded)
        .await
        .unwrap();

    plain.close().await.unwrap();
    compacted.close().await.unwrap();
}
