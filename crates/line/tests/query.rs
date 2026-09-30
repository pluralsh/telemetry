use std::time::Duration;

use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig, StorageConfig};
use line::{
    CompactionConfig, Config, Direction, Field, Fields, Label, Labels, LogBatch, LogDb, LogEntry,
    Namespace, PageConfig, QueryOptions, QueryRequest, QueryResult,
};

const S: i64 = 1_000_000_000;

fn config(path: &str) -> Config {
    Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: path.to_owned(),
            object_store: ObjectStoreConfig::InMemory,
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        }),
        segment_duration: Duration::from_secs(10),
        retention: None,
        write_buffer: Default::default(),
        page: PageConfig {
            target_size_bytes: 1024,
            max_rows: 2,
            rows_per_block: 1,
        },
        compaction: CompactionConfig {
            enabled: false,
            ..CompactionConfig::default()
        },
    }
}

fn labels(app: &str, env: &str) -> Labels {
    Labels::new(vec![Label::new("app", app), Label::new("env", env)]).unwrap()
}

async fn database(path: &str) -> (LogDb, Namespace) {
    let db = LogDb::open(config(path)).await.unwrap();
    let namespace = Namespace::new("tenant").unwrap();
    db.write(
        &namespace,
        vec![
            LogBatch::new(
                labels("api", "prod"),
                vec![
                    LogEntry::new(
                        S,
                        r#"{"method":"GET","status":200,"bytes":"1KiB","latency":"500ms","ip":"10.1.2.3"}"#,
                    ),
                    LogEntry::new(
                        2 * S,
                        "\u{1b}[31mlevel=error status=500 value=2 user=bob ip=192.168.1.2\u{1b}[0m",
                    ),
                    LogEntry::new(3 * S, "GET /orders/42 3"),
                    LogEntry::with_structured_metadata(
                        4 * S,
                        r#"{"_entry":"packed message","trace":"abc","value":4}"#,
                        Fields::new(vec![Field::new("source", "otlp")]).unwrap(),
                    ),
                ],
            ),
            LogBatch::new(
                labels("worker", "dev"),
                vec![
                    LogEntry::new(2 * S, "level=info value=10 user=alice"),
                    LogEntry::new(5 * S, "level=error value=20 user=alice"),
                ],
            ),
        ],
    )
    .await
    .unwrap();
    (db, namespace)
}

fn forward(limit: usize) -> QueryOptions {
    QueryOptions {
        limit,
        ..QueryOptions::default()
    }
}

#[tokio::test]
async fn selectors_filters_order_limits_and_match_fallback() {
    let (db, namespace) = database("query-selectors").await;
    let request = QueryRequest::range(
        r#"{app=~"api|worker",env!="dev"} | match "error status" != "missing""#,
        0,
        6 * S,
        S,
    );
    let result = db.query(&namespace, &request, forward(10)).await.unwrap();
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].entries.len(), 1);
    assert!(streams[0].entries[0].line.contains("status=500"));

    let mut options = forward(2);
    options.direction = Direction::Backward;
    let result = db
        .query(
            &namespace,
            &QueryRequest::range(r#"{app="api"}"#, 0, 6 * S, S),
            options,
        )
        .await
        .unwrap();
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    assert_eq!(
        streams[0]
            .entries
            .iter()
            .map(|entry| entry.timestamp_ns)
            .collect::<Vec<_>>(),
        vec![4 * S, 3 * S]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn indexed_match_ranks_scores_segments_and_repeats_exactly() {
    let db = LogDb::open(config("query-indexed-match")).await.unwrap();
    let namespace = Namespace::new("tenant").unwrap();
    db.write(
        &namespace,
        vec![LogBatch::new(
            labels("search", "prod"),
            vec![
                LogEntry::new(S, "needle"),
                LogEntry::new(2 * S, "needle needle needle"),
                LogEntry::new(3 * S, "needle needle needle"),
                LogEntry::new(11 * S, "needle in another segment"),
                LogEntry::new(12 * S, "unrelated"),
            ],
        )],
    )
    .await
    .unwrap();

    let request = QueryRequest::range(r#"{app="search"} | match "needle""#, 0, 20 * S, S);
    let first = db.query(&namespace, &request, forward(10)).await.unwrap();
    let second = db.query(&namespace, &request, forward(10)).await.unwrap();
    assert_eq!(first, second, "cold and repeated queries must agree");
    let QueryResult::Streams(streams) = first else {
        panic!("expected streams");
    };
    let entries = &streams[0].entries;
    assert_eq!(entries.len(), 4);
    let mut scored = Vec::new();
    for entry in entries {
        let score = entry
            .structured_metadata
            .iter()
            .find(|field| field.name == "__line_bm25_score")
            .expect("indexed direct matches carry a reserved score");
        let score = score.value.parse::<f32>().unwrap();
        assert!(score.is_finite());
        scored.push((entry.timestamp_ns, score));
    }
    assert!(scored.windows(2).all(|pair| pair[0].1 >= pair[1].1));
    let tied = scored
        .iter()
        .filter(|(timestamp, _)| matches!(*timestamp, value if value == 2 * S || value == 3 * S))
        .collect::<Vec<_>>();
    assert_eq!(tied.len(), 2);
    assert_eq!(tied[0].1.to_bits(), tied[1].1.to_bits());
    assert_eq!(tied[0].0, 2 * S, "score ties use deterministic row order");

    let metric = db
        .query(
            &namespace,
            &QueryRequest::instant(
                r#"count_over_time({app="search"} | match "needle" [20s])"#,
                20 * S,
            ),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Vector(metric) = metric else {
        panic!("expected vector");
    };
    assert_eq!(metric[0].sample.value, 4.0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn match_planner_falls_back_when_prior_stage_rewrites_source() {
    let db = LogDb::open(config("query-match-rewrite-fallback"))
        .await
        .unwrap();
    let namespace = Namespace::default();
    db.write(
        &namespace,
        vec![LogBatch::new(
            labels("fallback", "test"),
            vec![LogEntry::new(S, "original text")],
        )],
    )
    .await
    .unwrap();
    let result = db
        .query(
            &namespace,
            &QueryRequest::range(
                r#"{app="fallback"} | line_format "synthetic needle" | match "needle""#,
                0,
                2 * S,
                S,
            ),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    assert_eq!(streams[0].entries[0].line, "synthetic needle");
    assert!(
        streams[0].entries[0]
            .structured_metadata
            .iter()
            .all(|field| field.name != "__line_bm25_score")
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn parsers_formats_typed_filters_and_error_labels() {
    let (db, namespace) = database("query-pipelines").await;
    let cases = [
        (
            r#"{app="api"} | json | status >= 200 and bytes >= 1KiB and latency <= 1s and ip = ip("10.0.0.0/8") | line_format "{{.method}} {{.status}}""#,
            "GET 200",
        ),
        (
            r#"{app="api"} | logfmt | status = "500" | decolorize | label_format actor=user | keep app, actor"#,
            "level=error status=500 value=2 user=bob ip=192.168.1.2",
        ),
        (
            r#"{app="api"} | regexp "(?P<verb>GET) (?P<path>[^ ]+) (?P<value>[0-9]+)" | verb = "GET" | line_format "{{.path}}""#,
            "/orders/42",
        ),
        (
            r#"{app="api"} | pattern "<verb> <path> <value>" | path = "/orders/42""#,
            "GET /orders/42 3",
        ),
        (
            r#"{app="api"} | unpack | trace = "abc" | line_format "{{.trace}} {{.source}}""#,
            "abc otlp",
        ),
    ];
    for (query, expected) in cases {
        let result = db
            .query(
                &namespace,
                &QueryRequest::range(query, 0, 6 * S, S),
                forward(10),
            )
            .await
            .unwrap();
        let QueryResult::Streams(streams) = result else {
            panic!("expected streams for {query}");
        };
        assert_eq!(streams[0].entries[0].line, expected, "{query}");
    }

    let result = db
        .query(
            &namespace,
            &QueryRequest::range(r#"{app="api"} | json | __error__ != """#, 0, 6 * S, S),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    assert!(streams.iter().any(|stream| {
        stream
            .labels
            .iter()
            .any(|label| label.name == "__error__" && label.value == "JSONParserErr")
    }));
    db.close().await.unwrap();
}

#[tokio::test]
async fn range_functions_grouping_offsets_and_window_boundaries() {
    let (db, namespace) = database("query-range").await;
    let vector = db
        .query(
            &namespace,
            &QueryRequest::instant(r#"sum by (app) (count_over_time({app=~".+"}[2s]))"#, 3 * S),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Vector(vector) = vector else {
        panic!("expected vector");
    };
    let api = vector
        .iter()
        .find(|sample| {
            sample
                .labels
                .iter()
                .any(|label| label.name == "app" && label.value == "api")
        })
        .unwrap();
    // (1s, 3s] excludes the line exactly on the lower boundary.
    assert_eq!(api.sample.value, 2.0);

    let offset = db
        .query(
            &namespace,
            &QueryRequest::instant(r#"count_over_time({app="api"}[1s] offset 1s)"#, 4 * S),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Vector(offset) = offset else {
        panic!("expected vector");
    };
    assert_eq!(offset[0].sample.value, 1.0);

    let absent = db
        .query(
            &namespace,
            &QueryRequest::instant(r#"absent_over_time({app="missing"}[1s])"#, 4 * S),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Vector(absent) = absent else {
        panic!("expected vector");
    };
    assert_eq!(absent[0].sample.value, 1.0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn unwrap_aggregations_binary_modifiers_and_stepping() {
    let (db, namespace) = database("query-metrics").await;
    let result = db
        .query(
            &namespace,
            &QueryRequest::instant(
                r#"avg_over_time({app="api"} | logfmt | unwrap value [3s]) + 1"#,
                4 * S,
            ),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Vector(vector) = result else {
        panic!("expected vector");
    };
    assert_eq!(vector[0].sample.value, 3.0);

    let result = db
        .query(
            &namespace,
            &QueryRequest::instant(
                r#"sum by (env) (count_over_time({app=~".+"}[5s])) > bool on (env) vector(0)"#,
                5 * S,
            ),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Vector(vector) = result else {
        panic!("expected vector");
    };
    assert!(vector.iter().all(|sample| sample.sample.value == 1.0));

    let result = db
        .query(
            &namespace,
            &QueryRequest::range(r#"rate({app="api"}[2s])"#, 2 * S, 4 * S, S),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Matrix(matrix) = result else {
        panic!("expected matrix");
    };
    assert_eq!(matrix[0].samples.len(), 3);
    db.close().await.unwrap();
}

#[tokio::test]
async fn bounded_page_reads_fail_before_unbounded_io() {
    let (db, namespace) = database("query-page-bound").await;
    let error = db
        .query(
            &namespace,
            &QueryRequest::range(r#"{app="api"}"#, 0, 6 * S, S),
            QueryOptions {
                max_pages: 1,
                ..forward(10)
            },
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("max_pages"));
    db.close().await.unwrap();
}

#[tokio::test]
async fn log_queries_stop_reading_once_the_limit_survives_the_pipeline() {
    let db = LogDb::open(config("query-early-stop")).await.unwrap();
    let namespace = Namespace::new("tenant").unwrap();
    // One two-row page per ten-second segment.
    let lines = [
        (S, "keep"),
        (2 * S, "drop"),
        (11 * S, "keep"),
        (12 * S, "keep"),
        (21 * S, "drop"),
        (22 * S, "drop"),
        (31 * S, "keep"),
        (32 * S, "keep"),
    ];
    db.write(
        &namespace,
        vec![LogBatch::new(
            labels("api", "prod"),
            lines
                .iter()
                .map(|&(timestamp, line)| LogEntry::new(timestamp, line))
                .collect(),
        )],
    )
    .await
    .unwrap();
    let request = QueryRequest::range(r#"{app="api"} |= "keep""#, 0, 40 * S, S);
    let timestamps = |result: QueryResult| {
        let QueryResult::Streams(streams) = result else {
            panic!("expected streams");
        };
        streams
            .into_iter()
            .flat_map(|stream| stream.entries)
            .map(|entry| entry.timestamp_ns)
            .collect::<Vec<_>>()
    };

    let bounded = |limit, max_pages, direction| QueryOptions {
        limit,
        max_pages,
        direction,
        ..QueryOptions::default()
    };
    // The first segment yields one survivor, so the second must be read.
    let result = db
        .query(&namespace, &request, bounded(2, 2, Direction::Forward))
        .await
        .unwrap();
    assert_eq!(timestamps(result), vec![S, 11 * S]);
    let result = db
        .query(&namespace, &request, bounded(2, 1, Direction::Backward))
        .await
        .unwrap();
    assert_eq!(timestamps(result), vec![32 * S, 31 * S]);
    let error = db
        .query(&namespace, &request, bounded(10, 2, Direction::Forward))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("max_pages"));
    db.close().await.unwrap();
}

#[tokio::test]
async fn executes_every_metric_operator_category() {
    let (db, namespace) = database("query-operator-matrix").await;
    for operation in [
        "count_over_time",
        "rate",
        "bytes_over_time",
        "bytes_rate",
        "absent_over_time",
    ] {
        let query = format!(r#"{operation}({{app="api"}}[5s])"#);
        db.query(
            &namespace,
            &QueryRequest::instant(query, 5 * S),
            forward(10),
        )
        .await
        .unwrap();
    }
    for operation in [
        "rate_counter",
        "avg_over_time",
        "sum_over_time",
        "min_over_time",
        "max_over_time",
        "stddev_over_time",
        "stdvar_over_time",
        "first_over_time",
        "last_over_time",
    ] {
        let query = format!(r#"{operation}({{app=~".+"}} | logfmt | unwrap value [5s])"#);
        db.query(
            &namespace,
            &QueryRequest::instant(query, 5 * S),
            forward(10),
        )
        .await
        .unwrap();
    }
    db.query(
        &namespace,
        &QueryRequest::instant(
            r#"quantile_over_time(0.5, {app=~".+"} | logfmt | unwrap value [5s])"#,
            5 * S,
        ),
        forward(10),
    )
    .await
    .unwrap();
    db.query(
        &namespace,
        &QueryRequest::instant(
            r#"approx_count_distinct(user, {app=~".+"} | logfmt [5s]) by (env)"#,
            5 * S,
        ),
        forward(10),
    )
    .await
    .unwrap();

    let base = r#"count_over_time({app=~".+"}[5s])"#;
    for operation in [
        "sum",
        "avg",
        "min",
        "max",
        "count",
        "stddev",
        "stdvar",
        "sort",
        "sort_desc",
    ] {
        let query = format!("{operation} by (env) ({base})");
        db.query(
            &namespace,
            &QueryRequest::instant(query, 5 * S),
            forward(10),
        )
        .await
        .unwrap();
    }
    for operation in ["topk", "bottomk", "approx_topk"] {
        let query = format!("{operation}(1, {base})");
        db.query(
            &namespace,
            &QueryRequest::instant(query, 5 * S),
            forward(10),
        )
        .await
        .unwrap();
    }
    for operation in [
        "+", "-", "*", "/", "%", "^", "==", "!=", ">", ">=", "<", "<=",
    ] {
        let query = format!("{base} {operation} 2");
        db.query(
            &namespace,
            &QueryRequest::instant(query, 5 * S),
            forward(10),
        )
        .await
        .unwrap();
    }
    for operation in ["and", "or", "unless"] {
        let query = format!("{base} {operation} {base}");
        db.query(
            &namespace,
            &QueryRequest::instant(query, 5 * S),
            forward(10),
        )
        .await
        .unwrap();
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn uses_loki_log_bounds_and_instant_lookback() {
    let (db, namespace) = database("query-log-boundaries").await;
    let result = db
        .query(
            &namespace,
            &QueryRequest::range(r#"{app="api"}"#, S, 3 * S, S),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    assert_eq!(
        streams[0]
            .entries
            .iter()
            .map(|entry| entry.timestamp_ns)
            .collect::<Vec<_>>(),
        vec![S, 2 * S]
    );

    let request = QueryRequest::instant_logs(r#"{app=~".+"}"#, 31 * S);
    assert_eq!(request.start_ns, S);
    assert_eq!(request.end_ns, 31 * S);
    let result = db.query(&namespace, &request, forward(20)).await.unwrap();
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    assert_eq!(
        streams
            .iter()
            .map(|stream| stream.entries.len())
            .sum::<usize>(),
        6
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn supports_loki_template_functions_and_error_labels() {
    let (db, namespace) = database("query-template-conformance").await;
    let result = db
        .query(
            &namespace,
            &QueryRequest::range(
                r#"{app="worker"} | logfmt | user="alice" | line_format "{{if .level | contains \"error\"}}{{.user | upper | repeat 2}}{{else}}{{ __line__ | count \"=\" }}{{end}} {{.value | add 2}}""#,
                0,
                6 * S,
                S,
            ),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    let mut lines = streams
        .iter()
        .flat_map(|stream| stream.entries.iter().map(|entry| entry.line.as_str()))
        .collect::<Vec<_>>();
    lines.sort_unstable();
    assert_eq!(lines, vec!["3 12", "ALICEALICE 22"]);

    let result = db
        .query(
            &namespace,
            &QueryRequest::range(
                r#"{app="api"} | logfmt | label_format encoded="{{.user | b64enc}}", bad="{{.value | unknownFunction}}" | __error__ = "TemplateFormatErr""#,
                0,
                6 * S,
                S,
            ),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    assert!(streams.iter().any(|stream| {
        stream
            .labels
            .iter()
            .any(|label| label.name == "encoded" && label.value == "Ym9i")
    }));
    db.close().await.unwrap();
}

#[tokio::test]
async fn parses_nested_json_paths_and_logfmt_escapes() {
    let db = LogDb::open(config("query-parser-conformance"))
        .await
        .unwrap();
    let namespace = Namespace::default();
    db.write(
        &namespace,
        vec![LogBatch::new(
            labels("parser", "test"),
            vec![
                LogEntry::new(
                    S,
                    r#"{"pod":{"deployment":{"params":[{"field with space":"yes"}]}}}"#,
                ),
                LogEntry::new(2 * S, r#"msg="hello\nworld" empty= broken==== ok=value"#),
            ],
        )],
    )
    .await
    .unwrap();

    let result = db
        .query(
            &namespace,
            &QueryRequest::range(
                r#"{app="parser"} | json found="pod.deployment.params[0][\"field with space\"]", missing="pod.none" | found="yes" and missing="""#,
                0,
                3 * S,
                S,
            ),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    assert_eq!(streams[0].entries.len(), 1);

    let result = db
        .query(
            &namespace,
            &QueryRequest::range(
                r#"{app="parser"} | logfmt --strict --keep-empty | __error__ = "LogfmtParserErr""#,
                0,
                3 * S,
                S,
            ),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    assert!(streams.iter().any(|stream| {
        stream
            .labels
            .iter()
            .any(|label| label.name == "msg" && label.value == "hello\nworld")
    }));
    assert!(streams.iter().any(|stream| {
        stream
            .labels
            .iter()
            .any(|label| label.name == "empty" && label.value.is_empty())
    }));
    db.close().await.unwrap();
}

#[tokio::test]
async fn extrapolates_counter_resets_and_rejects_many_to_many_matches() {
    let (db, namespace) = database("query-rate-cardinality").await;
    db.write(
        &namespace,
        vec![
            LogBatch::new(
                labels("counter", "prod"),
                vec![
                    LogEntry::new(S, "value=10"),
                    LogEntry::new(2 * S, "value=20"),
                    LogEntry::new(3 * S, "value=5"),
                    LogEntry::new(4 * S, "value=15"),
                ],
            ),
            LogBatch::new(
                labels("frontend", "prod"),
                vec![LogEntry::new(4 * S, "request")],
            ),
        ],
    )
    .await
    .unwrap();
    let result = db
        .query(
            &namespace,
            &QueryRequest::instant(
                r#"rate_counter({app="counter"} | logfmt | unwrap value [5s])"#,
                5 * S,
            ),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Vector(vector) = result else {
        panic!("expected vector");
    };
    assert!(
        (vector[0].sample.value - 8.333_333_333).abs() < 1e-6,
        "actual rate was {}",
        vector[0].sample.value
    );

    let error = db
        .query(
            &namespace,
            &QueryRequest::instant(
                r#"count_over_time({env="prod"}[5s]) + on (env) count_over_time({env="prod"}[5s])"#,
                5 * S,
            ),
            forward(10),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("many-to-many matching not allowed")
    );
    db.close().await.unwrap();
}

#[test]
fn parser_exposes_only_current_loki_vector_operators() {
    assert!(line::logql::parse(r#"topk(1, count_over_time({app="api"}[1m]))"#).is_ok());
    assert!(line::logql::parse(r#"approx_topk(1, count_over_time({app="api"}[1m]))"#).is_ok());
    assert!(line::logql::parse(r#"limitk(1, count_over_time({app="api"}[1m]))"#).is_err());
    assert!(line::logql::parse(r#"limit_ratio(0.5, count_over_time({app="api"}[1m]))"#).is_err());
}
