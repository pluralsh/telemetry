use std::collections::BTreeMap;
use std::time::Duration;

use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig, StorageConfig};
use plural_logs::{
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
            disk: Default::default(),
        }),
        segment_duration: Duration::from_secs(10),
        discovery_rollup: Some(Duration::from_secs(20)),
        retention: None,
        write_buffer: Default::default(),
        block_cache_capacity_bytes: plural_logs::DEFAULT_BLOCK_CACHE_CAPACITY_BYTES,
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
                r#"{app="api"} | logfmt | label_format encoded="{{.user | b64enc}}", bad="{{div 1 0}}" | __error__ = "TemplateFormatErr""#,
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

    let error = db
        .query(
            &namespace,
            &QueryRequest::range(
                r#"{app="api"} | logfmt | label_format bad="{{.value | unknownFunction}}""#,
                0,
                6 * S,
                S,
            ),
            forward(10),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unknownFunction"), "{error}");
    db.close().await.unwrap();
}

#[tokio::test]
async fn renders_go_printf_and_jinja_templates() {
    let (db, namespace) = database("query-template-syntaxes").await;
    let lines = |result: QueryResult| {
        let QueryResult::Streams(streams) = result else {
            panic!("expected streams");
        };
        let mut lines = streams
            .iter()
            .flat_map(|stream| stream.entries.iter().map(|entry| entry.line.clone()))
            .collect::<Vec<_>>();
        lines.sort_unstable();
        lines
    };
    let query = |source: &'static str| {
        let db = &db;
        let namespace = &namespace;
        async move {
            db.query(
                namespace,
                &QueryRequest::range(source, 0, 6 * S, S),
                forward(10),
            )
            .await
            .unwrap()
        }
    };

    let result = query(
        r#"{app="worker"} | logfmt | line_format "{{ printf \"%-5s|%3d\" .level (int .value) }}""#,
    )
    .await;
    assert_eq!(lines(result), vec!["error| 20", "info | 10"]);

    let result = query(
        r#"{app="worker"} | logfmt | line_format jinja "{% if level == 'error' %}{{ user | upper }}{% else %}{{ value | add(1) }}{% endif %}""#,
    )
    .await;
    assert_eq!(lines(result), vec!["11", "ALICE"]);

    let result = query(
        r#"{app="worker"} | logfmt | label_format jinja tag="{{ level }}-{{ user | trunc(1) }}""#,
    )
    .await;
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    let mut tags = streams
        .iter()
        .flat_map(|stream| stream.labels.iter())
        .filter(|label| label.name == "tag")
        .map(|label| label.value.clone())
        .collect::<Vec<_>>();
    tags.sort_unstable();
    assert_eq!(tags, vec!["error-a", "info-a"]);
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

#[tokio::test]
async fn first_and_last_over_time_break_timestamp_ties_by_loki_stream_hash() {
    let db = LogDb::open(config("query-stream-hash-ties")).await.unwrap();
    let namespace = Namespace::new("tenant").unwrap();
    let stream = |app: &str, region: &str, pod: &str| {
        Labels::new(
            [
                ("app", app),
                ("env", "prod"),
                ("fuzz_run", "20261002T031128Z-af6a39d9"),
                ("job", "fuzz"),
                ("pod", pod),
                ("region", region),
            ]
            .into_iter()
            .map(|(name, value)| Label::new(name, value))
            .collect(),
        )
        .unwrap()
    };
    // From a fuzz run: Loki's labels hash puts billing-964 before checkout-105.
    db.write(
        &namespace,
        vec![
            LogBatch::new(
                stream("checkout", "us-east-1", "checkout-105"),
                vec![LogEntry::new(4 * S, r#"{"level":"warn","latency_ms":7.6}"#)],
            ),
            LogBatch::new(
                stream("billing", "us-west-2", "billing-964"),
                vec![LogEntry::new(
                    4 * S,
                    r#"{"level":"warn","latency_ms":76.0}"#,
                )],
            ),
        ],
    )
    .await
    .unwrap();
    for (operation, expected) in [("first_over_time", 76.0), ("last_over_time", 7.6)] {
        let query =
            format!(r#"{operation}({{job="fuzz"}} | json | unwrap latency_ms [10s]) by (level)"#);
        let result = db
            .query(
                &namespace,
                &QueryRequest::instant(&query, 5 * S),
                forward(10),
            )
            .await
            .unwrap();
        let QueryResult::Vector(vector) = result else {
            panic!("expected vector");
        };
        assert_eq!(vector.len(), 1, "{operation}");
        assert_eq!(vector[0].sample.value, expected, "{operation}");
    }
    db.close().await.unwrap();
}

#[test]
fn parser_exposes_only_current_loki_vector_operators() {
    assert!(plural_logs::logql::parse(r#"topk(1, count_over_time({app="api"}[1m]))"#).is_ok());
    assert!(
        plural_logs::logql::parse(r#"approx_topk(1, count_over_time({app="api"}[1m]))"#).is_ok()
    );
    assert!(plural_logs::logql::parse(r#"limitk(1, count_over_time({app="api"}[1m]))"#).is_err());
    assert!(
        plural_logs::logql::parse(r#"limit_ratio(0.5, count_over_time({app="api"}[1m]))"#).is_err()
    );
}

async fn fuzz_regression_database(path: &str) -> (LogDb, Namespace) {
    let db = LogDb::open(config(path)).await.unwrap();
    let namespace = Namespace::new("tenant").unwrap();
    db.write(
        &namespace,
        vec![
            LogBatch::new(
                Labels::new(vec![Label::new("app", "api")]).unwrap(),
                vec![
                    LogEntry::new(S, "needle"),
                    LogEntry::new(2 * S, "a needle"),
                    LogEntry::new(3 * S, "a needle b"),
                    LogEntry::new(4 * S, "needle b"),
                ],
            ),
            LogBatch::new(
                Labels::new(vec![Label::new("app", "api"), Label::new("tier", "gold")]).unwrap(),
                vec![LogEntry::new(S, "not json")],
            ),
        ],
    )
    .await
    .unwrap();
    db.flush().await.unwrap();
    (db, namespace)
}

fn stream_lines(result: QueryResult) -> Vec<String> {
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    streams
        .into_iter()
        .flat_map(|stream| stream.entries.into_iter().map(|entry| entry.line))
        .collect()
}

#[tokio::test]
async fn empty_equality_matcher_selects_streams_without_the_label() {
    let (db, namespace) = fuzz_regression_database("query-empty-matcher").await;
    let lines = stream_lines(
        db.query(
            &namespace,
            &QueryRequest::range(r#"{app="api", tier=""}"#, 0, 10 * S, S),
            forward(10),
        )
        .await
        .unwrap(),
    );
    assert_eq!(lines.len(), 4, "{lines:?}");
    assert!(!lines.contains(&"not json".to_owned()));

    let result = db
        .query(
            &namespace,
            &QueryRequest::instant(
                r#"absent_over_time({app="api", tier="", env="x"}[5s])"#,
                10 * S,
            ),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Vector(vector) = result else {
        panic!("expected vector");
    };
    let names = vector[0]
        .labels
        .iter()
        .map(|label| label.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["app", "env"]);

    // Another matcher on a label, even after its equality, drops it.
    let result = db
        .query(
            &namespace,
            &QueryRequest::instant(
                r#"absent_over_time({app="api", env=~"x.*", env="x", region="a", region="b"}[5s])"#,
                10 * S,
            ),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Vector(vector) = result else {
        panic!("expected vector");
    };
    let names = vector[0]
        .labels
        .iter()
        .map(|label| label.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["app"]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn pattern_line_filter_requires_non_empty_captures() {
    let (db, namespace) = fuzz_regression_database("query-pattern-filter").await;
    let matching = |query: &'static str| {
        let (db, namespace) = (&db, &namespace);
        async move {
            stream_lines(
                db.query(
                    namespace,
                    &QueryRequest::range(query, 0, 10 * S, S),
                    forward(10),
                )
                .await
                .unwrap(),
            )
        }
    };
    assert_eq!(
        matching(r#"{app="api", tier=""} |> "<_>needle<_>""#).await,
        ["a needle b"]
    );
    assert_eq!(
        matching(r#"{app="api", tier=""} |> "<_>needle""#).await,
        ["a needle"]
    );
    assert_eq!(
        matching(r#"{app="api", tier=""} |> "needle<_>""#).await,
        ["a needle b", "needle b"]
    );
    assert_eq!(
        matching(r#"{app="api", tier=""} !> "<_>needle<_>""#).await,
        ["needle", "a needle", "needle b"]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn absent_over_time_ignores_parser_errors() {
    let (db, namespace) = fuzz_regression_database("query-absent-errors").await;
    let result = db
        .query(
            &namespace,
            &QueryRequest::range(
                r#"absent_over_time({tier="gold"} | json [2s])"#,
                0,
                6 * S,
                S,
            ),
            forward(10),
        )
        .await
        .unwrap();
    let QueryResult::Matrix(matrix) = result else {
        panic!("expected matrix");
    };
    let present = matrix[0]
        .samples
        .iter()
        .map(|sample| sample.timestamp_ns / S)
        .collect::<Vec<_>>();
    assert_eq!(present, [0, 3, 4, 5, 6]);
    db.close().await.unwrap();
}

#[test]
fn frontend_alignment_rounds_metric_range_queries_out_to_the_step() {
    let aligned = QueryRequest::range(
        r#"count_over_time({app="api"}[1m])"#,
        61 * S,
        119 * S,
        60 * S,
    )
    .frontend_step_aligned();
    assert_eq!((aligned.start_ns, aligned.end_ns), (60 * S, 120 * S));

    let logs =
        QueryRequest::range(r#"{app="api"}"#, 61 * S, 119 * S, 60 * S).frontend_step_aligned();
    assert_eq!((logs.start_ns, logs.end_ns), (61 * S, 119 * S));
}

#[tokio::test]
async fn identical_entries_in_a_stream_are_deduplicated_like_loki() {
    let db = LogDb::open(config("query-dedupe")).await.unwrap();
    let namespace = Namespace::new("tenant").unwrap();
    let labels = || Labels::new(vec![Label::new("app", "api")]).unwrap();
    for _ in 0..2 {
        db.write(
            &namespace,
            vec![LogBatch::new(
                labels(),
                vec![
                    LogEntry::new(S, "status=200 a"),
                    LogEntry::new(S, "status=200 b"),
                    LogEntry::new(2 * S, "status=500 a"),
                ],
            )],
        )
        .await
        .unwrap();
        db.flush().await.unwrap();
    }

    let lines = |query: &'static str| {
        let (db, namespace) = (&db, &namespace);
        async move {
            stream_lines(
                db.query(
                    namespace,
                    &QueryRequest::range(query, 0, 10 * S, S),
                    forward(100),
                )
                .await
                .unwrap(),
            )
        }
    };
    assert_eq!(
        lines(r#"{app="api"}"#).await,
        ["status=200 a", "status=200 b", "status=500 a"]
    );
    assert_eq!(
        lines(r#"{app="api"} | regexp "status=(?P<status>\\d+)" | line_format "{{.status}}""#)
            .await,
        ["200", "500"]
    );

    let count = |query: &'static str| {
        let (db, namespace) = (&db, &namespace);
        async move {
            let result = db
                .query(
                    namespace,
                    &QueryRequest::instant(query, 10 * S),
                    forward(100),
                )
                .await
                .unwrap();
            let QueryResult::Vector(vector) = result else {
                panic!("expected vector");
            };
            vector[0].sample.value
        }
    };
    assert_eq!(
        count(r#"sum(count_over_time({app="api"}[10s]))"#).await,
        3.0
    );
    // Samples dedupe on the stored line, so formatting does not merge them.
    assert_eq!(
        count(r#"sum(count_over_time({app="api"} | line_format "x" [10s]))"#).await,
        3.0
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn parsers_are_skipped_when_samples_keep_no_labels() {
    let db = LogDb::open(config("query-parser-hints")).await.unwrap();
    let namespace = Namespace::new("tenant").unwrap();
    db.write(
        &namespace,
        vec![LogBatch::new(
            Labels::new(vec![Label::new("app", "api")]).unwrap(),
            vec![
                LogEntry::new(S, "status=200"),
                LogEntry::new(2 * S, r#"{"status":500}"#),
            ],
        )],
    )
    .await
    .unwrap();
    db.flush().await.unwrap();

    let query = |query: &'static str| {
        let (db, namespace) = (&db, &namespace);
        async move {
            db.query(
                namespace,
                &QueryRequest::instant(query, 10 * S),
                forward(100),
            )
            .await
        }
    };
    // Like Loki, a `sum` without grouping needs no labels, so the strict
    // parser never runs and never flags the JSON line.
    let Ok(QueryResult::Vector(vector)) =
        query(r#"sum(count_over_time({app="api"} | logfmt --strict [10s]))"#).await
    else {
        panic!("expected vector");
    };
    assert_eq!(vector[0].sample.value, 2.0);
    // Grouping or filtering on a label needs the parser, and its error.
    assert!(
        query(r#"sum by (status) (count_over_time({app="api"} | logfmt --strict [10s]))"#)
            .await
            .is_err()
    );
    assert!(
        query(r#"sum(count_over_time({app="api"} | logfmt --strict | status != "x" [10s]))"#)
            .await
            .is_err()
    );
    // Counts never see the line, so Loki drops a `line_format` no later
    // parser or line filter reads, and with it the reason to parse.
    let Ok(QueryResult::Vector(vector)) =
        query(r#"sum(count_over_time({app="api"} | json | line_format "{{.status}}" [10s]))"#)
            .await
    else {
        panic!("expected vector");
    };
    assert_eq!(vector[0].sample.value, 2.0);
    let Ok(QueryResult::Vector(vector)) = query(
        r#"sum(count_over_time({app="api"} | logfmt | line_format "s{{.status}}" |= "s200" [10s]))"#,
    )
    .await
    else {
        panic!("expected vector");
    };
    assert_eq!(vector[0].sample.value, 1.0);
    // Byte counts read the line, so the format stays.
    let Ok(QueryResult::Vector(vector)) =
        query(r#"sum(bytes_over_time({app="api"} | logfmt | line_format "{{.status}}" [10s]))"#)
            .await
    else {
        panic!("expected vector");
    };
    assert_eq!(vector[0].sample.value, 3.0);
    db.close().await.unwrap();
}

async fn per_line_series_database(path: &str) -> (LogDb, Namespace) {
    let db = LogDb::open(config(path)).await.unwrap();
    let namespace = Namespace::new("tenant").unwrap();
    let entries = |app: &str| {
        (1..=8i64)
            .map(|second| {
                let level = if second % 3 == 0 { "error" } else { "info" };
                LogEntry::new(
                    second * S,
                    format!("level={level} user={app}{second} bytes={}", second * 10),
                )
            })
            .collect()
    };
    db.write(
        &namespace,
        vec![
            LogBatch::new(labels("api", "prod"), entries("a")),
            LogBatch::new(labels("worker", "prod"), entries("w")),
        ],
    )
    .await
    .unwrap();
    db.flush().await.unwrap();
    (db, namespace)
}

async fn matrix(db: &LogDb, namespace: &Namespace, query: &str) -> Vec<plural_logs::MatrixSeries> {
    let result = db
        .query(
            namespace,
            &QueryRequest::range(query, 0, 10 * S, S),
            QueryOptions::default(),
        )
        .await
        .unwrap();
    let QueryResult::Matrix(matrix) = result else {
        panic!("expected matrix for {query}");
    };
    matrix
}

#[tokio::test]
async fn sum_grouping_pushed_into_range_aggregations_keeps_results() {
    let (db, namespace) = per_line_series_database("query-sum-pushdown").await;
    // `* 1` keeps the range aggregation from being the sum's direct operand,
    // so it is evaluated with a series per parsed label set.
    for (pushed, per_line) in [
        (
            r#"sum by (level) (count_over_time({env="prod"} | logfmt [3s]))"#,
            r#"sum by (level) (count_over_time({env="prod"} | logfmt [3s]) * 1)"#,
        ),
        (
            r#"sum without (user, bytes) (rate({env="prod"} | logfmt [3s]))"#,
            r#"sum without (user, bytes) (rate({env="prod"} | logfmt [3s]) * 1)"#,
        ),
        (
            r#"sum(sum_over_time({env="prod"} | logfmt | unwrap bytes [4s]))"#,
            r#"sum(sum_over_time({env="prod"} | logfmt | unwrap bytes [4s]) * 1)"#,
        ),
        (
            r#"sum by (app) (bytes_over_time({env="prod"} | logfmt [2s]))"#,
            r#"sum by (app) (bytes_over_time({env="prod"} | logfmt [2s]) * 1)"#,
        ),
    ] {
        let expected = matrix(&db, &namespace, per_line).await;
        assert!(!expected.is_empty(), "{per_line}");
        assert_eq!(matrix(&db, &namespace, pushed).await, expected, "{pushed}");
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn binary_matching_holds_at_every_step() {
    let (db, namespace) = per_line_series_database("query-binary-steps").await;
    // Every parsed line is its own series on both sides.
    let ratio = matrix(
        &db,
        &namespace,
        r#"count_over_time({env="prod"} | logfmt [3s]) / count_over_time({env="prod"} | logfmt [3s])"#,
    )
    .await;
    assert_eq!(ratio.len(), 16);
    for series in &ratio {
        assert_eq!(series.samples.len(), 3, "{:?}", series.labels);
        assert!(series.samples.iter().all(|sample| sample.value == 1.0));
    }
    // Aggregated operands get fresh labels at every step.
    let difference = matrix(
        &db,
        &namespace,
        r#"sum by (level) (count_over_time({env="prod"} | logfmt [3s])) - on (level) sum by (level) (count_over_time({env="prod"} | logfmt [3s]) * 2)"#,
    )
    .await;
    let counts = matrix(
        &db,
        &namespace,
        r#"sum by (level) (count_over_time({env="prod"} | logfmt [3s]))"#,
    )
    .await;
    assert_eq!(difference.len(), 2);
    for (difference, count) in difference.iter().zip(&counts) {
        assert_eq!(difference.labels, count.labels);
        let negated = count
            .samples
            .iter()
            .map(|sample| (sample.timestamp_ns, -sample.value))
            .collect::<Vec<_>>();
        let actual = difference
            .samples
            .iter()
            .map(|sample| (sample.timestamp_ns, sample.value))
            .collect::<Vec<_>>();
        assert_eq!(actual, negated);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn pipelines_spread_across_batches_keep_results_and_order() {
    let db = LogDb::open(config("query-parallel-pipelines"))
        .await
        .unwrap();
    let namespace = Namespace::new("tenant").unwrap();
    let entries = || {
        (0..6000i64)
            .map(|index| {
                let level = if index % 3 == 0 { "error" } else { "info" };
                LogEntry::new(index * 1_000_000, format!("level={level} bytes={index}"))
            })
            .collect()
    };
    db.write(
        &namespace,
        vec![
            LogBatch::new(labels("api", "prod"), entries()),
            LogBatch::new(labels("worker", "prod"), entries()),
        ],
    )
    .await
    .unwrap();
    db.flush().await.unwrap();
    let vector = |query: &'static str| {
        let (db, namespace) = (&db, &namespace);
        async move {
            let result = db
                .query(
                    namespace,
                    &QueryRequest::instant(query, 7 * S),
                    QueryOptions::default(),
                )
                .await
                .unwrap();
            let QueryResult::Vector(samples) = result else {
                panic!("expected vector for {query}");
            };
            samples
                .into_iter()
                .map(|sample| {
                    let labels = sample
                        .labels
                        .iter()
                        .map(|label| format!("{}={}", label.name, label.value))
                        .collect::<Vec<_>>()
                        .join(",");
                    (labels, sample.sample.value)
                })
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(
        vector(r#"sum by (level) (count_over_time({env="prod"} | logfmt [10s]))"#).await,
        [
            ("level=error".to_owned(), 4000.0),
            ("level=info".to_owned(), 8000.0)
        ]
    );
    assert_eq!(
        vector(
            r#"sum(sum_over_time({env="prod"} | logfmt | level="error" | unwrap bytes [10s])) + sum(count_over_time({app="api"} |= "level=info" [10s]))"#
        )
        .await,
        [(String::new(), 11_998_000.0)]
    );
    let result = db
        .query(
            &namespace,
            &QueryRequest::range(r#"{app="api"} |= "level=error""#, 0, 7 * S, S),
            forward(3000),
        )
        .await
        .unwrap();
    let QueryResult::Streams(streams) = result else {
        panic!("expected streams");
    };
    let timestamps = streams[0]
        .entries
        .iter()
        .map(|entry| entry.timestamp_ns)
        .collect::<Vec<_>>();
    let errors = (0..2000i64)
        .map(|index| index * 3_000_000)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, errors);
    // Unindexed log queries stop reading at their limit; these read past the
    // rows run inline into batched ones.
    for (direction, limit, expected) in [
        (Direction::Forward, 1500, errors[..1500].to_vec()),
        (
            Direction::Backward,
            1500,
            errors[500..].iter().rev().copied().collect(),
        ),
        (Direction::Forward, 3000, errors.clone()),
    ] {
        let mut options = forward(limit);
        options.direction = direction;
        let result = db
            .query(
                &namespace,
                &QueryRequest::range(r#"{app="api"} | logfmt | level="error""#, 0, 7 * S, S),
                options,
            )
            .await
            .unwrap();
        let QueryResult::Streams(streams) = result else {
            panic!("expected streams");
        };
        // `bytes` differs per line, so each kept line is its own stream.
        let mut timestamps = streams
            .iter()
            .flat_map(|stream| stream.entries.iter().map(|entry| entry.timestamp_ns))
            .collect::<Vec<_>>();
        timestamps.sort_unstable();
        if direction == Direction::Backward {
            timestamps.reverse();
        }
        assert_eq!(timestamps, expected, "{direction:?} limit {limit}");
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn label_replace_rewrites_every_step_of_per_line_series() {
    let (db, namespace) = per_line_series_database("query-label-replace-steps").await;
    let base = r#"count_over_time({env="prod"} | logfmt [3s])"#;
    let counts = matrix(&db, &namespace, base).await;
    assert_eq!(counts.len(), 16);
    let rewritten = |series: &plural_logs::MatrixSeries, copied: Option<&str>| {
        let mut labels = series
            .labels
            .iter()
            .map(|label| (label.name.clone(), label.value.clone()))
            .collect::<BTreeMap<_, _>>();
        if let Some(copied) = copied {
            labels.insert("copied".to_owned(), copied.to_owned());
        }
        labels
    };
    let as_map = |series: &plural_logs::MatrixSeries| rewritten(series, None);
    for (query, copy) in [
        (
            format!(r#"label_replace({base}, "copied", "$1", "app", "(.*)")"#),
            Some("app"),
        ),
        (
            format!(r#"label_replace({base}, "copied", "x", "app", "nomatch")"#),
            None,
        ),
        (
            format!(
                r#"label_replace(label_replace({base}, "copied", "$1", "app", "(.*)"), "copied", "$1-again", "copied", "(.*)")"#
            ),
            Some("again"),
        ),
    ] {
        let actual = matrix(&db, &namespace, &query).await;
        assert_eq!(actual.len(), counts.len(), "{query}");
        let expected = counts
            .iter()
            .map(|series| {
                let app = series
                    .labels
                    .iter()
                    .find(|label| label.name == "app")
                    .map(|label| label.value.clone())
                    .unwrap();
                let copied = match copy {
                    Some("app") => Some(app),
                    Some(_) => Some(format!("{app}-again")),
                    None => None,
                };
                (rewritten(series, copied.as_deref()), series.samples.clone())
            })
            .collect::<BTreeMap<_, _>>();
        let actual = actual
            .iter()
            .map(|series| (as_map(series), series.samples.clone()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, expected, "{query}");
    }
    db.close().await.unwrap();
}

/// Times fine-stepped range aggregations over parsed, high-cardinality
/// rows, where nearly every row is its own series. Run with
/// `cargo test --release --test query -- --ignored --nocapture`.
#[tokio::test]
#[ignore]
async fn profile_fine_stepped_unwrap_ranges() {
    let mut config = config("query-profile-unwrap");
    config.segment_duration = Duration::from_secs(3600);
    config.discovery_rollup = None;
    config.page = PageConfig::default();
    let db = LogDb::open(config).await.unwrap();
    let namespace = Namespace::new("tenant").unwrap();
    let apps = ["api", "auth", "gateway", "worker"];
    for (index, app) in apps.iter().enumerate() {
        let entries = (0..1_500i64)
            .map(|row| {
                LogEntry::new(
                    (row * 4 + index as i64) * S / 5,
                    format!(
                        r#"{{"msg":"request {row}","user":"u{}","latency_ms":{},"ctx":{{"attempt":{}}}}}"#,
                        row % 97,
                        row % 503,
                        row % 5
                    ),
                )
            })
            .collect();
        db.write(
            &namespace,
            vec![LogBatch::new(labels(app, "prod"), entries)],
        )
        .await
        .unwrap();
    }
    for query in [
        r#"label_replace(sum by (app) (sum_over_time({env="prod"} | json | unwrap ctx_attempt [2m])), "copied", "$1", "app", "(.*)")"#,
        r#"sum(sum_over_time({env="prod"} | json | unwrap latency_ms | __error__="" [10m]))"#,
        r#"min_over_time({env="prod"} | json | unwrap latency_ms [5m])"#,
        r#"count_over_time({env="prod"} | json [5m])"#,
    ] {
        let started = std::time::Instant::now();
        let result = db
            .query(
                &namespace,
                &QueryRequest::range(query, 0, 1_200 * S, S),
                QueryOptions::default(),
            )
            .await
            .unwrap();
        let QueryResult::Matrix(matrix) = result else {
            panic!("expected matrix");
        };
        println!(
            "{:>8.1} ms  {:>5} series  {query}",
            started.elapsed().as_secs_f64() * 1e3,
            matrix.len()
        );
    }
    db.close().await.unwrap();
}
