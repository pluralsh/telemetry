use line::logql::{
    BinaryOp, Expr, FilterValue, LabelFilterExpr, LineFilterOp, ParserStage, PipelineStage,
    QueryError, ValidationOptions, parse, parse_syntax, parse_with_options,
};
use proptest::prelude::*;

fn round_trip(source: &str) {
    let first = parse(source).unwrap_or_else(|error| panic!("{source}: {error}"));
    let canonical = first.to_string();
    let second =
        parse(&canonical).unwrap_or_else(|error| panic!("{canonical} (from {source}): {error}"));
    assert_eq!(canonical, second.to_string());
}

#[test]
fn parses_selectors_line_filters_and_match() {
    let query = parse(
        r#"{app="api",env=~"prod|stage"} |= "error" or ip("10.0.0.0/8") !~ `timeout` | match "status >= 500""#,
    )
    .unwrap();
    let Expr::Log(ref log) = query.value else {
        panic!("expected log query");
    };
    assert_eq!(log.selector.value.matchers.len(), 2);
    assert_eq!(log.stages.len(), 3);
    assert!(matches!(log.stages[2].value, PipelineStage::Match(_)));
}

#[test]
fn parses_all_parser_and_format_stages() {
    round_trip(
        r#"{app="api"} | json method="request.method", code="status" | logfmt --strict --keep-empty user="user" | regexp "(?P<id>[0-9]+)" | pattern "<method> <path>" | unpack | line_format "{{.method}} {{.path}}" | label_format route="{{.path}}", service=app | drop debug, trace_id=~".*" | keep app, route | decolorize"#,
    );
}

#[test]
fn parses_label_filter_value_kinds_and_precedence() {
    let query = parse_syntax(
        r#"{app="api"} | status >= 500 and size > 10MiB or latency < 2s and peer = ip("10.0.0.1")"#,
    )
    .unwrap();
    let Expr::Log(ref log) = query.value else {
        panic!("expected log query");
    };
    let PipelineStage::LabelFilter(filter) = &log.stages[0].value else {
        panic!("expected label filter");
    };
    assert!(matches!(filter.value, LabelFilterExpr::Or(_, _)));

    struct Values(Vec<&'static str>);
    impl line::logql::Visitor for Values {
        fn visit_label_filter(&mut self, filter: &line::logql::Spanned<LabelFilterExpr>) {
            if let LabelFilterExpr::Predicate(predicate) = &filter.value {
                self.0.push(match predicate.value {
                    FilterValue::Number(_) => "number",
                    FilterValue::Bytes(_) => "bytes",
                    FilterValue::Duration(_) => "duration",
                    FilterValue::Ip(_) => "ip",
                    _ => "other",
                });
            }
        }
    }
    let mut values = Values(Vec::new());
    line::logql::walk(&mut values, &query);
    assert_eq!(values.0, ["number", "bytes", "duration", "ip"]);
}

#[test]
fn parses_unwrap_conversions_ranges_and_offsets() {
    for conversion in ["bytes", "duration", "duration_seconds"] {
        round_trip(&format!(
            r#"avg_over_time({{app="api"}} | unwrap {conversion}(latency) | __error__ = "" [5m] offset 1m) by (app)"#
        ));
    }
    round_trip(r#"rate({app="api"}[1m30s])"#);
}

#[test]
fn parses_every_range_aggregation() {
    let without_unwrap = [
        "count_over_time",
        "rate",
        "bytes_over_time",
        "bytes_rate",
        "absent_over_time",
    ];
    for operation in without_unwrap {
        round_trip(&format!(r#"{operation}({{app="api"}}[5m])"#));
    }
    let with_unwrap = [
        "rate_counter",
        "avg_over_time",
        "sum_over_time",
        "min_over_time",
        "max_over_time",
        "stddev_over_time",
        "stdvar_over_time",
        "first_over_time",
        "last_over_time",
    ];
    for operation in with_unwrap {
        round_trip(&format!(
            r#"{operation}({{app="api"}} | unwrap value [5m])"#
        ));
    }
    round_trip(r#"quantile_over_time(0.95, {app="api"} | unwrap latency [5m])"#);
}

#[test]
fn parses_every_vector_aggregation() {
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
        round_trip(&format!(
            r#"{operation} by (app) (rate({{app="api"}}[5m]))"#
        ));
    }
    for operation in ["topk", "bottomk", "approx_topk"] {
        round_trip(&format!(r#"{operation}(3, rate({{app="api"}}[5m]))"#));
    }
}

#[test]
fn parses_literals_functions_binary_precedence_and_modifiers() {
    let query = parse_syntax("1 + 2 * 3 ^ 4").unwrap();
    let Expr::Binary { op, rhs, .. } = &query.value else {
        panic!("expected binary expression");
    };
    assert_eq!(*op, BinaryOp::Add);
    assert!(matches!(
        rhs.value,
        Expr::Binary {
            op: BinaryOp::Mul,
            ..
        }
    ));

    round_trip(r#"vector(1) > bool 2"#);
    round_trip(r#"label_replace(rate({app="api"}[5m]), "service", "$1", "app", "(.*)")"#);
    round_trip(
        r#"sum(rate({app="api"}[5m])) / on (app) group_left (zone) count(rate({app="api"}[5m]))"#,
    );
    round_trip(r#"rate({app="api"}[5m]) unless ignoring (instance) rate({app="worker"}[5m])"#);
}

#[test]
fn reports_precise_syntax_and_validation_spans() {
    let error = parse_syntax(r#"{app="api"} | json foo="#).unwrap_err();
    assert_eq!(error.span.start, error.span.end);
    assert_eq!(error.span.start, r#"{app="api"} | json foo="#.len());

    let error = parse(r#"{app=~".*"}"#).unwrap_err();
    let QueryError::Validation(error) = error else {
        panic!("expected validation error");
    };
    assert_eq!(error.span.start, 0);
    assert!(error.message.contains("empty value"));

    let error = parse(r#"{bad.name="value"}"#).unwrap_err();
    assert!(error.to_string().contains("invalid label name"));

    assert!(parse(r#"{app!="api"}"#).is_err());
    assert!(parse(r#"{app!=""}"#).is_ok());
    assert!(parse(r#"{app!~".+"}"#).is_err());
    assert!(parse(r#"{app!~".*"}"#).is_ok());
    assert!(parse(r#"{app="api", app!="worker"}"#).is_ok());
}

#[test]
fn enforces_query_limits_and_depth() {
    let options = ValidationOptions {
        max_query_bytes: 5,
        max_depth: 64,
    };
    assert!(parse_with_options(r#"{app="api"}"#, options).is_err());

    let options = ValidationOptions {
        max_query_bytes: 1024,
        max_depth: 2,
    };
    assert!(parse_with_options("1 + (2 + 3)", options).is_err());
}

#[test]
fn parses_pattern_filters_with_loki_or_semantics() {
    let query = parse(r#"{app="api"} |> "GET <_>" or "POST" !> "health""#).unwrap();
    let Expr::Log(log) = query.value else {
        panic!("expected log query");
    };
    let PipelineStage::LineFilter(first) = &log.stages[0].value else {
        panic!("expected pattern filter");
    };
    assert_eq!(first.branches[0].op, LineFilterOp::Pattern);
    assert_eq!(first.branches[1].op, LineFilterOp::Pattern);
    let PipelineStage::LineFilter(second) = &log.stages[1].value else {
        panic!("expected negative pattern filter");
    };
    assert_eq!(second.branches[0].op, LineFilterOp::NotPattern);
}

#[test]
fn parses_label_aggregation_and_enforces_constraints() {
    round_trip(
        r#"approx_count_distinct(mac, {app="api"} | json [1h] offset 5m) by (version, region)"#,
    );
    assert!(parse(r#"approx_count_distinct(mac, {app="api"}[1h]) without (version)"#).is_err());
    assert!(parse(r#"approx_count_distinct(mac, {app="api"}[1h]) by (mac)"#).is_err());
    assert!(parse(r#"approx_count_distinct(mac, {app="api"} | unwrap mac [1h])"#).is_err());
}

#[test]
fn supports_comments_signed_and_hex_float_literals() {
    let query = parse_syntax(
        r#"
            # top-level expression
            0x1.8p1 + -2.5e-1 # trailing comment
        "#,
    )
    .unwrap();
    assert_eq!(query.to_string(), "3 + -0.25");
    round_trip(
        r#"{app="api"}
           # |= "disabled"
           | json"#,
    );
}

#[test]
fn accepts_current_range_pipeline_and_unwrap_placements() {
    for query in [
        r#"rate(({app="api"} | json)[5m])"#,
        r#"rate(({app="api"}[5m]))"#,
        r#"rate({app="api"}[5m] | json)"#,
        r#"rate({app="api"}[5m] offset 1m | json)"#,
        r#"rate({app="api"}[5m] | unwrap value)"#,
        r#"rate(({app="api"} | json | unwrap value)[5m] offset 1m)"#,
    ] {
        round_trip(query);
    }
}

#[test]
fn parses_logfmt_flags_and_bare_extractions() {
    let query =
        parse(r#"{app="api"} | logfmt --strict --keep-empty msg, err, code="status""#).unwrap();
    let Expr::Log(log) = query.value else {
        panic!("expected log query");
    };
    let PipelineStage::Parser(ParserStage::Logfmt {
        strict,
        keep_empty,
        expressions,
    }) = &log.stages[0].value
    else {
        panic!("expected logfmt parser");
    };
    assert!(*strict && *keep_empty);
    assert_eq!(expressions.len(), 3);
    assert_eq!(expressions[0].label, expressions[0].expression);
    assert!(parse_syntax(r#"{app="api"} | logfmt --strict --strict"#).is_err());
}

#[test]
fn parses_implicit_and_comma_label_filter_conjunctions() {
    let query =
        parse_syntax(r#"{app="api"} | status >= 200 status < 300, method = "GET""#).unwrap();
    let Expr::Log(log) = query.value else {
        panic!("expected log query");
    };
    let PipelineStage::LabelFilter(filter) = &log.stages[0].value else {
        panic!("expected label filter");
    };
    assert!(matches!(filter.value, LabelFilterExpr::And(_, _)));
}

#[test]
fn rejects_invalid_binary_modifier_combinations() {
    assert!(parse(r#"vector(1) + bool vector(2)"#).is_err());
    assert!(
        parse(r#"rate({app="a"}[1m]) and on (app) group_left (zone) rate({app="b"}[1m])"#).is_err()
    );
    assert!(
        parse(r#"rate({app="a"}[1m]) + on (app) group_left (app) rate({app="b"}[1m])"#).is_err()
    );
}

#[test]
fn feature_matrix_corpus_parses() {
    let corpus = [
        ("selector", r#"{app="api", env=~"prod|stage"}"#),
        (
            "line-ip-or",
            r#"{app="api"} |= ip("10.0.0.0/8") or "local""#,
        ),
        ("pattern-filter", r#"{app="api"} |> "<_> /api/<_>""#),
        (
            "json-expressions",
            r#"{app="api"} | json method, code="status""#,
        ),
        ("logfmt-flags", r#"{app="api"} | logfmt --strict msg"#),
        (
            "regexp-parser",
            r#"{app="api"} | regexp "(?P<code>[0-9]+)""#,
        ),
        (
            "pattern-parser",
            r#"{app="api"} | pattern "<method> <path>""#,
        ),
        ("unpack", r#"{app="api"} | unpack"#),
        (
            "label-filter",
            r#"{app="api"} | size > 1MiB and latency < 2s"#,
        ),
        (
            "formats",
            r#"{app="api"} | line_format "{{.msg}}" | label_format service=app"#,
        ),
        ("drop-keep", r#"{app="api"} | drop debug | keep app"#),
        ("decolorize", r#"{app="api"} | decolorize"#),
        ("match", r#"{app="api"} | match "status >= 500""#),
        (
            "unwrap-range",
            r#"avg_over_time({app="api"} | unwrap duration(latency) [5m])"#,
        ),
        (
            "label-aggregation",
            r#"approx_count_distinct(user, {app="api"}[1h])"#,
        ),
        (
            "vector-aggregation",
            r#"sum by (app) (rate({app="api"}[5m]))"#,
        ),
        (
            "label-replace",
            r#"label_replace(rate({app="api"}[5m]), "x", "$1", "app", "(.*)")"#,
        ),
        ("binary-modifier", r#"vector(1) > bool on () vector(0)"#),
    ];
    for (category, query) in corpus {
        parse(query).unwrap_or_else(|error| panic!("{category}: {query}: {error}"));
    }
}

proptest! {
    #[test]
    fn parse_format_parse_is_stable(
        label in "[a-z][a-z0-9_]{0,12}",
        value in "[a-zA-Z0-9 _./-]{1,24}",
        needle in "[a-zA-Z0-9 _./-]{0,24}",
        seconds in 1u16..3600,
    ) {
        let value = serde_json::to_string(&value).unwrap();
        let needle = serde_json::to_string(&needle).unwrap();
        let source = format!(
            "rate({{{label}={value}}} |= {needle}[{seconds}s])"
        );
        let first = parse(&source).unwrap();
        let canonical = first.to_string();
        let second = parse(&canonical).unwrap();
        prop_assert_eq!(canonical, second.to_string());
    }
}
