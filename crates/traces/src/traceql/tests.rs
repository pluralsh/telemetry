use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};

use super::*;
use crate::{AttributeValue, Trace, TraceId};

fn value(value: any_value::Value) -> Option<AnyValue> {
    Some(AnyValue { value: Some(value) })
}

fn attribute(name: &str, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: name.to_owned(),
        value: self::value(value),
    }
}

fn test_trace() -> Trace {
    let trace_id = TraceId::new([0x11; 16]).unwrap();
    let make_span = |id: u8,
                     parent: Vec<u8>,
                     name: &str,
                     start: u64,
                     attributes: Vec<KeyValue>,
                     kind: i32,
                     status: i32| Span {
        trace_id: trace_id.as_bytes().to_vec(),
        span_id: vec![id; 8],
        parent_span_id: parent,
        name: name.to_owned(),
        kind,
        start_time_unix_nano: start,
        end_time_unix_nano: start + 100,
        attributes,
        status: Some(Status {
            message: if status == 2 {
                "failed".to_owned()
            } else {
                String::new()
            },
            code: status,
        }),
        ..Default::default()
    };
    Trace::new(
        trace_id,
        vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    attribute(
                        "service.name",
                        any_value::Value::StringValue("api".to_owned()),
                    ),
                    attribute("region", any_value::Value::StringValue("west".to_owned())),
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "grpc".to_owned(),
                    version: "1.2".to_owned(),
                    attributes: vec![attribute(
                        "lib",
                        any_value::Value::StringValue("tonic".to_owned()),
                    )],
                    ..Default::default()
                }),
                spans: vec![
                    make_span(
                        1,
                        Vec::new(),
                        "root",
                        10,
                        vec![
                            attribute("http.status", any_value::Value::IntValue(200)),
                            attribute("typed", any_value::Value::StringValue("7".to_owned())),
                        ],
                        2,
                        1,
                    ),
                    make_span(
                        2,
                        vec![1; 8],
                        "db.query",
                        20,
                        vec![
                            attribute("db.system", any_value::Value::StringValue("pg".to_owned())),
                            attribute("latency", any_value::Value::DoubleValue(7.5)),
                            attribute("typed", any_value::Value::IntValue(7)),
                        ],
                        3,
                        2,
                    ),
                    make_span(
                        3,
                        vec![1; 8],
                        "cache",
                        30,
                        vec![attribute("hit", any_value::Value::BoolValue(true))],
                        1,
                        0,
                    ),
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    )
    .unwrap()
}

fn run(source: &str) -> Option<TraceQlResult> {
    let query = parse(source).unwrap();
    execute(&test_trace(), &query, 100).unwrap()
}

#[test]
fn parses_empty_filter() {
    assert!(parse("{}").is_ok());
}

#[test]
fn parses_string_static() {
    assert!(parse(r#"{ span.name = "root" }"#).is_ok());
}

#[test]
fn parses_bool_static() {
    assert!(parse("{ .hit = true }").is_ok());
}

#[test]
fn parses_integer_static() {
    assert!(parse("{ .code = 200 }").is_ok());
}

#[test]
fn parses_float_static() {
    assert!(parse("{ .ratio >= 1.5 }").is_ok());
}

#[test]
fn parses_duration_static() {
    assert!(parse("{ duration > 1h2m3.5s }").is_ok());
}

#[test]
fn parses_status_static() {
    assert!(parse("{ status = error }").is_ok());
}

#[test]
fn parses_kind_static() {
    assert!(parse("{ kind = client }").is_ok());
}

#[test]
fn parses_nil_static() {
    assert!(parse("{ .missing = nil }").is_ok());
}

#[test]
fn parses_quoted_attribute() {
    assert!(parse(r#"{ resource."service name" = "api" }"#).is_ok());
}

#[test]
fn arithmetic_precedes_comparison_and_logical() {
    let query = parse("{ .x + 2 * 3 > 6 && true }").unwrap();
    assert!(query.to_string().contains(".x + 2 * 3 > 6 && true"));
}

#[test]
fn unary_expressions_parse() {
    assert!(parse("{ !false && -1 < 0 }").is_ok());
}

#[test]
fn structural_operators_parse() {
    assert!(parse(r#"{ name = "db" } >> { name = "root" }"#).is_ok());
}

#[test]
fn structural_variants_parse() {
    for operator in [
        ">", "<", ">>", "<<", "~", "!>", "!<", "!>>", "!<<", "!~", "&>", "&<", "&>>", "&<<", "&~",
    ] {
        assert!(
            parse(&format!("{{ true }} {operator} {{ true }}")).is_ok(),
            "{operator}"
        );
    }
}

#[test]
fn pipeline_stages_parse() {
    assert!(parse("{} | by(.foo) | coalesce() | select(span:name, resource.region)").is_ok());
}

#[test]
fn aggregate_filters_parse() {
    for source in [
        "{} | count() >= 2",
        "{} | min(.latency) > 1.0",
        "{} | max(.latency) > 1",
        "{} | avg(.latency) > 1",
        "{} | sum(.latency) > 1",
    ] {
        assert!(parse(source).is_ok(), "{source}");
    }
}

#[test]
fn hints_parse() {
    let query = parse(r#"{} with(sample = true, job = "api")"#).unwrap();
    assert_eq!(query.hints.len(), 2);
}

#[test]
fn metrics_parse_but_execution_is_unsupported() {
    let query = parse("{} | rate()").unwrap();
    let error = execute(&test_trace(), &query, 100).unwrap_err();
    assert!(matches!(error, QueryError::Unsupported(_)));
}

#[test]
fn display_round_trips() {
    let query = parse(
        r#"{ resource."service name" = "api" && span:duration > 2ms } | by(span:name) with(foo = 1)"#,
    )
    .unwrap();
    let displayed = query.to_string();
    assert_eq!(parse(&displayed).unwrap().to_string(), displayed);
}

#[test]
fn invalid_syntax_has_source_span() {
    let error = parse("{ .x = }").unwrap_err();
    assert!(matches!(error, QueryError::Parse(ParseError { span, .. }) if span.end >= span.start));
}

#[test]
fn type_error_has_source_span() {
    let error = parse(r#"{ true + "x" }"#).unwrap_err();
    assert!(
        matches!(error, QueryError::Validation(ValidationError { span, .. }) if span.end > span.start)
    );
}

#[test]
fn invalid_regex_is_validation_error() {
    let error = parse(r#"{ name =~ "[" }"#).unwrap_err();
    assert!(matches!(error, QueryError::Validation(_)));
}

#[test]
fn select_rejects_arbitrary_expression() {
    assert!(parse("{} | select(.x + 1)").is_err());
}

#[test]
fn pushdown_extracts_positive_scoped_equalities() {
    let plan = plan(parse(r#"{ resource.region = "west" && span.ok = true }"#).unwrap()).unwrap();
    assert_eq!(plan.pushdown.len(), 2);
    assert_eq!(plan.pushdown[0].len(), 1);
    assert_eq!(plan.pushdown[0][0].field, IndexField::Resource);
    assert_eq!(
        plan.pushdown[1][0].test,
        IndexTest::Exact(AttributeValue::Bool(true))
    );
}

#[test]
fn pushdown_checks_both_scopes_for_unscoped_attributes() {
    let plan = plan(parse(r#"{ .region = "west" }"#).unwrap()).unwrap();
    let fields = plan.pushdown[0]
        .iter()
        .map(|predicate| predicate.field)
        .collect::<Vec<_>>();
    assert_eq!(fields, vec![IndexField::Span, IndexField::Resource]);
}

#[test]
fn pushdown_numeric_equality_covers_both_numeric_types() {
    let values = |source: &str| {
        plan(parse(source).unwrap()).unwrap().pushdown[0]
            .iter()
            .map(|predicate| match &predicate.test {
                IndexTest::Exact(value) => value.clone(),
                test => panic!("expected an exact lookup, got {test:?}"),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        values("{ span.code = 200 }"),
        vec![AttributeValue::Int(200), AttributeValue::Double(200.0)]
    );
    assert_eq!(
        values("{ span.ratio = 2.0 }"),
        vec![AttributeValue::Double(2.0), AttributeValue::Int(2)]
    );
    assert_eq!(
        values("{ span.ratio = 2.5 }"),
        vec![AttributeValue::Double(2.5)]
    );
    assert_eq!(values("{ span.zero = 0 }").len(), 3);
    let huge = plan(parse("{ span.huge = 18446744073709551616.0 }").unwrap()).unwrap();
    assert!(matches!(
        huge.pushdown[0][0].test,
        IndexTest::Compare(BinaryOp::Equal, StaticValue::Float(_))
    ));
}

fn single_test(source: &str) -> IndexTest {
    let planned = plan(parse(source).unwrap()).unwrap();
    assert_eq!(planned.pushdown.len(), 1, "{source}");
    assert_eq!(planned.pushdown[0].len(), 1, "{source}");
    planned.pushdown[0][0].test.clone()
}

#[test]
fn pushdown_scans_values_for_regex_ranges_and_existence() {
    for source in [
        r#"{ span.path =~ "/api/.*" }"#,
        r#"{ span.path !~ "/api/.*" }"#,
        "{ span.code >= 500 }",
        "{ 500 <= span.code }",
        "{ span.code != nil }",
        "{ span.code != 500 }",
    ] {
        assert!(
            matches!(single_test(source), IndexTest::Compare(..)),
            "{source}"
        );
    }
    let flipped = single_test("{ 500 < span.code }");
    assert_eq!(
        flipped,
        IndexTest::Compare(BinaryOp::Greater, StaticValue::Int(500))
    );
    let regex = plan(parse(r#"{ span.path =~ "/api/.*" }"#).unwrap()).unwrap();
    let predicate = &regex.pushdown[0][0];
    assert!(predicate.admits(&AttributeValue::String("/api/users".to_owned())));
    assert!(!predicate.admits(&AttributeValue::String("/health".to_owned())));
    assert!(!predicate.admits(&AttributeValue::Int(1)));
}

#[test]
fn pushdown_skips_predicates_that_match_missing_attributes() {
    for source in ["{ span.a = nil }", "{ instrumentation.a = 1 }"] {
        assert!(
            plan(parse(source).unwrap()).unwrap().pushdown.is_empty(),
            "{source}"
        );
    }
}

#[test]
fn pushdown_indexes_intrinsics() {
    let intrinsic = |source: &str| {
        let planned = plan(parse(source).unwrap()).unwrap();
        assert_eq!(planned.pushdown.len(), 1, "{source}");
        planned.pushdown[0]
            .iter()
            .map(|predicate| {
                assert_eq!(predicate.field, IndexField::Intrinsic);
                (predicate.name.clone(), predicate.test.clone())
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        intrinsic(r#"{ name = "GET" }"#),
        [(
            "name".to_owned(),
            IndexTest::Exact(AttributeValue::String("GET".to_owned()))
        )]
    );
    assert_eq!(
        intrinsic("{ status = error }"),
        [(
            "status".to_owned(),
            IndexTest::Exact(AttributeValue::Int(2))
        )]
    );
    assert_eq!(
        intrinsic("{ status != error }"),
        [
            (
                "status".to_owned(),
                IndexTest::Exact(AttributeValue::Int(0))
            ),
            (
                "status".to_owned(),
                IndexTest::Exact(AttributeValue::Int(1))
            ),
        ]
    );
    assert_eq!(
        intrinsic("{ kind = server }"),
        [("kind".to_owned(), IndexTest::Exact(AttributeValue::Int(2)))]
    );
    assert_eq!(intrinsic("{ kind != server }").len(), 5);
    assert!(matches!(
        intrinsic(r#"{ name =~ "GET.*" }"#)[0].1,
        IndexTest::Compare(BinaryOp::Regex, _)
    ));
    assert!(matches!(
        intrinsic("{ duration > 2ms }")[0].1,
        IndexTest::Duration(BinaryOp::Greater, _)
    ));
}

#[test]
fn duration_buckets_admit_only_overlapping_ranges() {
    let planned = plan(parse("{ duration > 1000ns && duration <= 4000ns }").unwrap()).unwrap();
    let [greater, at_most] = &planned.pushdown[..] else {
        panic!("expected two clauses: {:?}", planned.pushdown);
    };
    let admits = |clause: &PushdownClause, nanoseconds: u64| {
        let bucket = i64::from(u64::BITS - nanoseconds.leading_zeros());
        clause[0].admits(&AttributeValue::Int(bucket))
    };
    assert!(!admits(greater, 511));
    assert!(admits(greater, 512));
    assert!(admits(greater, 1001));
    assert!(admits(greater, 1 << 40));
    assert!(admits(at_most, 0));
    assert!(admits(at_most, 4000));
    assert!(!admits(at_most, 8192));
}

#[test]
fn pushdown_unions_disjunction_sides() {
    let planned =
        plan(parse(r#"{ span.a = "x" || (span.b = "y" && span.c = "z") }"#).unwrap()).unwrap();
    assert_eq!(planned.pushdown.len(), 1);
    let names = planned.pushdown[0]
        .iter()
        .map(|predicate| predicate.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["a", "b"]);
    assert!(
        plan(parse(r#"{ span.a = 1 || span.b = nil }"#).unwrap())
            .unwrap()
            .pushdown
            .is_empty()
    );
}

#[test]
fn executes_resource_and_span_attributes() {
    let result = run(r#"{ resource.region = "west" && span."db.system" = "pg" }"#).unwrap();
    assert_eq!(result.matched_spans.len(), 1);
    assert_eq!(result.matched_spans[0].name, "db.query");
}

#[test]
fn execution_preserves_typed_distinction() {
    let string = run(r#"{ span.typed = "7" }"#).unwrap();
    let integer = run("{ span.typed = 7 }").unwrap();
    assert_eq!(string.matched_spans[0].name, "root");
    assert_eq!(integer.matched_spans[0].name, "db.query");
}

#[test]
fn comparisons_against_missing_attributes_only_match_nil() {
    assert_eq!(names("{ span.http.status != 500 }"), ["root"]);
    assert_eq!(names(r#"{ span.db.system !~ "my.*" }"#), ["db.query"]);
    assert_eq!(names("{ span.http.status = nil }"), ["cache", "db.query"]);
    assert_eq!(names("{ span.http.status != nil }"), ["root"]);
}

#[test]
fn executes_regex_and_logical_ops() {
    let result = run(r"{ name =~ `^db\.` || .hit = true }").unwrap();
    assert_eq!(result.matched_spans.len(), 2);
}

#[test]
fn executes_intrinsics() {
    let result = run("{ status = error && kind = client && duration = 100ns }").unwrap();
    assert_eq!(result.matched_spans[0].name, "db.query");
}

#[test]
fn executes_trace_intrinsics() {
    let result = run(
        r#"{ trace:rootService = "api" && trace:rootName = "root" && trace:duration = 120ns }"#,
    )
    .unwrap();
    assert_eq!(result.matched_spans.len(), 3);
}

fn names(source: &str) -> Vec<String> {
    let mut names = run(source)
        .map(|result| {
            result
                .matched_spans
                .into_iter()
                .map(|span| span.name)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    names.sort();
    names
}

#[test]
fn executes_child_relationship() {
    assert_eq!(
        names(r#"{ name = "root" } > { name = "db.query" }"#),
        ["db.query"]
    );
    assert!(names(r#"{ name = "db.query" } > { name = "root" }"#).is_empty());
}

#[test]
fn executes_descendant_relationship() {
    assert_eq!(
        names(r#"{ name = "root" } >> { name = "db.query" }"#),
        ["db.query"]
    );
    assert!(names(r#"{ name = "cache" } >> { name = "db.query" }"#).is_empty());
}

#[test]
fn executes_sibling_relationship() {
    assert_eq!(
        names(r#"{ name = "db.query" } ~ { name = "cache" }"#),
        ["cache"]
    );
    assert!(names(r#"{ name = "root" } ~ { name = "root" }"#).is_empty());
}

#[test]
fn executes_parent_and_ancestor_relationships() {
    assert_eq!(
        names(r#"{ name = "db.query" } < { name = "root" }"#),
        ["root"]
    );
    assert_eq!(names("{ } << { }"), ["root"]);
    assert_eq!(names(r#"{ } !>> { name = "root" }"#), ["root"]);
    assert_eq!(names("{ } !~ { }"), ["root"]);
    assert!(names(r#"{ name = "cache" } &>> { name = "db.query" }"#).is_empty());
}

#[test]
fn union_relationships_include_both_sides() {
    assert_eq!(
        names(r#"{ name = "root" } &> { name = "db.query" }"#),
        ["db.query", "root"]
    );
    assert_eq!(
        names(r#"{ name = "db.query" } &~ { }"#),
        ["cache", "db.query"]
    );
}

#[test]
fn executes_negated_relationship() {
    assert!(run(r#"{ name = "root" } !> { name = "cache" }"#).is_none());
    assert_eq!(
        names(r#"{ name = "cache" } !> { name = "db.query" }"#),
        ["db.query"]
    );
    assert_eq!(
        names(r#"{ name = "nope" } !< { }"#),
        ["cache", "db.query", "root"]
    );
}

#[test]
fn executes_spanset_filter_stage() {
    assert_eq!(names(r#"{ } | { name = "cache" }"#), ["cache"]);
    assert!(run(r#"{ name = "root" } | { name = "cache" }"#).is_none());
}

#[test]
fn leading_stage_implies_all_spans() {
    assert!(run("count() = 3").is_some());
}

#[test]
fn durations_compare_with_numbers() {
    assert_eq!(names("{ duration = 100 }"), ["cache", "db.query", "root"]);
    assert!(run("{ duration > 100.5 }").is_none());
    assert!(run("{ } | max(duration) > 1ns").is_some());
    assert!(run("{ } | max(duration) > 1h").is_none());
}

#[test]
fn executes_instrumentation_scope() {
    assert_eq!(
        names(r#"{ instrumentation:name = "grpc" && instrumentation:version = "1.2" }"#).len(),
        3
    );
    assert_eq!(names(r#"{ instrumentation.lib = "tonic" }"#).len(), 3);
    assert!(run(r#"{ instrumentation.lib = "other" }"#).is_none());
    assert!(
        plan(parse(r#"{ instrumentation.lib = "tonic" }"#).unwrap())
            .unwrap()
            .pushdown
            .is_empty()
    );
}

#[test]
fn executes_nested_set_intrinsics() {
    assert_eq!(names("{ nestedSetParent = -1 }"), ["root"]);
    assert_eq!(
        names("{ nestedSetLeft = 1 && nestedSetRight = 6 }"),
        ["root"]
    );
    assert_eq!(names("{ nestedSetParent = 1 }"), ["cache", "db.query"]);
    assert_eq!(
        names("{ nestedSetLeft = 2 && nestedSetRight = 3 }"),
        ["db.query"]
    );
}

#[test]
fn pushdown_covers_structural_operands_and_stage_filters() {
    let count = |source: &str| plan(parse(source).unwrap()).unwrap().pushdown.len();
    assert_eq!(count(r#"{ span.a = 1 } >> { span.b = 2 }"#), 2);
    assert_eq!(count(r#"{ span.a = 1 } &~ { span.b = 2 }"#), 2);
    assert_eq!(count(r#"{ span.a = 1 } !> { span.b = 2 }"#), 1);
    assert_eq!(count(r#"{ span.a = 1 } || { span.b = 2 }"#), 1);
    assert_eq!(count(r#"{ span.a = 1 } || { span.b = nil }"#), 0);
    assert_eq!(count(r#"{ } | count() > 1 | { span.b = 2 }"#), 1);
}

#[test]
fn executes_by_and_coalesce() {
    let grouped = run("{} | by(span:kind)").unwrap();
    assert_eq!(grouped.spanset_count, 3);
    let coalesced = run("{} | by(span:kind) | coalesce()").unwrap();
    assert_eq!(coalesced.spanset_count, 1);
}

#[test]
fn executes_select_projection() {
    let result = run(r#"{ name = "db.query" } | select(span:name, resource.region)"#).unwrap();
    assert_eq!(result.matched_spans[0].selected.len(), 2);
}

#[test]
fn executes_count_filter() {
    assert!(run("{} | count() = 3").is_some());
    assert!(run("{} | count() > 3").is_none());
}

#[test]
fn executes_numeric_aggregate_filters() {
    assert!(run("{ .latency != nil } | avg(.latency) > 7.0").is_some());
    assert!(run("{ .latency != nil } | sum(.latency) < 7.0").is_none());
}

#[test]
fn span_limit_is_explicit() {
    let query = parse("{}").unwrap();
    assert!(matches!(
        execute(&test_trace(), &query, 2),
        Err(QueryError::Limit(_))
    ));
}
