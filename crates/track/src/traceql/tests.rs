use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};

use super::*;
use crate::{AttributeScope as IndexScope, AttributeValue, Trace, TraceId};

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
    assert_eq!(plan.pushdown[0].scope, IndexScope::Resource);
    assert_eq!(plan.pushdown[1].value, AttributeValue::Bool(true));
}

#[test]
fn pushdown_ignores_unscoped_attributes() {
    let plan = plan(parse(r#"{ .region = "west" }"#).unwrap()).unwrap();
    assert!(plan.pushdown.is_empty());
}

#[test]
fn pushdown_ignores_disjunction() {
    let plan = plan(parse(r#"{ span.a = 1 || span.b = 2 }"#).unwrap()).unwrap();
    assert!(plan.pushdown.is_empty());
}

#[test]
fn pushdown_ignores_negative_match() {
    let plan = plan(parse(r#"{ span.a != 1 }"#).unwrap()).unwrap();
    assert!(plan.pushdown.is_empty());
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

#[test]
fn executes_child_relationship() {
    let result = run(r#"{ name = "db.query" } > { name = "root" }"#).unwrap();
    assert_eq!(result.matched_spans.len(), 2);
}

#[test]
fn executes_descendant_relationship() {
    let result = run(r#"{ name = "db.query" } >> { name = "root" }"#).unwrap();
    assert_eq!(result.matched_spans.len(), 2);
}

#[test]
fn executes_sibling_relationship() {
    let result = run(r#"{ name = "db.query" } ~ { name = "cache" }"#).unwrap();
    assert_eq!(result.matched_spans.len(), 2);
}

#[test]
fn executes_negated_relationship() {
    let result = run(r#"{ name = "root" } !> { name = "cache" }"#).unwrap();
    assert_eq!(result.matched_spans[0].name, "root");
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
