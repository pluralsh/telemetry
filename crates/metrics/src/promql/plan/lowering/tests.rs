use super::*;

const START_MS: i64 = 1_700_000_000_000;
const END_MS: i64 = 1_700_000_060_000;
const STEP_MS: i64 = 1_000;
const LOOKBACK_MS: i64 = 5 * 60 * 1000;

fn ctx() -> LoweringContext {
    LoweringContext::new(START_MS, END_MS, STEP_MS, LOOKBACK_MS)
}

fn parse(input: &str) -> parser::Expr {
    parser::parse(input).unwrap_or_else(|e| panic!("parse({input:?}) failed: {e}"))
}

#[test]
fn should_lower_vector_selector() {
    // given: a bare vector selector
    let expr = parse("http_requests_total{job=\"api\"}");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: it lowers to VectorSelector carrying the parser struct + ctx lookback
    match plan {
        LogicalPlan::VectorSelector {
            selector,
            offset,
            at,
            lookback_ms,
        } => {
            assert_eq!(selector.name.as_deref(), Some("http_requests_total"));
            assert_eq!(offset, Offset::Pos(0));
            assert_eq!(at, None);
            assert_eq!(lookback_ms, Some(LOOKBACK_MS));
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_matrix_selector_with_range() {
    // given: a matrix selector with a 5m range
    let expr = parse("http_requests_total[5m]");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: it lowers to MatrixSelector with range_ms = 5 * 60_000
    match plan {
        LogicalPlan::MatrixSelector {
            selector,
            range_ms,
            offset,
            at,
        } => {
            assert_eq!(selector.name.as_deref(), Some("http_requests_total"));
            assert_eq!(range_ms, 5 * 60 * 1000);
            assert_eq!(offset, Offset::Pos(0));
            assert_eq!(at, None);
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_number_literal_to_scalar() {
    // given: a number literal
    let expr = parse("42");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: it lowers to Scalar(42.0)
    assert_eq!(plan, LogicalPlan::Scalar(42.0));
}

#[test]
fn should_lower_abs_as_instant_fn() {
    // given: `abs(x)`
    let expr = parse("abs(foo)");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: it becomes `InstantFn { Abs, VectorSelector(foo) }`
    match plan {
        LogicalPlan::InstantFn { kind, child } => {
            assert_eq!(kind, InstantFnKind::Abs);
            assert!(matches!(*child, LogicalPlan::VectorSelector { .. }));
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_pi_as_scalar_literal() {
    // given
    let expr = parse("pi()");

    // when
    let plan = lower(&expr, &ctx()).unwrap();

    // then
    assert_eq!(plan, LogicalPlan::Scalar(std::f64::consts::PI));
}

#[test]
fn should_lower_time_as_scalar_leaf() {
    // given
    let expr = parse("time()");

    // when
    let plan = lower(&expr, &ctx()).unwrap();

    // then
    assert_eq!(plan, LogicalPlan::Time);
}

#[test]
fn should_lower_vector_over_scalar_expression() {
    // given
    let expr = parse("vector(1 + 1)");

    // when
    let plan = lower(&expr, &ctx()).unwrap();

    // then
    match plan {
        LogicalPlan::Vectorize { child } => match *child {
            LogicalPlan::Binary { .. } => {}
            other => panic!("unexpected vector child: {other:?}"),
        },
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_scalar_over_vector_expression() {
    // given
    let expr = parse("scalar(foo)");

    // when
    let plan = lower(&expr, &ctx()).unwrap();

    // then
    match plan {
        LogicalPlan::Scalarize { child } => {
            assert!(matches!(*child, LogicalPlan::VectorSelector { .. }));
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_zero_arg_calendar_function_via_vectorized_time() {
    // given
    let expr = parse("minute()");

    // when
    let plan = lower(&expr, &ctx()).unwrap();

    // then
    match plan {
        LogicalPlan::InstantFn {
            kind: InstantFnKind::Minute,
            child,
        } => match *child {
            LogicalPlan::Vectorize { child } => {
                assert!(matches!(*child, LogicalPlan::Time));
            }
            other => panic!("unexpected calendar default arg: {other:?}"),
        },
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_label_replace_as_label_manip() {
    // given
    let expr = parse(r#"label_replace(foo, "dst", "$1", "src", "(.*)")"#);

    // when
    let plan = lower(&expr, &ctx()).unwrap();

    // then
    match plan {
        LogicalPlan::LabelManip {
            kind:
                LabelManipKind::Replace {
                    dst_label,
                    replacement,
                    src_label,
                    regex,
                },
            child,
        } => {
            assert_eq!(dst_label, "dst");
            assert_eq!(replacement, "$1");
            assert_eq!(src_label, "src");
            assert_eq!(regex, "(.*)");
            assert!(matches!(*child, LogicalPlan::VectorSelector { .. }));
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_label_join_as_label_manip() {
    // given
    let expr = parse(r#"label_join(foo, "dst", "-", "src", "src1")"#);

    // when
    let plan = lower(&expr, &ctx()).unwrap();

    // then
    match plan {
        LogicalPlan::LabelManip {
            kind:
                LabelManipKind::Join {
                    dst_label,
                    separator,
                    src_labels,
                },
            child,
        } => {
            assert_eq!(dst_label, "dst");
            assert_eq!(separator, "-");
            assert_eq!(
                src_labels.as_ref(),
                &["src".to_string(), "src1".to_string()]
            );
            assert!(matches!(*child, LogicalPlan::VectorSelector { .. }));
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_reject_label_replace_with_invalid_regex() {
    // given
    let expr = parse(r#"label_replace(foo, "dst", "$1", "src", "(.*")"#);

    // when
    let err = lower(&expr, &ctx()).unwrap_err();

    // then
    match err {
        PlanError::InvalidArgument { function, .. } => assert_eq!(function, "label_replace"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn should_lower_rate_as_rollup_over_matrix_selector() {
    // given: `rate(foo[5m])`
    let expr = parse("rate(foo[5m])");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: it becomes `Rollup { Rate, MatrixSelector(foo, 5m) }`
    match plan {
        LogicalPlan::Rollup { kind, child } => {
            assert_eq!(kind, RollupKind::Rate);
            match *child {
                LogicalPlan::MatrixSelector { range_ms, .. } => {
                    assert_eq!(range_ms, 5 * 60 * 1000);
                }
                other => panic!("unexpected child: {other:?}"),
            }
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_reject_rate_on_instant_vector() {
    // given: a manually-built `rate(foo)` AST — the parser itself rejects
    // this at parse time, so we hand-build an `Expr::Call` carrying an
    // instant-vector argument. This is the shape an (incorrect) later
    // optimizer pass could produce, and the lowering layer must defend
    // against it.
    use promql_parser::label::Matchers;
    use promql_parser::parser::value::ValueType;
    use promql_parser::parser::{Call, Function, FunctionArgs, VectorSelector};
    let inner = parser::Expr::VectorSelector(VectorSelector::new(
        Some("foo".to_string()),
        Matchers::empty(),
    ));
    // Synthesise a `rate` function signature directly — we need to bypass
    // the parser's static typing check to exercise the lowering guard.
    let func = Function::new("rate", vec![ValueType::Matrix], 0, ValueType::Vector, false);
    let call = Call {
        func,
        args: FunctionArgs::new_args(inner),
    };
    let expr = parser::Expr::Call(call);
    // when: lowered
    let err = lower(&expr, &ctx()).unwrap_err();
    // then: error is InvalidArgument for `rate`
    match err {
        PlanError::InvalidArgument { function, .. } => assert_eq!(function, "rate"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn should_lower_clamp_with_plan_time_constants() {
    // given: `clamp(foo, 0, 10)`
    let expr = parse("clamp(foo, 0, 10)");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: kind carries the folded scalar bounds
    match plan {
        LogicalPlan::InstantFn { kind, .. } => {
            assert_eq!(
                kind,
                InstantFnKind::Clamp {
                    min: 0.0,
                    max: 10.0
                }
            );
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_sum_by_as_aggregate_streaming() {
    // given: `sum by (pod) (foo)`
    let expr = parse("sum by (pod) (foo)");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: Aggregate{Sum, By([pod]), child: VectorSelector}
    match plan {
        LogicalPlan::Aggregate {
            kind,
            child,
            param,
            grouping,
        } => {
            assert_eq!(kind, AggregateKind::Sum);
            assert!(param.is_none());
            assert!(matches!(*child, LogicalPlan::VectorSelector { .. }));
            match grouping {
                AggregateGrouping::By(labels) => {
                    assert_eq!(labels.as_ref(), &["pod".to_string()]);
                }
                other => panic!("unexpected grouping: {other:?}"),
            }
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_topk_as_aggregate_breaker() {
    // given: `topk(5, foo)`
    let expr = parse("topk(5, foo)");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: AggregateKind::Topk(5) with no dynamic param child
    match plan {
        LogicalPlan::Aggregate { kind, param, .. } => {
            assert_eq!(kind, AggregateKind::Topk(5));
            assert!(param.is_none());
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_count_values_specialized() {
    // given: `count_values("version", foo)`
    let expr = parse("count_values(\"version\", foo)");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: dedicated CountValues variant carries the label name
    match plan {
        LogicalPlan::CountValues { label, .. } => {
            assert_eq!(label, "version");
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_binary_with_matching() {
    // given: `a + on(instance) b`
    let expr = parse("a + on(instance) b");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: Binary with BinaryMatching { On, [instance], OneToOne }
    match plan {
        LogicalPlan::Binary {
            op,
            lhs: _,
            rhs: _,
            matching,
        } => {
            assert_eq!(op, BinaryOpKind::Add);
            let m = matching.expect("matching present");
            assert_eq!(m.axis, MatchingAxis::On);
            assert_eq!(m.labels.as_ref(), &["instance".to_string()]);
            assert!(matches!(m.cardinality, Cardinality::OneToOne));
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_subquery_with_range_and_step() {
    // given: `rate(foo[5m:30s])`
    let expr = parse("rate(foo[5m:30s])");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: Rollup{Rate} over Subquery{range=5m, step=30s}
    match plan {
        LogicalPlan::Rollup { kind, child } => {
            assert_eq!(kind, RollupKind::Rate);
            match *child {
                LogicalPlan::Subquery {
                    range_ms, step_ms, ..
                } => {
                    assert_eq!(range_ms, 5 * 60 * 1000);
                    assert_eq!(step_ms, 30 * 1000);
                }
                other => panic!("unexpected child: {other:?}"),
            }
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_unwrap_parentheses() {
    // given: `(foo)`
    let expr = parse("(foo)");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: lowering strips the paren — direct VectorSelector
    assert!(matches!(plan, LogicalPlan::VectorSelector { .. }));
}

#[test]
fn should_lower_unary_minus() {
    // given: `-foo`
    let expr = parse("-foo");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: Binary{Mul, Scalar(-1.0), VectorSelector}
    match plan {
        LogicalPlan::Binary {
            op,
            lhs,
            rhs,
            matching,
        } => {
            assert_eq!(op, BinaryOpKind::Mul);
            assert_eq!(*lhs, LogicalPlan::Scalar(-1.0));
            assert!(matches!(*rhs, LogicalPlan::VectorSelector { .. }));
            assert!(matching.is_none());
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_reject_unknown_function() {
    // given: a function the parser knows but our lowering table does not
    let expr = parse("holt_winters(foo[5m], 0.5, 0.5)");
    // when: lowered
    let err = lower(&expr, &ctx()).unwrap_err();
    // then: UnknownFunction
    match err {
        PlanError::UnknownFunction(name) => assert_eq!(name, "holt_winters"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn should_lower_histogram_quantile_with_literal_phi() {
    let plan = lower(&parse("histogram_quantile(0.9, foo_bucket)"), &ctx()).unwrap();
    match plan {
        LogicalPlan::Histogram { kind, child } => {
            assert_eq!(kind, HistogramFnKind::Quantile(0.9));
            assert!(matches!(*child, LogicalPlan::VectorSelector { .. }));
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_histogram_fraction_bounds() {
    let plan = lower(&parse("histogram_fraction(-1, 0.5, foo_bucket)"), &ctx()).unwrap();
    assert!(matches!(
        plan,
        LogicalPlan::Histogram {
            kind: HistogramFnKind::Fraction {
                lower: -1.0,
                upper: 0.5
            },
            ..
        }
    ));
}

#[test]
fn should_reject_non_literal_histogram_quantile_phi() {
    let err = lower(&parse("histogram_quantile(scalar(q), foo_bucket)"), &ctx()).unwrap_err();
    assert!(matches!(err, PlanError::InvalidArgument { .. }), "{err:?}");
}

#[test]
fn should_lower_sort_and_sort_desc() {
    let plan = lower(&parse("sort(foo)"), &ctx()).unwrap();
    assert!(matches!(
        plan,
        LogicalPlan::Sort {
            order: InstantVectorSort::AscendingValue,
            ..
        }
    ));
    let plan = lower(&parse("sort_desc(foo)"), &ctx()).unwrap();
    assert!(matches!(
        plan,
        LogicalPlan::Sort {
            order: InstantVectorSort::DescendingValue,
            ..
        }
    ));
}

fn absent_labels_of(query: &str) -> Vec<(String, String)> {
    match lower(&parse(query), &ctx()).unwrap() {
        LogicalPlan::Absent { labels, .. } => labels
            .iter()
            .map(|l| (l.name.clone(), l.value.clone()))
            .collect(),
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_derive_absent_labels_from_equality_matchers() {
    let pair = |n: &str, v: &str| (n.to_string(), v.to_string());
    assert_eq!(
        absent_labels_of(r#"absent(foo{job="a", env=~"p.*", zone="z"})"#),
        vec![pair("job", "a"), pair("zone", "z")]
    );
    // a label with more than one matcher is dropped
    assert_eq!(
        absent_labels_of(r#"absent(foo{job="a", job="b", zone="z"})"#),
        vec![pair("zone", "z")]
    );
    assert_eq!(
        absent_labels_of(r#"absent_over_time(foo{job="a"}[5m])"#),
        vec![pair("job", "a")]
    );
    // a later `=` cannot restore a label an earlier matcher removed, and
    // empty values never become labels
    assert_eq!(
        absent_labels_of(r#"absent(foo{env=~"p.*", env="prod", code="", job="a"})"#),
        vec![pair("job", "a")]
    );
    // non-selector arguments carry no labels
    assert!(absent_labels_of(r#"absent(sum(foo{job="a"}))"#).is_empty());
}

#[test]
fn should_lower_absent_over_time_to_count_over_time() {
    let plan = lower(&parse("absent_over_time(foo[5m])"), &ctx()).unwrap();
    match plan {
        LogicalPlan::Absent { child, .. } => assert!(matches!(
            *child,
            LogicalPlan::Rollup {
                kind: RollupKind::CountOverTime,
                ..
            }
        )),
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_nested_expression_e2e() {
    // given: `sum by (pod) (rate(http_requests_total[5m]))`
    let expr = parse("sum by (pod) (rate(http_requests_total[5m]))");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: the full Aggregate→Rollup→MatrixSelector chain is preserved
    match plan {
        LogicalPlan::Aggregate {
            kind,
            child,
            param,
            grouping,
        } => {
            assert_eq!(kind, AggregateKind::Sum);
            assert!(param.is_none());
            match grouping {
                AggregateGrouping::By(labels) => {
                    assert_eq!(labels.as_ref(), &["pod".to_string()]);
                }
                other => panic!("unexpected grouping: {other:?}"),
            }
            match *child {
                LogicalPlan::Rollup {
                    kind: rk,
                    child: grandchild,
                } => {
                    assert_eq!(rk, RollupKind::Rate);
                    assert!(matches!(*grandchild, LogicalPlan::MatrixSelector { .. }));
                }
                other => panic!("unexpected Rollup child: {other:?}"),
            }
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_lower_topk_with_scalar_param_expression() {
    // given: `topk(scalar(foo), bar)`
    let expr = parse("topk(scalar(foo), bar)");

    // when
    let plan = lower(&expr, &ctx()).unwrap();

    // then
    match plan {
        LogicalPlan::Aggregate {
            kind,
            child,
            param,
            grouping,
        } => {
            assert_eq!(kind, AggregateKind::Topk(0));
            assert!(matches!(*child, LogicalPlan::VectorSelector { .. }));
            assert!(matches!(
                *param.expect("dynamic param"),
                LogicalPlan::Scalarize { .. }
            ));
            assert_eq!(
                grouping,
                AggregateGrouping::By(Arc::from(Vec::<String>::new()))
            );
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_fold_unary_negative_clamp_min_bound() {
    // given: `clamp_min(foo, -1)` — the planner folds `-1` as a literal
    let expr = parse("clamp_min(foo, -1)");
    // when: lowered
    let plan = lower(&expr, &ctx()).unwrap();
    // then: kind carries `min = -1.0`
    match plan {
        LogicalPlan::InstantFn {
            kind: InstantFnKind::ClampMin { min },
            ..
        } => {
            assert_eq!(min, -1.0);
        }
        other => panic!("unexpected lowering: {other:?}"),
    }
}

#[test]
fn should_build_lowering_context_for_instant_query() {
    // given: an instant-query helper
    let ictx = LoweringContext::for_instant(12345, 60_000);
    // when: the shape is inspected
    // then: start == end and the query is flagged instant
    assert_eq!(ictx.start_ms, 12345);
    assert_eq!(ictx.end_ms, 12345);
    assert!(ictx.is_instant());
    assert_eq!(ictx.lookback_delta_ms, 60_000);
}
