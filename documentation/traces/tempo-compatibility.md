# Tempo compatibility

Traces implements Tempo's trace and search HTTP APIs (see
[Supported APIs](apis.md)) and the TraceQL query language. Against Tempo's
TraceQL example corpus, Traces handles 300 of 463 queries exactly as Tempo
does. Most of the rest use TraceQL metrics; the differences are listed under
each feature below.

To run the conformance suite against a Tempo checkout:

```sh
TEMPO_SRC=../tempo cargo test -p plural-traces --test tempo_conformance -- --nocapture
```

## Spanset filters

Supported: attributes scoped to `span.`, `resource.`, and `instrumentation.`,
unscoped attributes (`.x`), comparisons, regex, arithmetic, and `nil`.

Not supported:

- Span event attributes and intrinsics (`event.x`, `event:name`).
- Span link attributes and intrinsics (`link.x`, `link:traceID`).

## Intrinsics

Supported: `name`, `status`, `statusMessage`, `kind`, `duration`, `trace:id`,
`span:id`, `span:parentID`, `trace:rootName`, `trace:rootService`,
`trace:duration`, `span:childCount`, `instrumentation:name`,
`instrumentation:version`, `nestedSetLeft`, `nestedSetRight`, and
`nestedSetParent`.

## Structural operators

Supported: `>`, `<`, `>>`, `<<`, `~`, their negated (`!>`) and union (`&>`)
forms, and spanset `&&` and `||`.

Not supported:

- A pipeline as a structural operand, such as `({ a } | { false }) > { b }`.

## Pipelines and aggregates

Supported: `count()`, `sum()`, `avg()`, `min()`, and `max()` with scalar
filters, `by()`, `coalesce()`, `select()`, and spanset filter stages.

Differences:

- Arithmetic and comparisons between aggregates, such as
  `min(.field) + max(.field) > 1`, run; Tempo rejects them.

## TraceQL metrics

Not supported: `rate()`, `count_over_time()`, `quantile_over_time()`, the
other `*_over_time` functions, and `compare()`. They parse but fail at run
time.
