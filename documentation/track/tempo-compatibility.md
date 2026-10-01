# Tempo compatibility

Track speaks Tempo's trace and search HTTP APIs and runs its own TraceQL
engine. This page lists what matches Tempo and the gaps that remain.

Compatibility is measured against Tempo's TraceQL example corpus
(`pkg/traceql/test_examples.yaml`) by `crates/track/tests/tempo_conformance.rs`.
The corpus sorts queries into four groups: `valid`, `parse_fails`,
`validate_fails`, and `unsupported`. The test checks that Track puts each
query in the same group. Tempo is AGPL-3.0, so the corpus is not vendored:
the test reads it from a Tempo checkout named by `TEMPO_SRC` (default: a
`tempo` directory beside this workspace) and is skipped when none is present.

```sh
TEMPO_SRC=../tempo cargo test -p track --test tempo_conformance -- --nocapture
```

Divergences that are accepted are listed in `KNOWN_GAPS` with a reason. Any
other divergence, or a known gap that starts passing, fails the test.

## Results

The corpus has 463 queries. Track agrees with Tempo on 300 of them. The other
163 break down as follows:

- 148 are known gaps, listed below.
- 8 are rejected by both engines, but at a different stage. For example,
  Track's validator rejects a query that Tempo's parser rejects.
- 7 are queries Tempo parses but refuses to run, which Track runs:
  arithmetic and comparisons between aggregates, such as
  `min(.field) + max(.field) > 1` and `{ true } | count() + count() = 1`.

## Gaps

| Feature | Corpus queries | Behaviour in Track |
| --- | --- | --- |
| TraceQL metrics: `rate()`, `count_over_time()`, `quantile_over_time()`, and the other `*_over_time` functions | 127 | Parsed; execution fails with "metric stage is parsed but not executable" |
| `compare()` | 10 | As for metrics |
| Span event attributes and intrinsics (`event.x`, `event:name`) | 3 | Rejected |
| Span link attributes and intrinsics (`link.x`, `link:traceID`) | 3 | Rejected |
| A pipeline as a structural operand, such as `({ a } \| { false }) > { b }` | 5 | Rejected |

## Supported

- **Spanset filters** with attributes scoped to `span.`, `resource.`, and
  `instrumentation.`, and unscoped attributes (`.x`, which looks at the span
  first and then the resource). Filters support comparisons, regex,
  arithmetic, and `nil`.
- **Intrinsics**:
  - `name`, `status`, `statusMessage`, `kind`, and `duration`, with or
    without the `span:` prefix;
  - `trace:id`, `span:id`, and `span:parentID`;
  - `trace:rootName`, `trace:rootService`, and `trace:duration`;
  - `span:childCount`;
  - `instrumentation:name` and `instrumentation:version`;
  - `nestedSetLeft`, `nestedSetRight`, and `nestedSetParent`, computed per
    trace the way Tempo assigns them at ingest.
- **Structural operators**:
  - `>`, `<`, `>>`, `<<`, and `~`;
  - their negations `!>`, `!<`, `!>>`, `!<<`, and `!~`;
  - their union forms `&>`, `&<`, `&>>`, `&<<`, and `&~`;
  - spanset `&&` and `||`.

  As in Tempo, `{ A } > { B }` returns the `B` spans whose parent matches
  `A`. The negated forms return the `B` spans with no such relation, and the
  union forms return both the `B` spans and the matching `A` spans.
- **Pipelines**:
  - `count()`, `sum()`, `avg()`, `min()`, and `max()` with scalar filters;
  - `by()`, `coalesce()`, and `select()`;
  - spanset filter stages such as `| { status = error }`.
- **Validation**:
  - ints, floats, and durations compare with one another;
  - `= nil` on an intrinsic or on `resource.service.name` is rejected;
  - `by()` and aggregate fields must reference the span.

## Query planning

TraceQL is evaluated per trace, so planning is mostly a matter of choosing
candidate traces from the attribute index before loading and evaluating them.

- **Equality on a span or resource attribute** with a string, bool, int, or
  float literal is a direct index lookup. Clauses joined by `&&` intersect.
  Clauses joined by `||`, either inside a filter or between spansets, union
  the narrowest clause from each side. A side with no indexable clause turns
  off pushdown for the whole `||`.
- **Regular expressions, range comparisons, and `!= nil`** on an attribute
  scan the index's distinct values for that attribute and keep those that
  pass the same comparison the evaluator uses. Literals on either side of the
  operator work, as in `500 <= span.code`.
- **Intrinsics** are indexed at ingest. `name` supports the same comparisons
  as attributes. `status` and `kind` support `=` and `!=`. `duration`
  supports `=`, `<`, `<=`, `>`, and `>=` through power-of-two buckets, so
  the index narrows candidates and the evaluator applies the exact bound.
  Pages written before intrinsics were indexed have no intrinsic postings.
  Their traces stay candidates for every intrinsic clause, so results stay
  correct after an upgrade.
- Both operands of `>`, `>>`, `&>`, and the other positive relations are
  pushed down. For negated relations only the right-hand operand is, because
  those return right-hand spans.
- Spanset filter stages, such as `| { span.x = "y" }`, are pushed down like
  the first spanset.
- Candidates are loaded in batches, with each page fetched once per batch and
  traces evaluated concurrently. Evaluation stops once `limit` results are
  found.

Not pushed down are predicates that a span without the attribute can
satisfy: `!=` with a non-nil literal and `= nil`. Instrumentation-scope
attributes, other intrinsics such as `span:childCount`, and comparisons
between two fields are not pushed down either. A query made only of these
considers every trace in the time range, up to `max_candidate_traces`. Beyond
that it fails rather than return partial results.
