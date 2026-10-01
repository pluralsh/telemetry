# Loki compatibility

Line speaks the Loki push and query HTTP APIs and runs its own LogQL engine.
This page lists what matches Loki, where Line deliberately differs, and the
gaps that remain, so you can check a workload before migrating it.

Compatibility is measured against Loki's own engine conformance scripts
(`pkg/logql/internal/logqltest/testdata/*.logqltest`) by
`crates/line/tests/loki_conformance.rs`. Loki is AGPL-3.0, so the scripts are
not vendored: the test reads them from a Loki checkout named by `LOKI_SRC`
(default: a `loki` directory beside this workspace) and is skipped when none
is present.

```sh
LOKI_SRC=../loki cargo test -p line --test loki_conformance -- --nocapture
```

Each script's log lines are pushed into a fresh Line database and every
`eval` is compared on series labels, values, and log lines. Divergences that
are accepted are listed in `KNOWN_GAPS` with a reason. Any other divergence,
or a known gap that starts passing, fails the test.

## Results

1385 evals; 1373 match Loki and 12 are known gaps.

| Script | Evals | Known gaps |
| --- | --- | --- |
| `binary_operations` | 291 | 8 |
| `conversions` | 14 | 0 |
| `formatters` | 61 | 0 |
| `functions` | 31 | 0 |
| `label_filters` | 300 | 0 |
| `line_filters` | 56 | 0 |
| `log_selection` | 19 | 0 |
| `parsers` | 77 | 2 |
| `range_aggregations` | 350 | 2 |
| `vector_aggregations` | 186 | 0 |

## Known divergences

- **`bool` comparisons with `group_left` / `group_right`** keep the included
  labels, as Prometheus does. Loki drops them, for example
  `a > bool on (app) group_left (pool) b` returns series without `pool`.
- **Duplicate JSON keys**: `| json` keeps the last value of a repeated key;
  Loki keeps the first.
- **`approx_count_distinct`** counts distinct values exactly. Loki returns a
  HyperLogLog estimate, so its answers can be slightly off (19820 rather than
  20000 in the upstream test).

## Templates

`line_format` and `label_format` use a Rust implementation of the subset of Go
`text/template` that LogQL queries use in practice. There is no maintained
Rust crate that implements Go templates together with Loki's Sprig-derived
function set.

Templates are checked before the query runs. A call to a function that is not
listed below fails the query with `template: function "name" not defined`, as
an undefined function does in Loki. Runtime failures, such as integer
division by zero, set `__error__="TemplateFormatErr"` on the row instead.

Supported syntax:

- `{{ .label }}`, `{{ __line__ }}`, `{{ __timestamp__ }}`, `now`, `true`,
  `false`, and number and string literals. Strings take Go escapes, including
  `\x1b`, `\u00e9`, and octal.
- Pipelines (`{{ .a | upper | trunc 3 }}`) and parenthesised calls
  (`{{ upper (trim .a) }}`).
- `{{ if ... }} ... {{ else }} ... {{ end }}`, nested.
- `printf` with `%s`, `%v`, `%d`, `%f`, `%q`, and `%%`.

Functions: `Replace`, `ToLower`, `ToUpper`, `Trim`, `TrimLeft`, `TrimPrefix`,
`TrimRight`, `TrimSpace`, `TrimSuffix`, `add`, `addf`, `alignLeft`,
`alignRight`, `b64dec`, `b64enc`, `bytes`, `ceil`, `contains`, `count`,
`date`, `default`, `div`, `divf`, `duration`, `duration_seconds`, `float64`,
`floor`, `hasPrefix`, `hasSuffix`, `indent`, `int`, `lower`, `max`, `maxf`,
`min`, `minf`, `mod`, `mul`, `mulf`, `nindent`, `printf`, `regexReplaceAll`,
`regexReplaceAllLiteral`, `repeat`, `replace`, `round`, `sub`, `subf`,
`substr`, `title`, `toDate`, `toDateInZone`, `trim`, `trimAll`, `trimPrefix`,
`trimSuffix`, `trunc`, `unixEpoch`, `unixEpochMillis`, `unixEpochNanos`,
`unixToTime`, `upper`, `urldecode`, `urlencode`.

Not supported:

- `range`, `with`, `define`, `template`, `block`, `else if`, and variables
  (`$x := ...`).
- Go built-ins such as `eq`, `ne`, `lt`, `and`, `or`, `not`, `len`, and
  `index`; use `if` on a value, or a label filter after the stage.
- Whitespace trim markers (`{{-` and `-}}`).
- `printf` width, precision, and flag modifiers, and verbs other than those
  above.
- The rest of the Sprig library, such as `fromJson`, `sha256sum`, and `uuidv4`.

## Semantics

Line follows Loki in these areas, which differ from a naive reading of the
LogQL docs.

- **Error labels.** Parser and filter failures set `__error__` and
  `__error_details__` and keep the row. A metric query whose rows still carry
  `__error__` fails, unless `__preserve_error__="true"`. Line sets that label
  only for parser errors, and only when the query filters on `__error__`.
  Error labels survive `by` grouping in range aggregations and `keep`, and
  `label_format` cannot overwrite them.
- **Label filters.** A missing label fails numeric, bytes, and duration
  comparisons. A value that cannot be converted sets
  `__error__="LabelFilterErr"`, unless the row already has an error, and keeps
  the row. `and` evaluates both sides; `or` stops at the first match.
- **Empty values.** Empty stream labels are dropped at ingest, so
  `{app="a", env=""}` is stored as `{app="a"}`. Parsed labels and structured
  metadata keep empty values.
- **Structured metadata** is included in metric series labels, so
  `count_over_time({app="a"}[5m])` returns one series per distinct metadata
  label set.
- **Parsers.** `pattern` matches leniently: each capture runs to the next
  literal, or to the end of the line if that literal is missing. `unpack`
  extracts string values only, and `json` skips nulls and arrays.
- **Unwrap.** `duration` and `duration_seconds` unwrap to seconds; `bytes`
  accepts `1,024KB`-style values and truncates fractions.
- **Metric evaluation.** Range steps include `end`, as in Prometheus. `sort`
  and `sort_desc` order is kept in instant results, and `NaN` sorts last in
  both directions. `min` and `max` ignore `NaN` unless every value is `NaN`.
  Division and modulo by zero return `NaN`. `approx_topk` is instant-only.
- **Rejected at parse time**, as in Loki:
  - `==` with a string;
  - `ip()` with operators other than `=` and `!=`;
  - a negative quantile;
  - a typed comparison on `__error__`;
  - named captures in pattern line filters (`|> "<_> /api/<_>"` is valid,
    `|> "<method> <path>"` is not).

  `or` in a pattern line filter keeps the pattern type:
  `|> "GET <_>" or "POST <_>"`.

## Query planning

Line scans the matching streams once per query, even for binary expressions
over several log selectors. Every log expression's pipeline runs on each row
as its page is decoded, so only rows that some pipeline keeps are held in
memory. The read stays bounded by `max_pages` and `max_in_flight_bytes`.
Queries that use `match` read only the pages and rows that the term index
nominates, and those pages load concurrently. Unindexed log queries stop
reading once `limit` rows survive the pipeline.

Range aggregations compute each row's group, value, and any error once. Each
step then binary-searches its window within every group. `count_over_time`,
`bytes_over_time`, `bytes_rate`, and `rate` without `unwrap` read their
window from running integer totals. Other aggregations sum or scan the
window's values directly, because subtracting floating-point prefix sums
would drift from Loki's results.

Line filters (`|=`, `|~`) deliberately do not use the term index; only
`match` does. A filter must find every substring or regex match, while the
term index nominates rows by whole tokens, so using it for filters would
change Loki's semantics.
