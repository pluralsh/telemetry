# Loki compatibility

Logs implements Loki's push and query HTTP APIs (see [Supported APIs](apis.md))
and the LogQL query language. Against Loki's own LogQL conformance scripts,
1373 of 1385 evaluations match; the differences are listed under each feature
below.

To run the conformance suite against a Loki checkout:

```sh
LOKI_SRC=../loki cargo test -p plural-logs --test loki_conformance -- --nocapture
```

## Log selection and line filters

Supported: stream selectors (`=`, `!=`, `=~`, `!~`), `offset`, line filters
(`|=`, `!=`, `|~`, `!~`), pattern filters (`|>`, `!>`), `ip()` filters, and
`or` chains.

Differences:

- Empty stream labels are dropped at ingest, so `{app="a", env=""}` is stored
  as `{app="a"}`.

Logs also adds `| match "..."`, an indexed full-text search stage that Loki
does not have.

## Parsers and label stages

Supported: `json` and `logfmt` (with or without extracted label lists),
`regexp`, `pattern`, `unpack`, `decolorize`, `drop`, `keep`, and structured
metadata.

Differences:

- `json` keeps the last value of a repeated key; Loki keeps the first.
- `pattern` matches leniently: when a literal is missing, the capture before
  it runs to the end of the line.
- `unpack` extracts string values only, and `json` skips nulls and arrays.

## Label filters

Supported: string, regex, numeric, bytes, duration, and `ip()` comparisons,
combined with `and`, `or`, and `,`. Conversion failures set
`__error__="LabelFilterErr"`, as in Loki.

## Formatting

Supported: `line_format` and `label_format` with Go templates, as in Loki,
including the full `text/template` language and Loki's function library.

```logql
{app="api"} | logfmt | line_format "{{ .method }} {{ .path | upper }}"
```

Logs also accepts Jinja templates, selected with the `jinja` keyword:

```logql
{app="api"} | logfmt | line_format jinja "{{ method }} {{ path | upper }}"
{app="api"} | logfmt | label_format jinja route="{{ path | trunc(20) }}"
```

Labels are top-level variables, along with `__line__`, `__timestamp__`, and
`__labels__`. Loki's functions are available both as functions and as filters.
Where a name clashes with a built-in Jinja filter, such as `upper` or
`default`, the Jinja filter wins.

Differences:

- Numeric functions fail on a non-numeric string; Sprig treats it as `0`.
- `date`, `toDate`, and `toDateInZone` support common Go layouts in UTC only;
  `toDateInZone` ignores its zone.
- The rest of the Sprig library, such as `sha256sum` and `uuidv4`, is not
  available.

## Metric queries

Supported:

- **Range aggregations**: `count_over_time`, `rate`, `rate_counter`,
  `bytes_over_time`, `bytes_rate`, `avg_over_time`, `sum_over_time`,
  `min_over_time`, `max_over_time`, `stddev_over_time`, `stdvar_over_time`,
  `quantile_over_time`, `first_over_time`, `last_over_time`,
  `absent_over_time`, and `approx_count_distinct`, with `by` / `without`.
- **Unwrap** with `bytes()`, `duration()`, and `duration_seconds()`.
- **Vector aggregations**: `sum`, `avg`, `min`, `max`, `count`, `stddev`,
  `stdvar`, `topk`, `bottomk`, `sort`, `sort_desc`, and `approx_topk`
  (instant queries only, as in Loki).
- **Binary operations**: arithmetic, comparisons (with and without `bool`),
  `and`, `or`, `unless`, with `on`, `ignoring`, `group_left`, and
  `group_right`.
- `vector()`, `label_replace()`, and scalar literals.

Differences:

- `bool` comparisons with `group_left` / `group_right` keep the included
  labels, as Prometheus does; Loki drops them.
- `approx_count_distinct` is exact; Loki returns a HyperLogLog estimate.

## Error handling

Supported: `__error__` and `__error_details__` on parser, filter, and template
failures; filtering on `__error__`; and `__preserve_error__`. As in Loki, a
metric query fails if rows still carry `__error__` after the pipeline.
