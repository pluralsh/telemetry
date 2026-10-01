# Prometheus compatibility

Metrics implements the Prometheus remote-write protocol, the HTTP query API,
and the PromQL query language, so most dashboards and Grafana data sources
work unchanged. Compatibility is checked against the upstream Prometheus
`promqltest` corpus; the differences are listed under each feature below.
Unsupported constructs return an error rather than a wrong answer.

## Ingestion

Supported: Remote Write 1.0 and 2.0, OTLP metrics over HTTP at `/v1/metrics`,
float samples, staleness markers, native histograms (integer, float, and
custom buckets), metric metadata (Remote Write 2.0 and OTLP), and
out-of-order samples.

Not supported:

- Exemplars are dropped.
- Remote Write 1.0 metadata and Remote Write 2.0 created timestamps are
  ignored.
- Remote read (`/api/v1/read`).

## HTTP query API

Supported: `/api/v1/query`, `/api/v1/query_range`, `/api/v1/series`,
`/api/v1/labels`, `/api/v1/label/{name}/values`, `/api/v1/metadata`, and
`/federate`, with Prometheus' response format, error types, and parameter
formats. See [Supported APIs](apis.md).

Not supported:

- The `timeout`, `limit`, `lookback_delta`, and `stats` parameters are
  ignored; the lookback delta is fixed at 5 minutes.
- Annotations (`warnings` / `infos`) are never returned.
- `/federate` returns text format only, without `# HELP` / `# TYPE` lines.
- `/api/v1/query_exemplars`, `/api/v1/format_query`, `/api/v1/parse_query`,
  `/api/v1/status/*`, `/api/v1/targets`, `/api/v1/rules`, `/api/v1/alerts`,
  and the admin APIs. Metrics has no scraper, rule evaluator, or alerting.

## Selectors and subqueries

Supported: `=`, `!=`, `=~`, `!~`, `__name__` matchers, `offset`, `@`,
`@ start()` / `@ end()`, and nested subqueries.

Not supported:

- A bare range vector or subquery as the whole instant query, such as
  `foo[5m]`. Wrap it in a function such as `last_over_time`.

## Aggregations

Supported: `sum`, `avg`, `min`, `max`, `count`, `group`, `stddev`, `stdvar`,
`topk`, `bottomk`, `quantile`, and `count_values`, with `by` and `without`.

Not supported:

- `count_values` inside another expression; it works only at the top level.
- A non-literal parameter for `quantile`.
- `limitk` and `limit_ratio` (experimental upstream).
- Keeping `__name__` through aggregation (upstream's delayed name removal).

Differences:

- `avg` of values near `±1e308`, or mixing `+Inf` with finite values, can
  return `NaN` or a wrong value.
- `quantile` orders `NaN` differently.

## Binary operators

Supported: `+ - * / % ^ atan2`, comparisons with and without `bool`, and
`and`, `or`, `unless`, with `on`, `ignoring`, `group_left`, and `group_right`.

Differences:

- `or` drops right-hand series whose labels match nothing on the left.
- `and`, `or`, and `unless` with `on` or `ignoring` return only the matching
  labels instead of the left-hand series' full label set.
- A scalar on the left of a comparison, such as `1000 < sum(x)`, returns the
  scalar instead of the vector value.
- Many-to-one matching without `group_left` / `group_right`, and results with
  duplicate label sets, succeed instead of failing.
- Comparisons and set operators with a native histogram on the left return no
  samples.

## Functions

Supported: every stable Prometheus range function (`rate`, `increase`,
`*_over_time`, and so on) and instant function (math, trigonometry, `clamp*`,
`timestamp`, `absent`, `sort*`, `label_replace`, `label_join`, and the
calendar functions).

Not supported:

- Non-literal numeric parameters for `quantile_over_time`, `predict_linear`,
  `round`, and `clamp*`.
- Experimental upstream functions: `first_over_time`, `mad_over_time`, the
  `ts_of_*_over_time` functions, `sort_by_label`, `sort_by_label_desc`,
  `double_exponential_smoothing`, and `info`.

Differences:

- `label_replace` accepts an invalid UTF-8 target label, and fails when the
  result has duplicate label sets where Prometheus succeeds.

## Histograms

Supported: `histogram_quantile` and `histogram_fraction` over classic and
native histograms, `histogram_count`, `histogram_sum`, `histogram_avg`,
`histogram_stddev`, `histogram_stdvar`, and native-histogram arithmetic in
`rate`, `increase`, `sum`, and `avg`.

Not supported:

- Non-literal numeric parameters for `histogram_quantile` and
  `histogram_fraction`.
