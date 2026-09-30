# Prometheus compatibility

Meter speaks the Prometheus HTTP query API and remote-write protocol and runs
its own PromQL engine. Most dashboards, recording-rule style queries, and
Grafana data sources work unchanged. This page lists what is fully supported
and the gaps that remain, so you can check a workload before migrating it.

Compatibility is measured against the upstream Prometheus `promqltest` corpus,
ported into `crates/meter/src/promql/promqltest/testdata`. Blocks that Meter
does not pass yet are wrapped in `ignore` / `resume`; the gaps below are drawn
from those blocks and from constructs the planner rejects.

## Ingestion

| Feature | Status |
| --- | --- |
| Remote Write 1.0 (`prometheus.WriteRequest`) | Supported |
| Remote Write 2.0 (`io.prometheus.write.v2.Request`) | Supported, including the `X-Prometheus-Remote-Write-*-Written` response headers |
| Float samples and staleness markers | Supported |
| Native histograms (integer and float, exponential and custom-bucket schemas) | Supported |
| Metric metadata (type, help, unit) | Supported for Remote Write 2.0 and OTLP |
| OTLP metrics over HTTP (protobuf and JSON, optionally gzip-compressed) | Supported at `/v1/metrics` |
| Out-of-order and late samples | Accepted into any retained bucket, with no out-of-order window limit |

Gaps:

- **Exemplars** are dropped. Remote Write 2.0 responses report
  `exemplars-written: 0`.
- **Remote Write 1.0 metadata** (the `metadata` field of `WriteRequest`) is
  ignored, so `/api/v1/metadata` is empty for metrics that only arrive over
  Remote Write 1.0.
- **Created timestamps** from Remote Write 2.0 are ignored.
- **Remote read** (`/api/v1/read`) is not implemented.

## HTTP query API

Supported routes (namespace-scoped; see [Supported APIs](apis.md)):

| Route | Notes |
| --- | --- |
| `/api/v1/query` | GET and POST form |
| `/api/v1/query_range` | GET and POST form |
| `/api/v1/series` | `match[]`, `start`, `end` |
| `/api/v1/labels` | `match[]`, `start`, `end` |
| `/api/v1/label/{name}/values` | `match[]`, `start`, `end` |
| `/api/v1/metadata` | `metric`, `limit`, `limit_per_metric` |
| `/federate` | `match[]` |

Responses use the Prometheus `{"status", "data"}` envelope. Float values use the
same string spellings as Prometheus (`"+Inf"`, `"NaN"`, shortest round-trip
decimals), and native histograms are returned as `histogram` / `histograms`.
Errors use `{"status":"error","errorType","error"}` with the Prometheus
status codes: `400 bad_data` for invalid parameters or queries, `422
execution` for evaluation failures, and `500 internal` for storage failures.
As in Prometheus,
`time`, `start`, and `end` accept Unix seconds or RFC 3339, and `step`
accepts seconds or a duration such as `15s`.

Gaps:

- **`timeout`, `limit`, `lookback_delta`, and `stats`** are ignored on query
  routes, and `limit` is ignored on `series`, `labels`, and label values.
- The lookback delta is fixed at 5 minutes.
- **Annotations** (`warnings` / `infos`) are never returned.
- **Federation** emits only text exposition format without `# HELP` / `# TYPE`
  lines, and does not negotiate protobuf.
- **Not implemented**: `/api/v1/query_exemplars`, `/api/v1/format_query`,
  `/api/v1/parse_query`, `/api/v1/status/*`, `/api/v1/targets`,
  `/api/v1/rules`, `/api/v1/alerts`, and the admin TSDB APIs. Meter has no
  scraper, rule evaluator, or alerting; run those in Prometheus, Grafana Agent,
  or the OpenTelemetry Collector and remote-write the results.

## PromQL

### Fully supported

- **Selectors**: `=`, `!=`, `=~`, `!~` with anchored regular expressions
  (including flags such as `(?i)`); Prometheus empty-string semantics (`=""`
  and `=~".*"` also match series without the label, `!=""` requires it); name
  matchers via `{__name__=~"..."}`; `offset` (including negative), `@`
  timestamps, and `@ start()` / `@ end()`.
- **Subqueries** under any range function, including nested subqueries and
  `offset` / `@` on the subquery.
- **Aggregations**: `sum`, `avg`, `min`, `max`, `count`, `group`, `stddev`,
  `stdvar`, `topk`, `bottomk`, `quantile`, and `count_values`, with `by` and
  `without`. `topk` / `bottomk` accept scalar expressions for `k`.
- **Binary operators**: arithmetic (`+ - * / % ^ atan2`), comparisons with and
  without `bool`, and set operators `and`, `or`, `unless`, with `on`,
  `ignoring`, `group_left`, and `group_right` (see the known issues below for
  set-operator edge cases).
- **Range functions**: `rate`, `irate`, `increase`, `delta`, `idelta`,
  `deriv`, `predict_linear`, `changes`, `resets`, `avg_over_time`,
  `sum_over_time`, `min_over_time`, `max_over_time`, `count_over_time`,
  `last_over_time`, `stddev_over_time`, `stdvar_over_time`,
  `quantile_over_time`, `present_over_time`, and `absent_over_time`. Counter
  extrapolation matches Prometheus.
- **Instant functions**: `abs`, `ceil`, `floor`, `round`, `exp`, `ln`, `log2`,
  `log10`, `sqrt`, `sgn`, the trigonometric and hyperbolic functions, `deg`,
  `rad`, `pi`, `clamp`, `clamp_min`, `clamp_max`, `timestamp`, `time`,
  `vector`, `scalar`, `absent`, `sort`, `sort_desc`, `label_replace`,
  `label_join`, and the calendar functions `year`, `month`, `day_of_month`,
  `day_of_year`, `day_of_week`, `days_in_month`, `hour`, `minute`.
- **Histograms**: `histogram_quantile` and `histogram_fraction` over classic
  `le` buckets and native histograms; `histogram_count`, `histogram_sum`,
  `histogram_avg`, `histogram_stddev`, `histogram_stdvar`; native-histogram
  arithmetic in `rate`, `increase`, `sum`, and `avg`.

### Unsupported constructs

These return an error rather than a wrong answer.

- **A bare range vector or subquery as the whole query**, for example the
  instant query `foo[5m]` or `foo[5m:1m]`. Wrap it in a range function such as
  `last_over_time`, or use `query_range`.
- **`count_values` nested under** an aggregation, a binary operator, a range
  function, or an instant function. It works at the top level of a query.
- **Non-literal numeric parameters** for `quantile`, `quantile_over_time`,
  `histogram_quantile`, `histogram_fraction`, `predict_linear`, `round`, and
  `clamp*`. For example `quantile(scalar(q), x)` is rejected; use a number.
- **Experimental Prometheus functions** (behind
  `--enable-feature=promql-experimental-functions` upstream):
  `first_over_time`, `mad_over_time`, `ts_of_min_over_time`,
  `ts_of_max_over_time`, `ts_of_first_over_time`, `ts_of_last_over_time`,
  `sort_by_label`, `sort_by_label_desc`, `double_exponential_smoothing`
  (`holt_winters`), `limitk`, `limit_ratio`, and `info`.
- **Delayed `__name__` removal** (upstream feature flag
  `promql-delayed-name-removal`), for example `sum by (__name__) (rate(x[5m]))`
  keeping the metric name.

### Known divergences

These queries run but can return results that differ from Prometheus.

- **`or` drops right-hand series whose labels match nothing on the left.**
  `a{x="1"} or b{y="2"}` returns only the `a` series; Prometheus returns both.
  `or` still fills gaps for series whose labels do match.
- **`and` / `unless` / `or` with `on(...)` or `ignoring(...)`** reduce the
  output labels to the matching labels. Prometheus keeps the left-hand
  series' full label set.
- **Scalar-on-the-left comparisons** return the scalar:
  `1000 < sum(x)` yields `1000` where Prometheus yields the vector value.
  `sum(x) > 1000` is correct.
- **`avg` overflow and infinities**: `avg` of values near `±1e308`, or of
  groups that mix `+Inf` with finite values, can return `NaN` or a wrong
  value; Prometheus switches to incremental averaging to avoid this.
- **`quantile` over groups containing `NaN`** orders `NaN` differently from
  Prometheus.
- **Missing evaluation errors**: many-to-one matching without `group_left` /
  `group_right`, and results with duplicate label sets (for example
  `-{__name__=~"a|b"}` when `a` and `b` share labels), succeed instead of
  failing.
- **Native histogram comparisons**: `h == h` and set operators whose left side
  is a native histogram return no samples.
- **`label_replace` edge cases**: an invalid UTF-8 target label is accepted,
  and a replacement that collides label sets fails with a duplicate-labelset
  error where Prometheus succeeds.
