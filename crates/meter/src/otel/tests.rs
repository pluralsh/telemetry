use super::*;
use crate::model::{MetricType, Temporality};
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value},
    metrics::v1::{
        AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge,
        Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
        Summary, SummaryDataPoint, exponential_histogram_data_point::Buckets, metric,
        number_data_point, summary_data_point,
    },
    resource::v1::Resource,
};

fn make_request(resource_metrics: Vec<ResourceMetrics>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest { resource_metrics }
}

fn make_resource_metrics(
    resource_attrs: Vec<KeyValue>,
    scope_metrics: Vec<ScopeMetrics>,
) -> ResourceMetrics {
    ResourceMetrics {
        resource: Some(Resource {
            attributes: resource_attrs,
            dropped_attributes_count: 0,
        }),
        scope_metrics,
        schema_url: String::new(),
    }
}

fn make_scope_metrics(scope_name: &str, scope_version: &str, metrics: Vec<Metric>) -> ScopeMetrics {
    ScopeMetrics {
        scope: Some(InstrumentationScope {
            name: scope_name.to_string(),
            version: scope_version.to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        }),
        metrics,
        schema_url: String::new(),
    }
}

fn make_scope_metrics_with_attrs(
    scope_name: &str,
    scope_version: &str,
    scope_attrs: Vec<KeyValue>,
    metrics: Vec<Metric>,
) -> ScopeMetrics {
    ScopeMetrics {
        scope: Some(InstrumentationScope {
            name: scope_name.to_string(),
            version: scope_version.to_string(),
            attributes: scope_attrs,
            dropped_attributes_count: 0,
        }),
        metrics,
        schema_url: String::new(),
    }
}

fn make_gauge(
    name: &str,
    unit: &str,
    description: &str,
    data_points: Vec<NumberDataPoint>,
) -> Metric {
    Metric {
        name: name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        metadata: vec![],
        data: Some(metric::Data::Gauge(Gauge { data_points })),
    }
}

fn make_sum(
    name: &str,
    unit: &str,
    description: &str,
    data_points: Vec<NumberDataPoint>,
    temporality: i32,
    monotonic: bool,
) -> Metric {
    Metric {
        name: name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        metadata: vec![],
        data: Some(metric::Data::Sum(Sum {
            data_points,
            aggregation_temporality: temporality,
            is_monotonic: monotonic,
        })),
    }
}

fn make_histogram(
    name: &str,
    unit: &str,
    description: &str,
    data_points: Vec<HistogramDataPoint>,
    temporality: i32,
) -> Metric {
    Metric {
        name: name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        metadata: vec![],
        data: Some(metric::Data::Histogram(Histogram {
            data_points,
            aggregation_temporality: temporality,
        })),
    }
}

fn make_exp_histogram(
    name: &str,
    unit: &str,
    description: &str,
    data_points: Vec<ExponentialHistogramDataPoint>,
    temporality: i32,
) -> Metric {
    Metric {
        name: name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        metadata: vec![],
        data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
            data_points,
            aggregation_temporality: temporality,
        })),
    }
}

fn make_summary(
    name: &str,
    unit: &str,
    description: &str,
    data_points: Vec<SummaryDataPoint>,
) -> Metric {
    Metric {
        name: name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        metadata: vec![],
        data: Some(metric::Data::Summary(Summary { data_points })),
    }
}

fn make_number_dp(
    value: number_data_point::Value,
    time_unix_nano: u64,
    attrs: Vec<KeyValue>,
) -> NumberDataPoint {
    NumberDataPoint {
        attributes: attrs,
        start_time_unix_nano: 0,
        time_unix_nano,
        exemplars: vec![],
        flags: 0,
        value: Some(value),
    }
}

fn make_histogram_dp(
    time_unix_nano: u64,
    count: u64,
    sum: f64,
    bucket_counts: Vec<u64>,
    explicit_bounds: Vec<f64>,
    attrs: Vec<KeyValue>,
) -> HistogramDataPoint {
    HistogramDataPoint {
        attributes: attrs,
        start_time_unix_nano: 0,
        time_unix_nano,
        count,
        sum: Some(sum),
        bucket_counts,
        explicit_bounds,
        exemplars: vec![],
        flags: 0,
        min: None,
        max: None,
    }
}

fn make_summary_dp(
    time_unix_nano: u64,
    count: u64,
    sum: f64,
    quantile_values: Vec<summary_data_point::ValueAtQuantile>,
    attrs: Vec<KeyValue>,
) -> SummaryDataPoint {
    SummaryDataPoint {
        attributes: attrs,
        start_time_unix_nano: 0,
        time_unix_nano,
        count,
        sum,
        quantile_values,
        flags: 0,
    }
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

/// Find all series whose `__name__` label contains `name_substr`.
fn find_series<'a>(series: &'a [Series], name_substr: &str) -> Vec<&'a Series> {
    series
        .iter()
        .filter(|s| s.name().contains(name_substr))
        .collect()
}

/// Get the value of a label on a series.
fn get_label<'a>(series: &'a Series, label_name: &str) -> Option<&'a str> {
    series
        .labels
        .iter()
        .find(|l| l.name == label_name)
        .map(|l| l.value.as_str())
}

fn build_default(request: &ExportMetricsServiceRequest) -> Vec<Series> {
    let builder = OtelConverter::new(OtelConfig::default());
    builder.convert(request).expect("convert should succeed")
}

fn ts_nanos(ms: u64) -> u64 {
    ms * 1_000_000
}

#[test]
fn should_convert_gauge_to_single_series() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge(
                "cpu_temperature",
                "",
                "CPU temperature",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(72.5),
                    ts_nanos(1000),
                    vec![],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "cpu_temperature");
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].metric_type, Some(MetricType::Gauge));
    assert_eq!(matched[0].samples.len(), 1);
    assert_eq!(matched[0].samples[0].value, 72.5);
    assert_eq!(matched[0].samples[0].timestamp_ms, 1000);
}

#[test]
fn should_include_gauge_data_point_attributes_as_labels() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge(
                "cpu_temperature",
                "",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(72.5),
                    ts_nanos(1000),
                    vec![kv("host", "server1"), kv("region", "us-east")],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "cpu_temperature");
    assert_eq!(matched.len(), 1);
    assert_eq!(get_label(matched[0], "host"), Some("server1"));
    assert_eq!(get_label(matched[0], "region"), Some("us-east"));
}

#[test]
fn should_handle_gauge_with_multiple_data_points() {
    // Each data point with distinct attributes becomes its own series.
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge(
                "cpu_temperature",
                "",
                "",
                vec![
                    make_number_dp(
                        number_data_point::Value::AsDouble(72.5),
                        ts_nanos(1000),
                        vec![kv("host", "server1")],
                    ),
                    make_number_dp(
                        number_data_point::Value::AsDouble(68.0),
                        ts_nanos(2000),
                        vec![kv("host", "server2")],
                    ),
                ],
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "cpu_temperature");
    assert_eq!(matched.len(), 2);
}

#[test]
fn should_handle_gauge_with_int_value() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge(
                "active_connections",
                "",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsInt(42),
                    ts_nanos(1000),
                    vec![],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "active_connections");
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].samples[0].value, 42.0);
}

#[test]
fn should_convert_monotonic_cumulative_sum_to_counter_with_total_suffix() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_sum(
                "http_requests",
                "",
                "Total HTTP requests",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(100.0),
                    ts_nanos(1000),
                    vec![],
                )],
                AggregationTemporality::Cumulative as i32,
                true,
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "http_requests_total");
    assert_eq!(matched.len(), 1);
    assert_eq!(
        matched[0].metric_type,
        Some(MetricType::Sum {
            monotonic: true,
            temporality: Temporality::Cumulative,
        })
    );
    assert_eq!(matched[0].samples[0].value, 100.0);
}

#[test]
fn should_convert_non_monotonic_sum_to_gauge() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_sum(
                "queue_size",
                "",
                "Current queue size",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(50.0),
                    ts_nanos(1000),
                    vec![],
                )],
                AggregationTemporality::Cumulative as i32,
                false,
            )],
        )],
    )]);

    let series = build_default(&request);
    // Non-monotonic sum should NOT have _total suffix.
    let matched = find_series(&series, "queue_size");
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].name(), "queue_size");
    assert_eq!(matched[0].metric_type, Some(MetricType::Gauge));
}

#[test]
fn should_drop_delta_sum_with_warning() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_sum(
                "delta_counter",
                "",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(10.0),
                    ts_nanos(1000),
                    vec![],
                )],
                AggregationTemporality::Delta as i32,
                true,
            )],
        )],
    )]);

    let series = build_default(&request);
    assert!(
        find_series(&series, "delta_counter").is_empty(),
        "delta temporality sums should be dropped"
    );
}

#[test]
fn should_decompose_histogram_into_bucket_sum_count() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_histogram(
                "http_request_duration",
                "",
                "",
                vec![make_histogram_dp(
                    ts_nanos(1000),
                    10,                  // count
                    5.5,                 // sum
                    vec![2, 3, 5, 10],   // bucket_counts (last is +Inf)
                    vec![0.1, 0.5, 1.0], // explicit_bounds
                    vec![],
                )],
                AggregationTemporality::Cumulative as i32,
            )],
        )],
    )]);

    let series = build_default(&request);

    // Should have _bucket series (one per bound + Inf), _sum, and _count.
    let buckets = find_series(&series, "_bucket");
    assert!(!buckets.is_empty(), "should produce _bucket series");

    let sums = find_series(&series, "_sum");
    assert_eq!(sums.len(), 1, "should produce one _sum series");
    assert_eq!(sums[0].samples[0].value, 5.5);

    let counts = find_series(&series, "_count");
    assert_eq!(counts.len(), 1, "should produce one _count series");
    assert_eq!(counts[0].samples[0].value, 10.0);
}

#[test]
fn should_include_le_label_on_histogram_buckets() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_histogram(
                "http_request_duration",
                "",
                "",
                vec![make_histogram_dp(
                    ts_nanos(1000),
                    10,
                    5.5,
                    vec![2, 3, 5, 10],
                    vec![0.1, 0.5, 1.0],
                    vec![],
                )],
                AggregationTemporality::Cumulative as i32,
            )],
        )],
    )]);

    let series = build_default(&request);
    let buckets = find_series(&series, "_bucket");

    for bucket in &buckets {
        assert!(
            get_label(bucket, "le").is_some(),
            "each _bucket series should have an 'le' label"
        );
    }

    // Check specific le values exist.
    let le_values: Vec<&str> = buckets.iter().filter_map(|s| get_label(s, "le")).collect();
    assert!(le_values.contains(&"0.1"));
    assert!(le_values.contains(&"0.5"));
    assert!(le_values.contains(&"1"));
}

#[test]
fn should_include_inf_bucket() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_histogram(
                "http_request_duration",
                "",
                "",
                vec![make_histogram_dp(
                    ts_nanos(1000),
                    10,
                    5.5,
                    vec![2, 3, 5, 10],
                    vec![0.1, 0.5, 1.0],
                    vec![],
                )],
                AggregationTemporality::Cumulative as i32,
            )],
        )],
    )]);

    let series = build_default(&request);
    let buckets = find_series(&series, "_bucket");

    let le_values: Vec<&str> = buckets.iter().filter_map(|s| get_label(s, "le")).collect();
    assert!(
        le_values.contains(&"+Inf"),
        "+Inf bucket should always be present"
    );
}

#[test]
fn should_set_histogram_series_metric_type_to_counter() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_histogram(
                "http_request_duration",
                "",
                "",
                vec![make_histogram_dp(
                    ts_nanos(1000),
                    10,
                    5.5,
                    vec![2, 3, 5, 10],
                    vec![0.1, 0.5, 1.0],
                    vec![],
                )],
                AggregationTemporality::Cumulative as i32,
            )],
        )],
    )]);

    let series = build_default(&request);
    let all_histogram = find_series(&series, "http_request_duration");
    for s in &all_histogram {
        assert_eq!(
            s.metric_type,
            Some(MetricType::Histogram {
                temporality: Temporality::Cumulative,
            })
        );
    }
}

#[test]
fn should_convert_exponential_histogram_to_explicit_buckets() {
    // Exponential histogram with scale=0: base = 2^(2^0) = 2
    // Positive buckets: offset=0, counts=[1, 2, 3]
    // Boundaries: [2^0, 2^1, 2^2] = [1, 2, 4]
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_exp_histogram(
                "request_latency",
                "",
                "",
                vec![ExponentialHistogramDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: ts_nanos(1000),
                    count: 6,
                    sum: Some(15.0),
                    scale: 0,
                    zero_count: 0,
                    positive: Some(Buckets {
                        offset: 0,
                        bucket_counts: vec![1, 2, 3],
                    }),
                    negative: None,
                    flags: 0,
                    exemplars: vec![],
                    min: None,
                    max: None,
                    zero_threshold: 0.0,
                }],
                AggregationTemporality::Cumulative as i32,
            )],
        )],
    )]);

    let series = build_default(&request);

    // Should decompose into _bucket, _sum, _count just like a regular histogram.
    let buckets = find_series(&series, "_bucket");
    assert!(!buckets.is_empty(), "should produce _bucket series");

    let sums = find_series(&series, "_sum");
    assert_eq!(sums.len(), 1);
    assert_eq!(sums[0].samples[0].value, 15.0);

    let counts = find_series(&series, "_count");
    assert_eq!(counts.len(), 1);
    assert_eq!(counts[0].samples[0].value, 6.0);
}

#[test]
fn should_include_zero_count_in_exp_histogram_buckets() {
    // Exponential histogram with scale=0, zero_count=5, positive=[1, 2, 3]
    // The zero bucket count should be included in the cumulative counts.
    // Expected cumulative: bucket at le=2 → 5+1=6, le=4 → 6+2=8, le=8 → 8+3=11 (wait, we
    // need to think about this more carefully)
    //
    // With scale=0: base = 2^(2^0) = 2
    // Positive bucket boundaries: base^(offset+i+1) with offset=0 → [2, 4, 8]
    // zero_count = 5 should appear in cumulative counts before positive buckets.
    // Cumulative: le=2 → 5+1=6, le=4 → 6+2=8, le=8 → 8+3=11
    // +Inf → dp.count = 11
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_exp_histogram(
                "request_size",
                "",
                "",
                vec![ExponentialHistogramDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: ts_nanos(1000),
                    count: 11,
                    sum: Some(30.0),
                    scale: 0,
                    zero_count: 5,
                    positive: Some(Buckets {
                        offset: 0,
                        bucket_counts: vec![1, 2, 3],
                    }),
                    negative: None,
                    flags: 0,
                    exemplars: vec![],
                    min: None,
                    max: None,
                    zero_threshold: 0.0,
                }],
                AggregationTemporality::Cumulative as i32,
            )],
        )],
    )]);

    let series = build_default(&request);
    let buckets = find_series(&series, "_bucket");

    // First positive bucket should include zero_count in its cumulative.
    let first_positive = buckets
        .iter()
        .find(|s| get_label(s, "le") == Some("2"))
        .expect("should have le=2 bucket");
    assert_eq!(
        first_positive.samples[0].value, 6.0,
        "first positive bucket should include zero_count (5) + bucket count (1) = 6"
    );

    // +Inf should equal dp.count
    let inf = buckets
        .iter()
        .find(|s| get_label(s, "le") == Some("+Inf"))
        .expect("should have +Inf bucket");
    assert_eq!(inf.samples[0].value, 11.0);
}

#[test]
fn should_not_emit_negative_bucket_boundaries_in_exp_histogram() {
    // Negative buckets cannot be represented as classic Prometheus le-style buckets.
    // They should be silently skipped (counts still appear in +Inf via dp.count).
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_exp_histogram(
                "temperature_delta",
                "",
                "",
                vec![ExponentialHistogramDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: ts_nanos(1000),
                    count: 10,
                    sum: Some(-5.0),
                    scale: 0,
                    zero_count: 4,
                    positive: Some(Buckets {
                        offset: 0,
                        bucket_counts: vec![1],
                    }),
                    negative: Some(Buckets {
                        offset: 0,
                        bucket_counts: vec![2, 3],
                    }),
                    flags: 0,
                    exemplars: vec![],
                    min: None,
                    max: None,
                    zero_threshold: 0.0,
                }],
                AggregationTemporality::Cumulative as i32,
            )],
        )],
    )]);

    let series = build_default(&request);
    let buckets = find_series(&series, "_bucket");

    // No negative le values should be emitted.
    let negative_buckets: Vec<_> = buckets
        .iter()
        .filter(|s| {
            get_label(s, "le")
                .and_then(|v| v.parse::<f64>().ok())
                .is_some_and(|v| v < 0.0)
        })
        .collect();
    assert!(
        negative_buckets.is_empty(),
        "should not produce buckets with negative le values"
    );

    // +Inf still reflects total dp.count (including negative observations).
    let inf = buckets
        .iter()
        .find(|s| get_label(s, "le") == Some("+Inf"))
        .expect("should have +Inf bucket");
    assert_eq!(inf.samples[0].value, 10.0);
}

#[test]
fn should_decompose_summary_into_quantile_sum_count() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_summary(
                "rpc_duration",
                "",
                "",
                vec![make_summary_dp(
                    ts_nanos(1000),
                    100,
                    500.0,
                    vec![
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.5,
                            value: 4.0,
                        },
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.99,
                            value: 8.0,
                        },
                    ],
                    vec![],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);

    // Per-quantile series.
    let quantiles = find_series(&series, "rpc_duration")
        .into_iter()
        .filter(|s| get_label(s, "quantile").is_some())
        .collect::<Vec<_>>();
    assert_eq!(quantiles.len(), 2, "should produce one series per quantile");

    // _sum and _count.
    let sums = find_series(&series, "_sum");
    assert_eq!(sums.len(), 1);
    assert_eq!(sums[0].samples[0].value, 500.0);

    let counts = find_series(&series, "_count");
    assert_eq!(counts.len(), 1);
    assert_eq!(counts[0].samples[0].value, 100.0);
}

#[test]
fn should_include_quantile_label() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_summary(
                "rpc_duration",
                "",
                "",
                vec![make_summary_dp(
                    ts_nanos(1000),
                    100,
                    500.0,
                    vec![
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.5,
                            value: 4.0,
                        },
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.99,
                            value: 8.0,
                        },
                    ],
                    vec![],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);
    let quantile_series: Vec<_> = series
        .iter()
        .filter(|s| get_label(s, "quantile").is_some())
        .collect();

    let quantile_values: Vec<&str> = quantile_series
        .iter()
        .map(|s| get_label(s, "quantile").unwrap())
        .collect();
    assert!(quantile_values.contains(&"0.5"));
    assert!(quantile_values.contains(&"0.99"));
}

#[test]
fn should_include_resource_attributes_as_labels() {
    let request = make_request(vec![make_resource_metrics(
        vec![kv("service.name", "my-svc"), kv("host.name", "node-1")],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge(
                "cpu_temp",
                "",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(70.0),
                    ts_nanos(1000),
                    vec![],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "cpu_temp");
    assert_eq!(matched.len(), 1);
    assert_eq!(get_label(matched[0], "service_name"), Some("my-svc"));
    assert_eq!(get_label(matched[0], "host_name"), Some("node-1"));
}

#[test]
fn should_include_scope_labels() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "my.library",
            "2.0.0",
            vec![make_gauge(
                "cpu_temp",
                "",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(70.0),
                    ts_nanos(1000),
                    vec![],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "cpu_temp");
    assert_eq!(matched.len(), 1);
    assert_eq!(get_label(matched[0], "otel_scope_name"), Some("my.library"));
    assert_eq!(get_label(matched[0], "otel_scope_version"), Some("2.0.0"));
}

#[test]
fn should_include_scope_attributes_as_labels() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics_with_attrs(
            "test",
            "1.0",
            vec![kv("scope.tag", "abc")],
            vec![make_gauge(
                "cpu_temp",
                "",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(70.0),
                    ts_nanos(1000),
                    vec![],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "cpu_temp");
    assert_eq!(matched.len(), 1);
    assert_eq!(get_label(matched[0], "scope_tag"), Some("abc"));
}

#[test]
fn should_exclude_resource_attrs_when_config_disabled() {
    let request = make_request(vec![make_resource_metrics(
        vec![kv("service.name", "my-svc")],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge(
                "cpu_temp",
                "",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(70.0),
                    ts_nanos(1000),
                    vec![],
                )],
            )],
        )],
    )]);

    let builder = OtelConverter::new(OtelConfig {
        include_resource_attrs: false,
        include_scope_attrs: true,
    });
    let series = builder.convert(&request).expect("convert should succeed");
    let matched = find_series(&series, "cpu_temp");
    assert_eq!(matched.len(), 1);
    assert_eq!(
        get_label(matched[0], "service_name"),
        None,
        "resource attrs should be excluded"
    );
}

#[test]
fn should_exclude_scope_attrs_when_config_disabled() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics_with_attrs(
            "test",
            "1.0",
            vec![kv("scope.tag", "abc")],
            vec![make_gauge(
                "cpu_temp",
                "",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(70.0),
                    ts_nanos(1000),
                    vec![],
                )],
            )],
        )],
    )]);

    let builder = OtelConverter::new(OtelConfig {
        include_resource_attrs: true,
        include_scope_attrs: false,
    });
    let series = builder.convert(&request).expect("convert should succeed");
    let matched = find_series(&series, "cpu_temp");
    assert_eq!(matched.len(), 1);
    assert_eq!(
        get_label(matched[0], "scope_tag"),
        None,
        "scope attrs should be excluded"
    );
    // Scope name/version labels should still be present even when scope attrs are disabled,
    // as they are considered scope identity, not attributes.
    assert_eq!(get_label(matched[0], "otel_scope_name"), Some("test"));
}

#[test]
fn should_omit_scope_labels_when_scope_is_empty() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "",
            "",
            vec![make_gauge(
                "cpu_temp",
                "",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(70.0),
                    ts_nanos(1000),
                    vec![],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "cpu_temp");
    assert_eq!(matched.len(), 1);
    assert_eq!(
        get_label(matched[0], "otel_scope_name"),
        None,
        "empty scope name should not produce a label"
    );
    assert_eq!(
        get_label(matched[0], "otel_scope_version"),
        None,
        "empty scope version should not produce a label"
    );
}

#[test]
fn should_append_unit_suffix_to_metric_name() {
    // Per OTEL spec: metric "http.request.duration" with unit "s" →
    // "http_request_duration_seconds"
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge(
                "http.request.duration",
                "s",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(0.5),
                    ts_nanos(1000),
                    vec![],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "http_request_duration_seconds");
    assert_eq!(
        matched.len(),
        1,
        "unit 's' should be expanded to '_seconds' suffix"
    );
}

#[test]
fn should_normalize_unit_to_prometheus_convention() {
    // Curly-brace units like "{requests}" should produce no suffix.
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge(
                "http_requests",
                "{requests}",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(1.0),
                    ts_nanos(1000),
                    vec![],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "http_requests");
    assert_eq!(matched.len(), 1);
    // Name should NOT have a suffix from curly-brace units.
    assert_eq!(matched[0].name(), "http_requests");
}

#[test]
fn should_handle_empty_request() {
    let request = make_request(vec![]);
    let series = build_default(&request);
    assert!(series.is_empty());
}

#[test]
fn should_handle_metric_with_no_data_points() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge("empty_gauge", "", "", vec![])],
        )],
    )]);

    let series = build_default(&request);
    assert!(
        series.is_empty(),
        "metric with no data points should produce no series"
    );
}

#[test]
fn should_handle_metric_with_no_data_field() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![Metric {
                name: "no_data".to_string(),
                description: String::new(),
                unit: String::new(),
                metadata: vec![],
                data: None,
            }],
        )],
    )]);

    let series = build_default(&request);
    assert!(series.is_empty(), "metric with data=None should be skipped");
}

#[test]
fn should_propagate_description_and_unit_to_series_metadata() {
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge(
                "cpu_temp",
                "Cel",
                "CPU temperature in Celsius",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(70.0),
                    ts_nanos(1000),
                    vec![],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "cpu_temp");
    assert_eq!(matched.len(), 1);
    assert_eq!(
        matched[0].description.as_deref(),
        Some("CPU temperature in Celsius")
    );
    assert!(matched[0].unit.is_some(), "unit should be set on series");
}

#[test]
fn should_sanitize_metric_name_for_prometheus() {
    // Dots should become underscores, leading digits should be prefixed.
    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge(
                "http.server.request.duration",
                "",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(1.0),
                    ts_nanos(1000),
                    vec![],
                )],
            )],
        )],
    )]);

    let series = build_default(&request);
    let matched = find_series(&series, "http_server_request_duration");
    assert_eq!(
        matched.len(),
        1,
        "dots in metric name should be replaced with underscores"
    );
}

#[test]
fn format_float_should_handle_values_beyond_i64_range() {
    // 1e19 exceeds i64::MAX (~9.2e18). format_float should not saturate.
    assert_eq!(format_float(1e19), "10000000000000000000");
    assert_eq!(format_float(-1e19), "-10000000000000000000");
}

#[test]
fn format_float_should_format_whole_numbers_without_decimal() {
    assert_eq!(format_float(1.0), "1");
    assert_eq!(format_float(100.0), "100");
    assert_eq!(format_float(0.0), "0");
    assert_eq!(format_float(-5.0), "-5");
}

#[test]
fn format_float_should_preserve_fractional_values() {
    assert_eq!(format_float(0.5), "0.5");
    assert_eq!(format_float(0.99), "0.99");
}
