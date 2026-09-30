use super::*;
use crate::model::{InstantSample, Label, Labels, MetricType};

// -----------------------------------------------------------------------
// PromSample serialization
// -----------------------------------------------------------------------

#[test]
fn prom_sample_serializes_as_tuple() {
    let sample = PromSample(3_900_000, 42.0);
    let json = serde_json::to_string(&sample).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(parsed.is_array());
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_f64().unwrap(), 3900.0);
    assert_eq!(arr[1].as_str().unwrap(), "42.0");
}

#[test]
fn prom_samples_serializes_as_array() {
    let samples = vec![(1000, 1.5), (2000, 2.5)];
    let wrapper = PromSamples(&samples);
    let json = serde_json::to_string(&wrapper).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0][0].as_f64().unwrap(), 1.0);
    assert_eq!(arr[0][1].as_str().unwrap(), "1.5");
    assert_eq!(arr[1][0].as_f64().unwrap(), 2.0);
    assert_eq!(arr[1][1].as_str().unwrap(), "2.5");
}

// -----------------------------------------------------------------------
// query_value_to_response
// -----------------------------------------------------------------------

#[test]
fn query_value_scalar_response() {
    let result = Ok(QueryValue::Scalar {
        timestamp_ms: 4_000_000,
        value: 42.5,
    });
    let resp = query_value_to_response(result);
    assert_eq!(resp.status, "success");

    // Verify the JSON wire format first (before moving data out)
    let json = serde_json::to_value(&resp).unwrap();
    let result_arr = &json["data"]["result"];
    assert_eq!(result_arr[0].as_f64().unwrap(), 4000.0);
    assert_eq!(result_arr[1].as_str().unwrap(), "42.5");

    let data = resp.data.unwrap();
    assert_eq!(data.result_type, "scalar");
    match data.result {
        QueryResultValue::Scalar(ts_ms, val) => {
            assert_eq!(ts_ms, 4_000_000);
            assert_eq!(val, 42.5);
        }
        _ => panic!("expected Scalar variant"),
    }
}

#[test]
fn query_value_vector_response() {
    let samples = vec![InstantSample {
        labels: Labels::new(vec![
            Label::metric_name("up"),
            Label::new("job", "prometheus"),
        ]),
        timestamp_ms: 3_900_000,
        value: 1.0,
    }];
    let result = Ok(QueryValue::Vector(samples));
    let resp = query_value_to_response(result);
    assert_eq!(resp.status, "success");

    // Verify the JSON wire format first (before moving data out)
    let json = serde_json::to_value(&resp).unwrap();
    let first = &json["data"]["result"][0];
    assert_eq!(first["value"][0].as_f64().unwrap(), 3900.0);
    assert_eq!(first["value"][1].as_str().unwrap(), "1.0");

    let data = resp.data.unwrap();
    assert_eq!(data.result_type, "vector");
    let results = match data.result {
        QueryResultValue::Vector(v) => v,
        _ => panic!("expected Vector variant"),
    };
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0.labels.get("__name__").unwrap(), "up");
    assert_eq!(results[0].0.labels.get("job").unwrap(), "prometheus");
    assert_eq!(results[0].0.timestamp_ms, 3_900_000);
    assert_eq!(results[0].0.value, 1.0);
}

#[test]
fn query_value_matrix_response() {
    let range_samples = vec![RangeSample {
        labels: Labels::new(vec![
            Label::metric_name("http_requests_total"),
            Label::new("job", "api"),
        ]),
        samples: vec![(1_000_000, 10.0), (2_000_000, 20.0), (3_000_000, 30.0)],
    }];
    let result = Ok(QueryValue::Matrix(range_samples));
    let resp = query_value_to_response(result);
    assert_eq!(resp.status, "success");

    // Verify the JSON wire format
    let json = serde_json::to_value(&resp).unwrap();
    let first = &json["data"]["result"][0];
    assert_eq!(first["metric"]["__name__"], "http_requests_total");
    assert_eq!(first["metric"]["job"], "api");
    let values = first["values"].as_array().unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0][0].as_f64().unwrap(), 1000.0);
    assert_eq!(values[0][1].as_str().unwrap(), "10.0");
    assert_eq!(values[2][0].as_f64().unwrap(), 3000.0);
    assert_eq!(values[2][1].as_str().unwrap(), "30.0");

    let data = resp.data.unwrap();
    assert_eq!(data.result_type, "matrix");
    let results = match data.result {
        QueryResultValue::Matrix(m) => m,
        _ => panic!("expected Matrix variant"),
    };
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].0.labels.get("__name__").unwrap(),
        "http_requests_total"
    );
    assert_eq!(results[0].0.samples.len(), 3);
}

#[test]
fn query_value_error_response() {
    let resp = query_value_to_response(Err(QueryError::InvalidQuery("bad syntax".into())));
    assert_eq!(resp.status, "error");
    assert_eq!(resp.error_type.as_deref(), Some("bad_data"));
    assert!(resp.data.is_none());

    let resp = query_value_to_response(Err(QueryError::Execution("boom".into())));
    assert_eq!(resp.error_type.as_deref(), Some("execution"));

    let resp = query_value_to_response(Err(QueryError::Timeout));
    assert_eq!(resp.error_type.as_deref(), Some("timeout"));
}

// -----------------------------------------------------------------------
// MatrixSeries serialize/deserialize roundtrip
// -----------------------------------------------------------------------

#[test]
fn matrix_series_roundtrip() {
    let ms = MatrixSeries(RangeSample {
        labels: Labels::new(vec![Label::metric_name("cpu")]),
        samples: vec![(1_000, 1.5), (2_000, 2.5)],
    });
    let json = serde_json::to_string(&ms).unwrap();
    let parsed: MatrixSeries = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.0.labels, ms.0.labels);
    assert_eq!(parsed.0.samples, ms.0.samples);
}

// -----------------------------------------------------------------------
// VectorSeries serialize/deserialize roundtrip
// -----------------------------------------------------------------------

#[test]
fn vector_series_roundtrip() {
    let vs = VectorSeries(InstantSample {
        labels: Labels::new(vec![Label::metric_name("up")]),
        timestamp_ms: 3_900_000,
        value: 42.0,
    });
    let json = serde_json::to_string(&vs).unwrap();
    let parsed: VectorSeries = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.0.labels, vs.0.labels);
    assert_eq!(parsed.0.timestamp_ms, vs.0.timestamp_ms);
    assert_eq!(parsed.0.value, vs.0.value);
}

// -----------------------------------------------------------------------
// QueryResultValue serialize/deserialize roundtrip
// -----------------------------------------------------------------------

#[test]
fn scalar_roundtrip() {
    let qr = QueryResponse {
        status: "success".to_string(),
        data: Some(QueryResult {
            result_type: "scalar".to_string(),
            result: QueryResultValue::Scalar(4_000_000, 42.5),
        }),
        error: None,
        error_type: None,
        trace: None,
    };
    let json = serde_json::to_string(&qr).unwrap();
    let parsed: QueryResponse = serde_json::from_str(&json).unwrap();
    match parsed.data.unwrap().result {
        QueryResultValue::Scalar(ts_ms, val) => {
            assert_eq!(ts_ms, 4_000_000);
            assert_eq!(val, 42.5);
        }
        _ => panic!("expected Scalar"),
    }
}

#[test]
fn matrix_roundtrip() {
    let qr = QueryResponse {
        status: "success".to_string(),
        data: Some(QueryResult {
            result_type: "matrix".to_string(),
            result: QueryResultValue::Matrix(vec![MatrixSeries(RangeSample {
                labels: Labels::new(vec![Label::metric_name("cpu")]),
                samples: vec![(1_000, 1.5), (2_000, 2.5)],
            })]),
        }),
        error: None,
        error_type: None,
        trace: None,
    };
    let json = serde_json::to_string(&qr).unwrap();
    let parsed: QueryResponse = serde_json::from_str(&json).unwrap();
    let data = parsed.data.unwrap();
    assert_eq!(data.result_type, "matrix");
    match data.result {
        QueryResultValue::Matrix(m) => {
            assert_eq!(m.len(), 1);
            assert_eq!(m[0].0.labels.get("__name__").unwrap(), "cpu");
            assert_eq!(m[0].0.samples, vec![(1_000, 1.5), (2_000, 2.5)]);
        }
        _ => panic!("expected Matrix"),
    }
}

#[test]
fn matrix_empty_result() {
    let result = Ok(QueryValue::Matrix(vec![]));
    let resp = query_value_to_response(result);
    assert_eq!(resp.status, "success");
    let data = resp.data.unwrap();
    assert_eq!(data.result_type, "matrix");
    match data.result {
        QueryResultValue::Matrix(m) => assert!(m.is_empty()),
        _ => panic!("expected Matrix variant"),
    }
}

// -----------------------------------------------------------------------
// series_to_response — sorting + limit
// -----------------------------------------------------------------------

#[test]
fn series_response_sorts_and_limits() {
    let labels_vec = vec![
        Labels::new(vec![
            Label::metric_name("zz_metric"),
            Label::new("env", "prod"),
        ]),
        Labels::new(vec![
            Label::metric_name("aa_metric"),
            Label::new("env", "dev"),
        ]),
        Labels::new(vec![
            Label::metric_name("mm_metric"),
            Label::new("env", "staging"),
        ]),
    ];
    let resp = series_to_response(Ok(labels_vec), Some(2));

    assert_eq!(resp.status, "success");
    let data = resp.data.unwrap();
    assert_eq!(data.len(), 2, "limit should truncate to 2");
    // Should be sorted by __name__
    assert_eq!(data[0].get("__name__").unwrap(), "aa_metric");
    assert_eq!(data[1].get("__name__").unwrap(), "mm_metric");
}

#[test]
fn labels_serialize_deserialize_roundtrip() {
    let labels = Labels::new(vec![
        Label::metric_name("http_requests"),
        Label::new("method", "GET"),
    ]);
    let json = serde_json::to_string(&labels).unwrap();
    let parsed: Labels = serde_json::from_str(&json).unwrap();
    assert_eq!(labels, parsed);

    // Verify JSON shape is a flat object
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(value.is_object());
    assert_eq!(value["__name__"], "http_requests");
    assert_eq!(value["method"], "GET");
}

// -----------------------------------------------------------------------
// metadata_to_response — limit + limit_per_metric
// -----------------------------------------------------------------------

#[test]
fn metadata_response_limits() {
    let entries = vec![
        model::MetricMetadata {
            metric_name: "cpu".into(),
            metric_type: Some(MetricType::Gauge),
            description: Some("CPU usage".into()),
            unit: Some("percent".into()),
        },
        model::MetricMetadata {
            metric_name: "cpu".into(),
            metric_type: Some(MetricType::Gauge),
            description: Some("CPU total".into()),
            unit: None,
        },
        model::MetricMetadata {
            metric_name: "mem".into(),
            metric_type: Some(MetricType::Gauge),
            description: Some("Memory".into()),
            unit: Some("bytes".into()),
        },
    ];

    // limit_per_metric caps entries per metric
    let resp = metadata_to_response(Ok(entries.clone()), None, Some(1));
    assert_eq!(resp.status, "success");
    let data = resp.data.unwrap();
    for entries in data.values() {
        assert!(
            entries.len() <= 1,
            "limit_per_metric=1 should cap each metric to 1 entry"
        );
    }

    // limit caps the number of metrics
    let resp = metadata_to_response(Ok(entries), Some(1), None);
    let data = resp.data.unwrap();
    assert_eq!(data.len(), 1, "limit=1 should return only 1 metric");
}

#[test]
fn metadata_response_converts_types() {
    let entries = vec![model::MetricMetadata {
        metric_name: "requests".into(),
        metric_type: Some(MetricType::Gauge),
        description: Some("Total requests".into()),
        unit: Some("1".into()),
    }];
    let resp = metadata_to_response(Ok(entries), None, None);
    let data = resp.data.unwrap();
    let meta = &data["requests"][0];
    assert_eq!(meta.0.metric_type.as_ref().unwrap().as_str(), "gauge");
    assert_eq!(meta.0.description.as_deref().unwrap(), "Total requests");
    assert_eq!(meta.0.unit.as_deref().unwrap(), "1");
}

// -----------------------------------------------------------------------
// Property-based tests
// -----------------------------------------------------------------------

mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Generate an arbitrary metric name (1–20 lowercase alpha chars).
    fn arb_metric_name() -> impl Strategy<Value = String> {
        "[a-z][a-z_]{0,19}".prop_filter("non-empty", |s| !s.is_empty())
    }

    /// Generate an arbitrary label key (1–10 lowercase alpha chars).
    fn arb_label_key() -> impl Strategy<Value = String> {
        "[a-z]{1,10}"
    }

    /// Generate an arbitrary label value.
    fn arb_label_value() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9_]{0,20}"
    }

    /// Generate a Labels with a __name__ and 0–3 extra labels.
    fn arb_labels() -> impl Strategy<Value = Labels> {
        (
            arb_metric_name(),
            prop::collection::vec((arb_label_key(), arb_label_value()), 0..4),
        )
            .prop_map(|(name, pairs)| {
                let mut labels = vec![Label::metric_name(&name)];
                for (k, v) in pairs {
                    labels.push(Label::new(k, v));
                }
                Labels::new(labels)
            })
    }

    /// Generate a MetricMetadata entry.
    fn arb_metadata() -> impl Strategy<Value = model::MetricMetadata> {
        (arb_metric_name(), any::<bool>(), any::<bool>()).prop_map(|(name, has_desc, has_unit)| {
            model::MetricMetadata {
                metric_name: name,
                metric_type: Some(MetricType::Gauge),
                description: if has_desc { Some("desc".into()) } else { None },
                unit: if has_unit { Some("unit".into()) } else { None },
            }
        })
    }

    proptest! {
        /// Scalar timestamp is always converted from ms to seconds in wire format.
        #[test]
        fn scalar_timestamp_is_ms_to_secs(ts_ms in 0i64..=i64::MAX / 2) {
            let resp = query_value_to_response(Ok(QueryValue::Scalar {
                timestamp_ms: ts_ms,
                value: 1.0,
            }));
            // Verify wire format first (before moving data out)
            let json = serde_json::to_value(&resp).unwrap();
            let ts_secs = json["data"]["result"][0].as_f64().unwrap();
            prop_assert!(
                (ts_secs - ts_ms as f64 / 1000.0).abs() < 1e-6,
                "expected {} / 1000 = {}, got {}",
                ts_ms,
                ts_ms as f64 / 1000.0,
                ts_secs,
            );
            // Verify internal representation
            let data = resp.data.unwrap();
            match data.result {
                QueryResultValue::Scalar(stored_ts, _) => {
                    prop_assert_eq!(stored_ts, ts_ms);
                }
                _ => panic!("expected Scalar"),
            };
        }

        /// Vector response preserves all samples and converts timestamps.
        #[test]
        fn vector_preserves_samples_and_converts_timestamps(
            timestamps in prop::collection::vec(0i64..=i64::MAX / 2, 1..10),
        ) {
            let samples: Vec<InstantSample> = timestamps
                .iter()
                .enumerate()
                .map(|(i, &ts)| InstantSample {
                    labels: Labels::new(vec![Label::metric_name(
                        format!("m{i}"),
                    )]),
                    timestamp_ms: ts,
                    value: i as f64,
                })
                .collect();
            let n = samples.len();
            let resp = query_value_to_response(Ok(QueryValue::Vector(samples)));
            let data = resp.data.unwrap();
            let results = match data.result {
                QueryResultValue::Vector(v) => v,
                _ => panic!("expected Vector"),
            };
            prop_assert_eq!(results.len(), n);
            for (result, &ts_ms) in results.iter().zip(timestamps.iter()) {
                prop_assert_eq!(result.0.timestamp_ms, ts_ms);
            }
        }

        /// series_to_response output is always sorted by __name__.
        #[test]
        fn series_output_is_sorted(
            labels_vec in prop::collection::vec(arb_labels(), 0..20),
        ) {
            let resp = series_to_response(Ok(labels_vec), None);
            let data = resp.data.unwrap();
            let names: Vec<&str> = data
                .iter()
                .map(|m| m.metric_name())
                .collect();
            for w in names.windows(2) {
                prop_assert!(w[0] <= w[1], "not sorted: {:?} > {:?}", w[0], w[1]);
            }
        }

        /// series_to_response with limit always returns at most `limit` entries.
        #[test]
        fn series_limit_is_respected(
            labels_vec in prop::collection::vec(arb_labels(), 0..20),
            limit in 0usize..25,
        ) {
            let input_len = labels_vec.len();
            let resp = series_to_response(Ok(labels_vec), Some(limit));
            let data = resp.data.unwrap();
            prop_assert!(data.len() <= limit, "len {} > limit {}", data.len(), limit);
            prop_assert_eq!(data.len(), input_len.min(limit));
        }

        /// metadata_to_response with limit caps the number of distinct metrics.
        #[test]
        fn metadata_limit_caps_metrics(
            entries in prop::collection::vec(arb_metadata(), 0..20),
            limit in 1usize..10,
        ) {
            let resp = metadata_to_response(Ok(entries), Some(limit), None);
            let data = resp.data.unwrap();
            prop_assert!(
                data.len() <= limit,
                "metric count {} > limit {}",
                data.len(),
                limit,
            );
        }

        /// metadata_to_response with limit_per_metric caps entries per metric.
        #[test]
        fn metadata_limit_per_metric_caps_entries(
            entries in prop::collection::vec(arb_metadata(), 0..20),
            limit_per in 1usize..5,
        ) {
            let resp = metadata_to_response(Ok(entries), None, Some(limit_per));
            let data = resp.data.unwrap();
            for (metric, entries) in &data {
                prop_assert!(
                    entries.len() <= limit_per,
                    "metric {metric}: {} entries > limit_per_metric {limit_per}",
                    entries.len(),
                );
            }
        }
    }
}

// -----------------------------------------------------------------------
// Existing tests
// -----------------------------------------------------------------------

/// Verify that all response structs omit the `data` key when it is None.
#[test]
fn error_responses_omit_data_field() {
    let query = QueryResponse {
        status: "error".into(),
        data: None,
        error: Some("bad".into()),
        error_type: Some("bad_data".into()),
        trace: None,
    };
    let json = serde_json::to_value(&query).unwrap();
    assert!(json.get("data").is_none(), "QueryResponse: {json}");

    let query_range = QueryRangeResponse {
        status: "error".into(),
        data: None,
        error: Some("bad".into()),
        error_type: Some("bad_data".into()),
        trace: None,
    };
    let json = serde_json::to_value(&query_range).unwrap();
    assert!(json.get("data").is_none(), "QueryRangeResponse: {json}");

    let series = SeriesResponse {
        status: "error".into(),
        data: None,
        error: Some("bad".into()),
        error_type: Some("bad_data".into()),
    };
    let json = serde_json::to_value(&series).unwrap();
    assert!(json.get("data").is_none(), "SeriesResponse: {json}");

    let labels = LabelsResponse {
        status: "error".into(),
        data: None,
        error: Some("bad".into()),
        error_type: Some("bad_data".into()),
    };
    let json = serde_json::to_value(&labels).unwrap();
    assert!(json.get("data").is_none(), "LabelsResponse: {json}");

    let label_values = LabelValuesResponse {
        status: "error".into(),
        data: None,
        error: Some("bad".into()),
        error_type: Some("bad_data".into()),
    };
    let json = serde_json::to_value(&label_values).unwrap();
    assert!(json.get("data").is_none(), "LabelValuesResponse: {json}");

    let metadata = MetadataResponse {
        status: "error".into(),
        data: None,
        error: Some("bad".into()),
        error_type: Some("bad_data".into()),
    };
    let json = serde_json::to_value(&metadata).unwrap();
    assert!(json.get("data").is_none(), "MetadataResponse: {json}");
}
