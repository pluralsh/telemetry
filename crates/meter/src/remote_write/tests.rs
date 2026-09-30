use super::*;
use crate::histogram::Bucket;
use crate::model::is_stale_nan;

fn integer_histogram(timestamp: i64) -> ProtobufHistogram {
    ProtobufHistogram {
        count: Some(histogram_count::Count::CountInt(9)),
        sum: 18.4,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(histogram_count::ZeroCount::ZeroCountInt(2)),
        positive_spans: vec![
            ProtobufBucketSpan {
                offset: 0,
                length: 2,
            },
            ProtobufBucketSpan {
                offset: 1,
                length: 1,
            },
        ],
        positive_deltas: vec![1, 1, 1],
        timestamp,
        ..ProtobufHistogram::default()
    }
}

fn v1_request(histograms: Vec<ProtobufHistogram>) -> WriteRequest {
    WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![ProtobufLabel {
                name: "__name__".to_string(),
                value: "latency".to_string(),
            }],
            samples: vec![],
            histograms,
        }],
    }
}

#[test]
fn should_select_protocol_from_content_type() {
    assert_eq!(Protocol::from_content_type(None), Some(Protocol::V1));
    assert_eq!(
        Protocol::from_content_type(Some("application/x-protobuf")),
        Some(Protocol::V1)
    );
    assert_eq!(
        Protocol::from_content_type(Some(
            "application/x-protobuf;proto=io.prometheus.write.v2.Request"
        )),
        Some(Protocol::V2)
    );
    assert_eq!(
        Protocol::from_content_type(Some(
            "application/x-protobuf; proto=\"prometheus.WriteRequest\""
        )),
        Some(Protocol::V1)
    );
    assert_eq!(
        Protocol::from_content_type(Some(
            "application/x-protobuf;proto=io.prometheus.write.v3.Request"
        )),
        None
    );
}

#[test]
fn should_decode_integer_histogram_deltas() {
    // when
    let batch = convert_write_request(v1_request(vec![integer_histogram(1000)])).unwrap();

    // then
    assert_eq!((batch.samples, batch.histograms), (0, 1));
    let h = &batch.series[0].histograms[0];
    assert_eq!(h.timestamp_ms, 1000);
    assert_eq!(h.histogram.count, 9.0);
    assert_eq!(h.histogram.zero_count, 2.0);
    assert_eq!(
        h.histogram.positive,
        vec![
            Bucket {
                index: 0,
                count: 1.0
            },
            Bucket {
                index: 1,
                count: 2.0
            },
            Bucket {
                index: 3,
                count: 3.0
            },
        ]
    );
}

#[test]
fn should_decode_float_histogram_counts_and_custom_bounds() {
    // given
    let h = ProtobufHistogram {
        count: Some(histogram_count::Count::CountFloat(3.5)),
        sum: 4.0,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![ProtobufBucketSpan {
            offset: 0,
            length: 2,
        }],
        positive_counts: vec![1.5, 2.0],
        custom_values: vec![0.5, 1.0],
        reset_hint: 3,
        timestamp: 5,
        ..ProtobufHistogram::default()
    };

    // when
    let batch = convert_write_request(v1_request(vec![h])).unwrap();

    // then
    let h = &batch.series[0].histograms[0].histogram;
    assert_eq!(h.counter_reset_hint, CounterResetHint::Gauge);
    assert_eq!(&*h.custom_values, &[0.5, 1.0]);
    assert_eq!(h.positive[1].count, 2.0);
}

#[test]
fn should_turn_stale_histogram_into_stale_float() {
    let stale = ProtobufHistogram {
        sum: f64::from_bits(STALE_NAN),
        timestamp: 2000,
        ..ProtobufHistogram::default()
    };
    let batch = convert_write_request(v1_request(vec![integer_histogram(1000), stale])).unwrap();
    let series = &batch.series[0];
    assert_eq!(series.histograms.len(), 1);
    assert_eq!(series.samples.len(), 1);
    assert!(is_stale_nan(series.samples[0].value));
}

#[test]
fn should_reject_invalid_histograms() {
    let mismatched = ProtobufHistogram {
        positive_deltas: vec![1],
        ..integer_histogram(1000)
    };
    assert!(convert_write_request(v1_request(vec![mismatched])).is_err());

    let bad_schema = ProtobufHistogram {
        schema: -9,
        ..integer_histogram(1000)
    };
    assert!(convert_write_request(v1_request(vec![bad_schema])).is_err());
}

#[test]
fn should_reduce_schema_above_maximum() {
    let fine = ProtobufHistogram {
        schema: 9,
        ..integer_histogram(1000)
    };
    let batch = convert_write_request(v1_request(vec![fine])).unwrap();
    assert_eq!(batch.series[0].histograms[0].histogram.schema, 8);
}

#[test]
fn should_resolve_v2_symbols_and_metadata() {
    // given
    let request = v2::Request {
        symbols: vec![
            "".to_string(),
            "__name__".to_string(),
            "http_requests_total".to_string(),
            "job".to_string(),
            "api".to_string(),
            "Total requests".to_string(),
        ],
        timeseries: vec![
            v2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4],
                samples: vec![v2::Sample {
                    value: 7.0,
                    timestamp: 1000,
                }],
                histograms: vec![],
                metadata: Some(v2::Metadata {
                    r#type: v2::METRIC_TYPE_COUNTER,
                    help_ref: 5,
                    unit_ref: 0,
                }),
            },
            v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![],
                histograms: vec![integer_histogram(1000)],
                metadata: None,
            },
        ],
    };

    // when
    let batch = convert_v2_request(request).unwrap();

    // then
    assert_eq!((batch.samples, batch.histograms), (1, 1));
    let series = &batch.series[0];
    assert_eq!(series.labels[1], Label::new("job", "api"));
    assert_eq!(series.description.as_deref(), Some("Total requests"));
    assert_eq!(series.unit, None);
    assert!(matches!(
        series.metric_type,
        Some(MetricType::Sum {
            monotonic: true,
            ..
        })
    ));
}

#[test]
fn should_reject_out_of_range_v2_symbol() {
    let request = v2::Request {
        symbols: vec!["".to_string()],
        timeseries: vec![v2::TimeSeries {
            labels_refs: vec![0, 3],
            samples: vec![v2::Sample {
                value: 1.0,
                timestamp: 1,
            }],
            histograms: vec![],
            metadata: None,
        }],
    };
    assert!(convert_v2_request(request).is_err());
}

#[test]
fn should_round_trip_snappy_v2_body() {
    // given
    let request = v2::Request {
        symbols: vec!["".to_string(), "__name__".to_string(), "up".to_string()],
        timeseries: vec![v2::TimeSeries {
            labels_refs: vec![1, 2],
            samples: vec![v2::Sample {
                value: 1.0,
                timestamp: 1,
            }],
            histograms: vec![],
            metadata: None,
        }],
    };
    let body = snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .unwrap();

    // when
    let batch = parse_remote_write(&body, Protocol::V2).unwrap();

    // then
    assert_eq!(batch.series.len(), 1);
    assert_eq!(batch.series[0].labels[0], Label::metric_name("up"));
}
