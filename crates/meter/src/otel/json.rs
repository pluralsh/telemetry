//! OTLP/JSON decoding for metrics export requests.
//!
//! The OTLP JSON encoding follows proto3 JSON mapping, which writes 64-bit
//! integers as strings (`"count": "10"`, `"asInt": "-3"`,
//! `"bucketCounts": ["1", "2"]`), permits plain numbers too, and omits
//! default-valued fields. `opentelemetry-proto`'s serde model only accepts
//! timestamps as strings and the remaining 64-bit fields as numbers, and
//! several of its messages require every field. It also deserializes a
//! metric's data through a flattened `Option`, which turns any error inside
//! the data into a silently missing payload. The body is therefore
//! normalized to the model's shape, and every metric that carried data is
//! checked to have decoded it.

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use serde_json::{Map, Number, Value, json};

use crate::error::Error;

const TIMESTAMP_KEYS: [&str; 2] = ["timeUnixNano", "startTimeUnixNano"];
const UNSIGNED_KEYS: [&str; 4] = ["count", "zeroCount", "timeUnixNano", "startTimeUnixNano"];
const SIGNED_KEYS: [&str; 1] = ["asInt"];
const UNSIGNED_ARRAY_KEYS: [&str; 1] = ["bucketCounts"];
const DATA_KEYS: [&str; 5] = [
    "gauge",
    "sum",
    "histogram",
    "exponentialHistogram",
    "summary",
];

/// Decodes an OTLP/JSON `ExportMetricsServiceRequest` body.
pub fn decode_metrics_json(body: &[u8]) -> Result<ExportMetricsServiceRequest, Error> {
    let mut value: Value = serde_json::from_slice(body).map_err(invalid)?;
    normalize(&mut value)?;
    let mut expected = Vec::new();
    for metric in metrics_mut(&mut value) {
        fill_data_defaults(metric);
        let has_data = DATA_KEYS.iter().any(|key| metric.contains_key(*key));
        let name = metric.get("name").and_then(Value::as_str).unwrap_or("");
        expected.push(has_data.then(|| name.to_owned()));
    }
    let request: ExportMetricsServiceRequest = serde_json::from_value(value).map_err(invalid)?;
    let decoded = request
        .resource_metrics
        .iter()
        .flat_map(|resource| &resource.scope_metrics)
        .flat_map(|scope| &scope.metrics);
    for (metric, expected) in decoded.zip(expected) {
        if let (None, Some(name)) = (&metric.data, expected) {
            return Err(Error::InvalidInput(format!(
                "invalid OTLP JSON: malformed data for metric {name:?}"
            )));
        }
    }
    Ok(request)
}

fn invalid(error: serde_json::Error) -> Error {
    Error::InvalidInput(format!("invalid OTLP JSON: {error}"))
}

fn normalize(value: &mut Value) -> Result<(), Error> {
    match value {
        Value::Object(map) => normalize_object(map),
        Value::Array(items) => items.iter_mut().try_for_each(normalize),
        _ => Ok(()),
    }
}

fn normalize_object(map: &mut Map<String, Value>) -> Result<(), Error> {
    for (key, field) in map.iter_mut() {
        let key = key.as_str();
        if UNSIGNED_KEYS.contains(&key) {
            to_number(key, field, false)?;
        } else if SIGNED_KEYS.contains(&key) {
            to_number(key, field, true)?;
        } else if UNSIGNED_ARRAY_KEYS.contains(&key) {
            if let Value::Array(items) = field {
                for item in items {
                    to_number(key, item, false)?;
                }
            }
        } else {
            normalize(field)?;
        }
    }
    Ok(())
}

fn to_number(key: &str, field: &mut Value, signed: bool) -> Result<(), Error> {
    let Value::String(s) = field else {
        return Ok(());
    };
    let invalid = || Error::InvalidInput(format!("invalid OTLP JSON: `{key}` value {s:?}"));
    let number = if signed {
        Number::from(s.parse::<i64>().map_err(|_| invalid())?)
    } else {
        Number::from(s.parse::<u64>().map_err(|_| invalid())?)
    };
    *field = Value::Number(number);
    Ok(())
}

fn metrics_mut(request: &mut Value) -> impl Iterator<Item = &mut Map<String, Value>> {
    objects(request, "resourceMetrics")
        .flat_map(|resource| objects(resource, "scopeMetrics"))
        .flat_map(|scope| objects(scope, "metrics"))
        .filter_map(Value::as_object_mut)
}

fn objects<'a>(value: &'a mut Value, key: &str) -> impl Iterator<Item = &'a mut Value> {
    value
        .get_mut(key)
        .and_then(Value::as_array_mut)
        .into_iter()
        .flatten()
}

fn object_items<'a>(
    object: &'a mut Map<String, Value>,
    key: &str,
) -> impl Iterator<Item = &'a mut Map<String, Value>> {
    object
        .get_mut(key)
        .and_then(Value::as_array_mut)
        .into_iter()
        .flatten()
        .filter_map(Value::as_object_mut)
}

/// Adapts data points to the serde model: number and histogram points take
/// timestamps as strings (every other message takes numbers), and messages
/// without `#[serde(default)]` get their proto3 defaults filled in.
fn fill_data_defaults(metric: &mut Map<String, Value>) {
    for key in DATA_KEYS {
        let Some(data) = metric.get_mut(key).and_then(Value::as_object_mut) else {
            continue;
        };
        for point in object_items(data, "dataPoints") {
            match key {
                "exponentialHistogram" => {
                    fill(point, EXPONENTIAL_POINT_DEFAULTS);
                    for side in ["positive", "negative"] {
                        if let Some(buckets) = point.get_mut(side).and_then(Value::as_object_mut) {
                            fill(buckets, BUCKETS_DEFAULTS);
                        }
                    }
                }
                "summary" => {
                    fill(point, SUMMARY_POINT_DEFAULTS);
                    object_items(point, "quantileValues")
                        .for_each(|quantile| fill(quantile, QUANTILE_DEFAULTS));
                }
                _ => {
                    for key in TIMESTAMP_KEYS {
                        if let Some(Value::Number(n)) = point.get(key) {
                            let text = n.to_string();
                            point.insert(key.to_owned(), Value::String(text));
                        }
                    }
                }
            }
            object_items(point, "exemplars").for_each(|exemplar| fill(exemplar, EXEMPLAR_DEFAULTS));
        }
    }
}

type Defaults = &'static [(&'static str, fn() -> Value)];

const EXPONENTIAL_POINT_DEFAULTS: Defaults = &[
    ("attributes", || json!([])),
    ("startTimeUnixNano", || json!(0)),
    ("timeUnixNano", || json!(0)),
    ("count", || json!(0)),
    ("scale", || json!(0)),
    ("zeroCount", || json!(0)),
    ("flags", || json!(0)),
    ("exemplars", || json!([])),
    ("zeroThreshold", || json!(0.0)),
];
const BUCKETS_DEFAULTS: Defaults = &[("offset", || json!(0)), ("bucketCounts", || json!([]))];
const SUMMARY_POINT_DEFAULTS: Defaults = &[
    ("attributes", || json!([])),
    ("startTimeUnixNano", || json!(0)),
    ("timeUnixNano", || json!(0)),
    ("count", || json!(0)),
    ("sum", || json!(0.0)),
    ("quantileValues", || json!([])),
    ("flags", || json!(0)),
];
const QUANTILE_DEFAULTS: Defaults = &[("quantile", || json!(0.0)), ("value", || json!(0.0))];
const EXEMPLAR_DEFAULTS: Defaults = &[
    ("filteredAttributes", || json!([])),
    ("timeUnixNano", || json!(0)),
    ("spanId", || json!("")),
    ("traceId", || json!("")),
];

fn fill(object: &mut Map<String, Value>, defaults: Defaults) {
    for (key, default) in defaults {
        if !object.contains_key(*key) {
            object.insert((*key).to_owned(), default());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::any_value;
    use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point};

    #[test]
    fn should_decode_spec_json_with_string_encoded_integers() {
        // given: the shape the OpenTelemetry Collector's otlphttp exporter emits
        let body = br#"{
          "resourceMetrics": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "api"}}]},
            "scopeMetrics": [{
              "scope": {"name": "lib"},
              "metrics": [
                {"name": "requests", "sum": {
                  "aggregationTemporality": 2, "isMonotonic": true,
                  "dataPoints": [{"timeUnixNano": "1700000000000000000", "asInt": "42",
                                  "attributes": [{"key": "code", "value": {"intValue": "200"}}]}]}},
                {"name": "latency", "histogram": {
                  "aggregationTemporality": 2,
                  "dataPoints": [{"startTimeUnixNano": "1690000000000000000",
                                  "timeUnixNano": "1700000000000000000", "count": "3", "sum": 1.5,
                                  "bucketCounts": ["1", "2"], "explicitBounds": [0.5]}]}},
                {"name": "sizes", "exponentialHistogram": {
                  "aggregationTemporality": 2,
                  "dataPoints": [{"timeUnixNano": "1700000000000000000", "count": "2",
                                  "zeroCount": "1", "scale": 0,
                                  "positive": {"bucketCounts": ["1"]}}]}},
                {"name": "rpc", "summary": {
                  "dataPoints": [{"timeUnixNano": "1700000000000000000", "count": "4",
                                  "sum": 2.0, "quantileValues": [{"quantile": 0.5, "value": 0.4}, {}]}]}}
              ]
            }]
          }]
        }"#;

        // when
        let request = decode_metrics_json(body).unwrap();

        // then
        let metrics = &request.resource_metrics[0].scope_metrics[0].metrics;
        let Some(metric::Data::Sum(sum)) = &metrics[0].data else {
            panic!("expected sum");
        };
        let point = &sum.data_points[0];
        assert_eq!(point.time_unix_nano, 1_700_000_000_000_000_000);
        assert_eq!(point.value, Some(number_data_point::Value::AsInt(42)));
        assert_eq!(
            point.attributes[0].value.as_ref().unwrap().value,
            Some(any_value::Value::IntValue(200))
        );
        let Some(metric::Data::Histogram(histogram)) = &metrics[1].data else {
            panic!("expected histogram");
        };
        assert_eq!(histogram.data_points[0].count, 3);
        assert_eq!(histogram.data_points[0].bucket_counts, vec![1, 2]);
        let Some(metric::Data::ExponentialHistogram(exp)) = &metrics[2].data else {
            panic!("expected exponential histogram");
        };
        assert_eq!(exp.data_points[0].zero_count, 1);
        assert_eq!(
            exp.data_points[0].positive.as_ref().unwrap().bucket_counts,
            vec![1]
        );
        let Some(metric::Data::Summary(summary)) = &metrics[3].data else {
            panic!("expected summary");
        };
        assert_eq!(summary.data_points[0].count, 4);
        assert_eq!(summary.data_points[0].quantile_values.len(), 2);
    }

    #[test]
    fn should_fill_omitted_exemplar_fields() {
        // given: an exemplar without trace context, as proto3 JSON omits empty ids
        let body = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [
          {"name": "g", "gauge": {"dataPoints": [{"timeUnixNano": "1", "asDouble": 1.0,
            "exemplars": [{"timeUnixNano": "1", "asDouble": 1.0}]}]}}
        ]}]}]}"#;

        // when
        let request = decode_metrics_json(body).unwrap();

        // then
        let Some(metric::Data::Gauge(gauge)) =
            &request.resource_metrics[0].scope_metrics[0].metrics[0].data
        else {
            panic!("expected gauge");
        };
        assert_eq!(gauge.data_points[0].exemplars.len(), 1);
    }

    #[test]
    fn should_accept_numeric_integers_and_timestamps() {
        // given
        let body = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [
          {"name": "g", "gauge": {"dataPoints": [{"timeUnixNano": 1700000000000000000, "asDouble": 1.5}]}}
        ]}]}]}"#;

        // when
        let request = decode_metrics_json(body).unwrap();

        // then
        let Some(metric::Data::Gauge(gauge)) =
            &request.resource_metrics[0].scope_metrics[0].metrics[0].data
        else {
            panic!("expected gauge");
        };
        assert_eq!(
            gauge.data_points[0].time_unix_nano,
            1_700_000_000_000_000_000
        );
        assert_eq!(
            gauge.data_points[0].value,
            Some(number_data_point::Value::AsDouble(1.5))
        );
    }

    #[test]
    fn should_reject_malformed_integer_strings() {
        let body = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [
          {"name": "h", "histogram": {"dataPoints": [{"timeUnixNano": "1", "count": "many"}]}}
        ]}]}]}"#;

        let error = decode_metrics_json(body).unwrap_err();

        assert!(error.to_string().contains("count"), "{error}");
    }

    #[test]
    fn should_reject_malformed_data_instead_of_dropping_it() {
        // given: a bucket count that is neither a number nor a numeric string
        let body = br#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [
          {"name": "broken", "exponentialHistogram": {"dataPoints": [
            {"timeUnixNano": "1", "positive": {"bucketCounts": [true]}}
          ]}}
        ]}]}]}"#;

        // when
        let error = decode_metrics_json(body).unwrap_err();

        // then
        assert!(error.to_string().contains("broken"), "{error}");
    }
}
