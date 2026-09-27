import base64

from harness.track.fixture import (
    JAEGER_TRACE_ID,
    TRACE_IDS,
    assert_spans_writer_ranges,
    fixture_shard,
    regression_fixture,
    zipkin_fixture,
)
from harness.track.normalize import (
    normalize_search,
    normalize_tag_values,
    normalize_trace,
)
from harness.track.wire import jaeger_request, protobuf_body, zipkin_body
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)

BASE_NS = 1_700_000_000_000_000_000


def test_fixture_is_deterministic_typed_and_spans_writer_ranges() -> None:
    left = regression_fixture(BASE_NS)
    right = regression_fixture(BASE_NS)
    assert protobuf_body(left.request) == protobuf_body(right.request)
    assert left.trace_ids == TRACE_IDS
    assert_spans_writer_ranges()
    assert {fixture_shard(value) < 8 for value in TRACE_IDS} == {False, True}
    spans = left.request.resource_spans[2].scope_spans[0].spans
    attributes = {value.key: value.value for value in spans[0].attributes}
    assert attributes["http.status_code"].int_value == 500
    assert attributes["regression.error"].bool_value is True
    assert attributes["regression.ratio"].double_value == 2.5


def test_otlp_protobuf_round_trip() -> None:
    fixture = regression_fixture(BASE_NS)
    decoded = ExportTraceServiceRequest.FromString(protobuf_body(fixture.request))
    assert decoded == fixture.request


def test_zipkin_encoding_is_stable() -> None:
    fixture = zipkin_fixture(BASE_NS)
    assert zipkin_body(fixture) == zipkin_body(fixture)
    assert b'"traceId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' in zipkin_body(fixture)


def test_jaeger_encoding_is_stable_and_contains_trace_identity() -> None:
    request = jaeger_request(JAEGER_TRACE_ID, BASE_NS)
    assert request == jaeger_request(JAEGER_TRACE_ID, BASE_NS)
    assert bytes.fromhex(JAEGER_TRACE_ID) in request
    assert b"jaeger-regression" in request


def test_trace_normalization_accepts_tempo_and_otlp_envelopes() -> None:
    trace_id = bytes.fromhex(TRACE_IDS[0])
    span_id = bytes.fromhex("0101010101010101")
    span = {
        "traceId": base64.b64encode(trace_id).decode(),
        "spanId": base64.b64encode(span_id).decode(),
        "name": "root",
        "kind": "SPAN_KIND_SERVER",
        "startTimeUnixNano": "10",
        "endTimeUnixNano": "20",
        "attributes": [{"key": "typed", "value": {"intValue": "7"}}],
        "status": {"code": "STATUS_CODE_OK"},
    }
    batch = {
        "resource": {
            "attributes": [{"key": "service.name", "value": {"stringValue": "api"}}]
        },
        "scopeSpans": [{"scope": {"name": "fixture"}, "spans": [span]}],
    }
    assert normalize_trace({"batches": [batch]}) == normalize_trace(
        {"resourceSpans": [batch]}
    )
    hex_span = {**span, "traceId": TRACE_IDS[0], "spanId": span_id.hex()}
    hex_batch = {
        **batch,
        "scopeSpans": [{"scope": {"name": "fixture"}, "spans": [hex_span]}],
    }
    assert normalize_trace({"resourceSpans": [hex_batch]}) == normalize_trace(
        {"batches": [batch]}
    )
    normalized = normalize_trace({"batches": [batch]})
    assert ("typed", "int", 7) in normalized[0][9]


def test_search_and_typed_tag_normalization_sorts_without_coercion() -> None:
    search = {
        "traces": [
            {
                "traceID": "bb",
                "rootServiceName": "b",
                "rootTraceName": "root",
                "startTimeUnixNano": "2",
                "durationMs": 1.5,
            },
            {
                "traceID": "aa",
                "rootServiceName": "a",
                "rootTraceName": "root",
                "startTimeUnixNano": "1",
                "durationMs": 2,
            },
        ]
    }
    assert normalize_search(search)[0][0] == "aa"
    assert normalize_search(search)[1][-1] == 1_500_000
    assert normalize_tag_values(
        {"tagValues": [{"type": "int", "value": "7"}]}
    ) != normalize_tag_values({"tagValues": [{"type": "string", "value": "7"}]})
