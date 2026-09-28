"""Deterministic OTLP trace fixtures shared by Track and Tempo."""

from __future__ import annotations

from dataclasses import dataclass

import blake3
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import (
    AnyValue,
    InstrumentationScope,
    KeyValue,
)
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import (
    ResourceSpans,
    ScopeSpans,
    Span,
    Status,
)

TRACE_IDS = (
    "11111111111111111111111111111111",
    "22222222222222222222222222222222",
    "33333333333333333333333333333333",
    "44444444444444444444444444444444",
)
ZIPKIN_TRACE_ID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
JAEGER_TRACE_ID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


@dataclass(frozen=True)
class TraceFixture:
    request: ExportTraceServiceRequest
    trace_ids: tuple[str, ...]
    start_ns: int
    end_ns: int


def _value(value: str | bool | int | float) -> AnyValue:
    if isinstance(value, bool):
        return AnyValue(bool_value=value)
    if isinstance(value, int):
        return AnyValue(int_value=value)
    if isinstance(value, float):
        return AnyValue(double_value=value)
    return AnyValue(string_value=value)


def _attribute(key: str, value: str | bool | int | float) -> KeyValue:
    return KeyValue(key=key, value=_value(value))


def regression_fixture(base_ns: int) -> TraceFixture:
    resources = []
    for index, trace_id in enumerate(TRACE_IDS):
        start = base_ns + index * 1_000_000_000
        root_id = bytes([index + 1]) * 8
        child_id = bytes([index + 17]) * 8
        error = index == 2
        common = {"trace_id": bytes.fromhex(trace_id)}
        root = Span(
            **common,
            span_id=root_id,
            name=f"checkout-{index}",
            kind=Span.SPAN_KIND_SERVER,
            start_time_unix_nano=start,
            end_time_unix_nano=start + 50_000_000,
            attributes=[
                _attribute("http.method", "GET"),
                _attribute("http.status_code", 500 if error else 200),
                _attribute("regression.error", error),
                _attribute("regression.ratio", index + 0.5),
            ],
            events=[
                Span.Event(
                    time_unix_nano=start + 10_000_000,
                    name="fixture-event",
                    attributes=[_attribute("event.sequence", index)],
                )
            ],
            status=Status(
                code=Status.STATUS_CODE_ERROR if error else Status.STATUS_CODE_OK,
                message="failed" if error else "",
            ),
        )
        child = Span(
            **common,
            span_id=child_id,
            parent_span_id=root_id,
            name="database",
            kind=Span.SPAN_KIND_CLIENT,
            start_time_unix_nano=start + 5_000_000,
            end_time_unix_nano=start + 25_000_000,
            attributes=[
                _attribute("db.system", "postgresql"),
                _attribute("db.rows", index + 1),
            ],
            status=Status(code=Status.STATUS_CODE_OK),
        )
        resources.append(
            ResourceSpans(
                resource=Resource(
                    attributes=[
                        _attribute("service.name", "checkout"),
                        _attribute("deployment.environment", "regression"),
                        _attribute("service.instance.id", f"instance-{index}"),
                    ]
                ),
                scope_spans=[
                    ScopeSpans(
                        scope=InstrumentationScope(
                            name="track-regression", version="1"
                        ),
                        spans=[root, child],
                    )
                ],
            )
        )
    return TraceFixture(
        request=ExportTraceServiceRequest(resource_spans=resources),
        trace_ids=TRACE_IDS,
        start_ns=base_ns,
        end_ns=base_ns + 4_000_000_000,
    )


def fixture_shard(trace_id: str, namespace: str = "regression") -> int:
    namespace_bytes = namespace.encode()
    digest = blake3.blake3(
        len(namespace_bytes).to_bytes(4, "big")
        + namespace_bytes
        + bytes.fromhex(trace_id)
    ).digest()
    return (int.from_bytes(digest[:16], "big") * 16) >> 128


def assert_spans_writer_ranges(trace_ids: tuple[str, ...] = TRACE_IDS) -> None:
    shards = {fixture_shard(trace_id) for trace_id in trace_ids}
    assert any(shard < 8 for shard in shards)
    assert any(shard >= 8 for shard in shards)


def zipkin_fixture(base_ns: int) -> list[dict[str, object]]:
    timestamp_us = base_ns // 1_000
    return [
        {
            "traceId": ZIPKIN_TRACE_ID,
            "id": "aaaaaaaaaaaaaaaa",
            "name": "zipkin-root",
            "kind": "SERVER",
            "timestamp": timestamp_us,
            "duration": 25_000,
            "localEndpoint": {"serviceName": "zipkin-regression"},
            "tags": {"format": "zipkin", "http.method": "POST"},
            "annotations": [
                {"timestamp": timestamp_us + 1_000, "value": "zipkin-event"}
            ],
        }
    ]
