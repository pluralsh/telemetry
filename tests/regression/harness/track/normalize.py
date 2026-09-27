"""Semantic normalization for Tempo-compatible trace responses."""

from __future__ import annotations

import base64
from typing import Any


def _field(value: dict[str, Any], camel: str, snake: str) -> Any:
    return value.get(camel, value.get(snake))


def _id(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, list):
        return bytes(value).hex()
    if len(value) in (16, 32):
        try:
            bytes.fromhex(value)
        except ValueError:
            pass
        else:
            return value.lower()
    decoded = base64.b64decode(value)
    return decoded.hex()


def _typed(value: dict[str, Any] | None) -> tuple[str, Any]:
    assert value is not None, "attribute value is missing"
    aliases = {
        "stringValue": "string",
        "string_value": "string",
        "boolValue": "bool",
        "bool_value": "bool",
        "intValue": "int",
        "int_value": "int",
        "doubleValue": "float",
        "double_value": "float",
        "bytesValue": "bytes",
        "bytes_value": "bytes",
    }
    present = [(aliases[key], raw) for key, raw in value.items() if key in aliases]
    assert len(present) == 1, f"expected one typed scalar value, got {value!r}"
    kind, raw = present[0]
    if kind == "int":
        raw = int(raw)
    elif kind == "float":
        raw = float(raw)
    elif kind == "bytes":
        raw = base64.b64decode(raw)
    return kind, raw


def _attributes(
    values: list[dict[str, Any]] | None,
) -> tuple[tuple[str, str, Any], ...]:
    return tuple(
        sorted((item["key"], *_typed(item.get("value"))) for item in values or [])
    )


_KINDS = {
    "SPAN_KIND_UNSPECIFIED": 0,
    "SPAN_KIND_INTERNAL": 1,
    "SPAN_KIND_SERVER": 2,
    "SPAN_KIND_CLIENT": 3,
    "SPAN_KIND_PRODUCER": 4,
    "SPAN_KIND_CONSUMER": 5,
}
_STATUS = {
    "STATUS_CODE_UNSET": 0,
    "STATUS_CODE_OK": 1,
    "STATUS_CODE_ERROR": 2,
}


def _enum(value: Any, names: dict[str, int]) -> int:
    if value is None:
        return 0
    if isinstance(value, str) and value in names:
        return names[value]
    return int(value)


def normalize_trace(response: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    response = response.get("trace", response)
    resources = (
        response.get("batches")
        or response.get("resourceSpans")
        or response.get("resource_spans")
    )
    assert resources is not None, (
        f"trace response has no resource batches: {response!r}"
    )
    spans = []
    for resource_spans in resources:
        resource = resource_spans.get("resource") or {}
        resource_attributes = _attributes(resource.get("attributes"))
        scopes = (
            resource_spans.get("scopeSpans")
            or resource_spans.get("scope_spans")
            or resource_spans.get("instrumentationLibrarySpans")
            or []
        )
        for scope_spans in scopes:
            scope = scope_spans.get("scope") or scope_spans.get(
                "instrumentationLibrary", {}
            )
            scope_key = (
                scope.get("name", ""),
                scope.get("version", ""),
                _attributes(scope.get("attributes")),
            )
            for span in scope_spans.get("spans", []):
                events = tuple(
                    sorted(
                        (
                            int(_field(event, "timeUnixNano", "time_unix_nano") or 0),
                            event.get("name", ""),
                            _attributes(event.get("attributes")),
                        )
                        for event in span.get("events", [])
                    )
                )
                status = span.get("status") or {}
                spans.append(
                    (
                        _id(_field(span, "traceId", "trace_id")),
                        _id(_field(span, "spanId", "span_id")),
                        _id(_field(span, "parentSpanId", "parent_span_id")),
                        span.get("name", ""),
                        _enum(span.get("kind"), _KINDS),
                        int(_field(span, "startTimeUnixNano", "start_time_unix_nano")),
                        int(_field(span, "endTimeUnixNano", "end_time_unix_nano")),
                        resource_attributes,
                        scope_key,
                        _attributes(span.get("attributes")),
                        events,
                        (
                            _enum(status.get("code"), _STATUS),
                            status.get("message", ""),
                        ),
                    )
                )
    return tuple(sorted(spans))


def normalize_search(response: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        sorted(
            (
                item["traceID"].lower(),
                item.get("rootServiceName", ""),
                item.get("rootTraceName", ""),
                int(item.get("startTimeUnixNano", 0)),
                round(float(item.get("durationMs", 0)) * 1_000_000),
            )
            for item in response.get("traces", [])
        )
    )


def normalize_tag_names(
    response: dict[str, Any],
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if "scopes" in response:
        return tuple(
            sorted(
                (scope["name"], tuple(sorted(scope.get("tags", []))))
                for scope in response["scopes"]
            )
        )
    return (("unscoped", tuple(sorted(response.get("tagNames", [])))),)


def normalize_tag_values(response: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    values = response.get("tagValues", [])
    return tuple(
        sorted(
            (
                value.get("type", "string"),
                str(value["value"]),
            )
            if isinstance(value, dict)
            else ("string", str(value))
            for value in values
        )
    )


def assert_trace_equivalent(expected: dict[str, Any], actual: dict[str, Any]) -> None:
    expected_trace = normalize_trace(expected)
    actual_trace = normalize_trace(actual)
    assert actual_trace == expected_trace, (
        f"Track trace:\n{actual_trace!r}\nTempo trace:\n{expected_trace!r}"
    )
