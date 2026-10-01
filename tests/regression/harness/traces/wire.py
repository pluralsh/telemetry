"""Wire encodings used by the trace regression suite."""

from __future__ import annotations

import json
import struct
from typing import Any

from google.protobuf.message import Message


def protobuf_body(message: Message) -> bytes:
    return message.SerializeToString(deterministic=True)


def zipkin_body(spans: list[dict[str, Any]]) -> bytes:
    return json.dumps(spans, sort_keys=True, separators=(",", ":")).encode()


def _varint(value: int) -> bytes:
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _field(number: int, wire_type: int, value: bytes) -> bytes:
    return _varint(number << 3 | wire_type) + value


def _bytes(number: int, value: bytes) -> bytes:
    return _field(number, 2, _varint(len(value)) + value)


def _string(number: int, value: str) -> bytes:
    return _bytes(number, value.encode())


def _message(number: int, value: bytes) -> bytes:
    return _bytes(number, value)


def _timestamp(unix_ns: int) -> bytes:
    return _field(1, 0, _varint(unix_ns // 1_000_000_000)) + _field(
        2, 0, _varint(unix_ns % 1_000_000_000)
    )


def _duration(duration_ns: int) -> bytes:
    return _field(1, 0, _varint(duration_ns // 1_000_000_000)) + _field(
        2, 0, _varint(duration_ns % 1_000_000_000)
    )


def _jaeger_string_tag(key: str, value: str) -> bytes:
    return _string(1, key) + _string(3, value)


def jaeger_request(trace_id: str, start_ns: int) -> bytes:
    """Encode an Apache Jaeger API v2 PostSpansRequest without generated stubs."""
    process = _string(1, "jaeger-regression") + _message(
        2, _jaeger_string_tag("deployment.environment", "regression")
    )
    log = _message(1, _timestamp(start_ns + 1_000_000)) + _message(
        2, _jaeger_string_tag("event", "jaeger-event")
    )
    span = b"".join(
        (
            _bytes(1, bytes.fromhex(trace_id)),
            _bytes(2, bytes.fromhex("bbbbbbbbbbbbbbbb")),
            _string(3, "jaeger-root"),
            _field(5, 0, _varint(1)),
            _message(6, _timestamp(start_ns)),
            _message(7, _duration(30_000_000)),
            _message(8, _jaeger_string_tag("http.method", "PUT")),
            _message(
                8,
                _string(1, "regression.ratio")
                + _field(2, 0, _varint(3))
                + _field(6, 1, struct.pack("<d", 1.5)),
            ),
            _message(9, log),
        )
    )
    batch = _message(1, span) + _message(2, process)
    return _message(1, batch)
