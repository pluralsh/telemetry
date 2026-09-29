"""Dependency-free Loki HTTP client and deterministic wire fixtures."""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


def _varint(value: int) -> bytes:
    output = bytearray()
    while value > 0x7F:
        output.append((value & 0x7F) | 0x80)
        value >>= 7
    output.append(value)
    return bytes(output)


def _field(number: int, wire_type: int) -> bytes:
    return _varint((number << 3) | wire_type)


def _bytes_field(number: int, value: bytes) -> bytes:
    return _field(number, 2) + _varint(len(value)) + value


def _string_field(number: int, value: str) -> bytes:
    return _bytes_field(number, value.encode())


def _timestamp(timestamp_ns: int) -> bytes:
    seconds, nanos = divmod(timestamp_ns, 1_000_000_000)
    return _field(1, 0) + _varint(seconds) + _field(2, 0) + _varint(nanos)


def _snappy_literal(value: bytes) -> bytes:
    """Encode one literal-only raw Snappy block (valid, deterministic, tiny)."""
    length = len(value)
    if length < 61:
        tag = bytes(((length - 1) << 2,))
    else:
        encoded_length = (length - 1).to_bytes(4, "little").rstrip(b"\0")
        tag = bytes(((59 + len(encoded_length)) << 2,)) + encoded_length
    return _varint(length) + tag + value


def snappy_push(*, app: str, line: str, timestamp_ns: int) -> bytes:
    entry = _bytes_field(1, _timestamp(timestamp_ns)) + _string_field(2, line)
    stream = _string_field(1, f'{{app="{app}"}}') + _bytes_field(2, entry)
    return _snappy_literal(_bytes_field(1, stream))


def otlp_json(*, service: str, line: str, timestamp_ns: int) -> bytes:
    return json.dumps(
        {
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": service}}
                        ]
                    },
                    "scopeLogs": [
                        {
                            "logRecords": [
                                {
                                    "timeUnixNano": str(timestamp_ns),
                                    "severityText": "INFO",
                                    "body": {"stringValue": line},
                                }
                            ]
                        }
                    ],
                }
            ]
        },
        separators=(",", ":"),
    ).encode()


@dataclass(frozen=True)
class LokiClient:
    base_url: str
    read_prefix: str = ""
    write_prefix: str = ""

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> bytes:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers=headers or {},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read()

    def push_json(self, streams: list[dict[str, Any]]) -> None:
        self._request(
            "POST",
            f"{self.write_prefix}/loki/api/v1/push",
            body=json.dumps({"streams": streams}).encode(),
            headers={"Content-Type": "application/json"},
        )

    def push_snappy(self, *, app: str, line: str, timestamp_ns: int) -> None:
        self._request(
            "POST",
            f"{self.write_prefix}/loki/api/v1/push",
            body=snappy_push(app=app, line=line, timestamp_ns=timestamp_ns),
            headers={"Content-Type": "application/x-protobuf"},
        )

    def push_otlp(self, *, service: str, line: str, timestamp_ns: int) -> None:
        self._request(
            "POST",
            f"{self.write_prefix}/otlp/v1/logs",
            body=otlp_json(service=service, line=line, timestamp_ns=timestamp_ns),
            headers={"Content-Type": "application/json"},
        )

    def query(
        self,
        query: str,
        *,
        timestamp_ns: int,
        limit: int = 100,
        direction: str = "forward",
    ) -> dict[str, Any]:
        parameters = urllib.parse.urlencode(
            {
                "query": query,
                "time": str(timestamp_ns),
                "limit": limit,
                "direction": direction,
            }
        )
        body = self._request(
            "GET", f"{self.read_prefix}/loki/api/v1/query?{parameters}"
        )
        return json.loads(body)

    def query_range(
        self,
        query: str,
        *,
        start_ns: int,
        end_ns: int,
        step: str = "1s",
        limit: int = 100,
        direction: str = "forward",
        categorize_labels: bool = False,
    ) -> dict[str, Any]:
        parameters = urllib.parse.urlencode(
            {
                "query": query,
                "start": str(start_ns),
                "end": str(end_ns),
                "step": step,
                "limit": limit,
                "direction": direction,
            }
        )
        body = self._request(
            "GET",
            f"{self.read_prefix}/loki/api/v1/query_range?{parameters}",
            headers=(
                {"X-Loki-Response-Encoding-Flags": "categorize-labels"}
                if categorize_labels
                else None
            ),
        )
        return json.loads(body)

    def label_names(self, *, start_ns: int, end_ns: int) -> dict[str, Any]:
        parameters = urllib.parse.urlencode({"start": start_ns, "end": end_ns})
        return json.loads(
            self._request(
                "GET", f"{self.read_prefix}/loki/api/v1/labels?{parameters}"
            )
        )

    def label_values(
        self, name: str, *, start_ns: int, end_ns: int
    ) -> dict[str, Any]:
        parameters = urllib.parse.urlencode({"start": start_ns, "end": end_ns})
        name = urllib.parse.quote(name, safe="")
        return json.loads(
            self._request(
                "GET",
                f"{self.read_prefix}/loki/api/v1/label/{name}/values?{parameters}",
            )
        )

    def series(
        self, selectors: list[str], *, start_ns: int, end_ns: int
    ) -> dict[str, Any]:
        parameters = urllib.parse.urlencode(
            [
                *(("match[]", selector) for selector in selectors),
                ("start", str(start_ns)),
                ("end", str(end_ns)),
            ]
        )
        return json.loads(
            self._request(
                "GET", f"{self.read_prefix}/loki/api/v1/series?{parameters}"
            )
        )

    def wait_for(
        self,
        query: str,
        *,
        start_ns: int,
        end_ns: int,
        timeout: float,
    ) -> tuple[dict[str, Any], float]:
        started = time.monotonic()
        deadline = started + timeout
        while True:
            response = self.query_range(query, start_ns=start_ns, end_ns=end_ns)
            if response["data"]["result"]:
                return response, time.monotonic() - started
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{query!r} was not visible after {timeout}s")
            time.sleep(0.1)
