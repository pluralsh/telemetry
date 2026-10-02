"""Canonical Loki response shapes used by differential assertions."""

from __future__ import annotations

import json
import math
from decimal import Decimal
from typing import Any


def _number(value: str) -> str:
    number = Decimal(value)
    if not number.is_finite():
        return str(number)
    normalized = number.normalize()
    return format(normalized, "f")


def _labels(value: dict[str, Any]) -> dict[str, str]:
    return {key: str(value[key]) for key in sorted(value)}


def _metadata(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _metadata(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        items = [_metadata(item) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True))
    return value


def canonicalize(response: dict[str, Any]) -> dict[str, Any]:
    """Remove implementation metadata and deterministically order Loki results."""
    if response.get("status") != "success":
        return response
    data = response["data"]
    if isinstance(data, list):
        return {"status": "success", "data": _metadata(data)}
    kind = data["resultType"]
    result = data["result"]
    if kind == "streams":
        streams = []
        for stream in result:
            values = []
            for value in stream["values"]:
                entry = [str(value[0]), value[1]]
                if len(value) > 2:
                    entry.append(value[2])
                values.append(entry)
            values.sort(
                key=lambda value: (
                    int(value[0]),
                    value[1],
                    json.dumps(value[2:]),
                )
            )
            streams.append({"stream": _labels(stream["stream"]), "values": values})
        # Values break ties between streams whose labels match once
        # `__error_details__` is masked.
        streams.sort(
            key=lambda stream: (
                json.dumps(stream["stream"], sort_keys=True),
                json.dumps(stream["values"]),
            )
        )
        result = streams
    elif kind == "vector":
        result = [
            {
                "metric": _labels(sample["metric"]),
                "value": [str(sample["value"][0]), _number(sample["value"][1])],
            }
            for sample in result
        ]
        result.sort(key=lambda sample: json.dumps(sample["metric"], sort_keys=True))
    elif kind == "matrix":
        result = [
            {
                "metric": _labels(series["metric"]),
                "values": [
                    [str(sample[0]), _number(sample[1])]
                    for sample in sorted(
                        series["values"], key=lambda sample: Decimal(sample[0])
                    )
                ],
            }
            for series in result
        ]
        result.sort(key=lambda series: json.dumps(series["metric"], sort_keys=True))
    elif kind == "scalar":
        result = [str(result[0]), _number(result[1])]
    else:
        raise AssertionError(f"unknown Loki result type {kind!r}")
    return {"status": "success", "data": {"resultType": kind, "result": result}}


def assert_equivalent(left: dict[str, Any], right: dict[str, Any]) -> None:
    left = canonicalize(left)
    right = canonicalize(right)
    if left == right:
        return
    # Loki implementations occasionally choose different last digits for
    # floating-point metric operators. Compare those leaves with tight bounds.
    _assert_value(left, right, path="$")


def _assert_value(left: Any, right: Any, *, path: str) -> None:
    if isinstance(left, dict) and isinstance(right, dict):
        assert left.keys() == right.keys(), f"{path}: {left.keys()} != {right.keys()}"
        for key in left:
            _assert_value(left[key], right[key], path=f"{path}.{key}")
        return
    if isinstance(left, list) and isinstance(right, list):
        assert len(left) == len(right), f"{path}: {len(left)} != {len(right)}"
        for index, (left_item, right_item) in enumerate(zip(left, right, strict=True)):
            _assert_value(left_item, right_item, path=f"{path}[{index}]")
        return
    if isinstance(left, str) and isinstance(right, str):
        try:
            left_number = float(left)
            right_number = float(right)
        except ValueError:
            pass
        else:
            if math.isclose(left_number, right_number, rel_tol=1e-9, abs_tol=1e-12):
                return
    assert left == right, f"{path}: {left!r} != {right!r}"
