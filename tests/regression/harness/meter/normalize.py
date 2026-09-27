"""Prometheus response normalization and differential assertions."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any

REL_TOLERANCE = 1e-9
ABS_TOLERANCE = 1e-12


@dataclass(frozen=True, order=True)
class NormalizedSeries:
    labels: tuple[tuple[str, str], ...]
    values: tuple[tuple[float, float], ...]


def float_close(left: float, right: float) -> bool:
    return math.isclose(
        left,
        right,
        rel_tol=REL_TOLERANCE,
        abs_tol=ABS_TOLERANCE,
    )


def ensure_success(name: str, value: dict[str, Any]) -> None:
    assert value.get("status") == "success", f"{name}: unsuccessful response {value}"


def result_len(value: dict[str, Any]) -> int:
    result = value.get("data", {}).get("result", [])
    return len(result) if isinstance(result, list) else 0


def normalize_query(
    value: dict[str, Any], *, ignored_labels: tuple[str, ...] = ()
) -> tuple[NormalizedSeries, ...]:
    result = value.get("data", {}).get("result")
    if not isinstance(result, list):
        raise AssertionError("query response has no data.result")
    normalized = []
    for entry in result:
        labels = tuple(
            sorted(
                (str(name), str(label_value))
                for name, label_value in entry.get("metric", {}).items()
                if name not in ignored_labels
            )
        )
        samples = entry.get("values")
        if samples is None:
            sample = entry.get("value")
            samples = [] if sample is None else [sample]
        values = tuple((float(sample[0]), float(sample[1])) for sample in samples)
        normalized.append(NormalizedSeries(labels, values))
    return tuple(sorted(normalized))


def assert_query_equivalent(
    name: str,
    expected: dict[str, Any],
    actual: dict[str, Any],
    *,
    ignored_labels: tuple[str, ...] = (),
) -> None:
    ensure_success(name, expected)
    ensure_success(name, actual)
    left = normalize_query(expected, ignored_labels=ignored_labels)
    right = normalize_query(actual, ignored_labels=ignored_labels)
    assert len(left) == len(right), (
        f"{name}: result count differs: expected {len(left)}, got {len(right)}"
    )
    for expected_series, actual_series in zip(left, right, strict=True):
        assert expected_series.labels == actual_series.labels, (
            f"{name}: labels differ: {expected_series} != {actual_series}"
        )
        assert len(expected_series.values) == len(actual_series.values), (
            f"{name}: sample counts differ"
        )
        for (left_time, left_value), (right_time, right_value) in zip(
            expected_series.values,
            actual_series.values,
            strict=True,
        ):
            assert float_close(left_time, right_time), (
                f"{name}: timestamps differ: {left_time} != {right_time}"
            )
            assert float_close(left_value, right_value), (
                f"{name}: values differ: {left_value} != {right_value}"
            )


def _sort_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sort_json(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        items = [_sort_json(item) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True))
    return value


def assert_json_data_equivalent(
    name: str, expected: dict[str, Any], actual: dict[str, Any]
) -> None:
    ensure_success(name, expected)
    ensure_success(name, actual)
    left = _sort_json(expected["data"])
    right = _sort_json(actual["data"])
    assert left == right, f"{name} differs: {left} != {right}"


def normalize_metadata(value: dict[str, Any]) -> Any:
    data = value["data"]
    normalized = {
        metric: [
            {key: item for key, item in entry.items() if key != "help"}
            for entry in entries
        ]
        for metric, entries in data.items()
    }
    return _sort_json(normalized)


def assert_metadata_equivalent(
    name: str, expected: dict[str, Any], actual: dict[str, Any]
) -> None:
    ensure_success(name, expected)
    ensure_success(name, actual)
    left = normalize_metadata(expected)
    right = normalize_metadata(actual)
    assert left == right, f"{name} differs: {left} != {right}"
