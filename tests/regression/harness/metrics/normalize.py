"""Prometheus response normalization and differential assertions."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any

REL_TOLERANCE = 1e-9
ABS_TOLERANCE = 1e-12


@dataclass(frozen=True, order=True)
class NormalizedHistogram:
    count: float
    sum: float
    # (boundary rule, lower, upper, count) as in the Prometheus JSON encoding.
    buckets: tuple[tuple[int, float, float, float], ...]


@dataclass(frozen=True, order=True)
class NormalizedSeries:
    labels: tuple[tuple[str, str], ...]
    values: tuple[tuple[float, float], ...]
    histograms: tuple[tuple[float, NormalizedHistogram], ...] = ()


def float_close(left: float, right: float) -> bool:
    if math.isnan(left) or math.isnan(right):
        return math.isnan(left) and math.isnan(right)
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
        histograms = entry.get("histograms")
        if histograms is None:
            histogram = entry.get("histogram")
            histograms = [] if histogram is None else [histogram]
        normalized.append(
            NormalizedSeries(
                labels,
                values,
                tuple(
                    (float(timestamp), _normalize_histogram(histogram))
                    for timestamp, histogram in histograms
                ),
            )
        )
    return tuple(sorted(normalized))


def _normalize_histogram(value: dict[str, Any]) -> NormalizedHistogram:
    return NormalizedHistogram(
        count=float(value["count"]),
        sum=float(value["sum"]),
        buckets=tuple(
            (int(rule), float(lower), float(upper), float(count))
            for rule, lower, upper, count in value.get("buckets", ())
        ),
    )


def _assert_histogram_close(
    name: str,
    expected: NormalizedHistogram,
    actual: NormalizedHistogram,
) -> None:
    assert float_close(expected.count, actual.count), (
        f"{name}: histogram counts differ: {expected.count} != {actual.count}"
    )
    assert float_close(expected.sum, actual.sum), (
        f"{name}: histogram sums differ: {expected.sum} != {actual.sum}"
    )
    assert len(expected.buckets) == len(actual.buckets), (
        f"{name}: histogram buckets differ: {expected.buckets} != {actual.buckets}"
    )
    for left, right in zip(expected.buckets, actual.buckets, strict=True):
        assert left[0] == right[0] and all(
            float_close(a, b) for a, b in zip(left[1:], right[1:], strict=True)
        ), f"{name}: histogram bucket differs: {left} != {right}"


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
        assert len(expected_series.histograms) == len(actual_series.histograms), (
            f"{name}: histogram sample counts differ for {expected_series.labels}: "
            f"expected {len(expected_series.histograms)}, "
            f"got {len(actual_series.histograms)}"
        )
        for (left_time, left_histogram), (right_time, right_histogram) in zip(
            expected_series.histograms,
            actual_series.histograms,
            strict=True,
        ):
            assert float_close(left_time, right_time), (
                f"{name}: histogram timestamps differ: {left_time} != {right_time}"
            )
            _assert_histogram_close(name, left_histogram, right_histogram)


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
