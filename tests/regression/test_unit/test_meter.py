import math

import jwt
import pytest
import snappy
from harness.meter.auth import JWT_KEY_ID, JWT_SECRET, bearer
from harness.meter.fixture import (
    CLASSIC_BOUNDS,
    assert_spans_writer_ranges,
    classic_histogram_fixture,
    fixture_shard,
    native_histogram_fixture,
    regression_fixture,
)
from harness.meter.normalize import (
    assert_metadata_equivalent,
    assert_query_equivalent,
    float_close,
    normalize_query,
)
from harness.meter.wire import (
    decode_remote_write,
    remote_write_body,
    remote_write_protobuf,
)

BASE_MS = 1_700_000_000_000


def test_fixture_is_deterministic_and_spans_writer_ranges() -> None:
    value = regression_fixture(BASE_MS)
    assert value == regression_fixture(BASE_MS)
    assert len(value) == 8
    assert_spans_writer_ranges(value)
    assert {fixture_shard(item) < 8 for item in value} == {False, True}


def test_remote_write_is_snappy_protobuf() -> None:
    value = regression_fixture(BASE_MS)
    body = remote_write_body(value)
    decoded = decode_remote_write(body)
    assert snappy.decompress(body) == remote_write_protobuf(value)
    assert len(decoded.timeseries) == len(value)
    for expected, actual in zip(value, decoded.timeseries, strict=True):
        assert [(item.name, item.value) for item in actual.labels] == list(
            expected.labels
        )
        assert [(item.timestamp, item.value) for item in actual.samples] == [
            (item.timestamp_ms, item.value) for item in expected.samples
        ]


def test_native_histograms_encode_as_delta_spans() -> None:
    value = native_histogram_fixture(BASE_MS)
    decoded = decode_remote_write(remote_write_body(value))
    assert len(decoded.timeseries) == 3
    for expected, actual in zip(value, decoded.timeseries, strict=True):
        assert not actual.samples
        assert len(actual.histograms) == len(expected.histograms)
        for source, wire in zip(expected.histograms, actual.histograms, strict=True):
            assert wire.count_int == source.count
            assert wire.schema == source.schema
            assert wire.timestamp == source.timestamp_ms
            assert [(s.offset, s.length) for s in wire.positive_spans] == [
                (offset, len(counts)) for offset, counts in source.positive
            ]
            absolute = [count for _, counts in source.positive for count in counts]
            running = 0
            decoded_counts = []
            for delta in wire.positive_deltas:
                running += delta
                decoded_counts.append(running)
            assert decoded_counts == absolute
            assert list(wire.custom_values) == list(source.custom_values)


def test_native_fixture_resets_instance_b() -> None:
    by_instance = {
        dict(item.labels)["instance"]: item
        for item in native_histogram_fixture(BASE_MS)
        if dict(item.labels)["__name__"] == "regression_native_seconds"
    }
    counts = [histogram.count for histogram in by_instance["b"].histograms]
    assert counts[6] < counts[5]
    growing = [histogram.count for histogram in by_instance["a"].histograms]
    assert growing == sorted(growing)


def test_classic_fixture_buckets_are_cumulative() -> None:
    value = classic_histogram_fixture(BASE_MS)
    counts = {}
    for instance in ("a", "b"):
        by_name_le = {
            (labels["__name__"], labels.get("le")): [s.value for s in item.samples]
            for item in value
            if (labels := dict(item.labels))["instance"] == instance
        }
        buckets = [
            by_name_le[("regression_classic_seconds_bucket", le)]
            for le in CLASSIC_BOUNDS
        ]
        for step in zip(*buckets, strict=True):
            assert list(step) == sorted(step)
        counts[instance] = by_name_le[("regression_classic_seconds_count", None)]
        assert counts[instance] == buckets[-1]
    assert counts["a"] == sorted(counts["a"])
    assert counts["b"][6] < counts["b"][5]


def _histogram_response(buckets: list[list[object]]) -> dict[str, object]:
    return {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {"__name__": "h"},
                    "histogram": [
                        1.0,
                        {"count": "3", "sum": "1.5", "buckets": buckets},
                    ],
                }
            ]
        },
    }


def test_histogram_normalization_and_comparison() -> None:
    expected = _histogram_response([[0, "1", "2", "3"]])
    (value,) = normalize_query(expected)
    assert value.values == ()
    assert value.histograms[0][1].buckets == ((0, 1.0, 2.0, 3.0),)
    assert_query_equivalent("same", expected, _histogram_response([[0, "1", "2", "3"]]))
    with pytest.raises(AssertionError, match="bucket differs"):
        assert_query_equivalent(
            "rule", expected, _histogram_response([[3, "1", "2", "3"]])
        )
    with pytest.raises(AssertionError, match="buckets differ"):
        assert_query_equivalent("missing", expected, _histogram_response([]))


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    (
        (1.0, 1.0 + 5e-10, True),
        (1.0, 1.0 + 2e-9, False),
        (0.0, 5e-13, True),
        (math.nan, math.nan, True),
        (math.nan, 1.0, False),
    ),
)
def test_float_tolerance_boundary(left: float, right: float, expected: bool) -> None:
    assert float_close(left, right) is expected


def test_normalization_sorts_labels_and_series() -> None:
    response = {
        "status": "success",
        "data": {
            "result": [
                {"metric": {"b": "2", "a": "1"}, "value": [1.0, "2.0"]},
                {"metric": {"a": "0"}, "value": [1.0, "1.0"]},
            ]
        },
    }
    value = normalize_query(response)
    assert value[0].labels == (("a", "0"),)
    assert value[1].labels == (("a", "1"), ("b", "2"))


def test_metadata_comparison_ignores_help_only() -> None:
    expected = {
        "status": "success",
        "data": {
            "metric": [
                {"type": "gauge", "help": "description", "unit": ""},
            ]
        },
    }
    actual = {
        "status": "success",
        "data": {
            "metric": [
                {"type": "gauge", "help": "", "unit": ""},
            ]
        },
    }
    assert_metadata_equivalent("metadata", expected, actual)


@pytest.mark.parametrize(
    ("name", "namespace", "permission"),
    (
        ("regression-read-token", "^regression$", "read"),
        ("regression-write", "^regression$", "write"),
        ("other-read", "^other$", "read"),
        ("other-write", "^other$", "write"),
        ("global-read", "^(regression|other)$", "read"),
        ("global-write", "^(regression|other)$", "write"),
    ),
)
def test_jwt_fixtures_match_configured_claims(
    name: str,
    namespace: str,
    permission: str,
) -> None:
    token = bearer(name, now=1_700_000_000).removeprefix("Bearer ")
    assert jwt.get_unverified_header(token)["kid"] == JWT_KEY_ID
    claims = jwt.decode(
        token,
        JWT_SECRET,
        algorithms=["HS256"],
        options={"verify_exp": False},
    )
    assert claims == {
        "exp": 1_700_003_600,
        "namespace": namespace,
        "permission": permission,
    }
