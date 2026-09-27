import jwt
import pytest
import snappy
from harness.meter.auth import JWT_KEY_ID, JWT_SECRET, bearer
from harness.meter.fixture import (
    assert_spans_writer_ranges,
    fixture_shard,
    regression_fixture,
)
from harness.meter.normalize import (
    assert_metadata_equivalent,
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


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    (
        (1.0, 1.0 + 5e-10, True),
        (1.0, 1.0 + 2e-9, False),
        (0.0, 5e-13, True),
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
