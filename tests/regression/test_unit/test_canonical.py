import pytest
from harness.canonical import assert_equivalent, canonicalize


def test_streams_and_labels_are_sorted_and_metadata_is_dropped() -> None:
    response = {
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": {"z": "last", "a": "first"},
                    "values": [["2", "second"], ["1", "first"]],
                }
            ],
            "stats": {"summary": {"execTime": 1}},
        },
    }
    assert canonicalize(response) == {
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": {"a": "first", "z": "last"},
                    "values": [["1", "first"], ["2", "second"]],
                }
            ],
        },
    }


@pytest.mark.parametrize("kind", ["vector", "matrix", "scalar"])
def test_metric_numbers_are_normalized(kind: str) -> None:
    results = {
        "vector": [{"metric": {"app": "api"}, "value": [2, "2.000"]}],
        "matrix": [{"metric": {}, "values": [[2, "2.0"], [1, "1.00"]]}],
        "scalar": [2, "2.0000"],
    }
    canonical = canonicalize(
        {"status": "success", "data": {"resultType": kind, "result": results[kind]}}
    )
    assert "2.000" not in str(canonical)


def test_equivalence_allows_tiny_float_differences() -> None:
    left = {
        "status": "success",
        "data": {
            "resultType": "scalar",
            "result": ["1", "1.0000000001"],
        },
    }
    right = {
        "status": "success",
        "data": {"resultType": "scalar", "result": ["1", "1"]},
    }
    assert_equivalent(left, right)
