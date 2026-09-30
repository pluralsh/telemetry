from pathlib import Path

import pytest
from harness.compose import ComposeProject
from harness.meter.normalize import result_len
from harness.meter.suite import (
    CLASSIC_INSTANT_QUERIES,
    CLASSIC_RANGE_QUERIES,
    DISCOVERY_QUERIES,
    INSTANT_QUERIES,
    NATIVE_INSTANT_QUERIES,
    NATIVE_RANGE_QUERIES,
    RANGE_QUERIES,
    AuthCase,
    MeterSuite,
)
from harness.process import wait_http

PRODUCT = Path(__file__).parents[1] / "products" / "meter"


@pytest.fixture(scope="module")
def meter() -> MeterSuite:
    project = ComposeProject(
        file=PRODUCT / "docker-compose.yml",
        name="meter-regression-pytest",
        services=(
            "prometheus",
            "minio",
            "minio-init",
            "meter-writer-0",
            "meter-writer-1",
        ),
        readiness_urls=(
            "http://localhost:19090/-/ready",
            "http://localhost:18080/-/ready",
            "http://localhost:18081/-/ready",
        ),
    )
    with project:
        # Readers require every writer-created shard manifest to exist.
        project.up("meter-reader")
        wait_http("http://localhost:18082/-/ready")
        suite = MeterSuite.from_env()
        suite.seed()
        yield suite


@pytest.mark.docker
def test_remote_write_is_idempotent_and_visible(meter: MeterSuite) -> None:
    response = meter.client.query(
        meter.reader,
        "/api/v1/query",
        [("query", "regression_gauge"), ("time", meter.end_seconds)],
    )
    assert result_len(response) == 2


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "expression", "ignored_labels"),
    (
        *INSTANT_QUERIES,
        ("boundary", "", ()),
    ),
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_instant_promql_matches_prometheus(
    meter: MeterSuite,
    name: str,
    expression: str,
    ignored_labels: tuple[str, ...],
) -> None:
    meter.compare_instant(name, expression, ignored_labels)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "expression", "ignored_labels"),
    RANGE_QUERIES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_range_promql_matches_prometheus(
    meter: MeterSuite,
    name: str,
    expression: str,
    ignored_labels: tuple[str, ...],
) -> None:
    meter.compare_range(name, expression, ignored_labels)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "expression", "ignored_labels"),
    NATIVE_INSTANT_QUERIES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_native_histogram_instant_matches_prometheus(
    meter: MeterSuite,
    name: str,
    expression: str,
    ignored_labels: tuple[str, ...],
) -> None:
    meter.compare_instant(name, expression, ignored_labels)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "expression", "ignored_labels"),
    NATIVE_RANGE_QUERIES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_native_histogram_range_matches_prometheus(
    meter: MeterSuite,
    name: str,
    expression: str,
    ignored_labels: tuple[str, ...],
) -> None:
    meter.compare_range(name, expression, ignored_labels)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "expression", "ignored_labels"),
    CLASSIC_INSTANT_QUERIES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_classic_histogram_instant_matches_prometheus(
    meter: MeterSuite,
    name: str,
    expression: str,
    ignored_labels: tuple[str, ...],
) -> None:
    meter.compare_instant(name, expression, ignored_labels)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "expression", "ignored_labels"),
    CLASSIC_RANGE_QUERIES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_classic_histogram_range_matches_prometheus(
    meter: MeterSuite,
    name: str,
    expression: str,
    ignored_labels: tuple[str, ...],
) -> None:
    meter.compare_range(name, expression, ignored_labels)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "path"),
    DISCOVERY_QUERIES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_discovery_matches_prometheus(
    meter: MeterSuite,
    name: str,
    path: str,
) -> None:
    meter.compare_discovery(name, path)


@pytest.mark.docker
def test_remote_write_metadata_endpoints(meter: MeterSuite) -> None:
    meter.check_remote_write_metadata_endpoints()


@pytest.mark.docker
def test_otlp_ingest_samples_and_metadata(meter: MeterSuite) -> None:
    meter.check_otlp()


@pytest.mark.docker
def test_namespace_isolation(meter: MeterSuite) -> None:
    meter.check_namespace_isolation()


@pytest.mark.docker
@pytest.mark.parametrize(
    "case",
    MeterSuite.from_env().read_auth_cases(),
    ids=lambda case: case.name,
)
def test_read_auth_matrix(meter: MeterSuite, case: AuthCase) -> None:
    meter.check_read_auth(case)


@pytest.mark.docker
@pytest.mark.parametrize(
    "case",
    MeterSuite.from_env().write_auth_cases(),
    ids=lambda case: case.name,
)
def test_write_auth_matrix(meter: MeterSuite, case: AuthCase) -> None:
    meter.check_write_auth(case)


@pytest.mark.docker
def test_reader_is_read_only(meter: MeterSuite) -> None:
    meter.check_reader_rejects_writes()
