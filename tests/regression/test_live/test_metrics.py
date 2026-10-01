from pathlib import Path

import pytest
from harness.compose import ComposeProject
from harness.metrics.normalize import result_len
from harness.metrics.suite import (
    CLASSIC_INSTANT_QUERIES,
    CLASSIC_RANGE_QUERIES,
    DISCOVERY_QUERIES,
    INSTANT_QUERIES,
    NATIVE_INSTANT_QUERIES,
    NATIVE_RANGE_QUERIES,
    RANGE_QUERIES,
    AuthCase,
    MetricsSuite,
)
from harness.process import wait_http

PRODUCT = Path(__file__).parents[1] / "products" / "metrics"


@pytest.fixture(scope="module")
def metrics() -> MetricsSuite:
    project = ComposeProject(
        file=PRODUCT / "docker-compose.yml",
        name="metrics-regression-pytest",
        services=(
            "prometheus",
            "minio",
            "minio-init",
            "metrics-writer-0",
            "metrics-writer-1",
        ),
        readiness_urls=(
            "http://localhost:19090/-/ready",
            "http://localhost:18080/-/ready",
            "http://localhost:18081/-/ready",
        ),
    )
    with project:
        # Readers require every writer-created shard manifest to exist.
        project.up("metrics-reader")
        wait_http("http://localhost:18082/-/ready")
        suite = MetricsSuite.from_env()
        suite.seed()
        yield suite


@pytest.mark.docker
def test_remote_write_is_idempotent_and_visible(metrics: MetricsSuite) -> None:
    response = metrics.client.query(
        metrics.reader,
        "/api/v1/query",
        [("query", "regression_gauge"), ("time", metrics.end_seconds)],
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
    metrics: MetricsSuite,
    name: str,
    expression: str,
    ignored_labels: tuple[str, ...],
) -> None:
    metrics.compare_instant(name, expression, ignored_labels)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "expression", "ignored_labels"),
    RANGE_QUERIES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_range_promql_matches_prometheus(
    metrics: MetricsSuite,
    name: str,
    expression: str,
    ignored_labels: tuple[str, ...],
) -> None:
    metrics.compare_range(name, expression, ignored_labels)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "expression", "ignored_labels"),
    NATIVE_INSTANT_QUERIES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_native_histogram_instant_matches_prometheus(
    metrics: MetricsSuite,
    name: str,
    expression: str,
    ignored_labels: tuple[str, ...],
) -> None:
    metrics.compare_instant(name, expression, ignored_labels)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "expression", "ignored_labels"),
    NATIVE_RANGE_QUERIES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_native_histogram_range_matches_prometheus(
    metrics: MetricsSuite,
    name: str,
    expression: str,
    ignored_labels: tuple[str, ...],
) -> None:
    metrics.compare_range(name, expression, ignored_labels)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "expression", "ignored_labels"),
    CLASSIC_INSTANT_QUERIES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_classic_histogram_instant_matches_prometheus(
    metrics: MetricsSuite,
    name: str,
    expression: str,
    ignored_labels: tuple[str, ...],
) -> None:
    metrics.compare_instant(name, expression, ignored_labels)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "expression", "ignored_labels"),
    CLASSIC_RANGE_QUERIES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_classic_histogram_range_matches_prometheus(
    metrics: MetricsSuite,
    name: str,
    expression: str,
    ignored_labels: tuple[str, ...],
) -> None:
    metrics.compare_range(name, expression, ignored_labels)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "path"),
    DISCOVERY_QUERIES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_discovery_matches_prometheus(
    metrics: MetricsSuite,
    name: str,
    path: str,
) -> None:
    metrics.compare_discovery(name, path)


@pytest.mark.docker
def test_remote_write_metadata_endpoints(metrics: MetricsSuite) -> None:
    metrics.check_remote_write_metadata_endpoints()


@pytest.mark.docker
def test_otlp_ingest_samples_and_metadata(metrics: MetricsSuite) -> None:
    metrics.check_otlp()


@pytest.mark.docker
def test_namespace_isolation(metrics: MetricsSuite) -> None:
    metrics.check_namespace_isolation()


@pytest.mark.docker
@pytest.mark.parametrize(
    "case",
    MetricsSuite.from_env().read_auth_cases(),
    ids=lambda case: case.name,
)
def test_read_auth_matrix(metrics: MetricsSuite, case: AuthCase) -> None:
    metrics.check_read_auth(case)


@pytest.mark.docker
@pytest.mark.parametrize(
    "case",
    MetricsSuite.from_env().write_auth_cases(),
    ids=lambda case: case.name,
)
def test_write_auth_matrix(metrics: MetricsSuite, case: AuthCase) -> None:
    metrics.check_write_auth(case)


@pytest.mark.docker
def test_reader_is_read_only(metrics: MetricsSuite) -> None:
    metrics.check_reader_rejects_writes()
