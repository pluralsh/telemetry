from dataclasses import replace
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


READER_GROUP_URLS = (
    "http://127.0.0.1:18085/read/ns/regression",
    "http://127.0.0.1:18086/read/ns/regression",
)


@pytest.fixture(scope="module")
def stack() -> tuple[ComposeProject, MetricsSuite]:
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
        profiles=("reader-groups",),
    )
    with project:
        # Readers require every writer-created shard manifest to exist.
        project.up("metrics-reader", "metrics-reader-0", "metrics-reader-1")
        for port in (18082, 18085, 18086):
            wait_http(f"http://localhost:{port}/-/ready")
        suite = MetricsSuite.from_env()
        suite.seed()
        yield project, suite


@pytest.fixture(scope="module")
def metrics(stack: tuple[ComposeProject, MetricsSuite]) -> MetricsSuite:
    return stack[1]


def reader_group(suite: MetricsSuite, shard: int) -> MetricsSuite:
    """`suite` querying the reader group serving `shard`, which reads the
    other shard from its group over gRPC."""
    return replace(
        suite,
        reader=replace(suite.reader, base_url=READER_GROUP_URLS[shard]),
    )


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


@pytest.mark.docker
@pytest.mark.parametrize("shard", (0, 1))
def test_reader_groups_match_prometheus(metrics: MetricsSuite, shard: int) -> None:
    suite = reader_group(metrics, shard)
    for name, expression, ignored_labels in (
        *INSTANT_QUERIES,
        *NATIVE_INSTANT_QUERIES,
        *CLASSIC_INSTANT_QUERIES,
        ("boundary", "", ()),
    ):
        suite.compare_instant(name, expression, ignored_labels)
    for name, expression, ignored_labels in (
        *RANGE_QUERIES,
        *NATIVE_RANGE_QUERIES,
        *CLASSIC_RANGE_QUERIES,
    ):
        suite.compare_range(name, expression, ignored_labels)
    for name, path in DISCOVERY_QUERIES:
        suite.compare_discovery(name, path)
    suite.check_namespace_isolation()


@pytest.mark.docker
def test_reader_group_fails_closed_without_its_peer(
    stack: tuple[ComposeProject, MetricsSuite],
) -> None:
    project, metrics = stack
    suite = reader_group(metrics, 0)
    parameters = [("query", "regression_gauge"), ("time", metrics.end_seconds)]
    project.execute("stop", "metrics-reader-1", timeout=120)
    try:
        response = suite.client.request(
            "GET", suite.reader, "/api/v1/query", parameters=parameters
        )
        assert response.status >= 500, response.body
    finally:
        project.up("metrics-reader-1")
        wait_http("http://localhost:18086/-/ready")
    recovered = suite.client.query(suite.reader, "/api/v1/query", parameters)
    assert result_len(recovered) == 2
