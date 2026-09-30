from __future__ import annotations

import time
from pathlib import Path

import pytest
from harness.compose import ComposeProject
from harness.process import wait_http
from harness.track.auth import basic, bearer
from harness.track.client import Target, TraceClient
from harness.track.fixture import JAEGER_TRACE_ID, TRACE_IDS, regression_fixture
from harness.track.suite import TRACEQL_CASES, TrackSuite
from harness.track.wire import protobuf_body

PRODUCT = Path(__file__).parents[1] / "products" / "track"


@pytest.fixture(scope="module")
def track() -> tuple[TrackSuite, ComposeProject]:
    project = ComposeProject(
        file=PRODUCT / "docker-compose.yml",
        name="track-regression-pytest",
        services=(
            "tempo",
            "minio",
            "minio-init",
            "track-writer-0",
            "track-writer-1",
        ),
        readiness_urls=(
            "http://localhost:13200/ready",
            "http://localhost:13201/-/ready",
            "http://localhost:13202/-/ready",
        ),
    )
    with project:
        suite = TrackSuite.create()
        suite.seed()
        project.up("track-reader")
        wait_http("http://localhost:13203/-/ready")
        suite.wait_for_reader()
        yield suite, project


@pytest.mark.docker
@pytest.mark.parametrize("trace_id", (*TRACE_IDS, JAEGER_TRACE_ID))
def test_trace_by_id_matches_tempo(
    track: tuple[TrackSuite, ComposeProject], trace_id: str
) -> None:
    suite, _ = track
    suite.compare_trace(trace_id)


@pytest.mark.docker
@pytest.mark.parametrize(
    ("name", "query", "expected_ids"),
    TRACEQL_CASES,
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_non_metrics_traceql_matches_tempo(
    track: tuple[TrackSuite, ComposeProject],
    name: str,
    query: str,
    expected_ids: set[str],
) -> None:
    del name
    suite, _ = track
    suite.compare_search(query, expected_ids)


@pytest.mark.docker
def test_tag_names_and_typed_values_match_tempo(
    track: tuple[TrackSuite, ComposeProject],
) -> None:
    suite, _ = track
    suite.compare_tags()


@pytest.mark.docker
def test_otlp_http_grpc_zipkin_and_jaeger_are_queryable(
    track: tuple[TrackSuite, ComposeProject],
) -> None:
    suite, _ = track
    suite.compare_search(
        '{ resource."service.name" = "zipkin-regression" }',
        {"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
    )
    suite.compare_search(
        '{ resource."service.name" = "jaeger-regression" }',
        {JAEGER_TRACE_ID},
    )


@pytest.mark.docker
def test_auth_namespace_isolation_and_reader_mode(
    track: tuple[TrackSuite, ComposeProject],
) -> None:
    suite, _ = track
    suite.check_auth_and_namespace_isolation()
    suite.check_reader_rejects_writes()


@pytest.mark.docker
@pytest.mark.extended
def test_reader_restart_preserves_sharded_traces(
    track: tuple[TrackSuite, ComposeProject],
) -> None:
    suite, project = track
    before = suite.client.wait_for_trace(suite.reader, TRACE_IDS[0])
    project.restart("track-reader")
    wait_http("http://localhost:13203/-/ready")
    after = suite.client.wait_for_trace(suite.reader, TRACE_IDS[0])
    from harness.track.normalize import normalize_trace

    assert normalize_trace(after) == normalize_trace(before)


@pytest.mark.docker
@pytest.mark.extended
def test_retention_expires_persisted_traces() -> None:
    project = ComposeProject(
        file=PRODUCT / "docker-compose.yml",
        name="track-retention-pytest",
        services=("track-retention",),
        profiles=("extended",),
        readiness_urls=("http://localhost:13204/-/ready",),
    )
    target = Target(
        "http://localhost:13204",
        "regression",
        bearer("regression-write"),
        read_prefix="/read/ns/regression",
        write_prefix="/write/ns/regression",
    )
    client = TraceClient()
    fixture = regression_fixture(time.time_ns())
    with project:
        assert client.otlp_http(target, protobuf_body(fixture.request)).status == 200
        read_target = Target(
            target.base_url,
            "regression",
            basic("regression-reader", "regression-read"),
            read_prefix=target.read_prefix,
        )
        client.wait_for_trace(read_target, TRACE_IDS[0])
        time.sleep(5)
        project.restart("track-retention")
        response = client.request(
            "GET",
            read_target,
            f"/api/v2/traces/{TRACE_IDS[0]}",
            read=True,
        )
        assert response.status == 404
