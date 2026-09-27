from __future__ import annotations

import time
from pathlib import Path

import pytest
from harness.canonical import assert_equivalent, canonicalize
from harness.compose import ComposeProject
from harness.loki import LokiClient

PRODUCT = Path(__file__).parents[1] / "products" / "line"
NAMESPACE = "regression"
LOKI = LokiClient("http://localhost:13100")
LINE = LokiClient(
    "http://localhost:13101",
    read_prefix=f"/read/ns/{NAMESPACE}",
    write_prefix=f"/write/ns/{NAMESPACE}",
)


@pytest.fixture(scope="module")
def stack() -> tuple[ComposeProject, int, int]:
    project = ComposeProject(
        file=PRODUCT / "docker-compose.yml",
        name="line-regression-pytest",
        services=("loki", "line"),
        readiness_urls=(
            "http://localhost:13100/ready",
            "http://localhost:13101/-/ready",
        ),
    )
    with project:
        second = 1_000_000_000
        step_seconds = 5
        now_seconds = time.time_ns() // second
        start = (now_seconds - now_seconds % step_seconds - 30) * second
        end = start + 20 * second
        streams = [
            {
                "stream": {"app": "json", "env": "test"},
                "values": [
                    [str(start + 3 * second), "third"],
                    [str(start + second), "first"],
                    [str(start + 2 * second), "second"],
                ],
            },
            *[
                {
                    "stream": {"app": "sparse", "shard": str(index)},
                    "values": [[str(start + (index % 3) * second), f"sparse-{index}"]],
                }
                for index in range(24)
            ],
            {
                "stream": {"app": "search"},
                "values": [
                    [str(start + 4 * second), "needle"],
                    [str(start + 5 * second), "needle needle needle"],
                    [str(start + 6 * second), "haystack"],
                ],
            },
        ]
        for client in (LOKI, LINE):
            client.push_json(streams)
            client.push_snappy(
                app="protobuf", line="snappy fixture", timestamp_ns=start + 7 * second
            )
            client.push_otlp(
                service="regression-otlp",
                line="otlp fixture",
                timestamp_ns=start + 8 * second,
            )
            client.wait_for('{app="json"}', start_ns=start, end_ns=end, timeout=8)
        yield project, start, end


@pytest.mark.docker
@pytest.mark.parametrize(
    "query",
    [
        '{app="json"}',
        '{app="json"} |= "second"',
        '{app=~"json|protobuf"}',
        '{app="sparse"}',
    ],
)
def test_log_queries_match_loki(
    stack: tuple[ComposeProject, int, int], query: str
) -> None:
    _, start, end = stack
    assert_equivalent(
        LOKI.query_range(query, start_ns=start, end_ns=end),
        LINE.query_range(query, start_ns=start, end_ns=end),
    )


@pytest.mark.docker
def test_metric_endpoints_match_loki(
    stack: tuple[ComposeProject, int, int],
) -> None:
    _, start, end = stack
    query = 'count_over_time({app="json"}[1m])'
    assert_equivalent(
        LOKI.query(query, timestamp_ns=end),
        LINE.query(query, timestamp_ns=end),
    )
    assert_equivalent(
        LOKI.query_range(query, start_ns=start, end_ns=end, step="5s"),
        LINE.query_range(query, start_ns=start, end_ns=end, step="5s"),
    )


@pytest.mark.docker
def test_snappy_and_otlp_fixtures_are_queryable(
    stack: tuple[ComposeProject, int, int],
) -> None:
    _, start, end = stack
    assert_equivalent(
        LOKI.query_range('{app="protobuf"}', start_ns=start, end_ns=end),
        LINE.query_range('{app="protobuf"}', start_ns=start, end_ns=end),
    )
    for client in (LOKI, LINE):
        result = canonicalize(
            client.query_range(
                '{service_name="regression-otlp"}', start_ns=start, end_ns=end
            )
        )
        assert result["data"]["result"][0]["values"][0][1] == "otlp fixture"


@pytest.mark.docker
def test_bm25_is_ranked_and_warm_query_is_stable(
    stack: tuple[ComposeProject, int, int],
) -> None:
    _, start, end = stack
    cold = LINE.query_range(
        '{app="search"} | match "needle"',
        start_ns=start,
        end_ns=end,
        categorize_labels=True,
    )
    warm = LINE.query_range(
        '{app="search"} | match "needle"',
        start_ns=start,
        end_ns=end,
        categorize_labels=True,
    )
    assert cold == warm
    values = cold["data"]["result"][0]["values"]
    lines = [value[1] for value in values]
    assert set(lines) == {"needle needle needle", "needle"}
    scores = [
        float(value[2]["structuredMetadata"]["__line_bm25_score"]) for value in values
    ]
    assert scores == sorted(scores, reverse=True)


@pytest.mark.docker
def test_visibility_lag_stays_within_documented_bound(
    stack: tuple[ComposeProject, int, int],
) -> None:
    _, _, end = stack
    timestamp = end + 1_000_000_000
    LINE.push_json(
        [
            {
                "stream": {"app": "visibility"},
                "values": [[str(timestamp), "visible after periodic flush"]],
            }
        ]
    )
    _, elapsed = LINE.wait_for(
        '{app="visibility"}',
        start_ns=timestamp - 1,
        end_ns=timestamp + 1,
        timeout=4,
    )
    assert elapsed <= 3, f"visibility took {elapsed:.3f}s with a 1s configured interval"


@pytest.mark.docker
@pytest.mark.extended
def test_restart_preserves_logs(
    stack: tuple[ComposeProject, int, int],
) -> None:
    project, start, end = stack
    before = LINE.query_range('{app="json"}', start_ns=start, end_ns=end)
    project.restart("line")
    after = LINE.query_range('{app="json"}', start_ns=start, end_ns=end)
    assert canonicalize(after) == canonicalize(before)


@pytest.mark.docker
@pytest.mark.extended
def test_retention_expires_persisted_pages() -> None:
    project = ComposeProject(
        file=PRODUCT / "docker-compose.yml",
        name="line-retention-pytest",
        services=("line-retention",),
        profiles=("extended",),
        readiness_urls=("http://localhost:13102/-/ready",),
    )
    client = LokiClient(
        "http://localhost:13102",
        read_prefix=f"/read/ns/{NAMESPACE}",
        write_prefix=f"/write/ns/{NAMESPACE}",
    )
    with project:
        timestamp = time.time_ns()
        client.push_json(
            [
                {
                    "stream": {"app": "retention"},
                    "values": [[str(timestamp), "short lived"]],
                }
            ]
        )
        client.wait_for(
            '{app="retention"}',
            start_ns=timestamp - 1,
            end_ns=timestamp + 1,
            timeout=4,
        )
        time.sleep(5)
        # Reopen SlateDB so this verifies persisted TTL behavior rather than an
        # already-materialized in-process page.
        project.restart("line-retention")
        result = client.query_range(
            '{app="retention"}',
            start_ns=timestamp - 1,
            end_ns=timestamp + 1,
        )
        assert result["data"]["result"] == []
