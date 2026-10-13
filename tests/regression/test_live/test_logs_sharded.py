"""Logs split across two static-shard writers and read through reader groups:
each reader opens one shard and reads the other from its group over gRPC."""

from __future__ import annotations

import time
import urllib.error
from pathlib import Path

import pytest
from harness.canonical import assert_equivalent
from harness.compose import ComposeProject
from harness.loki import LokiClient
from harness.process import wait_http

PRODUCT = Path(__file__).parents[1] / "products" / "logs"
NAMESPACE = "regression"
LOKI = LokiClient("http://localhost:13100")
WRITERS = tuple(
    LokiClient(f"http://localhost:{port}", write_prefix=f"/write/ns/{NAMESPACE}")
    for port in (13103, 13104)
)
READER_PORTS = (13105, 13106)
READERS = tuple(
    LokiClient(f"http://localhost:{port}", read_prefix=f"/read/ns/{NAMESPACE}")
    for port in READER_PORTS
)
SPARSE_STREAMS = 32
MILLISECOND = 1_000_000


@pytest.fixture(scope="module")
def stack() -> tuple[ComposeProject, int, int]:
    project = ComposeProject(
        file=PRODUCT / "docker-compose.yml",
        name="logs-sharded-regression-pytest",
        services=("loki", "minio", "minio-init", "logs-writer-0", "logs-writer-1"),
        profiles=("sharded",),
        readiness_urls=(
            "http://localhost:13100/ready",
            "http://localhost:13103/-/ready",
            "http://localhost:13104/-/ready",
        ),
    )
    with project:
        # Readers open shard manifests the writers create.
        project.up("logs-reader-0", "logs-reader-1")
        for port in READER_PORTS:
            wait_http(f"http://localhost:{port}/-/ready")
        second = 1_000_000_000
        now_seconds = time.time_ns() // second
        start = (now_seconds - now_seconds % 5 - 40) * second
        end = start + 30 * second
        streams = [
            {
                "stream": {"app": "json", "env": "test"},
                "values": [
                    [str(start + 3 * second), "third"],
                    [str(start + second), "first"],
                    [str(start + 2 * second), "second"],
                ],
            },
            # Enough streams that both shards hold several of them. Loki breaks
            # timestamp and value ties by stream hash, so every entry has its
            # own millisecond and the three busiest streams have distinct
            # counts; limits and topk then cut across shards unambiguously.
            *[
                {
                    "stream": {"app": "sparse", "shard": str(index)},
                    "values": [
                        [
                            str(start + (index % 7) * second + index * MILLISECOND),
                            f"sparse-{index} error",
                        ],
                        [
                            str(
                                start + (index % 5 + 10) * second + index * MILLISECOND
                            ),
                            f"sparse-{index} ok",
                        ],
                        *[
                            [
                                str(
                                    start
                                    + 20 * second
                                    + index * MILLISECOND
                                    + extra * 10 * MILLISECOND
                                ),
                                f"sparse-{index} busy",
                            ]
                            for extra in range(max(0, 5 - index))
                        ],
                    ],
                }
                for index in range(SPARSE_STREAMS)
            ],
        ]
        LOKI.push_json(streams)
        # Each writer receives half the streams and forwards the other shard's.
        WRITERS[0].push_json(streams[: len(streams) // 2])
        WRITERS[1].push_json(streams[len(streams) // 2 :])
        for client in (LOKI, *WRITERS[:1]):
            client.push_snappy(
                app="protobuf", line="snappy fixture", timestamp_ns=start + 7 * second
            )
        for reader in READERS:
            wait_for_streams(reader, '{app="sparse"}', SPARSE_STREAMS, start, end)
            wait_for_streams(reader, '{app="protobuf"}', 1, start, end)
        wait_for_streams(LOKI, '{app="sparse"}', SPARSE_STREAMS, start, end)
        yield project, start, end


def wait_for_streams(
    client: LokiClient, query: str, streams: int, start: int, end: int
) -> None:
    deadline = time.monotonic() + 20
    while True:
        response = client.query_range(query, start_ns=start, end_ns=end, limit=1000)
        if len(response["data"]["result"]) >= streams:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"{client.base_url}: {query} has "
                f"{len(response['data']['result'])}/{streams} streams"
            )
        time.sleep(0.2)


@pytest.mark.docker
@pytest.mark.parametrize("reader", (0, 1))
@pytest.mark.parametrize(
    ("query", "direction", "limit"),
    [
        ('{app="json"}', "forward", 100),
        ('{app="sparse"}', "forward", 1000),
        ('{app="sparse"}', "backward", 1000),
        # Limits cut across streams held by different shards.
        ('{app="sparse"}', "forward", 7),
        ('{app="sparse"}', "backward", 7),
        ('{app="sparse"} |= "error"', "forward", 1000),
        ('{app=~"sparse|protobuf"} != "ok"', "backward", 1000),
    ],
)
def test_log_queries_match_loki(
    stack: tuple[ComposeProject, int, int],
    reader: int,
    query: str,
    direction: str,
    limit: int,
) -> None:
    _, start, end = stack
    assert_equivalent(
        LOKI.query_range(
            query, start_ns=start, end_ns=end, direction=direction, limit=limit
        ),
        READERS[reader].query_range(
            query, start_ns=start, end_ns=end, direction=direction, limit=limit
        ),
    )


@pytest.mark.docker
@pytest.mark.parametrize("reader", (0, 1))
@pytest.mark.parametrize(
    "query",
    [
        'count_over_time({app="sparse"}[1m])',
        'sum(count_over_time({app="sparse"}[1m]))',
        'sum by (app) (count_over_time({app=~"sparse|json"}[10s]))',
        'sum(rate({app="sparse"} |= "error" [30s]))',
    ],
)
def test_metric_queries_match_loki(
    stack: tuple[ComposeProject, int, int], reader: int, query: str
) -> None:
    _, start, end = stack
    assert_equivalent(
        LOKI.query(query, timestamp_ns=end),
        READERS[reader].query(query, timestamp_ns=end),
    )
    assert_equivalent(
        LOKI.query_range(query, start_ns=start, end_ns=end, step="5s"),
        READERS[reader].query_range(query, start_ns=start, end_ns=end, step="5s"),
    )


@pytest.mark.docker
@pytest.mark.parametrize("reader", (0, 1))
def test_topk_across_shards_matches_loki(
    stack: tuple[ComposeProject, int, int], reader: int
) -> None:
    # Instant only: earlier range steps tie, and Loki breaks ties by hash.
    _, _, end = stack
    query = 'topk(3, sum by (shard) (count_over_time({app="sparse"}[1m])))'
    assert_equivalent(
        LOKI.query(query, timestamp_ns=end),
        READERS[reader].query(query, timestamp_ns=end),
    )


@pytest.mark.docker
@pytest.mark.parametrize("reader", (0, 1))
def test_metadata_endpoints_match_loki(
    stack: tuple[ComposeProject, int, int], reader: int
) -> None:
    _, start, end = stack
    client = READERS[reader]
    assert_equivalent(
        LOKI.label_names(start_ns=start, end_ns=end),
        client.label_names(start_ns=start, end_ns=end),
    )
    for label in ("app", "shard"):
        assert_equivalent(
            LOKI.label_values(label, start_ns=start, end_ns=end),
            client.label_values(label, start_ns=start, end_ns=end),
        )
    assert_equivalent(
        LOKI.series(['{app=~"sparse|protobuf"}'], start_ns=start, end_ns=end),
        client.series(['{app=~"sparse|protobuf"}'], start_ns=start, end_ns=end),
    )


@pytest.mark.docker
def test_reader_group_fails_closed_without_its_peer(
    stack: tuple[ComposeProject, int, int],
) -> None:
    project, start, end = stack
    project.execute("stop", "logs-reader-1", timeout=120)
    try:
        with pytest.raises(urllib.error.HTTPError) as error:
            READERS[0].query_range('{app="sparse"}', start_ns=start, end_ns=end)
        assert error.value.code == 503
    finally:
        project.up("logs-reader-1")
        wait_http(f"http://localhost:{READER_PORTS[1]}/-/ready")
    assert_equivalent(
        LOKI.query_range('{app="sparse"}', start_ns=start, end_ns=end, limit=1000),
        READERS[0].query_range(
            '{app="sparse"}', start_ns=start, end_ns=end, limit=1000
        ),
    )
