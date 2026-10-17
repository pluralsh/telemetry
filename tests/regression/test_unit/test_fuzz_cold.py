from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import os
import random
import threading
import time
import urllib.request
from collections import defaultdict
from collections.abc import Iterator
from contextlib import nullcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
from harness.fuzz import bench, cold
from harness.fuzz.cold import ColdProduct, ColdRunner, ColdService, summarize, traffic
from harness.fuzz.config import ColdConfig, Endpoint, FuzzConfig
from harness.fuzz.history import entry as entries
from harness.fuzz.history import render
from harness.fuzz.logs import PUSH_BYTES, LogsFuzz
from harness.fuzz.metrics import MetricsFuzz
from harness.fuzz.rand import derive
from harness.fuzz.recorder import cold_markdown
from harness.fuzz.runner import Batch, Case, FuzzProduct, Mismatch, Probe
from harness.fuzz.traces import TracesFuzz
from harness.fuzz.transport import Exchange, Request
from harness.metrics.wire import decode_remote_write
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)

PROXY = Path(__file__).resolve().parents[1] / "products" / "latency-proxy" / "proxy.py"
END_NS = 1_750_000_000 * 1_000_000_000


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    def build(product: str, **env: str) -> FuzzConfig:
        monkeypatch.setenv("FUZZ_SEED", "7")
        monkeypatch.setenv("FUZZ_RUN_ID", "unit-run")
        monkeypatch.setenv("FUZZ_OUTPUT_DIR", str(tmp_path / product))
        monkeypatch.setenv("FUZZ_SCENARIO", "cold")
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        return FuzzConfig.from_env(product, default_window="10m")

    return build


def test_cold_config_comes_from_the_environment(config) -> None:
    settings = config(
        "logs",
        FUZZ_COLD_SPAN="6h",
        FUZZ_COLD_REPEATS="5",
        FUZZ_COLD_FAMILIES="log., metric.rate",
        MINIO_FIRST_BYTE_MS="35",
    )
    assert settings.object_store and not settings.historical
    assert settings.request_timeout_s == 120
    assert settings.cold == ColdConfig(
        span_s=6 * 3600,
        repeats=5,
        first_byte_ms=35.0,
        families=("log.", "metric.rate"),
    )
    assert settings.cold.selected("log.needle.1h")
    assert settings.cold.selected("metric.rate_by_app.6h")
    assert not settings.cold.selected("labels.24h")
    assert settings.describe()["cold"]["repeats"] == 5


def test_cold_config_is_absent_elsewhere(config) -> None:
    settings = config("logs", FUZZ_SCENARIO="historical")
    assert settings.cold is None and settings.object_store
    assert settings.request_timeout_s == 30
    assert "cold" not in settings.describe()


@pytest.mark.parametrize("env", [{"FUZZ_COLD_REPEATS": "0"}, {"FUZZ_COLD_SPAN": "30m"}])
def test_invalid_cold_settings_are_rejected(config, env: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        config("logs", **env)


def _load_proxy():
    spec = importlib.util.spec_from_file_location("latency_proxy", PROXY)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_proxy_recognizes_request_lines_only() -> None:
    proxy = _load_proxy()
    assert proxy.request_method(b"GET /bucket/key HTTP/1.1\r\n") == "GET"
    assert proxy.request_method(b"PUT /bucket/key HTTP/1.1\r\n") == "PUT"
    assert proxy.request_method(b"\x00\x01payload bytes") is None
    assert proxy.request_method(b"GETTING more body") is None


def test_proxy_counts_requests_and_bytes_per_client() -> None:
    proxy = _load_proxy()
    reply = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"
    put = b"PUT /b/three HTTP/1.1\r\nContent-Length: 4\r\n\r\nbody"

    async def upstream(reader, writer) -> None:
        while data := await reader.read(65536):
            if data.startswith((b"GET", b"PUT")):
                writer.write(reply)
                await writer.drain()
        writer.close()

    async def scenario() -> dict[str, Any]:
        stats = proxy.Stats()
        origin = await asyncio.start_server(upstream, "127.0.0.1", 0)
        port = origin.sockets[0].getsockname()[1]

        async def serve(reader, writer) -> None:
            await proxy._serve(
                reader, writer, host="127.0.0.1", port=port, delay_s=0, stats=stats
            )

        front = await asyncio.start_server(serve, "127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", front.sockets[0].getsockname()[1]
        )
        for request in (
            b"GET /b/one HTTP/1.1\r\n\r\n",
            b"GET /b/two HTTP/1.1\r\n\r\n",
            put,
        ):
            writer.write(request)
            await writer.drain()
            await reader.readexactly(len(reply))
        writer.close()
        await asyncio.sleep(0.05)
        front.close()
        origin.close()
        return stats.snapshot(20.0)

    snapshot = asyncio.run(scenario())
    assert snapshot["first_byte_ms"] == 20.0
    client = snapshot["clients"]["127.0.0.1"]
    assert client["GET"]["requests"] == 2
    assert client["GET"]["bytes"] == 2 * len(reply)
    assert client["PUT"] == {"requests": 1, "bytes": len(reply), "sent": len(put)}


def test_proxy_parses_request_targets_and_statuses() -> None:
    proxy = _load_proxy()
    head = b"GET /b/k?x=1 HTTP/1.1\r\nHost: m\r\nRange: bytes=10-19\r\n\r\nbody"
    assert proxy.request_target(head) == ("/b/k?x=1", "bytes=10-19")
    assert proxy.request_target(b"PUT /b/k HTTP/1.1\r\n\r\n") == ("/b/k", None)
    assert proxy.response_status(b"HTTP/1.1 206 Partial Content\r\n") == 206
    assert proxy.response_status(b"\x00binary") is None


def test_proxy_timeline_is_off_by_default_and_records_when_enabled() -> None:
    proxy = _load_proxy()
    reply = b"HTTP/1.1 206 Partial Content\r\nContent-Length: 5\r\n\r\nhello"

    async def upstream(reader, writer) -> None:
        while data := await reader.read(65536):
            if data.startswith(b"GET"):
                writer.write(reply)
                await writer.drain()
        writer.close()

    async def scenario(stats) -> None:
        origin = await asyncio.start_server(upstream, "127.0.0.1", 0)
        port = origin.sockets[0].getsockname()[1]

        async def serve(reader, writer) -> None:
            await proxy._serve(
                reader, writer, host="127.0.0.1", port=port, delay_s=0.01, stats=stats
            )

        front = await asyncio.start_server(serve, "127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", front.sockets[0].getsockname()[1]
        )
        for key in (b"one", b"two"):
            writer.write(b"GET /b/" + key + b" HTTP/1.1\r\nRange: bytes=0-4\r\n\r\n")
            await writer.drain()
            await reader.readexactly(len(reply))
        writer.close()
        await asyncio.sleep(0.05)
        front.close()
        origin.close()

    off = proxy.Stats()
    asyncio.run(scenario(off))
    disabled = off.timeline_since(0, None)
    assert not disabled["enabled"] and disabled["next"] == 0
    assert disabled["requests"] == []
    stats = proxy.Stats(timeline=True)
    asyncio.run(scenario(stats))
    requests = stats.timeline_since(0, "127.0.0.1")["requests"]
    assert [(r["seq"], r["path"], r["range"]) for r in requests] == [
        (0, "/b/one", "bytes=0-4"),
        (1, "/b/two", "bytes=0-4"),
    ]
    for request in requests:
        assert request["method"] == "GET" and request["status"] == 206
        assert request["bytes"] == len(reply)
        assert request["first_byte"] - request["start"] >= 10
        assert request["end"] >= request["first_byte"]
    assert requests[1]["start"] >= requests[0]["end"]
    assert stats.timeline_since(1, None)["requests"] == requests[1:]
    assert stats.timeline_since(0, "10.9.9.9")["requests"] == []
    assert stats.timeline_since(0, None)["next"] == 2


def test_proxy_serves_the_timeline(monkeypatch: pytest.MonkeyPatch) -> None:
    proxy = _load_proxy()
    stats = proxy.Stats(timeline=True)
    for client in ("10.0.0.5", "10.0.0.6", "10.0.0.5"):
        stats.begin(client, "GET", b"GET /b/k HTTP/1.1\r\n\r\n")

    async def scenario() -> dict[str, Any]:
        async def report(reader, writer) -> None:
            await proxy._report(reader, writer, stats=stats, first_byte_ms=20.0)

        server = await asyncio.start_server(report, "127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        writer.write(b"GET /timeline?since=1&client=10.0.0.5 HTTP/1.1\r\n\r\n")
        await writer.drain()
        raw = await reader.read()
        server.close()
        return json.loads(raw.split(b"\r\n\r\n", 1)[1])

    value = asyncio.run(scenario())
    assert value["enabled"] and value["next"] == 3
    assert [r["seq"] for r in value["requests"]] == [2]


def test_request_waves_measure_dependent_round_trips() -> None:
    def request(start: float, end: float) -> dict[str, Any]:
        return {"start": start, "end": end, "bytes": 10}

    assert cold.request_waves([])["waves"] == 0
    # Two parallel reads, then one that depends on them, then two more.
    shape = cold.request_waves(
        [
            request(0, 20),
            request(1, 21),
            request(21, 40),
            request(41, 60),
            request(42, 70),
        ]
    )
    assert shape == {
        "requests": 5,
        "bytes": 50,
        "span_ms": 70.0,
        "busy_ms": 69.0,
        "max_inflight": 2,
        "waves": 3,
    }
    assert cold.request_waves([request(0, 10), request(0, 10)])["waves"] == 1
    assert cold.request_waves([{"start": 5, "end": None}])["span_ms"] == 0.0


def test_rollup_passes_counts_successful_passes_of_one_product() -> None:
    scrape = "\n".join(
        [
            "# TYPE telemetry_rollup_passes_total counter",
            'telemetry_rollup_passes_total{product="metrics",outcome="ok"} 2',
            'telemetry_rollup_passes_total{product="metrics",outcome="error"} 5',
            'telemetry_rollup_passes_total{product="traces",outcome="ok"} 7',
            'telemetry_rollup_segments_sealed_total{product="metrics"} 9',
        ]
    )
    assert cold.rollup_passes(scrape, "metrics") == 2
    assert cold.rollup_passes(scrape, "traces") == 7
    assert cold.rollup_passes(scrape, "logs") == 0


@pytest.mark.parametrize("product", ["metrics", "traces"])
def test_settle_config_rolls_up_sooner_than_production(product: str) -> None:
    production = PROXY.parents[1] / product / "production" / "standalone-s3.yaml"
    settle = cold.settle_config(production)
    assert settle.startswith(production.read_text().rstrip())
    assert settle.endswith(
        f"\nrollup:\n  interval_seconds: {cold.ROLLUP_SETTLE_INTERVAL_S}\n"
    )
    compose = (PROXY.parents[1] / product / "docker-compose.yml").read_text()
    variable = f"{product.upper()}_S3_CONFIG"
    assert f"${{{variable}:-./production/standalone-s3.yaml}}" in compose


def test_settle_config_refuses_to_override_configured_rollups(tmp_path: Path) -> None:
    config = tmp_path / "standalone.yaml"
    config.write_text("mode: standalone\nrollup:\n  enabled: false\n")
    with pytest.raises(ValueError):
        cold.settle_config(config)


def test_settle_rollups_waits_for_two_passes_on_a_settle_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    production = tmp_path / "standalone-s3.yaml"
    production.write_text("mode: standalone\n")
    scrapes = iter([0, 1, 2])
    restarts: list[tuple[str, str, bool]] = []

    class Stack(cold.ColdStack):
        def restart(self, service, *, wipe, ready_timeout_s):
            mounted = Path(os.environ["METRICS_S3_CONFIG"])
            restarts.append((service.service, mounted.read_text(), wipe))
            return 0.0

    class Response(contextlib.AbstractContextManager):
        def __init__(self, passes: int) -> None:
            labels = 'product="metrics",outcome="ok"'
            self.body = f"telemetry_rollup_passes_total{{{labels}}} {passes}\n"

        def read(self) -> bytes:
            return self.body.encode()

        def __exit__(self, *exc: object) -> None:
            return None

    urls: list[str] = []

    def urlopen(url: str, timeout: float) -> Response:
        urls.append(url)
        return Response(next(scrapes))

    monkeypatch.setattr(cold.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(cold, "ROLLUP_SETTLE_INTERVAL_S", 0)
    monkeypatch.delenv("METRICS_S3_CONFIG", raising=False)
    stack = Stack(project=None)  # type: ignore[arg-type]
    stack.settle_rollups(
        ColdService("metrics-s3", "http://impl/-/ready"),
        product="metrics",
        config=production,
        env="METRICS_S3_CONFIG",
        metrics_url="http://impl/metrics",
        ready_timeout_s=1.0,
    )
    assert restarts == [
        ("metrics-s3", "mode: standalone\nrollup:\n  interval_seconds: 0\n", False)
    ]
    assert urls == ["http://impl/metrics"] * 3
    assert "METRICS_S3_CONFIG" not in os.environ

    def stuck(url: str, timeout: float) -> Response:
        return Response(1)

    monkeypatch.setattr(cold.urllib.request, "urlopen", stuck)
    with pytest.raises(RuntimeError):
        stack.settle_rollups(
            ColdService("metrics-s3", "http://impl/-/ready"),
            product="metrics",
            config=production,
            env="METRICS_S3_CONFIG",
            metrics_url="http://impl/metrics",
            ready_timeout_s=1.0,
            timeout_s=0.0,
        )
    assert "METRICS_S3_CONFIG" not in os.environ


def test_traffic_is_the_difference_of_two_snapshots() -> None:
    before = {"10.0.0.2": {"GET": {"requests": 3, "bytes": 300, "sent": 30}}}
    after = {
        "10.0.0.2": {
            "GET": {"requests": 7, "bytes": 900, "sent": 70},
            "PUT": {"requests": 1, "bytes": 10, "sent": 5000},
        },
        "10.0.0.3": {"GET": {"requests": 50, "bytes": 1, "sent": 1}},
    }
    assert traffic(before, after, "10.0.0.2") == {
        "requests": 5,
        "get": 4,
        "get_bytes": 600,
        "bytes": 610,
        "sent": 5040,
    }
    assert traffic(before, after, "10.0.0.9")["requests"] == 0
    assert traffic(before, after, None)["get"] == 0


def _measurement(family: str, impl_ms: float, oracle_ms: float, status=200):
    def side(ms: float, gets: int, status) -> dict[str, Any]:
        return {
            "status": status,
            "cold_ms": ms,
            "warm_ms": ms / 10,
            "query": {"requests": gets, "get": gets, "get_bytes": gets * 1000},
        }

    return {
        "family": family,
        "impl": side(impl_ms, 4, status),
        "oracle": side(oracle_ms, 40, 200),
    }


def test_summarize_reports_answered_measurements_per_family() -> None:
    measurements = [
        _measurement("a.1h", 100, 400),
        _measurement("a.1h", 300, 800),
        _measurement("b.24h", 200, 900),
        _measurement("b.24h", 0, 900, status=None),
    ]
    startups = {
        "impl": [{"ready_s": 1.0, "settle_s": 5.0, "get": 3, "get_bytes": 10}],
        "oracle": [{"ready_s": 4.0, "settle_s": 9.0, "get": 30, "get_bytes": 99}],
    }
    summary = summarize(measurements, startups)
    assert summary["measured"] == 4 and summary["answered"] == 3
    assert list(summary["families"]) == ["a.1h", "b.24h"]
    family = summary["families"]["a.1h"]
    assert family["cases"] == 2
    assert family["impl"]["cold_ms"]["p50"] == 100
    assert family["oracle"]["get"]["p50"] == 40
    assert family["impl"]["warm_ms"]["p50"] == 10
    assert summary["families"]["b.24h"]["cases"] == 1
    assert summary["totals"]["oracle"]["get_total"] == 120
    assert summary["startup"]["oracle"]["ready_s"]["max"] == 4.0
    lines = cold_markdown({**summary, **ColdConfig().describe(), "planned": 4})
    assert any(line.startswith("| a.1h | 2 |") for line in lines)
    assert "impl_starts" not in summary


def test_summarize_reports_each_implementation_start() -> None:
    def started(ms: float, gets: int, warm_s: float | None) -> dict[str, Any]:
        startup = {"ready_s": 1.0, "settle_s": 0.0, "get": gets, "get_bytes": gets * 10}
        if warm_s is not None:
            startup["warm_s"] = warm_s
        return {
            "status": 200,
            "cold_ms": ms,
            "warm_ms": None,
            "startup": startup,
            "query": {"requests": 1, "get": 1, "get_bytes": 1},
        }

    measurements = []
    for ms in (100, 300):
        item = _measurement("a.1h", ms, 400)
        item["impl_starts"] = {
            "off": started(ms, 2, None),
            "ready": started(ms * 2, 2, None),
            "warm": started(ms / 2, 9, 1.5),
        }
        measurements.append(item)
    summary = summarize(measurements, {"impl": [], "oracle": []})
    starts = summary["impl_starts"]
    assert list(starts) == ["off", "ready", "warm"]
    assert starts["ready"]["families"]["a.1h"]["cold_ms"]["p50"] == 200
    assert starts["warm"]["totals"]["cold_ms"]["p50"] == 50
    assert starts["warm"]["totals"]["startup"]["warm_s"]["max"] == 1.5
    assert starts["off"]["totals"]["startup"]["warm_s"]["p50"] is None
    assert starts["warm"]["totals"]["startup"]["get"]["p50"] == 9


def test_impl_starts_come_from_the_environment(config) -> None:
    settings = config("metrics", FUZZ_COLD_IMPL_STARTS="off, ready,warm")
    assert settings.cold is not None
    assert settings.cold.impl_starts == ("off", "ready", "warm")
    assert settings.cold.describe()["impl_starts"] == ["off", "ready", "warm"]
    with pytest.raises(ValueError, match="unknown starts"):
        config("metrics", FUZZ_COLD_IMPL_STARTS="cold")


def test_startup_warm_passes_count_every_outcome() -> None:
    runs = "telemetry_cache_warmer_runs_total"
    scrape = "\n".join(
        (
            f"# TYPE {runs} counter",
            runs + '{mode="continuous",product="metrics",status="ok"} 4',
            runs + '{mode="startup",product="metrics",status="ok"} 1',
            runs + '{mode="startup",product="metrics",status="timeout"} 1',
        )
    )
    assert cold.startup_warm_passes(scrape) == 2
    assert cold.startup_warm_passes("telemetry_rollup_passes_total 3") == 0


def test_warmer_off_config_appends_a_disabled_warmer(tmp_path: Path) -> None:
    config = tmp_path / "standalone-s3.yaml"
    config.write_text("mode: standalone\n")
    assert cold.warmer_off_config(config) == (
        "mode: standalone\ncache_warmer:\n  enabled: false\n"
    )
    config.write_text("cache_warmer:\n  enabled: true\n")
    with pytest.raises(ValueError, match="already configures"):
        cold.warmer_off_config(config)


COLD_PRODUCTS = (
    (LogsFuzz, {}),
    (MetricsFuzz, {"FUZZ_METRICS_CONFIG": "production"}),
    (TracesFuzz, {"FUZZ_TRACES_CONFIG": "production"}),
)


def _cold_product(product_type, env: dict[str, str], config):
    settings = config(product_type.name, FUZZ_SCALE="0.05", FUZZ_COLD_SPAN="3h", **env)
    product = product_type(settings)
    batches = list(product.cold_load(derive(7, "cold-data"), END_NS))
    return product, batches


@pytest.mark.parametrize(("product_type", "env"), COLD_PRODUCTS)
def test_cold_batteries_are_reproducible_and_scoped(product_type, env, config):
    first, batches = _cold_product(product_type, env, config)
    second, again = _cold_product(product_type, env, config)
    assert [b.impl.body for b in batches] == [b.impl.body for b in again]
    battery = first.cold_battery(derive(7, "cold-battery"))
    assert [c.request for c in battery] == [
        c.request for c in second.cold_battery(derive(7, "cold-battery"))
    ]
    families = [case.family for case in battery]
    assert len(families) == len(set(families)) >= 10
    assert any(family.endswith("24h") for family in families)
    for case in battery:
        text = json.dumps(case.request.describe())
        assert (
            "unit-run" in text
            or case.family.startswith(("labels", "label_values", "tags."))
            or case.family.startswith("trace.by_id")
        ), case.family
    services = first.cold_services()
    assert set(services) == {"impl", "oracle"}
    assert services["impl"].volume is None and services["oracle"].volume


def test_logs_cold_load_matches_its_probe(config) -> None:
    product, batches = _cold_product(LogsFuzz, {}, config)
    pushed = 0
    for batch in batches:
        assert len(batch.impl.body) <= PUSH_BYTES
        for stream in json.loads(batch.impl.body)["streams"]:
            assert stream["stream"]["fuzz_run"] == "unit-run"
            pushed += len(stream["values"])
    stats = product.cold_stats()
    assert pushed == stats["entries"] > 0
    assert stats["streams"] == 36
    (probe,) = product.cold_probes()
    total = {"data": {"result": [{"value": [0, str(pushed)]}]}}
    assert probe.ready(_exchange(total))
    total["data"]["result"][0]["value"][1] = str(pushed - 1)
    assert not probe.ready(_exchange(total))


def test_metrics_cold_load_counts_every_sample(config) -> None:
    product, batches = _cold_product(
        MetricsFuzz, {"FUZZ_METRICS_CONFIG": "production"}, config
    )
    samples: dict[str, int] = defaultdict(int)
    for batch in batches:
        assert batch.oracle.path == "/api/v1/push"
        for series in decode_remote_write(batch.impl.body).timeseries:
            labels = {label.name: label.value for label in series.labels}
            assert labels["fuzz_run"] == "unit-run"
            samples[labels["__name__"]] += len(series.samples)
    assert samples == product.cold_samples
    assert product.cold_stats()["samples"] == sum(samples.values())
    assert {probe.name for probe in product.cold_probes()} == {
        f"samples:{name}" for name in samples
    }


def test_traces_cold_load_has_a_sentinel_per_hour(config) -> None:
    product, batches = _cold_product(
        TracesFuzz, {"FUZZ_TRACES_CONFIG": "production"}, config
    )
    rounds: set[int] = set()
    spans = 0
    for batch in batches:
        request = ExportTraceServiceRequest()
        request.ParseFromString(batch.impl.body)
        for resource in request.resource_spans:
            attributes = {item.key: item.value for item in resource.resource.attributes}
            if attributes["service.name"].string_value == "fuzz-sentinel":
                rounds.add(attributes["fuzz.round"].int_value)
            else:
                spans += sum(len(scope.spans) for scope in resource.scope_spans)
    assert rounds == {0, 1, 2}
    assert product.cold_stats() == {**product.cold_counts, "hours": 3}
    assert spans == product.cold_counts["spans"]
    assert [probe.name for probe in product.cold_probes()][0] == "sentinels"


def _exchange(value: object, status: int = 200) -> Exchange:
    return Exchange(status, json.dumps(value).encode(), 1.0)


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args: object) -> None:
        pass

    def _reply(self, value: object) -> None:
        body = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers["Content-Length"]))
        self._reply({})

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/ready":
            self._reply({"ready": True})
            return
        number = int(parse_qs(url.query)["n"][0])
        divergent = self.server.divergent and number == 5  # type: ignore[attr-defined]
        self._reply({"n": -number if divergent else number})


@pytest.fixture
def servers() -> Iterator[tuple[str, str]]:
    started = []
    for divergent in (False, True):
        server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        server.divergent = divergent  # type: ignore[attr-defined]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        started.append(server)
    yield tuple(f"http://127.0.0.1:{server.server_port}" for server in started)
    for server in started:
        server.shutdown()


def _equal(oracle: Exchange, impl: Exchange) -> None:
    if oracle.json() != impl.json():
        raise Mismatch(f"{oracle.json()} != {impl.json()}")


class _ColdFake(FuzzProduct, ColdProduct):
    name = "fake"

    def __init__(self, config: FuzzConfig, oracle: str, impl: str) -> None:
        super().__init__(config)
        self.urls = {"oracle": oracle, "impl": impl}
        self.oracle_read = self.oracle_write = Endpoint(oracle)
        self.impl_read = self.impl_write = Endpoint(impl)
        self.flushed = False

    def stack(self):
        return nullcontext()

    def generate_round(self, index: int, rng: random.Random):
        raise NotImplementedError

    def next_case(self, rng: random.Random) -> Case:
        raise NotImplementedError

    def cold_services(self) -> dict[str, ColdService]:
        return {
            "impl": ColdService("impl-svc", f"{self.urls['impl']}/ready"),
            "oracle": ColdService(
                "oracle-svc", f"{self.urls['oracle']}/ready", "oracle-data"
            ),
        }

    def cold_load(self, rng: random.Random, end_ns: int) -> Iterator[Batch]:
        write = Request("POST", "/write", body=b"{}")
        for number in range(2):
            yield Batch(f"cold-{number}", write, write, 10)

    def cold_flush(self, stack) -> None:
        self.flushed = True

    def cold_probes(self) -> list[Probe]:
        return [Probe("ready", Request("GET", "/ready"), lambda value: value.ok())]

    def cold_battery(self, rng: random.Random) -> list[Case]:
        return [
            Case(
                f"fake.{'odd' if n % 2 else 'even'}",
                str(n),
                Request("GET", "/q", (("n", str(n)),)),
                _equal,
            )
            for n in range(1, 6)
        ]

    def cold_stats(self) -> dict[str, Any]:
        return {"items": 20}


class _FakeStack:
    """Every proxy snapshot adds two GETs to the service started last."""

    instances: list[_FakeStack] = []

    def __init__(self, project: object) -> None:
        self.events: list[tuple] = []
        self.active: str | None = None
        self.gets: dict[str, int] = defaultdict(int)
        self.requests: list[dict[str, Any]] = []
        _FakeStack.instances.append(self)

    def stop(self, service: ColdService) -> None:
        self.events.append(("stop", service.service))

    def start(self, service: ColdService, *, wipe: bool, ready_timeout_s: float):
        self.events.append(("start", service.service, wipe))
        self.active = service.service
        return 0.5

    def restart(self, service: ColdService, *, wipe: bool, ready_timeout_s: float):
        self.stop(service)
        return self.start(service, wipe=wipe, ready_timeout_s=ready_timeout_s)

    def address(self, service: str) -> str:
        return f"ip-{service}"

    def proxy_stats(self) -> dict[str, Any]:
        if self.active:
            self.gets[self.active] += 2
            for _ in range(2):
                start = float(len(self.requests))
                self.requests.append(
                    {
                        "seq": len(self.requests),
                        "client": f"ip-{self.active}",
                        "start": start,
                        "end": start + 1,
                        "bytes": 100,
                    }
                )
        return {
            f"ip-{name}": {"GET": {"requests": n, "bytes": 100 * n, "sent": 0}}
            for name, n in self.gets.items()
        }

    def proxy_timeline(self, since: int, client: str | None = None) -> dict[str, Any]:
        return {
            "now": float(len(self.requests)),
            "next": len(self.requests),
            "requests": [
                r
                for r in self.requests
                if r["seq"] >= since and client in (None, r["client"])
            ],
        }


def test_cold_runner_restarts_each_side_before_every_query(
    servers, config, monkeypatch: pytest.MonkeyPatch
) -> None:
    oracle, impl = servers
    monkeypatch.setattr(cold, "ColdStack", _FakeStack)
    monkeypatch.setattr(cold, "SETTLE_CAP_S", 0.0)
    monkeypatch.setattr(cold, "IDLE_POLL_S", 0.0)
    settings = config(
        "fake",
        FUZZ_DURATION="60s",
        FUZZ_COLD_REPEATS="2",
        FUZZ_COLD_SETTLE="0",
        FUZZ_COLD_QUIESCE="0",
    )
    product = _ColdFake(settings, oracle, impl)
    summary = ColdRunner(product, settings, project=None).run()  # type: ignore[arg-type]
    stack = _FakeStack.instances[-1]
    assert product.flushed
    assert summary.outcomes == {"match": 8, "mismatch": 2}
    starts = [event for event in stack.events if event[0] == "start"]
    assert len(starts) == 2 + 2 * 10
    assert all(event[2] for event in starts)
    root = settings.output_dir
    lines = [
        json.loads(line) for line in (root / "cold.jsonl").read_text().splitlines()
    ]
    assert len(lines) == 10
    assert {line["first"] for line in lines} == {"impl", "oracle"}
    assert all(line[side]["query"]["get"] == 2 for line in lines for side in cold.SIDES)
    assert all(line["impl"]["warm_ms"] is not None for line in lines)
    report = json.loads((root / "summary.json").read_text())["cold"]
    assert report["planned"] == report["measured"] == 10
    assert set(report["families"]) == {"fake.even", "fake.odd"}
    assert report["startup"]["impl"]["restarts"] == 10
    assert report["dataset"] == {"items": 20}
    assert "Cold reads:" in (root / "summary.md").read_text()
    assert not (root / "cold_timeline.jsonl").exists()


def test_cold_runner_records_request_timelines_when_asked(
    servers, config, monkeypatch: pytest.MonkeyPatch
) -> None:
    oracle, impl = servers
    monkeypatch.setattr(cold, "ColdStack", _FakeStack)
    monkeypatch.setattr(cold, "SETTLE_CAP_S", 0.0)
    monkeypatch.setattr(cold, "IDLE_POLL_S", 0.0)
    settings = config(
        "fake",
        FUZZ_DURATION="60s",
        FUZZ_COLD_REPEATS="1",
        FUZZ_COLD_SETTLE="0",
        FUZZ_COLD_QUIESCE="0",
        FUZZ_COLD_TIMELINE="1",
    )
    ColdRunner(_ColdFake(settings, oracle, impl), settings, project=None).run()  # type: ignore[arg-type]
    lines = [
        json.loads(line)
        for line in (settings.output_dir / "cold_timeline.jsonl")
        .read_text()
        .splitlines()
    ]
    assert len(lines) == 2 * 5
    for line in lines:
        assert line["client"] == f"ip-{line['side']}-svc"
        # The snapshot after the query falls in the query phase; idle
        # polling before it is startup.
        assert line["query"]["summary"]["requests"] == 2
        assert line["startup"]["summary"]["requests"] >= 2
        assert all(r["client"] == line["client"] for r in line["query"]["requests"])


class _TrafficStack:
    """Each proxy snapshot adds one request of `per_poll` bytes."""

    def __init__(self, per_poll: int) -> None:
        self.per_poll = per_poll
        self.polls = 0

    def proxy_stats(self) -> dict[str, Any]:
        self.polls += 1
        counts = {"requests": self.polls, "bytes": self.polls * self.per_poll}
        return {"ip": {"GET": {**counts, "sent": 0}}}


@pytest.mark.parametrize(("per_poll", "idle"), [(400, True), (256 << 10, False)])
def test_idle_ignores_metadata_polling_but_not_reads(
    config, monkeypatch: pytest.MonkeyPatch, per_poll: int, idle: bool
) -> None:
    monkeypatch.setattr(cold, "IDLE_S", 0.02)
    monkeypatch.setattr(cold, "IDLE_POLL_S", 0.002)
    monkeypatch.setattr(cold, "SETTLE_CAP_S", 0.3)
    runner = object.__new__(ColdRunner)
    runner.settings = config("fake", FUZZ_COLD_SETTLE="0").cold
    runner.stack = _TrafficStack(per_poll)
    started = time.monotonic()
    runner._idle("ip")
    assert (time.monotonic() - started < 0.2) is idle


def test_bench_runs_cold_only_where_there_is_a_battery(tmp_path: Path) -> None:
    plan = bench.parse_plan(
        ["--scenarios", "recent,cold", "--history-dir", str(tmp_path)], {}
    )
    cold_jobs = [job for job in bench.jobs(plan) if job.scenario == "cold"]
    assert cold_jobs == [
        bench.Job("logs", "cold"),
        bench.Job("metrics", "cold"),
        bench.Job("traces", "cold"),
    ]
    env = bench.product_environment(plan, cold_jobs[1], {"FUZZ_METRICS_ORACLE": "x"})
    assert env["FUZZ_SCENARIO"] == "cold"
    assert "FUZZ_METRICS_ORACLE" not in env


def _cold_summary() -> dict[str, Any]:
    measurements = [_measurement("log.needle.24h", 300, 900)] * 3
    startups = {
        side: [{"ready_s": 2.0, "settle_s": 5.0, "get": 7, "get_bytes": 100}] * 3
        for side in cold.SIDES
    }
    return {
        **ColdConfig().describe(),
        "dataset": {"entries": 1000, "streams": 4},
        "planned": 3,
        "timings_s": {"load_s": 10.0},
        **summarize(measurements, startups),
    }


def _entry(cold_summary: dict[str, Any] | None) -> dict[str, Any]:
    config = {
        "product": "logs",
        "seed": 1,
        "run_id": "r",
        "stack": "compose",
        "scale": 1.0,
        "scenario": "cold" if cold_summary else "recent",
    }
    if cold_summary:
        config["cold"] = ColdConfig().describe()
    summary: dict[str, Any] = {"config": config, "elapsed_s": 60.0, "cases": 3}
    if cold_summary:
        summary["cold"] = cold_summary
    return entries.build_entry(
        run={"config": config, "endpoints": {}, "started_at": 1_790_000_000.0},
        summary=summary,
        git={"short": "abc", "commit": "abcdef"},
        host={},
    )


def test_history_entries_carry_cold_results_only_for_cold_runs() -> None:
    plain = _entry(None)
    assert "cold" not in plain["results"] and "cold" not in plain["config"]
    assert "cold" not in render._index_record(plain)
    assert "Cold reads" not in render.render_entry(plain)

    entry = _entry(_cold_summary())
    assert entry["variant"] == "scenario=cold"
    assert entry["config"]["cold"]["repeats"] == 3
    results = entry["results"]["cold"]
    family = results["families"]["log.needle.24h"]
    assert family["impl"]["cold_ms"] == {"p50": 300, "p90": 300, "p99": 300}
    assert family["oracle"]["get"] == {"p50": 40, "max": 40}
    assert results["startup"]["impl"]["restarts"] == 3
    assert results["dataset"] == {"entries": 1000, "streams": 4}
    text = render.render_entry(entry)
    assert "## Cold reads from object storage" in text
    assert "| log.needle.24h | 3 | 300.0 / 300.0 | 900.0 / 900.0 |" in text
    record = render._index_record(entry)
    assert record["cold"]["families"]["log.needle.24h"]["oracle"] == {
        "cold_ms_p50": 900,
        "get_p50": 40,
    }


def test_proxy_stats_endpoint_serves_json(monkeypatch: pytest.MonkeyPatch) -> None:
    proxy = _load_proxy()
    ready = threading.Event()
    holder: dict[str, Any] = {}

    async def serve() -> None:
        stats = proxy.Stats()
        stats.clients["10.0.0.5"]["GET"]["requests"] = 3

        async def report(reader, writer) -> None:
            await proxy._report(reader, writer, stats=stats, first_byte_ms=20.0)

        server = await asyncio.start_server(report, "127.0.0.1", 0)
        holder["port"] = server.sockets[0].getsockname()[1]
        holder["loop"] = asyncio.get_running_loop()
        holder["server"] = server
        ready.set()
        await server.serve_forever()

    thread = threading.Thread(
        target=lambda: asyncio.run(_ignore_cancel(serve())), daemon=True
    )
    thread.start()
    ready.wait(5)
    url = f"http://127.0.0.1:{holder['port']}/"
    with urllib.request.urlopen(url, timeout=5) as response:
        value = json.loads(response.read())
    holder["loop"].call_soon_threadsafe(holder["server"].close)
    assert value["first_byte_ms"] == 20.0
    assert value["clients"]["10.0.0.5"]["GET"]["requests"] == 3


async def _ignore_cancel(coroutine) -> None:
    with contextlib.suppress(asyncio.CancelledError):
        await coroutine
