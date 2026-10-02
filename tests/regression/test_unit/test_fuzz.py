from __future__ import annotations

import json
import random
import re
import threading
from collections.abc import Iterator
from contextlib import nullcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from harness.fuzz.config import (
    Budget,
    Endpoint,
    FuzzConfig,
    parse_duration,
    parse_range,
)
from harness.fuzz.logs import PUSH_BYTES, LogsFuzz, _grouped_positional
from harness.fuzz.metrics import MetricsFuzz
from harness.fuzz.rand import derive, quote, regex_escape
from harness.fuzz.runner import (
    Batch,
    Case,
    FuzzProduct,
    Mismatch,
    Probe,
    Round,
    Runner,
    classify,
)
from harness.fuzz.traces import TracesFuzz, format_duration
from harness.fuzz.transport import Exchange, Request
from harness.metrics.wire import decode_remote_write
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)

FIXED_NS = 1_750_000_000 * 1_000_000_000
STRING_LITERAL = re.compile(r'"(?:[^"\\]|\\.)*"')


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    def build(product: str, **env: str) -> FuzzConfig:
        monkeypatch.setenv("FUZZ_SEED", "7")
        monkeypatch.setenv("FUZZ_RUN_ID", "unit-run")
        monkeypatch.setenv("FUZZ_OUTPUT_DIR", str(tmp_path / product))
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        return FuzzConfig.from_env(product, default_window="10m")

    return build


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    for module in ("logs", "metrics", "traces"):
        monkeypatch.setattr(f"harness.fuzz.{module}.time.time_ns", lambda: FIXED_NS)


@pytest.mark.parametrize(
    ("text", "seconds"),
    [("90", 90), ("90s", 90), ("30m", 1800), ("1h30m", 5400), ("250ms", 0.25)],
)
def test_durations_parse(text: str, seconds: float) -> None:
    assert parse_duration(text) == seconds


@pytest.mark.parametrize("text", ["", "10x", "m5", "5m 3s"])
def test_invalid_durations_are_rejected(text: str) -> None:
    with pytest.raises(ValueError):
        parse_duration(text)


def test_ranges_parse() -> None:
    assert parse_range("40-160") == (40, 160)
    assert parse_range("5") == (5, 5)
    with pytest.raises(ValueError):
        parse_range("9-3")


def test_budget_uses_the_injected_clock() -> None:
    now = [100.0]
    budget = Budget(10, clock=lambda: now[0])
    now[0] = 104.0
    assert budget.remaining() == 6.0
    assert not budget.expired(reserve=5)
    assert budget.expired(reserve=6)


def test_endpoint_overrides_come_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FUZZ_METRICS_IMPL_READ_URL", "https://remote/read/ns/x/")
    monkeypatch.setenv("FUZZ_METRICS_IMPL_READ_AUTHORIZATION", "")
    monkeypatch.setenv("FUZZ_METRICS_IMPL_READ_HEADERS", '{"X-Scope-OrgID": "x"}')
    from harness.fuzz.config import endpoint_from_env

    endpoint = endpoint_from_env(
        "metrics", "impl_read", Endpoint("http://local", "Basic abc")
    )
    assert endpoint == Endpoint(
        "https://remote/read/ns/x", None, (("X-Scope-OrgID", "x"),)
    )


def test_literal_helpers_escape_for_query_languages() -> None:
    assert quote('a "b" \\c') == '"a \\"b\\" \\\\c"'
    assert regex_escape("a.b|c d") == "a\\.b\\|c d"


def _case(compare) -> Case:
    return Case("unit", "q", Request("GET", "/q"), compare)


def _ok(body: object, status: int = 200) -> Exchange:
    return Exchange(status, json.dumps(body).encode(), 1.0)


def _equal(left: Exchange, right: Exchange) -> None:
    if left.json() != right.json():
        raise Mismatch("different")


@pytest.mark.parametrize(
    ("oracle", "impl", "outcome"),
    [
        (_ok(1), _ok(1), "match"),
        (_ok(1), _ok(2), "mismatch"),
        (_ok(1), Exchange(500, b"boom", 1.0), "impl_error"),
        (_ok(1), Exchange(None, b"", 1.0, "timeout"), "impl_timeout"),
        (Exchange(400, b"bad", 1.0), _ok(1), "oracle_error"),
        (Exchange(400, b"bad", 1.0), Exchange(400, b"bad", 1.0), "both_error"),
    ],
)
def test_classification(oracle: Exchange, impl: Exchange, outcome: str) -> None:
    assert classify(_case(_equal), oracle, impl)[0] == outcome


def test_normalizer_assertions_count_as_mismatches() -> None:
    def compare(left: Exchange, right: Exchange) -> None:
        raise AssertionError("labels differ")

    assert classify(_case(compare), _ok(1), _ok(1)) == (
        "mismatch",
        "AssertionError: labels differ",
    )


PRODUCTS = (LogsFuzz, MetricsFuzz, TracesFuzz)
RUN_SCOPE = {
    "logs": 'fuzz_run="unit-run"',
    "metrics": 'fuzz_run="unit-run"',
    "traces": 'resource.fuzz.run = "unit-run"',
}


def _session(product_type: type[FuzzProduct], config, cases: int = 300):
    product = product_type(config(product_type.name))
    rounds = [
        product.generate_round(index, derive(7, "data", index)) for index in range(3)
    ]
    rng = derive(7, "queries", 0)
    return rounds, [product.next_case(rng) for _ in range(cases)]


@pytest.mark.parametrize("product_type", PRODUCTS, ids=lambda value: value.name)
def test_generation_is_reproducible_from_the_seed(product_type, config) -> None:
    first_rounds, first_cases = _session(product_type, config)
    second_rounds, second_cases = _session(product_type, config)
    assert [batch.oracle.body for r in first_rounds for batch in r.batches] == [
        batch.oracle.body for r in second_rounds for batch in r.batches
    ]
    assert [case.request for case in first_cases] == [
        case.request for case in second_cases
    ]


@pytest.mark.parametrize("product_type", PRODUCTS, ids=lambda value: value.name)
def test_every_data_query_is_scoped_to_the_run(product_type, config) -> None:
    _, cases = _session(product_type, config, cases=1000)
    scope = RUN_SCOPE[product_type.name]
    unscoped_families = {
        "logql.labels",
        "logql.label_values",
        "trace.by_id",
        "tags.names",
        "tags.values",
    }
    families = set()
    selectors = 0
    for case in cases:
        families.add(case.family)
        if case.family in unscoped_families:
            continue
        query = " ".join(value for name, value in case.request.params if name != "time")
        code = STRING_LITERAL.sub('""', query)
        assert code.count("(") == code.count(")"), case.query
        assert code.count("{") == code.count("}"), case.query
        # Every selector or spanset carries the run scope; queries without
        # one (e.g. `minute()`) cannot read anyone's data.
        assert query.count(scope) == code.count("{"), f"{case.family}: {case.query}"
        selectors += code.count("{")
    assert selectors > len(cases) // 2
    # Enough variety that a regression in a generator branch is noticed.
    assert len(families) >= 4, families


@pytest.mark.parametrize("product_type", PRODUCTS, ids=lambda value: value.name)
def test_unscoped_metadata_requires_an_isolated_stack(product_type, config) -> None:
    product = product_type(config(product_type.name, FUZZ_STACK="external"))
    assert not product.config.isolated
    product.generate_round(0, derive(7, "data", 0))
    rng = derive(7, "queries", 0)
    families = {product.next_case(rng).family for _ in range(1000)}
    assert not families & {
        "logql.labels",
        "logql.label_values",
        "tags.names",
        "tags.values",
    }


def test_logs_batches_stay_under_the_push_size(config) -> None:
    product = LogsFuzz(config("logs", FUZZ_SCALE="3"))
    generated = product.generate_round(0, derive(1, "data", 0))
    entries = 0
    for batch in generated.batches:
        body = json.loads(batch.oracle.body)
        entries += sum(len(stream["values"]) for stream in body["streams"])
        assert len(batch.oracle.body) <= PUSH_BYTES * 1.1
    assert entries == generated.stats["entries"] + 4


def test_logs_grouped_positional_queries_are_detected() -> None:
    selector = '{job="fuzz"} | logfmt | unwrap duration(elapsed) [10s]'
    assert _grouped_positional(f"first_over_time({selector}) by (method)")
    assert _grouped_positional(f"sum(last_over_time({selector}) without (pod))")
    assert not _grouped_positional(f"sum by (method) (first_over_time({selector}))")
    assert not _grouped_positional(f"max_over_time({selector}) by (method)")


def test_metrics_batches_decode_as_unique_sorted_remote_write(config) -> None:
    product = MetricsFuzz(config("metrics"))
    for index in range(8):
        generated = product.generate_round(index, derive(3, "data", index))
        seen = set()
        for batch in generated.batches:
            for series in decode_remote_write(batch.oracle.body).timeseries:
                labels = tuple((label.name, label.value) for label in series.labels)
                assert list(labels) == sorted(labels)
                assert labels not in seen
                seen.add(labels)
                timestamps = [sample.timestamp for sample in series.samples]
                assert timestamps == sorted(set(timestamps))


def test_traces_batches_parse_and_keep_parents_in_trace(config) -> None:
    product = TracesFuzz(config("traces"))
    generated = product.generate_round(0, derive(5, "data", 0))
    spans = 0
    for batch in generated.batches:
        request = ExportTraceServiceRequest.FromString(batch.oracle.body)
        for resource in request.resource_spans:
            for scope in resource.scope_spans:
                spans += len(scope.spans)
    assert spans == generated.stats["spans"] + 16


def test_duration_literals() -> None:
    rng = random.Random(0)
    assert format_duration(999, rng) == "999ns"
    assert all(
        format_duration(1_500_000, random.Random(seed)) in ("2ms", "1500000ns")
        for seed in range(20)
    )


class _Handler(BaseHTTPRequestHandler):
    divergent = False

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
        divergent = self.server.divergent and number % 5 == 0  # type: ignore[attr-defined]
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


class _FakeProduct(FuzzProduct):
    name = "fake"

    def __init__(self, config: FuzzConfig, oracle: str, impl: str) -> None:
        super().__init__(config)
        self.oracle_read = self.oracle_write = Endpoint(oracle)
        self.impl_read = self.impl_write = Endpoint(impl)
        self.counter = 0

    def stack(self):
        return nullcontext()

    def generate_round(self, index: int, rng: random.Random) -> Round:
        write = Request("POST", "/write", body=b"{}")
        probe = Probe("ready", Request("GET", "/ready"), lambda value: value.ok())
        return Round(
            index, "unit", [Batch("only", write, write, 1)], [probe], {"i": index}
        )

    def next_case(self, rng: random.Random) -> Case:
        self.counter += 1
        return Case(
            "fake.echo",
            str(self.counter),
            Request("GET", "/q", (("n", str(self.counter)),)),
            _equal,
        )


def test_runner_records_outcomes_and_artifacts(servers, config) -> None:
    oracle, impl = servers
    settings = config(
        "fake",
        FUZZ_DURATION="20s",
        FUZZ_MAX_CASES="25",
        FUZZ_QUERIES_PER_ROUND="10",
        FUZZ_SETTLE="0",
    )
    summary = Runner(_FakeProduct(settings, oracle, impl), settings).run()
    assert summary.outcomes == {"match": 20, "mismatch": 5}
    assert summary.failed and summary.reasons == ["5 mismatch case(s)"]
    root = settings.output_dir
    cases = [
        json.loads(line) for line in (root / "cases.jsonl").read_text().splitlines()
    ]
    assert len(cases) == 25
    assert all("recheck" in case for case in cases if case["outcome"] == "mismatch")
    assert len(list((root / "artifacts").iterdir())) == 5
    assert len(list((root / "data").iterdir())) == 3
    assert json.loads((root / "summary.json").read_text())["cases"] == 25
    assert "fake.echo" in (root / "summary.md").read_text()


class _FakeDocker:
    """Two impl containers, one oracle and shared storage, each burning a
    fixed number of cores per sampled second."""

    def __init__(self) -> None:
        self.calls = 0
        self.cores = {"a": 0.5, "b": 0.25, "o": 2.0, "s": 0.1}
        self.memory = {"a": 100, "b": 50, "o": 400, "s": 10}
        self.services = {
            "a": "writer-0",
            "b": "reader",
            "o": "peer",
            "s": "minio",
        }

    def containers(self, project: str) -> dict[str, str]:
        assert project == "fake-fuzz"
        self.calls += 1
        return self.services

    def usage(self, container: str) -> tuple[int, int]:
        # Each listing advances simulated time by one second.
        cpu_ns = int(self.cores[container] * self.calls * 1e9)
        return cpu_ns, self.memory[container] * self.calls * 1024 * 1024


def test_resource_sampler_attributes_usage_to_roles(tmp_path, monkeypatch) -> None:
    from harness.fuzz import resources

    clock = iter(float(second) for second in range(100))
    monkeypatch.setattr(resources.time, "monotonic", lambda: next(clock))
    sampler = resources.ResourceSampler(
        _FakeDocker(),
        "fake-fuzz",
        {"writer-0": "impl", "reader": "impl", "peer": "oracle"},
        tmp_path / "resources.jsonl",
        interval_s=3600,
    )
    sampler.start()
    sampler._sample()
    summary = sampler.stop()
    roles = summary["roles"]
    assert set(roles) == {"impl", "oracle", "shared"}
    assert roles["impl"]["services"] == ["reader", "writer-0"]
    assert roles["impl"]["cpu_seconds"] == 1.5
    assert roles["impl"]["cpu_cores"]["max"] == 0.75
    assert roles["oracle"]["cpu_cores"]["p95"] == 2.0
    assert roles["impl"]["memory_mib"]["max"] == 450.0
    assert roles["shared"]["memory_mib"]["end"] == 30.0
    assert summary["service_peak_memory_mib"]["peer"] == 1200.0
    lines = (tmp_path / "resources.jsonl").read_text().splitlines()
    assert len(lines) == 12


def test_runner_respects_the_time_budget(servers, config) -> None:
    oracle, _ = servers
    settings = config("fake", FUZZ_DURATION="3s", FUZZ_SETTLE="0")
    summary = Runner(_FakeProduct(settings, oracle, oracle), settings).run()
    elapsed = json.loads((settings.output_dir / "summary.json").read_text())[
        "elapsed_s"
    ]
    assert elapsed <= 3.5
    assert not summary.failed
    assert summary.outcomes["match"] > 0
