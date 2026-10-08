from __future__ import annotations

import copy
import json
import random
import threading
from collections.abc import Iterator
from contextlib import nullcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from harness.fuzz import bench, cpus, history
from harness.fuzz.config import Endpoint, FuzzConfig
from harness.fuzz.history import entry as entries
from harness.fuzz.history import environment, render, store
from harness.fuzz.runner import Batch, Case, FuzzProduct, Probe, Round, run_fuzz
from harness.fuzz.transport import Exchange, Request

STARTED = 1_790_000_000.25
GIT = {
    "commit": "0123456789abcdef",
    "short": "0123456789",
    "branch": "main",
    "dirty": False,
}
HOST = {
    "label": "unit",
    "os": "Linux",
    "os_release": "6.8",
    "arch": "x86_64",
    "cpu": "Example CPU",
    "cores": 8,
    "memory_gib": 32.0,
    "python": "3.13.7",
    "docker": {
        "version": "28.0",
        "os": "Ubuntu",
        "arch": "x86_64",
        "cpus": 8,
        "memory_gib": 31.2,
    },
}


def _latency(base: float) -> dict[str, float]:
    return {
        "p50": base,
        "p90": base * 2,
        "p99": base * 4.123456,
        "p99.9": base * 5,
        "max": base * 6,
    }


def _run(product: str = "metrics", seed: int = 0xABCDEF01) -> dict[str, Any]:
    return {
        "config": {"product": product, "seed": seed, "run_id": f"run-{product}"},
        "endpoints": {
            "oracle": "prometheus",
            "oracle_read": {"url": "http://127.0.0.1:19090", "authorization": False},
            "impl_read": {"url": "http://127.0.0.1:18082", "authorization": True},
        },
        "started_at": STARTED,
    }


def _summary(product: str = "metrics", seed: int = 0xABCDEF01) -> dict[str, Any]:
    return {
        "config": {
            "product": product,
            "duration_s": 1800.0,
            "seed": seed,
            "run_id": f"run-{product}",
            "stack": "compose",
            "isolated": True,
            "scale": 1.0,
            "window_s": 1800.0,
            "queries_per_round": [40, 160],
            "max_rounds": 0,
            "max_cases": 0,
            "request_timeout_s": 30.0,
            "visibility_timeout_s": 90.0,
            "latency_ratio": 10.0,
            "latency_floor_ms": 250.0,
            "recheck": True,
            "fail_on": ["impl_error", "mismatch"],
        },
        "elapsed_s": 1799.6,
        "failed": True,
        "reasons": ["2 mismatch case(s)", "a | pipe"],
        "notes": [f"note {number}" for number in range(25)],
        "rounds": 12,
        "invisible_rounds": [3],
        "cases": 100,
        "outcomes": {"mismatch": 2, "match": 95, "both_error": 3},
        "ingest": {
            "outcomes": {"ok": 24},
            "oracle_ms": _latency(3.0),
            "impl_ms": _latency(20.0),
        },
        "latency": {
            "oracle_ms": _latency(4.0),
            "impl_ms": _latency(2.0),
            "ratio": {"p50": 0.5, "p90": 0.8, "p99": 1.5},
            "outliers": 1,
            "top_outliers": [{"id": "r0001-q000001", "query": "secret-ish"}],
        },
        "families": {
            "promql.range": {
                "outcomes": {"match": 50, "mismatch": 2},
                "oracle_ms": _latency(5.0),
                "impl_ms": _latency(2.5),
                "ratio": {"p50": 0.5, "p90": 0.9},
            },
            "promql.instant": {
                "outcomes": {"match": 45, "both_error": 3},
                "oracle_ms": _latency(3.0),
                "impl_ms": _latency(1.5),
                "ratio": {"p50": 0.5, "p90": 0.7},
            },
        },
        "failures": [{"id": "r0001-q000002", "detail": "huge body"}],
        "resources": {
            "interval_s": 2.0,
            "elapsed_s": 1800.1,
            "samples_failed": 0,
            "roles": {
                "oracle": {
                    "services": ["prometheus"],
                    "cpu_seconds": 120.0,
                    "cpu_cores": {"mean": 0.0667, "p95": 0.2, "max": 0.9},
                    "memory_mib": {
                        "mean": 100.0,
                        "p95": 150.0,
                        "max": 160.0,
                        "end": 155.0,
                    },
                },
                "impl": {
                    "services": ["metrics-reader", "metrics-writer-0"],
                    "cpu_seconds": 60.0,
                    "cpu_cores": {"mean": 0.0333, "p95": 0.1, "max": 0.5},
                    "memory_mib": {"mean": 50.0, "p95": 70.0, "max": 80.0, "end": 75.0},
                },
            },
            "service_peak_memory_mib": {"metrics-reader": 40.0, "prometheus": 160.0},
            "service_cpu_seconds": {"metrics-reader": 20.0, "prometheus": 120.0},
        },
    }


def _entry(product: str = "metrics", started: float = STARTED, **git: Any):
    run = _run(product)
    run["started_at"] = started
    return entries.build_entry(
        run=run,
        summary=_summary(product),
        git={**GIT, **git},
        host=HOST,
        environment={"FUZZ_METRICS_ORACLE": "prometheus"},
        images={"prometheus": {"image": "prom/prometheus:v3", "id": "abc123def456"}},
        batch="bench-unit",
    )


def test_entry_keeps_summary_level_data_only() -> None:
    entry = _entry()
    assert entry["schema_version"] == entries.SCHEMA_VERSION
    assert entry["name"] == "2026-09-21T1413Z-0123456789-abcdef01"
    assert entry["started_at"] == "2026-09-21T14:13:20Z"
    assert entry["finished_at"] == "2026-09-21T14:43:19Z"
    assert entry["settings"] == {"oracle": "prometheus"}
    assert entry["variant"] == "oracle=prometheus"
    results = entry["results"]
    assert results["status"] == "failed"
    assert list(results["outcomes"]) == ["match", "mismatch", "both_error"]
    assert results["non_match"] == 5
    assert results["invisible_rounds"] == 1
    assert len(results["notes"]) == 20 and results["notes_truncated"] == 5
    assert list(results["families"]) == ["promql.instant", "promql.range"]
    assert results["families"]["promql.range"]["non_match"] == 2
    assert results["latency"]["impl_ms"]["p99"] == 8.247
    assert results["ingest"]["batches"] == 24
    assert list(results["resources"]["roles"]) == ["impl", "oracle"]
    assert results["resources"]["services"]["prometheus"] == {
        "cpu_seconds": 120.0,
        "peak_memory_mib": 160.0,
    }
    text = json.dumps(entry)
    for leaked in ("top_outliers", "failures", "huge body", "secret-ish", "19090"):
        assert leaked not in text


def test_entry_names_mark_dirty_trees_and_missing_git() -> None:
    assert _entry(dirty=True)["name"].endswith("-0123456789-dirty-abcdef01")
    assert "-nogit-" in _entry(short=None, commit=None)["name"]


def test_variant_includes_non_default_scale_and_stack() -> None:
    entry = _entry()
    entry["config"] = {**entry["config"], "scale": 4.0, "stack": "external"}
    assert entries.variant(entry) == "oracle=prometheus, stack=external, scale=4.0"


def test_rendered_entry_escapes_table_cells() -> None:
    markdown = render.render_entry(_entry())
    assert markdown.startswith("# metrics fuzz benchmark, 2026-09-21 14:13Z (failed)")
    assert "- a | pipe" in markdown
    assert "| promql.range | 52 | 2 |" in markdown
    assert "| implementation | 2.0 | 4.0 | 8.2 | 10.0 | 12.0 |" in markdown
    assert "| impl | metrics-reader, metrics-writer-0 | 60.0 |" in markdown
    assert "- … 5 more" in markdown
    row = render.history_row(_entry(), "x.md")
    assert row.count(" | ") == len(render.history_header()[0].split(" | ")) - 1


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _populate(root: Path, order: list[dict[str, Any]]) -> None:
    for entry in order:
        store.write_entry(root, copy.deepcopy(entry))
    store.rebuild(root)


def test_rebuild_is_deterministic_and_order_independent(tmp_path: Path) -> None:
    runs = [
        _entry("metrics", STARTED),
        _entry("metrics", STARTED + 86_400),
        _entry("traces", STARTED + 3600),
    ]
    first, second = tmp_path / "a", tmp_path / "b"
    _populate(first, runs)
    _populate(second, list(reversed(runs)))
    assert _snapshot(first) == _snapshot(second)
    assert store.rebuild(first) == []
    readme = (first / "README.md").read_text()
    assert readme.index("2026-09-22 14:13Z") < readme.index("2026-09-21 14:13Z")
    assert "| metrics | 2026-09-22 14:13Z |" in readme
    assert "No runs recorded yet." not in readme
    assert "No runs recorded yet." in (first / "history/logs/README.md").read_text()
    index = json.loads((first / "index.json").read_text())
    assert [item["product"] for item in index["entries"]] == [
        "metrics",
        "metrics",
        "traces",
    ]
    assert index["products"]["metrics"] == {
        "runs": 2,
        "latest": runs[1]["name"],
        "readme": "history/metrics/README.md",
    }
    record = index["entries"][0]
    assert record["latency"]["impl_ms"]["p50"] == 2.0
    assert record["resources"]["impl"] == {
        "cpu_cores_mean": 0.033,
        "memory_mib_p95": 70.0,
    }
    assert (first / record["markdown"]).is_file()


def test_rebuild_of_an_empty_directory_says_no_runs(tmp_path: Path) -> None:
    changed = store.rebuild(tmp_path)
    assert {path.relative_to(tmp_path).as_posix() for path in changed} == {
        "README.md",
        "index.json",
        "history/logs/README.md",
        "history/metrics/README.md",
        "history/traces/README.md",
    }
    assert "No runs recorded yet." in (tmp_path / "README.md").read_text()
    assert json.loads((tmp_path / "index.json").read_text())["entries"] == []


def test_rebuild_removes_pages_of_deleted_entries(tmp_path: Path) -> None:
    entry = _entry()
    path = store.write_entry(tmp_path, entry)
    store.rebuild(tmp_path)
    page = path.with_suffix(".md")
    assert page.is_file()
    path.unlink()
    assert page in store.rebuild(tmp_path)
    assert not page.exists()


def test_entries_are_never_overwritten(tmp_path: Path) -> None:
    first = store.write_entry(tmp_path, _entry())
    second = store.write_entry(tmp_path, _entry())
    assert second.name == first.stem + "-2.json"
    assert json.loads(second.read_text())["name"] == first.stem + "-2"
    assert len(store.load_entries(tmp_path)) == 2


def test_unknown_schema_versions_are_rejected(tmp_path: Path) -> None:
    path = store.write_entry(tmp_path, _entry())
    path.write_text(json.dumps({**_entry(), "schema_version": 99}))
    with pytest.raises(ValueError, match="schema_version"):
        store.load_entries(tmp_path)


def test_history_is_off_unless_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(history.HISTORY_ENV, raising=False)
    assert history.history_dir() is None
    monkeypatch.setenv(history.HISTORY_ENV, "documentation/benchmarks/fuzz")
    assert history.history_dir() == history.DEFAULT_DIR


def test_environment_keeps_only_benchmark_settings() -> None:
    captured = environment.collect_environment(
        {
            "FUZZ_TRACES_CONFIG": "production",
            "FUZZ_METRICS_IMPL_READ_AUTHORIZATION": "Bearer secret",
            "FUZZ_LOGS_STORAGE": "",
        }
    )
    assert captured == {"FUZZ_TRACES_CONFIG": "production"}
    assert environment.collect_ci({}) is None
    assert (
        environment.collect_ci({"GITHUB_ACTIONS": "true", "GITHUB_RUN_ID": "7"})[
            "run_id"
        ]
        == "7"
    )


class _Echo(BaseHTTPRequestHandler):
    def log_message(self, *args: object) -> None:
        pass

    def _reply(self) -> None:
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        self._reply()

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers["Content-Length"]))
        self._reply()


@pytest.fixture
def echo() -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Echo)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def _same(oracle: Exchange, impl: Exchange) -> None:
    return None


def _product(url: str) -> type[FuzzProduct]:
    class Echo(FuzzProduct):
        name = "echo"

        def __init__(self, config: FuzzConfig) -> None:
            super().__init__(config)
            self.oracle_read = self.oracle_write = Endpoint(url)
            self.impl_read = self.impl_write = Endpoint(url)

        def stack(self):
            return nullcontext()

        def generate_round(self, index: int, rng: random.Random) -> Round:
            write = Request("POST", "/write", body=b"{}")
            probe = Probe("ready", Request("GET", "/ready"), lambda value: value.ok())
            return Round(index, "unit", [Batch("only", write, write, 1)], [probe])

        def next_case(self, rng: random.Random) -> Case:
            return Case("echo.get", "q", Request("GET", "/q"), _same)

    return Echo


def test_run_fuzz_records_history_when_enabled(
    echo: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(history, "collect_git", lambda repo: GIT)
    monkeypatch.setattr(history, "collect_host", lambda: HOST)
    for name, value in {
        "FUZZ_SEED": "7",
        "FUZZ_RUN_ID": "unit-run",
        "FUZZ_DURATION": "20s",
        "FUZZ_MAX_CASES": "5",
        "FUZZ_QUERIES_PER_ROUND": "5",
        "FUZZ_SETTLE": "0",
        "FUZZ_OUTPUT_DIR": str(tmp_path / "out"),
        history.HISTORY_ENV: str(tmp_path / "history"),
    }.items():
        monkeypatch.setenv(name, value)
    summary = run_fuzz(_product(echo))
    assert not summary.failed
    assert summary.history is not None and summary.history.is_file()
    entry = json.loads(summary.history.read_text())
    assert entry["product"] == "echo"
    assert entry["results"]["outcomes"] == {"match": 5}
    assert entry["name"].endswith("-0123456789-00000007")
    assert (tmp_path / "history" / "README.md").is_file()
    assert summary.history.with_suffix(".md").is_file()


def test_bench_plan_passes_settings_to_each_product(tmp_path: Path) -> None:
    environ = {"FUZZ_RUN_ID": "stale", "FUZZ_TRACES_CONFIG": "production"}
    plan = bench.parse_plan(
        [
            "--products",
            "traces,logs",
            "--duration",
            "90s",
            "--history-dir",
            str(tmp_path),
            "--output-root",
            str(tmp_path / "out"),
        ],
        environ,
    )
    assert plan.products == ("traces", "logs")
    env = bench.product_environment(plan, bench.Job("logs", "recent"), environ)
    assert env["FUZZ_DURATION"] == "90s"
    assert env[history.HISTORY_ENV] == str(tmp_path)
    assert env[history.BATCH_ENV] == plan.batch
    assert env["FUZZ_OUTPUT_DIR"] == str(tmp_path / "out" / "logs")
    assert env["FUZZ_TRACES_CONFIG"] == "production"
    assert "FUZZ_RUN_ID" not in env and "FUZZ_SEED" not in env
    with pytest.raises(SystemExit):
        bench.parse_plan(["--products", "logs,nope"], {})


def test_bench_continues_after_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    class Completed:
        def __init__(self, returncode: int) -> None:
            self.returncode = returncode

    def fake_run(command, **kwargs):
        calls.append((command[-1], kwargs["env"].get("FUZZ_METRICS_ORACLE")))
        return Completed(1 if command[-1] == "logs" else 0)

    monkeypatch.setattr(bench.subprocess, "run", fake_run)
    plan = bench.parse_plan(["--history-dir", str(tmp_path), "--cooldown", "0"], {})
    outcomes = bench.run(plan, {})
    assert calls == [
        ("logs", None),
        ("metrics", "prometheus"),
        ("metrics", "mimir"),
        ("traces", None),
    ]
    assert [outcome.status for outcome in outcomes] == [
        "failed",
        "passed",
        "passed",
        "passed",
    ]
    assert (tmp_path / "index.json").is_file()


def test_bench_metrics_oracles_apply_to_recent_only(tmp_path: Path) -> None:
    plan = bench.parse_plan(
        [
            "--products",
            "metrics",
            "--scenarios",
            "recent,historical",
            "--output-root",
            str(tmp_path),
        ],
        {"FUZZ_METRICS_ORACLE": "mimir-blocks"},
    )
    assert plan.metrics_oracles == ("mimir-blocks",)
    plan = bench.parse_plan(
        [
            "--products",
            "metrics",
            "--scenarios",
            "recent,historical",
            "--metrics-oracles",
            "prometheus,mimir",
            "--output-root",
            str(tmp_path),
        ],
        {"FUZZ_METRICS_ORACLE": "mimir-blocks"},
    )
    planned = bench.jobs(plan)
    assert planned == [
        bench.Job("metrics", "recent", "prometheus"),
        bench.Job("metrics", "recent", "mimir"),
        bench.Job("metrics", "historical"),
    ]
    assert [bench.output_dir(plan, job).name for job in planned] == [
        "metrics-recent-prometheus",
        "metrics-recent-mimir",
        "metrics-historical",
    ]
    historical = bench.product_environment(
        plan, planned[2], {"FUZZ_METRICS_ORACLE": "mimir-blocks"}
    )
    assert "FUZZ_METRICS_ORACLE" not in historical
    with pytest.raises(SystemExit):
        bench.parse_plan(["--metrics-oracles", "thanos"], {})


def test_bench_lanes_never_run_one_product_twice_at_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[tuple[str, str, str]] = []
    live: list[FakeProcess] = []
    started: list[dict[str, str]] = []

    class FakeProcess:
        def __init__(self, command, **kwargs) -> None:
            self.label = f"{command[-1]}:{kwargs['env'].get('FUZZ_METRICS_ORACLE')}"
            self.env = kwargs["env"]
            started.append(self.env)
            self.polls = 0
            self.returncode: int | None = None
            events.append(("start", self.label, self.env[cpus.RANGE_ENV]))
            live.append(self)

        def poll(self) -> int | None:
            self.polls += 1
            if self.polls >= 2 and self.returncode is None:
                self.returncode = 0
                live.remove(self)
                events.append(("end", self.label, self.env[cpus.RANGE_ENV]))
            return self.returncode

        def wait(self) -> int | None:
            return self.returncode

    def fake_build(command, **kwargs):
        events.append(("build", command[3], ""))

    monkeypatch.setattr(bench.subprocess, "Popen", FakeProcess)
    monkeypatch.setattr(bench.subprocess, "run", fake_build)
    monkeypatch.setattr(bench, "docker_socket", lambda: "/var/run/docker.sock")
    monkeypatch.setattr(bench, "DockerApi", lambda path: None)
    monkeypatch.setattr(bench, "docker_cpus", lambda docker: 12)
    monkeypatch.setattr(bench, "POLL_S", 0)
    monkeypatch.delenv("FUZZ_IN_NETWORK", raising=False)
    plan = bench.parse_plan(
        [
            "--lanes",
            "2",
            "--products",
            "metrics,logs",
            "--history-dir",
            str(tmp_path),
            "--output-root",
            str(tmp_path / "out"),
            "--cooldown",
            "0",
        ],
        {},
    )
    outcomes = bench.run(plan, {})
    assert [e[0] for e in events[:2]] == ["build", "build"]
    starts = [e for e in events if e[0] == "start"]
    assert {e[2] for e in starts} == {"0-5", "6-11"}
    running: set[str] = set()
    for kind, label, _ in events[2:]:
        product = label.split(":")[0]
        if kind == "start":
            assert product not in running
            running.add(product)
        else:
            running.discard(product)
    assert [o.status for o in outcomes] == ["passed"] * 3
    assert [o.oracle for o in outcomes] == ["prometheus", "mimir", None]
    assert not live
    assert all(env["REGRESSION_BUILD"] == "0" for env in started)


def test_bench_runs_every_product_in_each_scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    class Completed:
        returncode = 0

    def fake_run(command, **kwargs):
        env = kwargs["env"]
        calls.append((command[-1], env["FUZZ_SCENARIO"], env["FUZZ_OUTPUT_DIR"]))
        return Completed()

    monkeypatch.setattr(bench.subprocess, "run", fake_run)
    plan = bench.parse_plan(
        [
            "--products",
            "logs",
            "--scenarios",
            "recent,historical",
            "--history-dir",
            str(tmp_path),
            "--output-root",
            str(tmp_path / "out"),
            "--cooldown",
            "0",
        ],
        {},
    )
    bench.run(plan, {})
    assert calls == [
        ("logs", "recent", str(tmp_path / "out" / "logs-recent")),
        ("logs", "historical", str(tmp_path / "out" / "logs-historical")),
    ]
