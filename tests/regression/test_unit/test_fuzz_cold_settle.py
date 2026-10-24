"""The cold scenario's settle steps: Logs rollups, SlateDB compaction and
Loki's compactor."""

from __future__ import annotations

import contextlib
import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from harness.fuzz import cold, logs
from harness.fuzz.cold import ColdService
from harness.fuzz.config import FuzzConfig
from harness.fuzz.logs import LogsFuzz, RollupLogWatch

PRODUCTS = Path(__file__).resolve().parents[1] / "products"


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


def _scrape(**values: float) -> str:
    """A scrape with each metric under two label sets that split its value."""
    lines = []
    for name, value in values.items():
        lines += [f'{name}{{path="a"}} {value / 2}', f'{name}{{path="b"}} {value / 2}']
    return "\n".join(["# TYPE x gauge", *lines, "unrelated_total 9"])


class _Response(contextlib.AbstractContextManager):
    def __init__(self, body: str) -> None:
        self.body = body

    def read(self) -> bytes:
        return self.body.encode()

    def __exit__(self, *exc: object) -> None:
        return None


def _serve(monkeypatch: pytest.MonkeyPatch, bodies: Iterator[str]) -> list[str]:
    urls: list[str] = []

    def urlopen(url: str, timeout: float) -> _Response:
        urls.append(url)
        return _Response(next(bodies))

    monkeypatch.setattr(cold.urllib.request, "urlopen", urlopen)
    return urls


def test_compaction_settings_come_from_the_environment(config) -> None:
    settings = config("logs")
    assert settings.cold.compaction and settings.cold.rollups
    assert settings.describe()["cold"]["compaction"] is True
    assert not config("logs", FUZZ_COLD_COMPACTION="0").cold.compaction


def test_scrape_sums_add_label_sets_and_skip_missing_names() -> None:
    scrape = _scrape(slatedb_db_sst_count=6, slatedb_db_sst_count_extra=100)
    assert cold.scrape_sums(scrape, ("slatedb_db_sst_count", "absent")) == {
        "slatedb_db_sst_count": 6.0
    }


def test_compaction_is_quiet_only_when_nothing_changes_or_runs() -> None:
    layout = {name: 4.0 for name in cold.SLATEDB_LAYOUT}
    idle = {**layout, **{name: 0.0 for name in cold.SLATEDB_COMPACTING}}
    assert cold.compaction_state(_scrape(**idle)) == idle
    assert cold.compaction_quiet(idle, idle)
    # Gauges a version doesn't export count as idle.
    assert cold.compaction_quiet(layout, layout)
    for name in cold.SLATEDB_LAYOUT:
        assert not cold.compaction_quiet(idle, {**idle, name: 3.0})
    for name in cold.SLATEDB_COMPACTING:
        assert not cold.compaction_quiet(idle, {**idle, name: 1.0})
    with pytest.raises(ValueError):
        cold.compaction_quiet({}, {"slatedb_db_l0_sst_count": 0.0})


class _CompactingStack(cold.ColdStack):
    def __init__(self, uploads: list[int]) -> None:
        super().__init__(project=None)  # type: ignore[arg-type]
        self.uploads = iter(uploads)
        self.sent = 0

    def address(self, service: str) -> str | None:
        return "10.0.0.5"

    def proxy_stats(self) -> dict[str, Any]:
        self.sent += next(self.uploads)
        return {"10.0.0.5": {"PUT": {"requests": 1, "bytes": 0, "sent": self.sent}}}


def test_settle_compaction_waits_for_a_quiet_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    idle = {"slatedb_db_sst_count": 9.0, "slatedb_compactor_running_compactions": 0.0}
    states = [
        {**idle, "slatedb_compactor_running_compactions": 1.0},
        {**idle, "slatedb_db_sst_count": 4.0},
        {**idle, "slatedb_db_sst_count": 4.0},  # quiet, but still uploading
        {**idle, "slatedb_db_sst_count": 4.0},
    ]
    urls = _serve(monkeypatch, iter(_scrape(**state) for state in states))
    stack = _CompactingStack([0, 0, cold.QUIESCE_BYTES, 0])
    waited, state = stack.settle_compaction(
        ColdService("logs-s3", "http://impl/-/ready"),
        metrics_url="http://impl/metrics",
        window_s=0.0,
    )
    assert waited >= 0 and state == {**idle, "slatedb_db_sst_count": 4.0}
    assert urls == ["http://impl/metrics"] * 4

    _serve(monkeypatch, iter(_scrape(**state) for state in states[:2]))
    with pytest.raises(RuntimeError):
        _CompactingStack([0, 0]).settle_compaction(
            ColdService("logs-s3", "http://impl/-/ready"),
            metrics_url="http://impl/metrics",
            window_s=0.0,
            timeout_s=0.0,
        )


def test_object_layout_groups_a_listing_by_directory() -> None:
    listing = "\n".join(
        json.dumps(item)
        for item in [
            {"type": "file", "key": "fuzz/shard-0000/compacted/a.sst", "size": 10},
            {"type": "file", "key": "fuzz/shard-0000/compacted/b.sst", "size": 30},
            {"type": "file", "key": "fuzz/shard-0000/wal/1.sst", "size": 5},
            {"type": "folder", "key": "fuzz/", "size": 0},
            {"type": "file", "key": "top", "size": 1},
        ]
    )
    assert cold.object_layout(listing + "\n\n") == {
        "": {"objects": 1, "bytes": 1, "largest": 1},
        "fuzz/shard-0000/compacted": {"objects": 2, "bytes": 40, "largest": 30},
        "fuzz/shard-0000/wal": {"objects": 1, "bytes": 5, "largest": 5},
    }


class _RestartingStack(cold.ColdStack):
    def __init__(self) -> None:
        super().__init__(project=None)  # type: ignore[arg-type]
        self.restarts: list[dict[str, Any]] = []

    def restart(self, service, *, wipe, ready_timeout_s):
        self.restarts.append(
            {
                "service": service.service,
                "wipe": wipe,
                "config": Path(os.environ["LOGS_S3_CONFIG"]).read_text(),
                "log": os.environ.get("LOGS_RUST_LOG"),
            }
        )
        return 0.0


def test_settle_rollups_polls_a_custom_signal_with_extra_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    production = tmp_path / "logs-s3.yaml"
    production.write_text("mode: standalone\n")
    monkeypatch.setattr(cold, "ROLLUP_SETTLE_INTERVAL_S", 0)
    monkeypatch.delenv("LOGS_S3_CONFIG", raising=False)
    monkeypatch.setenv("LOGS_RUST_LOG", "error")
    answers = iter([False, False, True])
    stack = _RestartingStack()
    stack.settle_rollups(
        ColdService("logs-s3", "http://impl/-/ready"),
        config=production,
        env="LOGS_S3_CONFIG",
        settled=lambda: next(answers),
        environment={"LOGS_RUST_LOG": logs.ROLLUP_LOG},
        ready_timeout_s=1.0,
    )
    assert stack.restarts == [
        {
            "service": "logs-s3",
            "wipe": False,
            "config": "mode: standalone\nrollup:\n  interval_seconds: 0\n",
            "log": logs.ROLLUP_LOG,
        }
    ]
    assert next(answers, None) is None
    assert "LOGS_S3_CONFIG" not in os.environ
    assert os.environ["LOGS_RUST_LOG"] == "error"
    with pytest.raises(ValueError):
        stack.settle_rollups(
            ColdService("logs-s3", "http://impl/-/ready"),
            config=production,
            env="LOGS_S3_CONFIG",
            ready_timeout_s=1.0,
        )


def _log(*lines: str) -> str:
    stamp = "\x1b[2m2026-10-17T12:00:00.000000Z\x1b[0m"
    return "\n".join(f"{stamp}  INFO plural_logs::db::rollup: {line}" for line in lines)


def test_rollup_progress_counts_started_sealed_and_failed() -> None:
    log = _log(
        "starting logs rollup segment namespace=regression segment=a",
        "sealed logs rollup segment namespace=regression segment=a",
        "starting logs rollup segment namespace=regression segment=b",
        "finished logs rollup pass sealed=1",
    )
    assert logs.rollup_progress(log) == (2, 1, 0)
    failed = "WARN plural_logs_server::state: Logs segment rollup failed error=x"
    assert logs.rollup_progress(failed) == (0, 0, 1)


def test_rollup_log_watch_waits_for_seals_and_a_quiet_pass() -> None:
    now = [0.0]
    started = "starting logs rollup segment segment=a"
    sealed = "sealed logs rollup segment segment=a"
    log = [""]
    watch = RollupLogWatch(lambda: log[0], quiet_s=25.0, clock=lambda: now[0])

    def at(seconds: float, *lines: str) -> bool:
        now[0] = seconds
        log[0] = _log(*lines)
        return watch()

    assert not at(0.0)
    assert not at(10.0, started)
    assert not at(40.0, started)  # copying or merging
    assert not at(50.0, started, sealed)
    assert not at(70.0, started, sealed)
    assert at(75.0, started, sealed)
    # A server with nothing to roll up settles after the quiet time.
    idle = RollupLogWatch(lambda: "", quiet_s=25.0, clock=lambda: now[0])
    now[0] = 100.0
    assert not idle()
    now[0] = 125.0
    assert idle()
    broken = RollupLogWatch(
        lambda: "WARN Logs segment rollup failed", quiet_s=0.0, clock=lambda: 0.0
    )
    with pytest.raises(RuntimeError):
        broken()


def test_loki_compactions_count_successful_runs() -> None:
    scrape = "\n".join(
        [
            f'{logs.LOKI_COMPACTIONS}{{status="success"}} 3',
            f'{logs.LOKI_COMPACTIONS}{{status="failure"}} 4',
            "loki_boltdb_shipper_compact_tables_operation_duration_seconds 0.1",
        ]
    )
    assert logs.loki_compactions(scrape) == 3
    assert logs.loki_compactions("") == 0


def test_loki_compactor_config_runs_the_compactor_sooner(tmp_path: Path) -> None:
    cold_config = PRODUCTS / "logs" / logs.LOKI_CONFIGS["cold"]
    settle = logs.loki_compactor_config(cold_config)
    assert settle.startswith(cold_config.read_text().rstrip())
    assert settle.endswith(
        f"\ncompactor:\n  compaction_interval: {logs.LOKI_COMPACTION_INTERVAL}\n"
    )
    configured = tmp_path / "loki.yaml"
    configured.write_text("compactor:\n  compaction_interval: 1m\n")
    with pytest.raises(ValueError):
        logs.loki_compactor_config(configured)


def test_summary_settle_waits_for_an_idle_pass_and_fails_on_errors(
    tmp_path: Path,
) -> None:
    for directory in logs.IMPL_CONFIG_DIRS.values():
        config = PRODUCTS / "logs" / directory / "logs-s3.yaml"
        settle = logs.summary_settle_config(config)
        assert settle.startswith(config.read_text().rstrip())
        assert settle.endswith("\nsummary_interval_seconds: 10\n")
    configured = tmp_path / "logs-s3.yaml"
    configured.write_text("summary_interval_seconds: 60\n")
    with pytest.raises(ValueError):
        logs.summary_settle_config(configured)
    scrape = (
        "# TYPE logs_summary_passes_total counter\n"
        'logs_summary_passes_total{outcome="ok"} 3\n'
        'logs_summary_passes_total{outcome="idle"} 1\n'
        "logs_summary_passes_total_other 9\n"
    )
    assert logs.summary_passes(scrape, "ok") == 3
    assert logs.summary_passes(scrape, "idle") == 1
    assert logs.summary_passes(scrape, "error") == 0


def test_logs_compose_mounts_the_settle_configs() -> None:
    compose = (PRODUCTS / "logs" / "docker-compose.yml").read_text()
    assert "${LOGS_S3_CONFIG:-./${LOGS_CONFIG_DIR:-.}/logs-s3.yaml}" in compose
    assert "${LOKI_FUZZ_CONFIG_PATH:-./${LOKI_FUZZ_CONFIG:-loki-fuzz.yaml}}" in compose
    assert "RUST_LOG: ${LOGS_RUST_LOG:-error}" in compose
    for directory in logs.IMPL_CONFIG_DIRS.values():
        config = PRODUCTS / "logs" / directory / "logs-s3.yaml"
        assert cold.settle_config(config).endswith("interval_seconds: 10\n")


class _FlushStack:
    """Records what `LogsFuzz.cold_flush` asks of the stack."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def stop(self, service: ColdService) -> None:
        self.calls.append(("stop", service.service))

    def start(self, service: ColdService, *, wipe: bool, ready_timeout_s: float):
        self.calls.append(("start", service.service))
        return 0.0

    def mc(self, *args: str) -> str:
        return "".join(f"index/index_{day}/a.tsdb.gz\n" for day in range(20000, 20300))

    def bucket_layout(self, prefix: str) -> dict[str, dict[str, int]]:
        self.calls.append(("layout", prefix))
        return {"fuzz/compacted": {"objects": 2, "bytes": 3 << 20, "largest": 2 << 20}}

    def settle_on(self, service: ColdService, **kwargs: Any) -> float:
        if kwargs["env"] == "LOGS_S3_CONFIG":
            self.calls.append(("settle_summaries", service.service))
            assert kwargs["config"].parent.name == "production"
            assert kwargs["text"].endswith("\nsummary_interval_seconds: 10\n")
            assert not kwargs.get("wipe")
            return 12.0
        self.calls.append(("settle_on", service.service))
        assert kwargs["env"] == "LOKI_FUZZ_CONFIG_PATH" and kwargs["wipe"]
        assert "compaction_interval: 10s" in kwargs["text"]
        return 31.0

    def settle_rollups(self, service: ColdService, **kwargs: Any) -> float:
        self.calls.append(("settle_rollups", service.service))
        assert kwargs["env"] == "LOGS_S3_CONFIG"
        assert kwargs["config"].parent.name == "production"
        assert kwargs["environment"] == {"LOGS_RUST_LOG": logs.ROLLUP_LOG}
        assert isinstance(kwargs["settled"], RollupLogWatch)
        return 40.0

    def settle_compaction(self, service: ColdService, *, metrics_url: str):
        self.calls.append(("settle_compaction", service.service))
        assert metrics_url.endswith(":3100/metrics") or metrics_url.endswith(
            ":13111/metrics"
        )
        return 30.0, {"slatedb_db_sst_count": 4.0}


@pytest.mark.parametrize(
    ("env", "settles"),
    [
        ({}, ["settle_on", "settle_rollups", "settle_compaction", "settle_summaries"]),
        (
            {"FUZZ_COLD_COMPACTION": "0"},
            ["start", "settle_rollups", "settle_summaries"],
        ),
        (
            {"FUZZ_COLD_ROLLUPS": "0"},
            ["settle_on", "settle_compaction", "settle_summaries"],
        ),
        (
            {"FUZZ_COLD_SUMMARIES": "0"},
            ["settle_on", "settle_rollups", "settle_compaction"],
        ),
        (
            {
                "FUZZ_COLD_ROLLUPS": "0",
                "FUZZ_COLD_COMPACTION": "0",
                "FUZZ_COLD_SUMMARIES": "0",
            },
            ["start"],
        ),
    ],
)
def test_logs_cold_flush_settles_both_sides(
    config, monkeypatch: pytest.MonkeyPatch, env: dict[str, str], settles: list[str]
) -> None:
    settings = config("logs", FUZZ_LOGS_CONFIG="production", FUZZ_SCALE="0.05", **env)
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    product = LogsFuzz(settings)
    product.cold_end = 20290 * 86_400 * 1_000_000_000
    monkeypatch.setattr(LogsFuzz, "_flush_loki", lambda self, what: None)
    monkeypatch.setattr(logs, "scrape", lambda url: "slatedb_db_sst_count 23\n")
    stack = _FlushStack()
    product.cold_flush(stack)  # type: ignore[arg-type]
    steps = [name for name, _ in stack.calls if name not in ("stop", "layout")]
    assert steps == settles
    report = json.loads((settings.output_dir / "cold_settle.json").read_text())
    assert report["before"]["summary"]["impl_ssts"] == {
        "objects": 2,
        "mib": 3,
        "largest_mib": 2,
    }
    assert report["before"]["summary"]["impl_manifest"]["sst_count"] == 23
    expected = {
        "settle_on": "loki_compactor",
        "settle_rollups": "rollups",
        "settle_compaction": "impl_compaction",
        "settle_summaries": "summaries",
    }
    assert set(report["settle_s"]) == {expected[s] for s in settles if s in expected}
    assert ("after" in report) == bool(report["settle_s"])
