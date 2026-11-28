"""The `split` implementation config: validation, topology and the evidence
check over reader replica scrapes."""

from __future__ import annotations

from pathlib import Path

import pytest
from harness.fuzz import split
from harness.fuzz.config import FuzzConfig
from harness.fuzz.logs import LogsFuzz
from harness.fuzz.metrics import MetricsFuzz
from harness.fuzz.recorder import split_markdown
from harness.fuzz.traces import TracesFuzz

PRODUCTS = (
    (MetricsFuzz, 18087, 18080),
    (TracesFuzz, 13207, 13201),
    (LogsFuzz, 13112, 13103),
)

REPLICA_0A = """\
# TYPE telemetry_read_group_split_decisions_total counter
telemetry_read_group_split_decisions_total{reason="split"} 5
telemetry_read_group_split_decisions_total{reason="small"} 2
# TYPE telemetry_read_group_fragments summary
telemetry_read_group_fragments{quantile="0.5"} 2
telemetry_read_group_fragments{quantile="1"} 4
telemetry_read_group_fragments_sum 14
telemetry_read_group_fragments_count 5
telemetry_read_group_hedges_total 1
metrics_pushdown_total{result="pushed"} 3
metrics_pushdown_total{result="fallback"} 1
metrics_split_reads_total 4
metrics_split_reads_totally_unrelated 9
"""
REPLICA_1A = """\
telemetry_read_group_split_decisions_total{reason="split"} 2
telemetry_read_group_split_decisions_total{reason="single_replica"} 1
telemetry_read_group_fragments_sum 4
telemetry_read_group_fragments_count 2
metrics_pushdown_total{result="pushed"} 1
"""


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.delenv("FUZZ_IN_NETWORK", raising=False)

    def build(product: str, **env: str) -> FuzzConfig:
        monkeypatch.setenv("FUZZ_SEED", "7")
        monkeypatch.setenv("FUZZ_RUN_ID", "unit-run")
        monkeypatch.setenv("FUZZ_OUTPUT_DIR", str(tmp_path / product))
        monkeypatch.setenv(f"FUZZ_{product.upper()}_CONFIG", "split")
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        return FuzzConfig.from_env(product, default_window="10m")

    return build


@pytest.mark.parametrize(("product_type", "read_port", "write_port"), PRODUCTS)
def test_split_reads_through_replica_0a_and_writes_to_the_writers(
    product_type, read_port, write_port, config
) -> None:
    product = product_type(config(product_type.name))
    assert product.impl_config == "split"
    assert product.impl_read.url.startswith(f"http://127.0.0.1:{read_port}/read/")
    assert product.impl_write.url.startswith(f"http://127.0.0.1:{write_port}/write/")
    replicas = split.replicas(product_type.name)
    assert replicas == tuple(
        f"{product_type.name}-split-{name}" for name in ("0a", "0b", "1a", "1b")
    )
    assert all(product.resource_roles[name] == "impl" for name in replicas)


def test_split_floor_covers_writers_and_every_replica(config) -> None:
    metrics = MetricsFuzz(config("metrics"))
    assert {"metrics-writer-0", "metrics-writer-1", *split.replicas("metrics")} <= set(
        metrics.floor_urls()
    )
    assert "metrics-reader" not in metrics.floor_urls()
    logs = LogsFuzz(config("logs"))
    assert {"logs-writer-0", "logs-writer-1", *split.replicas("logs")} <= set(
        logs.floor_urls()
    )
    assert "logs-s3" not in logs.floor_urls()
    traces = TracesFuzz(config("traces"))
    traces._reader_started = True
    assert set(split.replicas("traces")) <= set(traces.floor_urls())
    assert "traces-reader" not in traces.floor_urls()


@pytest.mark.parametrize(("product_type", "_read", "_write"), PRODUCTS)
@pytest.mark.parametrize(
    ("env", "message"),
    (
        ({"FUZZ_SCENARIO": "cold"}, "does not support FUZZ_SCENARIO=cold"),
        ({"FUZZ_STACK": "external"}, "FUZZ_STACK=compose"),
    ),
)
def test_split_rejects_cold_and_external_stacks(
    product_type, _read, _write, env, message, config
) -> None:
    with pytest.raises(ValueError, match=message):
        product_type(config(product_type.name, **env))


@pytest.mark.parametrize(
    ("product", "storage"),
    (("metrics", "FUZZ_METRICS_STORAGE"), ("logs", "FUZZ_LOGS_STORAGE")),
)
def test_split_requires_s3_storage(product, storage, config) -> None:
    product_type = {"metrics": MetricsFuzz, "logs": LogsFuzz}[product]
    with pytest.raises(ValueError, match="CONFIG=split requires .*STORAGE=s3"):
        product_type(config(product, **{storage: "local"}))


def test_split_is_allowed_in_the_historical_scenario(config) -> None:
    product = MetricsFuzz(config("metrics", FUZZ_SCENARIO="historical"))
    assert product.impl_config == "split"


def test_regression_config_reports_nothing(config, monkeypatch) -> None:
    monkeypatch.setenv("FUZZ_METRICS_CONFIG", "regression")
    product = MetricsFuzz(FuzzConfig.from_env("metrics", default_window="10m"))
    assert product.report() == ({}, [])


def test_split_evidence_sums_replicas_and_passes() -> None:
    evidence = split.split_evidence(
        {
            "metrics-split-0a": REPLICA_0A,
            "metrics-split-0b": "",
            "metrics-split-1a": REPLICA_1A,
            "metrics-split-1b": None,
        },
        split.METRICS_COUNTERS,
    )
    assert evidence["decisions"] == {"split": 7, "small": 2, "single_replica": 1}
    assert evidence["fragmented_reads"] == 7
    assert evidence["fragments"] == 18
    assert evidence["hedges"] == 1
    assert evidence["counters"] == {
        'metrics_pushdown_total{result="fallback"}': 1,
        'metrics_pushdown_total{result="pushed"}': 4,
        "metrics_split_reads_total": 4,
    }
    assert evidence["replicas"]["metrics-split-0b"] == {}
    assert evidence["replicas"]["metrics-split-1b"] is None
    assert evidence["unreachable"] == ["metrics-split-1b"]
    assert split.split_failure(evidence) is None
    markdown = "\n".join(split_markdown(evidence))
    assert "split 7" in markdown and "7 reads split into 18 fragments" in markdown
    assert '`metrics_pushdown_total{result="pushed"}` | 4' in markdown
    assert "Could not scrape: metrics-split-1b" in markdown


def test_split_failure_without_split_decisions() -> None:
    scrape = 'telemetry_read_group_split_decisions_total{reason="small"} 3\n'
    evidence = split.split_evidence({"logs-split-0a": scrape, "logs-split-1a": None})
    failure = split.split_failure(evidence)
    assert failure is not None
    assert 'telemetry_read_group_split_decisions_total{reason="split"} is 0' in failure
    assert "{'small': 3.0}" in failure and "logs-split-1a" in failure
    assert split.split_failure(split.split_evidence({})) is not None


def test_split_report_fails_when_no_replica_answers() -> None:
    extra, reasons = split.report({"traces-split-0a": "http://127.0.0.1:9/metrics"})
    assert extra["split"]["unreachable"] == ["traces-split-0a"]
    assert len(reasons) == 1 and "no read was split" in reasons[0]
