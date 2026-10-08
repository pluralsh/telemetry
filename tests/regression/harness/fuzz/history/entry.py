"""Compact, durable history entries built from a run's `run.json` and `summary.json`."""

from __future__ import annotations

import datetime
import math
from typing import Any

from ..recorder import OUTCOMES

SCHEMA_VERSION = 1

# Product `describe()` keys that are endpoints rather than benchmark settings.
_ENDPOINT_KEYS = frozenset({"oracle_read", "oracle_write", "impl_read", "impl_write"})
_CONFIG_KEYS = (
    "duration_s",
    "seed",
    "scale",
    "window_s",
    "queries_per_round",
    "max_rounds",
    "max_cases",
    "request_timeout_s",
    "visibility_timeout_s",
    "latency_ratio",
    "latency_floor_ms",
    "recheck",
    "fail_on",
    "stack",
    "scenario",
    "isolated",
)
_QUERY_PERCENTILES = ("p50", "p90", "p99", "p99.9", "max")
_FAMILY_PERCENTILES = ("p50", "p90", "p99")
_INGEST_PERCENTILES = ("p50", "p90", "p99", "max")
_MAX_NOTES = 20


def normalize(value: Any) -> Any:
    """Round floats so entries stay small and rewrite identically."""
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 3)
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    return value


def iso(epoch: float) -> str:
    moment = datetime.datetime.fromtimestamp(epoch, datetime.UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def settings(endpoints: dict[str, Any]) -> dict[str, Any]:
    return {
        key: endpoints[key] for key in sorted(endpoints) if key not in _ENDPOINT_KEYS
    }


def variant(entry: dict[str, Any]) -> str:
    """Settings that make two runs of one product incomparable, as `k=v` text."""
    parts = [f"{key}={value}" for key, value in entry["settings"].items()]
    config = entry["config"]
    if config["stack"] != "compose":
        parts.append(f"stack={config['stack']}")
    if config.get("scenario") not in (None, "recent"):
        parts.append(f"scenario={config['scenario']}")
    if config["scale"] != 1:
        parts.append(f"scale={config['scale']}")
    layout = entry.get("layout") or {}
    if layout.get("network") == "compose-network":
        parts.append("network=compose")
    if (layout.get("cpus") or {}).get("pinned"):
        parts.append("pinned")
    return ", ".join(parts) or "default"


def _pick(stats: dict[str, Any] | None, keys: tuple[str, ...]) -> dict[str, Any]:
    stats = stats or {}
    return {key: stats.get(key) for key in keys}


def _outcomes(counts: dict[str, int]) -> dict[str, int]:
    known = {name: counts[name] for name in OUTCOMES if counts.get(name)}
    extra = {name: counts[name] for name in sorted(counts) if name not in OUTCOMES}
    return {**known, **extra}


def _families(families: dict[str, Any]) -> dict[str, Any]:
    compact = {}
    for family in sorted(families):
        stats = families[family]
        counts = stats.get("outcomes", {})
        cases = sum(counts.values())
        compact[family] = {
            "cases": cases,
            "non_match": cases - counts.get("match", 0),
            "outcomes": _outcomes(counts),
            "oracle_ms": _pick(stats.get("oracle_ms"), _FAMILY_PERCENTILES),
            "impl_ms": _pick(stats.get("impl_ms"), _FAMILY_PERCENTILES),
            "ratio": _pick(stats.get("ratio"), ("p50", "p90")),
        }
    return compact


def _resources(resources: dict[str, Any] | None) -> dict[str, Any] | None:
    if not resources:
        return None
    peaks = resources.get("service_peak_memory_mib", {})
    cpu = resources.get("service_cpu_seconds", {})
    return {
        "interval_s": resources.get("interval_s"),
        "elapsed_s": resources.get("elapsed_s"),
        "samples_failed": resources.get("samples_failed", 0),
        "roles": {
            role: {
                "services": stats.get("services", []),
                "cpu_seconds": stats.get("cpu_seconds"),
                "cpu_cores": _pick(stats.get("cpu_cores"), ("mean", "p95", "max")),
                "memory_mib": _pick(stats.get("memory_mib"), ("mean", "p95", "max")),
            }
            for role, stats in sorted(resources.get("roles", {}).items())
        },
        "services": {
            service: {
                "cpu_seconds": cpu.get(service),
                "peak_memory_mib": peaks.get(service),
            }
            for service in sorted(set(peaks) | set(cpu))
        },
    }


def results(summary: dict[str, Any]) -> dict[str, Any]:
    counts = summary.get("outcomes", {})
    cases = summary.get("cases", sum(counts.values()))
    ingest = summary.get("ingest", {})
    latency = summary.get("latency", {})
    notes = summary.get("notes", [])
    return {
        "status": "failed" if summary.get("failed") else "passed",
        "reasons": list(summary.get("reasons", [])),
        "notes": notes[:_MAX_NOTES],
        "notes_truncated": max(0, len(notes) - _MAX_NOTES),
        "elapsed_s": summary.get("elapsed_s"),
        "rounds": summary.get("rounds", 0),
        "invisible_rounds": len(summary.get("invisible_rounds", [])),
        "cases": cases,
        "non_match": cases - counts.get("match", 0),
        "outcomes": _outcomes(counts),
        "ingest": {
            "batches": sum(ingest.get("outcomes", {}).values()),
            "outcomes": dict(sorted(ingest.get("outcomes", {}).items())),
            "oracle_ms": _pick(ingest.get("oracle_ms"), _INGEST_PERCENTILES),
            "impl_ms": _pick(ingest.get("impl_ms"), _INGEST_PERCENTILES),
        },
        "latency": {
            "oracle_ms": _pick(latency.get("oracle_ms"), _QUERY_PERCENTILES),
            "impl_ms": _pick(latency.get("impl_ms"), _QUERY_PERCENTILES),
            "ratio": _pick(latency.get("ratio"), ("p50", "p90", "p99")),
            "outliers": latency.get("outliers", 0),
        },
        "families": _families(summary.get("families", {})),
        "floor": {
            service: _pick(stats, ("role", "p50", "p99"))
            for service, stats in sorted((summary.get("floor") or {}).items())
        },
        "resources": _resources(summary.get("resources")),
    }


def build_entry(
    *,
    run: dict[str, Any],
    summary: dict[str, Any],
    git: dict[str, Any],
    host: dict[str, Any],
    environment: dict[str, str] | None = None,
    images: dict[str, Any] | None = None,
    ci: dict[str, Any] | None = None,
    batch: str | None = None,
) -> dict[str, Any]:
    """`finished_at` is the end of the fuzz loop, which stays exact when an
    old run is imported after the fact."""
    config = {**run.get("config", {}), **summary.get("config", {})}
    started_at = float(run["started_at"])
    finished_at = started_at + float(summary.get("elapsed_s") or 0)
    entry: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "name": "",
        "product": config["product"],
        "run_id": config.get("run_id"),
        "batch": batch,
        "started_at": iso(started_at),
        "finished_at": iso(finished_at),
        "git": git,
        "host": host,
        "ci": ci,
        "config": {key: config.get(key) for key in _CONFIG_KEYS},
        "settings": settings(run.get("endpoints", {})),
        "environment": dict(sorted((environment or {}).items())),
        "images": dict(sorted((images or {}).items())),
        "layout": {
            "network": run.get("network", "host-ports"),
            "cpus": (run.get("layout") or {}).get("cpus"),
        },
        "variant": "",
        "results": results(summary),
    }
    entry["variant"] = variant(entry)
    entry["name"] = entry_name(entry)
    return normalize(entry)


def entry_name(entry: dict[str, Any]) -> str:
    """`<YYYY-MM-DD>T<HHMM>Z-<shortsha>[-dirty]-<seed hex>`, sortable by start."""
    started = entry["started_at"]
    stamp = f"{started[:10]}T{started[11:13]}{started[14:16]}Z"
    git = entry.get("git") or {}
    revision = git.get("short") or "nogit"
    if git.get("dirty"):
        revision += "-dirty"
    seed = entry["config"].get("seed")
    suffix = f"{int(seed):08x}" if isinstance(seed, int) else "noseed"
    return f"{stamp}-{revision}-{suffix}"
