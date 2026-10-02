"""Durable run artifacts: per-case JSONL, mismatch bodies, and a summary report."""

from __future__ import annotations

import gzip
import json
import math
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import FuzzConfig
from .transport import Exchange, Request

OUTCOMES = (
    "match",
    "mismatch",
    "inconclusive",
    "impl_error",
    "impl_timeout",
    "oracle_error",
    "oracle_timeout",
    "both_error",
    "unstable_oracle",
    "unstable_impl",
)


@dataclass(frozen=True)
class CaseRecord:
    id: str
    round: int
    family: str
    query: str
    request: Request
    outcome: str
    detail: str | None
    oracle: Exchange
    impl: Exchange
    recheck: tuple[Exchange, Exchange] | None = None


@dataclass(frozen=True)
class IngestRecord:
    round: int
    batch: str
    items: int
    oracle: Exchange
    impl: Exchange

    @property
    def outcome(self) -> str:
        oracle, impl = self.oracle.ok(), self.impl.ok()
        if oracle and impl:
            return "ok"
        if oracle:
            return "impl_error"
        if impl:
            return "oracle_error"
        return "both_error"


@dataclass(frozen=True)
class RoundRecord:
    index: int
    profile: str
    batches: int
    stats: dict[str, Any]
    visible: dict[str, bool]
    load_ms: float
    truncated: bool = False


@dataclass
class Summary:
    failed: bool
    reasons: list[str]
    output_dir: Path
    markdown: str
    outcomes: Counter[str] = field(default_factory=Counter)


PERCENTILES = ("p50", "p90", "p99", "p99.9", "max")


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[rank]


def _latency_stats(values: list[float]) -> dict[str, float | None]:
    return {
        "p50": percentile(values, 0.5),
        "p90": percentile(values, 0.9),
        "p99": percentile(values, 0.99),
        "p99.9": percentile(values, 0.999),
        "max": max(values) if values else None,
    }


def _ms(value: float | None) -> str:
    return "-" if value is None else f"{value:.1f}"


def _num(value: float | None, digits: int = 0) -> str:
    return "-" if value is None else f"{value:.{digits}f}"


def _ratio(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}x"


class Recorder:
    def __init__(self, config: FuzzConfig, endpoints: dict[str, object]) -> None:
        self.config = config
        self.root = config.output_dir
        (self.root / "artifacts").mkdir(parents=True, exist_ok=True)
        if config.record_data:
            (self.root / "data").mkdir(exist_ok=True)
        self.started = time.time()
        self._cases = (self.root / "cases.jsonl").open("w", encoding="utf-8")
        self._ingest = (self.root / "ingest.jsonl").open("w", encoding="utf-8")
        self._rounds = (self.root / "rounds.jsonl").open("w", encoding="utf-8")
        self.outcomes: Counter[str] = Counter()
        self.families: dict[str, Counter[str]] = defaultdict(Counter)
        self.oracle_latency: dict[str, list[float]] = defaultdict(list)
        self.impl_latency: dict[str, list[float]] = defaultdict(list)
        self.ratios: dict[str, list[float]] = defaultdict(list)
        self.outliers: list[dict[str, Any]] = []
        self.failures: list[dict[str, Any]] = []
        self.ingest_outcomes: Counter[str] = Counter()
        self.ingest_latency: dict[str, list[float]] = defaultdict(list)
        self.invisible_rounds: list[int] = []
        self.rounds = 0
        self.artifacts = 0
        self.notes: list[str] = []
        (self.root / "run.json").write_text(
            json.dumps(
                {
                    "config": config.describe(),
                    "endpoints": endpoints,
                    "started_at": self.started,
                },
                indent=2,
            )
        )

    def note(self, message: str) -> None:
        print(f"fuzz {self.config.product}: {message}", flush=True)
        self.notes.append(message)

    def _is_outlier(self, oracle: Exchange, impl: Exchange) -> bool:
        if oracle.status is None or impl.status is None:
            return False
        return (
            impl.latency_ms > self.config.latency_ratio * max(oracle.latency_ms, 1.0)
            and impl.latency_ms - oracle.latency_ms > self.config.latency_floor_ms
        )

    def record_case(self, record: CaseRecord) -> None:
        self.outcomes[record.outcome] += 1
        self.families[record.family][record.outcome] += 1
        outlier = self._is_outlier(record.oracle, record.impl)
        if record.oracle.status is not None and record.impl.status is not None:
            self.oracle_latency[record.family].append(record.oracle.latency_ms)
            self.impl_latency[record.family].append(record.impl.latency_ms)
            self.ratios[record.family].append(
                record.impl.latency_ms / max(record.oracle.latency_ms, 0.001)
            )
        line: dict[str, Any] = {
            "id": record.id,
            "round": record.round,
            "family": record.family,
            "query": record.query,
            "request": record.request.describe(),
            "outcome": record.outcome,
            "detail": record.detail,
            "oracle": record.oracle.describe(),
            "impl": record.impl.describe(),
            "latency_outlier": outlier,
        }
        if record.recheck:
            line["recheck"] = {
                "oracle": record.recheck[0].describe(),
                "impl": record.recheck[1].describe(),
            }
        self._cases.write(json.dumps(line, ensure_ascii=False) + "\n")
        summary = {
            "id": record.id,
            "family": record.family,
            "query": record.query,
            "oracle_ms": round(record.oracle.latency_ms, 1),
            "impl_ms": round(record.impl.latency_ms, 1),
        }
        if outlier:
            self.outliers.append(summary)
        if record.outcome != "match" or outlier:
            self._artifact(record, outlier)
        if record.outcome in self.config.fail_on:
            self.failures.append(
                {**summary, "outcome": record.outcome, "detail": record.detail}
            )

    def _artifact(self, record: CaseRecord, outlier: bool) -> None:
        if self.artifacts >= self.config.max_artifacts:
            return
        self.artifacts += 1
        limit = self.config.artifact_bytes

        def body(exchange: Exchange) -> Any:
            raw = exchange.body[:limit]
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                return raw.decode(errors="replace")

        value = {
            "id": record.id,
            "family": record.family,
            "outcome": record.outcome,
            "latency_outlier": outlier,
            "detail": record.detail,
            "query": record.query,
            "request": record.request.describe(),
            "oracle": {**record.oracle.describe(), "body": body(record.oracle)},
            "impl": {**record.impl.describe(), "body": body(record.impl)},
        }
        path = self.root / "artifacts" / f"{record.id}.json"
        path.write_text(json.dumps(value, indent=2, ensure_ascii=False))

    def record_ingest(self, record: IngestRecord) -> None:
        outcome = record.outcome
        self.ingest_outcomes[outcome] += 1
        if record.oracle.status is not None:
            self.ingest_latency["oracle"].append(record.oracle.latency_ms)
        if record.impl.status is not None:
            self.ingest_latency["impl"].append(record.impl.latency_ms)
        line = {
            "round": record.round,
            "batch": record.batch,
            "items": record.items,
            "outcome": outcome,
            "oracle": record.oracle.describe(),
            "impl": record.impl.describe(),
        }
        if outcome != "ok":
            line["oracle_body"] = record.oracle.body[:2000].decode(errors="replace")
            line["impl_body"] = record.impl.body[:2000].decode(errors="replace")
        self._ingest.write(json.dumps(line, ensure_ascii=False) + "\n")

    def record_round(self, record: RoundRecord) -> None:
        self.rounds += 1
        if record.truncated:
            self.note(f"round {record.index} was cut short by the time budget")
        elif not all(record.visible.values()):
            self.invisible_rounds.append(record.index)
            self.note(f"round {record.index} visibility: {record.visible}")
        self._rounds.write(
            json.dumps(
                {
                    "round": record.index,
                    "profile": record.profile,
                    "batches": record.batches,
                    "stats": record.stats,
                    "visible": record.visible,
                    "truncated": record.truncated,
                    "load_ms": round(record.load_ms, 1),
                }
            )
            + "\n"
        )
        self._rounds.flush()

    def record_dataset(self, index: int, dataset: Any) -> None:
        path = self.root / "data" / f"round-{index:04d}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as output:
            json.dump(dataset, output, ensure_ascii=False)

    def _failure_reasons(self) -> list[str]:
        reasons = []
        fail_on = self.config.fail_on
        for outcome in OUTCOMES:
            if outcome in fail_on and self.outcomes[outcome]:
                reasons.append(f"{self.outcomes[outcome]} {outcome} case(s)")
        if "ingest" in fail_on:
            if self.ingest_outcomes["impl_error"]:
                reasons.append(
                    f"{self.ingest_outcomes['impl_error']} write batch(es) accepted "
                    "by the oracle were rejected by the implementation"
                )
            if self.invisible_rounds:
                reasons.append(
                    f"rounds {self.invisible_rounds} never became fully visible"
                )
        if "latency" in fail_on and self.outliers:
            reasons.append(f"{len(self.outliers)} latency outlier(s)")
        if sum(self.outcomes.values()) == 0:
            reasons.append("no query cases executed")
        return reasons

    def finish(
        self, *, elapsed_s: float, resources: dict[str, Any] | None = None
    ) -> Summary:
        for handle in (self._cases, self._ingest, self._rounds):
            handle.close()
        reasons = self._failure_reasons()
        families = {}
        for family in sorted(self.families):
            families[family] = {
                "outcomes": dict(self.families[family]),
                "oracle_ms": _latency_stats(self.oracle_latency[family]),
                "impl_ms": _latency_stats(self.impl_latency[family]),
                "ratio": {
                    "p50": percentile(self.ratios[family], 0.5),
                    "p90": percentile(self.ratios[family], 0.9),
                },
            }
        all_oracle = [v for values in self.oracle_latency.values() for v in values]
        all_impl = [v for values in self.impl_latency.values() for v in values]
        all_ratios = [v for values in self.ratios.values() for v in values]
        outliers = sorted(
            self.outliers,
            key=lambda item: item["impl_ms"] / max(item["oracle_ms"], 0.001),
            reverse=True,
        )
        value = {
            "config": self.config.describe(),
            "elapsed_s": round(elapsed_s, 1),
            "failed": bool(reasons),
            "reasons": reasons,
            "notes": self.notes,
            "rounds": self.rounds,
            "invisible_rounds": self.invisible_rounds,
            "cases": sum(self.outcomes.values()),
            "outcomes": dict(self.outcomes),
            "ingest": {
                "outcomes": dict(self.ingest_outcomes),
                "oracle_ms": _latency_stats(self.ingest_latency["oracle"]),
                "impl_ms": _latency_stats(self.ingest_latency["impl"]),
            },
            "latency": {
                "oracle_ms": _latency_stats(all_oracle),
                "impl_ms": _latency_stats(all_impl),
                "ratio": {
                    "p50": percentile(all_ratios, 0.5),
                    "p90": percentile(all_ratios, 0.9),
                    "p99": percentile(all_ratios, 0.99),
                },
                "outliers": len(self.outliers),
                "top_outliers": outliers[:20],
            },
            "families": families,
            "failures": self.failures[:100],
            "resources": resources,
        }
        (self.root / "summary.json").write_text(
            json.dumps(value, indent=2, ensure_ascii=False)
        )
        markdown = render_markdown(value)
        (self.root / "summary.md").write_text(markdown)
        print(markdown, flush=True)
        return Summary(
            failed=bool(reasons),
            reasons=reasons,
            output_dir=self.root,
            markdown=markdown,
            outcomes=Counter(self.outcomes),
        )


def render_markdown(value: dict[str, Any]) -> str:
    config = value["config"]
    status = "FAILED" if value["failed"] else "passed"
    lines = [
        f"## Differential fuzz: {config['product']} ({status})",
        "",
        f"- seed `{config['seed']}`, run `{config['run_id']}`, stack "
        f"`{config['stack']}`, scale `{config['scale']}`",
        f"- {value['elapsed_s']}s elapsed of {config['duration_s']:.0f}s budget, "
        f"{value['rounds']} data rounds, {value['cases']} query cases",
    ]
    for reason in value["reasons"]:
        lines.append(f"- **failure:** {reason}")
    for note in value["notes"]:
        lines.append(f"- note: {note}")
    outcomes = value["outcomes"]
    lines += [
        "",
        "| outcome | cases |",
        "| --- | ---: |",
        *(f"| {name} | {outcomes[name]} |" for name in OUTCOMES if outcomes.get(name)),
    ]
    ingest = value["ingest"]
    lines += [
        "",
        f"Ingest batches: {ingest['outcomes']}; p50 write latency oracle "
        f"{_ms(ingest['oracle_ms']['p50'])} ms, implementation "
        f"{_ms(ingest['impl_ms']['p50'])} ms.",
    ]
    latency = value["latency"]
    lines += [
        "",
        "Query latency in ms over every answered case:",
        "",
        "| side | p50 | p90 | p99 | p99.9 | max |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
        *(
            f"| {side} | "
            + " | ".join(_ms(latency[key].get(p)) for p in PERCENTILES)
            + " |"
            for side, key in (("oracle", "oracle_ms"), ("implementation", "impl_ms"))
        ),
    ]
    lines += [
        "",
        "Query latency (implementation / oracle ratio p50 "
        f"{_ratio(latency['ratio']['p50'])}, p90 {_ratio(latency['ratio']['p90'])}, "
        f"p99 {_ratio(latency['ratio']['p99'])}; {latency['outliers']} outliers):",
        "",
        "| family | cases | non-match | oracle p50/p99 ms | impl p50/p99 ms "
        "| ratio p50/p90 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for family, stats in value["families"].items():
        counts = stats["outcomes"]
        total = sum(counts.values())
        non_match = total - counts.get("match", 0)
        lines.append(
            f"| {family} | {total} | {non_match} | "
            f"{_ms(stats['oracle_ms']['p50'])}/{_ms(stats['oracle_ms']['p99'])} | "
            f"{_ms(stats['impl_ms']['p50'])}/{_ms(stats['impl_ms']['p99'])} | "
            f"{_ratio(stats['ratio']['p50'])}/{_ratio(stats['ratio']['p90'])} |"
        )
    resources = value.get("resources")
    if resources and resources["roles"]:
        lines += [
            "",
            f"Container resources over {resources['elapsed_s']}s, sampled every "
            f"{resources['interval_s']:g}s (`shared` is object storage used by "
            "both):",
            "",
            "| role | services | CPU s | cores mean/p95/max "
            "| memory MiB mean/p95/max |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
        for role, stats in resources["roles"].items():
            cores, memory = stats["cpu_cores"], stats["memory_mib"]
            core_text = "/".join(_num(cores[key], 2) for key in ("mean", "p95", "max"))
            memory_text = "/".join(_num(memory[key]) for key in ("mean", "p95", "max"))
            lines.append(
                f"| {role} | {', '.join(stats['services'])} | "
                f"{stats['cpu_seconds']} | {core_text} | {memory_text} |"
            )
    if latency["top_outliers"]:
        lines += ["", "Slowest relative to the oracle:", ""]
        for item in latency["top_outliers"][:10]:
            lines.append(
                f"- `{item['id']}` {item['family']}: {item['impl_ms']} ms vs "
                f"{item['oracle_ms']} ms — `{_inline(item['query'])}`"
            )
    if value["failures"]:
        lines += ["", "First failing cases (full bodies under `artifacts/`):", ""]
        for item in value["failures"][:15]:
            detail = _inline(item["detail"] or "")[:300]
            lines.append(
                f"- `{item['id']}` **{item['outcome']}** {item['family']}: "
                f"`{_inline(item['query'])}` — {detail}"
            )
    return "\n".join(lines) + "\n"


def _inline(value: str) -> str:
    return value.replace("`", "'").replace("\n", " ")[:400]
