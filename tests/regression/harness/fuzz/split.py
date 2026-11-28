"""The `split` implementation config: the two sharded writers plus two reader
groups of two replicas each, with every fragmentable shard read split across
a group (`*-split.yaml`), and the evidence in the replicas' scrapes that reads
really were split."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Mapping
from typing import Any

from .cold import scrape
from .config import FuzzConfig

SPLIT = "split"
DECISIONS = "telemetry_read_group_split_decisions_total"
# A summary: `_count` reads were fragmented into `_sum` fragments.
FRAGMENTS = "telemetry_read_group_fragments"
HEDGES = "telemetry_read_group_hedges_total"
# Metrics only: PromQL pushdown outcomes and shard reads cut into time ranges.
METRICS_COUNTERS = ("metrics_pushdown_total", "metrics_split_reads_total")
# The group 0 replica the fuzzer reads through comes first.
REPLICAS = ("0a", "0b", "1a", "1b")

_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def replicas(product: str) -> tuple[str, ...]:
    """Compose services of the reader replicas, `<product>-split-<shard><replica>`."""
    return tuple(f"{product}-split-{replica}" for replica in REPLICAS)


def validate(product: str, config: FuzzConfig) -> None:
    setting = f"FUZZ_{product.upper()}_CONFIG={SPLIT}"
    if config.cold is not None:
        raise ValueError(
            f"{setting} does not support FUZZ_SCENARIO=cold, which restarts "
            "a single reader"
        )
    if config.stack != "compose":
        raise ValueError(
            f"{setting} starts and scrapes its own reader groups: FUZZ_STACK=compose"
        )


def _samples(exposition: str) -> list[tuple[str, dict[str, str], float]]:
    samples = []
    for line in exposition.splitlines():
        if not line or line.startswith("#"):
            continue
        name = re.split(r"[{ ]", line, maxsplit=1)[0]
        labels = line[len(name) :].rsplit(" ", 1)[0]
        samples.append(
            (name, dict(_LABEL.findall(labels)), float(line.rsplit(" ", 1)[1]))
        )
    return samples


def split_evidence(
    scrapes: Mapping[str, str | None], counters: tuple[str, ...] = ()
) -> dict[str, Any]:
    """Split decisions by reason, fragmented reads, fragments, hedges and
    each of `counters` (by label set) summed over the replicas' scrapes; a
    replica that could not be scraped maps to None and is listed as such."""
    decisions: Counter[str] = Counter()
    totals: Counter[str] = Counter()
    extra: Counter[str] = Counter()
    per_replica: dict[str, dict[str, float] | None] = {}
    for replica, text in scrapes.items():
        if text is None:
            per_replica[replica] = None
            continue
        own: Counter[str] = Counter()
        for name, labels, value in _samples(text):
            if name == DECISIONS:
                own[labels.get("reason", "")] += value
            elif name == f"{FRAGMENTS}_count":
                totals["fragmented_reads"] += value
            elif name == f"{FRAGMENTS}_sum":
                totals["fragments"] += value
            elif name == HEDGES:
                totals["hedges"] += value
            elif name in counters:
                rendered = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
                extra[f"{name}{{{rendered}}}" if rendered else name] += value
        per_replica[replica] = dict(own)
        decisions.update(own)
    return {
        "decisions": dict(decisions),
        "fragmented_reads": totals["fragmented_reads"],
        "fragments": totals["fragments"],
        "hedges": totals["hedges"],
        "counters": dict(sorted(extra.items())),
        "replicas": per_replica,
        "unreachable": [name for name, text in scrapes.items() if text is None],
    }


def split_failure(evidence: Mapping[str, Any]) -> str | None:
    """Why the run did not exercise splitting, or None when it did."""
    if evidence["decisions"].get(SPLIT, 0) > 0:
        return None
    reason = (
        f'no read was split: {DECISIONS}{{reason="{SPLIT}"}} is 0 across the '
        f"reader replicas (decisions {evidence['decisions']})"
    )
    if evidence["unreachable"]:
        reason += f"; could not scrape {evidence['unreachable']}"
    return reason


def report(
    urls: Mapping[str, str], counters: tuple[str, ...] = ()
) -> tuple[dict[str, Any], list[str]]:
    """Scrapes each replica's `/metrics` URL in `urls` and returns the
    summary section and failure reasons."""
    scrapes: dict[str, str | None] = {}
    for replica, url in urls.items():
        try:
            scrapes[replica] = scrape(url)
        except OSError as error:
            print(f"fuzz: could not scrape {replica} ({url}): {error}", flush=True)
            scrapes[replica] = None
    evidence = split_evidence(scrapes, counters)
    failure = split_failure(evidence)
    return {"split": evidence}, [failure] if failure else []
