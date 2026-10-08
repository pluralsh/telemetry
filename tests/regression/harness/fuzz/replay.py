"""Flatten a finished fuzz run into inputs for a crate's native replay test:

PYTHONPATH=tests/regression python -m harness.fuzz.replay PRODUCT RUN_DIR OUT_DIR

The run must have been recorded with `FUZZ_RECORD_DATA` (the default). Writes
`OUT_DIR/cases.json` (matched cases only, with the server's parameter
defaults applied) plus the data:

- logs: `rounds.json`, one list of Loki push streams per round;
- traces: `rounds.json`, compact OTLP resource spans per request per round;
- metrics: `rounds/round-NNNN-MMM.pb`, snappy remote-write bodies, so the
  replay ingests through the server's own decoder (native histograms included).

Each crate's ignored `profile_fuzz_cases` test reads `PROFILE_CASES=OUT_DIR`;
see `crates/{logs,traces}/src/db/profile.rs` and `crates/metrics/src/profile.rs`.
"""

from __future__ import annotations

import base64
import gzip
import json
import re
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from ..metrics.fixture import NativeHistogram, Sample, Series
from ..metrics.wire import remote_write_body

PRODUCTS = ("logs", "metrics", "traces")
METRICS_BATCH_SAMPLES = 20_000

_KINDS = {
    "SPAN_KIND_UNSPECIFIED": 0,
    "SPAN_KIND_INTERNAL": 1,
    "SPAN_KIND_SERVER": 2,
    "SPAN_KIND_CLIENT": 3,
    "SPAN_KIND_PRODUCER": 4,
    "SPAN_KIND_CONSUMER": 5,
}
_CODES = {"STATUS_CODE_UNSET": 0, "STATUS_CODE_OK": 1, "STATUS_CODE_ERROR": 2}
_LOG_RANGE_FUNCTIONS = (
    "count_over_time",
    "rate(",
    "bytes_over_time",
    "bytes_rate",
    "sum_over_time",
    "avg_over_time",
    "min_over_time",
    "max_over_time",
    "quantile_over_time",
    "first_over_time",
    "last_over_time",
    "absent_over_time",
)

Params = list[tuple[str, str]]


def _rounds(run: Path) -> Iterator[dict[str, Any]]:
    for path in sorted((run / "data").glob("round-*.json.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            yield json.load(handle)


def _one(params: Params) -> dict[str, str]:
    one: dict[str, str] = {}
    for key, value in params:
        one.setdefault(key, value)
    return one


def _many(params: Params, key: str) -> list[str]:
    return [value for name, value in params if name == key]


# Traces --------------------------------------------------------------------


def _hex_id(value: str | None) -> str:
    return base64.b64decode(value).hex() if value else ""


def _enum(value: int | str, names: dict[str, int]) -> int:
    return value if isinstance(value, int) else names[value]


def _scalar(value: dict[str, Any]) -> dict[str, Any]:
    ((kind, item),) = value.items()
    if kind == "stringValue":
        return {"s": item}
    if kind == "intValue":
        return {"i": int(item)}
    if kind == "doubleValue":
        return {"d": float(item)}
    if kind == "boolValue":
        return {"b": bool(item)}
    raise ValueError(f"unsupported attribute value {kind}")


def _attributes(items: list[dict[str, Any]] | None) -> list[list[Any]]:
    return [[item["key"], _scalar(item["value"])] for item in items or []]


def _span(span: dict[str, Any]) -> dict[str, Any]:
    status = span.get("status", {})
    return {
        "trace_id": _hex_id(span["traceId"]),
        "span_id": _hex_id(span["spanId"]),
        "parent_span_id": _hex_id(span.get("parentSpanId")),
        "name": span["name"],
        "kind": _enum(span.get("kind", 0), _KINDS),
        "start": int(span["startTimeUnixNano"]),
        "end": int(span["endTimeUnixNano"]),
        "attributes": _attributes(span.get("attributes")),
        "events": [
            [
                int(event["timeUnixNano"]),
                event.get("name", ""),
                _attributes(event.get("attributes")),
            ]
            for event in span.get("events", [])
        ],
        "status_code": _enum(status.get("code", 0), _CODES),
        "status_message": status.get("message", ""),
    }


def _resource(resource_spans: dict[str, Any]) -> dict[str, Any]:
    return {
        "resource": _attributes(resource_spans.get("resource", {}).get("attributes")),
        "scopes": [
            {
                "name": scope.get("scope", {}).get("name", ""),
                "version": scope.get("scope", {}).get("version", ""),
                "spans": [_span(span) for span in scope.get("spans", [])],
            }
            for scope in resource_spans.get("scopeSpans", [])
        ],
    }


def _traces_case(path: str, params: Params) -> dict[str, Any] | None:
    one = _one(params)

    def number(key: str) -> int | None:
        return int(one[key]) if key in one else None

    if match := re.search(r"/traces/([0-9a-fA-F]+)$", path):
        return {
            "kind": "by_id",
            "trace_id": match.group(1),
            "start": number("start"),
            "end": number("end"),
        }
    if path.endswith("/api/search"):
        return {
            "kind": "search",
            "query": one.get("q", "{}"),
            "start": int(one["start"]),
            "end": int(one["end"]),
            "limit": number("limit"),
            "spss": number("spss"),
        }
    if path.endswith("/search/tags"):
        return {
            "kind": "tags",
            "scope": one.get("scope"),
            "start": int(one["start"]),
            "end": int(one["end"]),
        }
    if match := re.search(r"/search/tag/([^/]+)/values$", path):
        return {
            "kind": "tag_values",
            "name": unquote(match.group(1)),
            "query": one.get("q"),
            "start": int(one["start"]),
            "end": int(one["end"]),
        }
    return None


# Logs ----------------------------------------------------------------------


def _is_log_query(query: str) -> bool:
    return query.lstrip().startswith("{") and not any(
        name in query for name in _LOG_RANGE_FUNCTIONS
    )


def _logs_case(path: str, params: Params) -> dict[str, Any] | None:
    one = _one(params)
    if path.endswith("/query_range"):
        start, end = int(one["start"]), int(one["end"])
        if "step" in one:
            step = int(float(one["step"]) * 1e9)
        else:
            step = max((end - start) // 10**9 // 250, 1) * 10**9
        return {
            "kind": "range",
            "query": one["query"],
            "start": start,
            "end": end,
            "step": step,
            "limit": int(one.get("limit", 100)),
            "direction": one.get("direction", "backward"),
        }
    if path.endswith("/query"):
        return {
            "kind": "instant_logs" if _is_log_query(one["query"]) else "instant",
            "query": one["query"],
            "time": int(one["time"]),
            "limit": int(one.get("limit", 100)),
            "direction": one.get("direction", "backward"),
        }
    if path.endswith("/labels"):
        return {"kind": "labels", "start": int(one["start"]), "end": int(one["end"])}
    if match := re.search(r"/label/([^/]+)/values$", path):
        return {
            "kind": "label_values",
            "name": unquote(match.group(1)),
            "start": int(one["start"]),
            "end": int(one["end"]),
        }
    if path.endswith("/series"):
        return {
            "kind": "series",
            "selectors": _many(params, "match[]"),
            "start": int(one["start"]),
            "end": int(one["end"]),
        }
    return None


# Metrics -------------------------------------------------------------------


def _seconds_ms(value: str) -> int:
    return round(float(value) * 1000)


def _metrics_case(path: str, params: Params) -> dict[str, Any] | None:
    one = _one(params)
    if path.endswith("/query_range"):
        return {
            "kind": "range",
            "query": one["query"],
            "start_ms": _seconds_ms(one["start"]),
            "end_ms": _seconds_ms(one["end"]),
            "step_ms": _seconds_ms(one["step"]),
        }
    if path.endswith("/query"):
        return {
            "kind": "instant",
            "query": one["query"],
            "time_ms": _seconds_ms(one["time"]),
        }
    bounds = {"start_ms": _seconds_ms(one["start"]), "end_ms": _seconds_ms(one["end"])}
    matchers = _many(params, "match[]")
    if path.endswith("/series"):
        return {"kind": "series", "matchers": matchers, **bounds}
    if path.endswith("/labels"):
        return {"kind": "labels", "matchers": matchers, **bounds}
    if match := re.search(r"/label/([^/]+)/values$", path):
        return {
            "kind": "label_values",
            "name": unquote(match.group(1)),
            "matchers": matchers,
            **bounds,
        }
    return None


def _spans(value: list[list[Any]]) -> tuple[tuple[int, tuple[int, ...]], ...]:
    return tuple(
        (int(offset), tuple(int(c) for c in counts)) for offset, counts in value
    )


def _metrics_series(item: dict[str, Any]) -> Series:
    return Series(
        labels=tuple(item["labels"].items()),
        samples=tuple(Sample(int(ts), float(value)) for ts, value in item["samples"]),
        histograms=tuple(
            NativeHistogram(
                timestamp_ms=int(h["timestamp_ms"]),
                schema=int(h["schema"]),
                zero_threshold=float(h["zero_threshold"]),
                zero_count=int(h["zero_count"]),
                sum=float(h["sum"]),
                positive=_spans(h.get("positive") or []),
                negative=_spans(h.get("negative") or []),
                custom_values=tuple(float(v) for v in h.get("custom_values") or ()),
            )
            for h in item.get("histograms") or ()
        ),
    )


def _metrics_batches(series: list[Series]) -> Iterator[tuple[Series, ...]]:
    batch: list[Series] = []
    weight = 0
    for item in series:
        batch.append(item)
        weight += len(item.samples) + 10 * len(item.histograms) + 1
        if weight >= METRICS_BATCH_SAMPLES:
            yield tuple(batch)
            batch, weight = [], 0
    if batch:
        yield tuple(batch)


# Driver --------------------------------------------------------------------


def _write_rounds(product: str, run: Path, out: Path) -> int:
    rounds = list(_rounds(run))
    if product == "logs":
        (out / "rounds.json").write_text(
            json.dumps([data["streams"] for data in rounds])
        )
    elif product == "traces":
        flattened = [
            [
                [_resource(rs) for rs in request["resourceSpans"]]
                for request in data["requests"]
            ]
            for data in rounds
        ]
        (out / "rounds.json").write_text(json.dumps(flattened))
    else:
        directory = out / "rounds"
        directory.mkdir(exist_ok=True)
        for stale in directory.glob("*.pb"):
            stale.unlink()
        for index, data in enumerate(rounds):
            series = [_metrics_series(item) for item in data["series"]]
            for number, batch in enumerate(_metrics_batches(series)):
                path = directory / f"round-{index:04d}-{number:03d}.pb"
                path.write_bytes(remote_write_body(batch))
    return len(rounds)


CASES: dict[str, Callable[[str, Params], dict[str, Any] | None]] = {
    "logs": _logs_case,
    "metrics": _metrics_case,
    "traces": _traces_case,
}


def prepare(product: str, run: Path, out: Path) -> tuple[int, int, int]:
    """Returns (rounds, cases, skipped)."""
    if product not in PRODUCTS:
        raise ValueError(f"product must be one of {PRODUCTS}")
    out.mkdir(parents=True, exist_ok=True)
    rounds = _write_rounds(product, run, out)
    convert = CASES[product]
    cases, skipped = [], 0
    with (run / "cases.jsonl").open(encoding="utf-8") as handle:
        for line in handle:
            case = json.loads(line)
            if case["outcome"] != "match":
                continue
            request = case["request"]
            params = [(str(k), str(v)) for k, v in request.get("params") or []]
            converted = convert(request["path"], params)
            if converted is None:
                skipped += 1
                continue
            cases.append(
                {
                    "id": case["id"],
                    "family": case["family"],
                    "impl_ms": case["impl"]["latency_ms"],
                    **converted,
                }
            )
    (out / "cases.json").write_text(json.dumps(cases))
    return rounds, len(cases), skipped


def main(argv: list[str]) -> int:
    if len(argv) != 3 or argv[0] not in PRODUCTS:
        products = ",".join(PRODUCTS)
        print(
            f"usage: python -m harness.fuzz.replay {{{products}}} RUN_DIR OUT_DIR",
            file=sys.stderr,
        )
        return 2
    rounds, cases, skipped = prepare(argv[0], Path(argv[1]), Path(argv[2]))
    print(f"{rounds} rounds, {cases} cases, {skipped} skipped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
