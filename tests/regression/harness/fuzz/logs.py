"""Differential LogQL fuzzing of Logs against Loki."""

from __future__ import annotations

import json
import math
import os
import random
import re
import time
from collections import defaultdict
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..canonical import assert_equivalent
from ..compose import ComposeProject
from .config import Endpoint, FuzzConfig, endpoint_from_env
from .rand import (
    ODD_STRINGS,
    WORDS,
    chance,
    quote,
    regex_escape,
    sample,
    scaled,
    weighted,
    zipf,
)
from .runner import (
    Batch,
    Case,
    FuzzProduct,
    Inconclusive,
    Mismatch,
    Probe,
    Round,
    json_body,
)
from .stack import fuzz_stack
from .transport import Exchange, Request

PRODUCT_DIR = Path(__file__).resolve().parents[2] / "products" / "logs"
# `production` uses the crate defaults for segment duration, page size and
# compaction instead of the small values that exercise edge cases.
IMPL_CONFIG_DIRS = {"regression": ".", "production": "production"}
SECOND = 1_000_000_000
MAX_LIMIT = 5000
MAX_POINTS = 10_000
SENTINELS = 4
PUSH_BYTES = 1_000_000
METADATA_LOOKBACK = 24 * 3600 * SECOND

APPS = ("checkout", "cart", "search", "auth", "billing", "gateway", "worker", "cron")
ENVS = ("prod", "staging", "dev")
REGIONS = ("us-east-1", "us-west-2", "eu-west-1", "ap-south-1")
LEVELS = ("debug", "info", "warn", "error", "fatal")
METHODS = ("GET", "POST", "PUT", "DELETE", "PATCH")
PATHS = ("/api/cart", "/api/checkout", "/healthz", "/api/search", "/login", "/api/v2/x")
STATUSES = (200, 201, 204, 301, 400, 401, 404, 429, 500, 502, 503)
DURATIONS = ("12us", "250ms", "1.5s", "3s", "45s", "2m")
SIZES = ("512B", "12KB", "3KiB", "1.5MB")
# Loki's frontend re-serializes byte filters through go-humanize's SI
# formatting, so `size < 3KiB` reaches the querier as `size < 3.1kB`; filter
# literals stick to values that survive that round trip.
FILTER_SIZES = ("512B", "3kB", "12KB", "1.5MB")
RANGES = ("10s", "30s", "47s", "1m", "2m", "5m", "10m")
STEPS_S = (1, 5, 7, 15, 30, 60, 90)
INEXACT = ("avg", "stddev", "stdvar", "quantile", "rate", "/", "^")
GROUP_LABELS = ("app", "env", "region", "pod", "tier", "fuzz_round", "level", "method")

FIELDS: dict[str, dict[str, tuple[str, ...]]] = {
    "json": {
        "numeric": ("status", "latency_ms", "bytes", "ctx_attempt"),
        "string": ("level", "method", "path", "user", "msg", "ctx_region"),
    },
    "logfmt": {
        "numeric": ("status", "latency_ms", "bytes"),
        "string": ("level", "method", "path", "user", "msg"),
        "duration": ("elapsed",),
        "bytes": ("size",),
    },
    "regexp": {"numeric": ("status",)},
    "pattern": {"numeric": ("status",), "string": ("method",)},
}

PROFILES = (
    (3.0, "mixed"),
    (2.0, "wide"),
    (2.0, "deep"),
    (1.0, "bursty"),
    (1.0, "sparse"),
    (0.7, "long_lines"),
    (0.7, "odd"),
)

# (streams low, streams high, entries/stream low, entries/stream high)
SHAPES = {
    "mixed": (10, 50, 5, 50),
    "wide": (50, 300, 1, 5),
    "deep": (1, 5, 200, 2000),
    "bursty": (5, 20, 20, 100),
    "sparse": (20, 100, 1, 3),
    "long_lines": (3, 10, 5, 30),
    "odd": (5, 30, 3, 20),
}


@dataclass
class Catalog:
    labels: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    streams: list[dict[str, str]] = field(default_factory=list)
    latest_end: int = 0

    def add_stream(self, labels: dict[str, str]) -> None:
        for name, value in labels.items():
            self.labels[name].add(value)
        self.streams.append(labels)


def _entries(response: Any) -> int:
    data = response.get("data") if isinstance(response, dict) else None
    if not isinstance(data, dict) or data.get("resultType") != "streams":
        return 0
    return sum(len(stream.get("values", ())) for stream in data.get("result", ()))


def _mask_error_details(response: Any) -> Any:
    """`__error_details__` carries parser-specific free text (Loki surfaces
    jsoniter messages); compare its presence, not its wording."""
    data = response.get("data") if isinstance(response, dict) else None
    if not isinstance(data, dict) or data.get("resultType") != "streams":
        return response
    for stream in data.get("result", ()):
        labels = stream.get("stream")
        if isinstance(labels, dict) and "__error_details__" in labels:
            labels["__error_details__"] = "<masked>"
    return response


def _ranked(response: Any) -> Any:
    """topk/bottomk may keep any of the series tied at the cut, and Loki picks
    in Go map order, so only each step's selected values are comparable."""
    data = response.get("data") if isinstance(response, dict) else None
    if not isinstance(data, dict) or data.get("resultType") not in ("vector", "matrix"):
        return response
    steps: dict[str, list[str]] = defaultdict(list)
    for series in data.get("result", ()):
        samples = series["values"] if "values" in series else [series["value"]]
        for timestamp, value in samples:
            steps[str(float(timestamp))].append(value)
    result = [
        {
            "metric": {"step": step},
            "values": [
                [str(index), value]
                for index, value in enumerate(sorted(values, key=float))
            ],
        }
        for step, values in steps.items()
    ]
    return {"status": "success", "data": {"resultType": "matrix", "result": result}}


def _is_ranked(query: str) -> bool:
    return query.startswith(("topk(", "bottomk("))


def _grouped_positional(query: str) -> bool:
    """Whether a first/last_over_time merges streams through `by`/`without`."""
    for match in re.finditer(r"\b(first|last)_over_time\(", query):
        depth, index = 1, match.end()
        while index < len(query) and depth:
            depth += {"(": 1, ")": -1}.get(query[index], 0)
            index += 1
        if re.match(r"\s*(by|without)\s*\(", query[index:]):
            return True
    return False


def _metric_comparator(query: str):
    return loki_comparator(
        None,
        ranked=_is_ranked(query),
        rate_counter="rate_counter(" in query,
        positional_ties=_grouped_positional(query),
    )


def _formatted_entries(response: Any) -> list[tuple[str, str]] | None:
    data = response.get("data") if isinstance(response, dict) else None
    if not isinstance(data, dict) or data.get("resultType") != "streams":
        return None
    return sorted(
        (str(value[0]), value[1])
        for stream in data.get("result", ())
        for value in stream["values"]
    )


def loki_comparator(
    limit: int | None,
    *,
    ranked: bool = False,
    rate_counter: bool = False,
    formatted: bool = False,
    positional_ties: bool = False,
):
    def compare(oracle: Exchange, impl: Exchange) -> None:
        expected = _mask_error_details(json_body(oracle))
        actual = _mask_error_details(json_body(impl))
        try:
            assert_equivalent(expected, actual)
        except AssertionError as error:
            if ranked:
                try:
                    assert_equivalent(_ranked(expected), _ranked(actual))
                except AssertionError:
                    pass
                else:
                    return
            if rate_counter:
                raise Inconclusive(
                    "released Lokis (through 3.7) mix nanoseconds and milliseconds in "
                    "rate_counter's extrapolation; Logs follows the fix in "
                    f"grafana/loki#23684: {error}"
                ) from error
            if positional_ties:
                raise Inconclusive(
                    "first/last_over_time grouped across streams picks among "
                    "same-timestamp samples by Loki's stream-hash merge order: "
                    f"{error}"
                ) from error
            if formatted and _formatted_entries(expected) == _formatted_entries(actual):
                raise Inconclusive(
                    "same-timestamp lines of one stream that line_format makes "
                    "identical collapse to one entry, and Loki's survivor "
                    f"(and so its parsed labels) is not predictable: {error}"
                ) from error
            counts = f"(oracle {_entries(expected)} entries, impl {_entries(actual)})"
            if (
                limit is not None
                and _entries(expected) >= limit
                and _entries(actual) >= limit
            ):
                raise Inconclusive(
                    f"both sides hit limit={limit}; entries tied at the boundary "
                    f"may legitimately differ {counts}: {error}"
                ) from error
            raise Mismatch(f"{error} {counts}") from error

    return compare


class LogsFuzz(FuzzProduct):
    name = "logs"
    default_window = "20m"
    resource_roles = {"loki-fuzz": "oracle", "logs": "impl", "logs-s3": "impl"}

    def __init__(self, config: FuzzConfig) -> None:
        super().__init__(config)
        self.storage = os.environ.get("FUZZ_LOGS_STORAGE", "s3")
        if self.storage not in ("s3", "local"):
            raise ValueError("FUZZ_LOGS_STORAGE must be 's3' or 'local'")
        self.impl_config = os.environ.get("FUZZ_LOGS_CONFIG") or "regression"
        if self.impl_config not in IMPL_CONFIG_DIRS:
            raise ValueError(
                f"FUZZ_LOGS_CONFIG must be one of {tuple(IMPL_CONFIG_DIRS)}"
            )
        if self.impl_config != "regression" and self.storage != "s3":
            raise ValueError(
                "FUZZ_LOGS_CONFIG=production requires FUZZ_LOGS_STORAGE=s3"
            )
        namespace = os.environ.get("FUZZ_LOGS_NAMESPACE", "regression")
        impl = (
            "http://localhost:13111"
            if self.storage == "s3"
            else ("http://localhost:13101")
        )
        oracle = Endpoint("http://localhost:13110")
        self.oracle_read = endpoint_from_env("logs", "oracle_read", oracle)
        self.oracle_write = endpoint_from_env("logs", "oracle_write", oracle)
        self.impl_read = endpoint_from_env(
            "logs", "impl_read", Endpoint(f"{impl}/read/ns/{namespace}")
        )
        self.impl_write = endpoint_from_env(
            "logs", "impl_write", Endpoint(f"{impl}/write/ns/{namespace}")
        )
        # Exact (timestamp, line) re-sends within a stream, which both sides
        # collapse at query time.
        self.duplicate_rate = float(os.environ.get("FUZZ_LOGS_DUPLICATE_RATE", "0.02"))
        self.unaligned_rate = float(os.environ.get("FUZZ_LOGS_UNALIGNED_RATE", "0.5"))
        self.window_ns = int(config.window_s * SECOND)
        self.catalog = Catalog()

    def stack(self) -> AbstractContextManager[object]:
        impl_service = "logs-s3" if self.storage == "s3" else "logs"
        impl_port = 13111 if self.storage == "s3" else 13101
        services = ["loki-fuzz", impl_service]
        if self.storage == "s3":
            services[1:1] = ["minio", "minio-init"]
        os.environ["LOGS_CONFIG_DIR"] = IMPL_CONFIG_DIRS[self.impl_config]
        return fuzz_stack(
            self.config,
            lambda: ComposeProject(
                file=PRODUCT_DIR / "docker-compose.yml",
                name="logs-fuzz",
                services=tuple(services),
                profiles=("fuzz",),
                readiness_urls=(
                    "http://localhost:13110/ready",
                    f"http://localhost:{impl_port}/-/ready",
                ),
            ),
        )

    def describe(self) -> dict[str, object]:
        return {
            **super().describe(),
            "storage": self.storage,
            "impl_config": self.impl_config,
        }

    # Data generation ---------------------------------------------------------

    def generate_round(self, index: int, rng: random.Random) -> Round:
        end = (time.time_ns() // SECOND - 2) * SECOND
        start = end - self.window_ns
        profile = weighted(rng, PROFILES)
        streams = self._streams(index, rng, profile, start, end)
        sentinel_labels = {
            "fuzz_run": self.config.run_id,
            "job": "fuzz-sentinel",
            "fuzz_round": str(index),
        }
        sentinels = [
            {
                "stream": {**sentinel_labels, "sentinel": str(number)},
                "values": [[str(end - number), f"sentinel {index} {number}"]],
            }
            for number in range(SENTINELS)
        ]
        self.catalog.latest_end = end
        entries = sum(len(stream["values"]) for stream in streams)
        return Round(
            index=index,
            profile=profile,
            batches=[
                *self._batches(streams, "data"),
                *self._batches(sentinels, "sentinel"),
            ],
            probes=[self._probe(sentinel_labels, start, end)],
            dataset={"window_ns": [start, end], "streams": streams},
            stats={
                "streams": len(streams),
                "entries": entries,
                "bytes": sum(len(v[1]) for s in streams for v in s["values"]),
            },
        )

    def _probe(self, labels: dict[str, str], start: int, end: int) -> Probe:
        selector = "{" + ", ".join(f"{k}={quote(v)}" for k, v in labels.items()) + "}"

        def ready(exchange: Exchange) -> bool:
            if not exchange.ok():
                return False
            try:
                return _entries(exchange.json()) >= SENTINELS
            except (ValueError, AttributeError):
                return False

        return Probe(
            "sentinel",
            Request(
                "GET",
                "/loki/api/v1/query_range",
                (
                    ("query", selector),
                    ("start", str(start)),
                    ("end", str(end + SECOND)),
                    ("limit", "100"),
                ),
            ),
            ready,
        )

    def _batches(self, streams: list[dict[str, Any]], prefix: str) -> list[Batch]:
        batches: list[Batch] = []
        current: list[dict[str, Any]] = []
        size = items = 0

        def flush() -> None:
            body = json.dumps({"streams": current}, ensure_ascii=False).encode()
            request = Request(
                "POST",
                "/loki/api/v1/push",
                body=body,
                headers=(("Content-Type", "application/json"),),
            )
            batches.append(Batch(f"{prefix}-{len(batches)}", request, request, items))

        for stream in streams:
            labels = stream["stream"]
            header = len(json.dumps(labels, ensure_ascii=False).encode()) + 32
            chunk: list[list[str]] = []
            for value in stream["values"]:
                entry = len(json.dumps(value, ensure_ascii=False).encode()) + 2
                cost = entry if chunk else entry + header
                if (current or chunk) and size + cost > PUSH_BYTES:
                    if chunk:
                        current.append({"stream": labels, "values": chunk})
                        chunk = []
                    flush()
                    current, size, items = [], 0, 0
                    cost = entry + header
                elif len(chunk) >= 1000:
                    current.append({"stream": labels, "values": chunk})
                    chunk = []
                    cost = entry + header
                chunk.append(value)
                size += cost
                items += 1
            if chunk:
                current.append({"stream": labels, "values": chunk})
        if current:
            flush()
        return batches

    def _streams(
        self, index: int, rng: random.Random, profile: str, start: int, end: int
    ) -> list[dict[str, Any]]:
        low, high, entries_low, entries_high = SHAPES[profile]
        count = scaled(rng, low, high, self.config.scale)
        # Appending to streams from earlier rounds grows them deep and
        # out of order across flushes, not just within one push.
        reuse = profile == "deep" or chance(rng, 0.3)
        streams = []
        for _ in range(count):
            if reuse and self.catalog.streams and chance(rng, 0.5):
                labels = dict(rng.choice(self.catalog.streams))
            else:
                labels = self._labels(index, rng, profile)
                self.catalog.add_stream(labels)
            line_format = {"long_lines": "long", "odd": "odd"}.get(
                profile,
                weighted(
                    rng,
                    [
                        (3, "logfmt"),
                        (3, "json"),
                        (2, "plain"),
                        (0.3, "long"),
                        (0.3, "odd"),
                    ],
                ),
            )
            entries = scaled(rng, entries_low, entries_high, self.config.scale)
            streams.append(
                {
                    "stream": labels,
                    "values": self._values(
                        rng, profile, line_format, entries, start, end
                    ),
                }
            )
        return streams

    def _labels(self, index: int, rng: random.Random, profile: str) -> dict[str, str]:
        app = zipf(rng, APPS)
        labels = {
            "fuzz_run": self.config.run_id,
            "job": "fuzz",
            "app": app,
            "env": weighted(rng, [(5, "prod"), (2, "staging"), (1, "dev")]),
            "region": rng.choice(REGIONS),
        }
        if profile == "wide" or chance(rng, 0.5):
            pods = 1000 if profile == "wide" else 20
            labels["pod"] = f"{app}-{rng.randrange(pods)}"
        if chance(rng, 0.5):
            labels["fuzz_round"] = str(index)
        if profile == "odd" or chance(rng, 0.03):
            labels["team"] = rng.choice(
                [value for value in ODD_STRINGS if value.strip()]
            )
        if chance(rng, 0.2):
            labels["tier"] = rng.choice(("gold", "silver", "bronze"))
        return labels

    def _values(
        self,
        rng: random.Random,
        profile: str,
        line_format: str,
        count: int,
        start: int,
        end: int,
    ) -> list[list[str]]:
        if profile == "bursty":
            low = rng.randint(start, end - 4 * SECOND)
            high = low + rng.randint(1, 3) * SECOND
        elif profile == "sparse":
            low, high = start, end
        else:
            low = rng.randint(start, end - 60 * SECOND)
            high = rng.randint(low + SECOND, end)
        coarse = profile == "bursty" or chance(rng, 0.1)
        timestamps = [rng.randint(low, high) for _ in range(count)]
        if coarse:
            timestamps = [value - value % SECOND for value in timestamps]
        if chance(rng, 0.4):
            timestamps.sort()
        seen: set[tuple[int, str]] = set()
        values = []
        for timestamp in timestamps:
            line = self._line(rng, line_format)
            # Exact duplicates come only from `duplicate_rate`.
            while (timestamp, line) in seen:
                line = f"{line} #{rng.randrange(1 << 30)}"
            seen.add((timestamp, line))
            values.append([str(timestamp), line])
            if self.duplicate_rate and chance(rng, self.duplicate_rate):
                values.append([str(timestamp), line])
        return values

    def _line(self, rng: random.Random, line_format: str) -> str:
        level = weighted(
            rng,
            [(1, "debug"), (5, "info"), (2, "warn"), (1.5, "error"), (0.2, "fatal")],
        )
        method = zipf(rng, METHODS)
        path = zipf(rng, PATHS)
        status = zipf(rng, STATUSES)
        latency = round(rng.lognormvariate(3, 1.2), rng.choice((0, 1, 3)))
        size = rng.randint(0, 100_000)
        user = f"u{rng.randrange(500)}"
        message = " ".join(rng.choices(WORDS, k=rng.randint(1, 6)))
        if line_format == "logfmt":
            return (
                f"level={level} method={method} path={path} status={status} "
                f"latency_ms={latency} bytes={size} elapsed={rng.choice(DURATIONS)} "
                f"size={rng.choice(SIZES)} user={user} msg={quote(message)}"
            )
        if line_format == "json":
            return json.dumps(
                {
                    "level": level,
                    "method": method,
                    "path": path,
                    "status": status,
                    "latency_ms": latency,
                    "bytes": size,
                    "user": user,
                    "msg": message,
                    "ctx": {
                        "region": rng.choice(REGIONS),
                        "attempt": rng.randint(0, 5),
                    },
                },
                ensure_ascii=False,
            )
        if line_format == "long":
            return " ".join(rng.choices(WORDS, k=rng.randint(300, 2000)))
        if line_format == "odd":
            colored = f"\x1b[31m{level}\x1b[0m" if chance(rng, 0.5) else level
            return (
                f"{rng.choice(ODD_STRINGS)} {message}\t{colored} "
                f"{rng.choice(ODD_STRINGS)} status={status}"
            )
        return f"{level.upper()} {message} status={status} took {latency}ms"

    # Query generation --------------------------------------------------------

    def next_case(self, rng: random.Random) -> Case:
        isolated = 1.0 if self.config.isolated else 0.0
        kind = weighted(
            rng,
            [
                (5, "log"),
                (5, "metric_range"),
                (3, "metric_instant"),
                (1, "series"),
                (0.5 * isolated, "labels"),
                (0.5 * isolated, "label_values"),
            ],
        )
        start, end = self._time_range(rng)
        if kind == "log":
            query, parsed = self._log_query(rng)
            limited = chance(rng, 0.2)
            limit = rng.randint(1, 100) if limited else MAX_LIMIT
            family = (
                "logql.log"
                + (".parsed" if parsed else "")
                + (".limited" if limited else "")
            )
            params = (
                ("query", query),
                ("start", str(start)),
                ("end", str(end)),
                ("limit", str(limit)),
                ("direction", rng.choice(("forward", "backward"))),
            )
            return Case(
                family,
                query,
                Request("GET", "/loki/api/v1/query_range", params),
                loki_comparator(limit, formatted="line_format" in query),
            )
        if kind == "metric_range":
            query = self._metric_query(rng, 0)
            span = max(1, (end - start) // SECOND)
            step = max(rng.choice(STEPS_S), math.ceil(span / MAX_POINTS))
            if not chance(rng, self.unaligned_rate):
                start -= start % (step * SECOND)
                end -= end % (step * SECOND)
            params = (
                ("query", query),
                ("start", str(start)),
                ("end", str(end)),
                ("step", str(step)),
            )
            return Case(
                "logql.metric.range",
                query,
                Request("GET", "/loki/api/v1/query_range", params),
                _metric_comparator(query),
            )
        if kind == "metric_instant":
            query = self._metric_query(rng, 0)
            at = rng.randint(start, end)
            return Case(
                "logql.metric.instant",
                query,
                Request(
                    "GET", "/loki/api/v1/query", (("query", query), ("time", str(at)))
                ),
                _metric_comparator(query),
            )
        # Metadata endpoints resolve time ranges at index/chunk granularity in
        # Loki, so exact edges are approximate; compare over the whole run.
        latest = self.catalog.latest_end
        window = (
            ("start", str(latest - METADATA_LOOKBACK)),
            ("end", str(latest + 60 * SECOND)),
        )
        if kind == "series":
            selectors = [self._selector(rng) for _ in range(rng.randint(1, 2))]
            return Case(
                "logql.series",
                " ".join(selectors),
                Request(
                    "GET",
                    "/loki/api/v1/series",
                    (*(("match[]", value) for value in selectors), *window),
                ),
                loki_comparator(None),
            )
        if kind == "labels":
            return Case(
                "logql.labels",
                "labels",
                Request("GET", "/loki/api/v1/labels", window),
                loki_comparator(None),
            )
        name = rng.choice(("app", "env", "region", "pod", "team", "fuzz_round", "nope"))
        return Case(
            "logql.label_values",
            name,
            Request("GET", f"/loki/api/v1/label/{name}/values", window),
            loki_comparator(None),
        )

    def _time_range(self, rng: random.Random) -> tuple[int, int]:
        end = self.catalog.latest_end
        window = self.window_ns
        shape = weighted(
            rng,
            [
                (4, "window"),
                (3, "slice"),
                (1, "narrow"),
                (1, "extended"),
                (0.3, "after"),
            ],
        )
        if shape == "window":
            return end - window, end + SECOND
        if shape == "slice":
            low = rng.randint(end - window, end - SECOND)
            return low, rng.randint(low + SECOND, end)
        if shape == "narrow":
            low = rng.randint(end - window, end)
            return low, low + rng.randint(1, 10) * SECOND
        if shape == "extended":
            return end - window * 3 // 2, end + 60 * SECOND
        return end + SECOND, end + rng.randint(2, 300) * SECOND

    def _selector(self, rng: random.Random) -> str:
        matchers = [f"fuzz_run={quote(self.config.run_id)}"]
        job = weighted(rng, [(8, 'job="fuzz"'), (1, 'job=~"fuzz.*"'), (1, "")])
        if job:
            matchers.append(job)
        for _ in range(weighted(rng, [(3, 0), (4, 1), (2, 2), (1, 3)])):
            matchers.append(self._matcher(rng))
        rng.shuffle(matchers)
        return "{" + ", ".join(matchers) + "}"

    def _matcher(self, rng: random.Random) -> str:
        label = weighted(
            rng,
            [
                (4, "app"),
                (2, "env"),
                (2, "region"),
                (1.5, "pod"),
                (1, "fuzz_round"),
                (0.5, "team"),
                (0.5, "tier"),
                (0.3, "absent_label"),
            ],
        )
        values = sorted(self.catalog.labels.get(label, ())) or ["none"]
        op = weighted(rng, [(5, "="), (2, "!="), (3, "=~"), (1, "!~")])
        if op in ("=", "!="):
            value = (
                rng.choice(values)
                if chance(rng, 0.85)
                else rng.choice(("missing", "", "ünïcødé"))
            )
        else:
            value = self._label_regex(rng, values)
        return f"{label}{op}{quote(value)}"

    def _label_regex(self, rng: random.Random, values: list[str]) -> str:
        form = weighted(rng, [(3, "alt"), (2, "prefix"), (1, "any"), (1, "class")])
        if form == "alt":
            return "|".join(regex_escape(value) for value in sample(rng, values, 1, 3))
        if form == "prefix":
            value = rng.choice(values)
            return regex_escape(value[: max(1, len(value) // 2)]) + ".*"
        if form == "any":
            return rng.choice((".*", ".+"))
        return "[a-m].*"

    def _term(self, rng: random.Random) -> str:
        form = weighted(
            rng,
            [
                (4, "word"),
                (2, "status"),
                (1, "level"),
                (1, "json"),
                (1, "path"),
                (0.5, "odd"),
            ],
        )
        if form == "word":
            return rng.choice(WORDS)
        if form == "status":
            return f"status={rng.choice(STATUSES)}"
        if form == "level":
            return rng.choice((*LEVELS, "ERROR", "INFO"))
        if form == "json":
            return f'"level":"{rng.choice(LEVELS)}"'
        if form == "path":
            return rng.choice(PATHS)
        return rng.choice([value for value in ODD_STRINGS if value])

    def _line_regex(self, rng: random.Random) -> str:
        first, second = rng.sample(WORDS, 2)
        return weighted(
            rng,
            [
                (3, regex_escape(first)),
                (1, f"(?i){first}"),
                (2, f"{first}|{second}"),
                (1, r"status=5\d\d"),
                (1, r"latency_ms=\d{3,}"),
                (1, r"^\{"),
                (0.5, r"u[0-9]+\b"),
                (0.3, ".*"),
            ],
        )

    def _line_filter(self, rng: random.Random) -> str:
        op = weighted(rng, [(4, "|="), (2, "!="), (2, "|~"), (1, "!~"), (0.5, "|>")])
        if op in ("|=", "!="):
            terms = [
                self._term(rng)
                for _ in range(weighted(rng, [(5, 1), (1, 2), (0.5, 3)]))
            ]
            return f"{op} " + " or ".join(quote(term) for term in terms)
        if op == "|>":
            return f"|> {quote('<_>' + rng.choice(WORDS) + '<_>')}"
        return f"{op} {quote(self._line_regex(rng))}"

    def _parser(self, rng: random.Random) -> tuple[str, dict[str, tuple[str, ...]]]:
        parser = weighted(
            rng, [(4, "json"), (4, "logfmt"), (1, "regexp"), (1, "pattern")]
        )
        if parser == "json":
            return "| json", FIELDS["json"]
        if parser == "logfmt":
            return rng.choice(("| logfmt", "| logfmt", "| logfmt --strict")), FIELDS[
                "logfmt"
            ]
        if parser == "regexp":
            return f"| regexp {quote(r'status=(?P<status>\d+)')}", FIELDS["regexp"]
        return (
            f"| pattern {quote('level=<_> method=<method> <_> status=<status> <_>')}",
            FIELDS["pattern"],
        )

    def _predicate(self, rng: random.Random, fields: dict[str, tuple[str, ...]]) -> str:
        kind = weighted(
            rng, [(3.0 if name == "numeric" else 2.0, name) for name in fields]
        )
        name = rng.choice(fields[kind])
        if kind == "numeric":
            op = rng.choice((">", ">=", "<", "<=", "==", "!="))
            value: object = {
                "status": rng.choice((*STATUSES, 499.5)),
                "latency_ms": round(rng.lognormvariate(3, 1.2), 1),
                "bytes": rng.randint(0, 100_000),
                "ctx_attempt": rng.randint(0, 5),
            }[name]
            return f"{name} {op} {value}"
        if kind == "duration":
            return f"{name} {rng.choice(('>', '<', '>='))} {rng.choice(DURATIONS)}"
        if kind == "bytes":
            return f"{name} {rng.choice(('>', '<', '<='))} {rng.choice(FILTER_SIZES)}"
        vocabulary = {
            "level": LEVELS,
            "method": METHODS,
            "path": PATHS,
            "user": tuple(f"u{index}" for index in range(0, 500, 37)),
            "msg": WORDS,
            "ctx_region": REGIONS,
        }[name]
        op = weighted(rng, [(4, "="), (2, "!="), (2, "=~"), (1, "!~")])
        if op in ("=~", "!~"):
            value = "|".join(
                regex_escape(item) for item in sample(rng, vocabulary, 1, 3)
            )
            if name == "msg":
                value = f".*{value}.*"
        else:
            value = rng.choice(vocabulary)
        return f"{name}{op}{quote(value)}"

    def _label_filter(
        self, rng: random.Random, fields: dict[str, tuple[str, ...]]
    ) -> str:
        first = self._predicate(rng, fields)
        if chance(rng, 0.2):
            joiner = rng.choice(("and", "or"))
            return f"| ({first} {joiner} {self._predicate(rng, fields)})"
        return f"| {first}"

    def _format_stage(
        self, rng: random.Random, fields: dict[str, tuple[str, ...]]
    ) -> str:
        strings = fields.get("string", ()) or fields.get("numeric", ())
        name = rng.choice(strings)
        form = weighted(
            rng, [(2, "line"), (1, "rename"), (1, "template"), (1, "drop"), (1, "keep")]
        )
        if form == "line":
            other = rng.choice(fields.get("numeric", strings))
            return f"| line_format {quote('{{.' + name + '}} {{.' + other + '}}')}"
        if form == "rename":
            return f"| label_format renamed={name}"
        if form == "template":
            return f"| label_format combined={quote('{{.app}}-{{.' + name + '}}')}"
        if form == "drop":
            return f"| drop {', '.join(sample(rng, ('pod', 'tier', name), 1, 2))}"
        return f"| keep app, env, {name}"

    def _log_query(self, rng: random.Random) -> tuple[str, bool]:
        parts = [self._selector(rng)]
        for _ in range(weighted(rng, [(3, 0), (4, 1), (2, 2), (1, 3)])):
            parts.append(self._line_filter(rng))
        parsed = chance(rng, 0.55)
        if parsed:
            stage, fields = self._parser(rng)
            parts.append(stage)
            for _ in range(weighted(rng, [(2, 0), (3, 1), (1, 2)])):
                parts.append(self._label_filter(rng, fields))
            if chance(rng, 0.3):
                parts.append('| __error__=""')
            if chance(rng, 0.25):
                parts.append(self._format_stage(rng, fields))
        elif chance(rng, 0.1):
            parts.append(rng.choice(("| decolorize", "| drop pod", "| keep app, env")))
        return " ".join(parts), parsed

    def _grouping(self, rng: random.Random) -> str:
        if chance(rng, 0.25):
            return ""
        keyword = "without" if chance(rng, 0.15) else "by"
        return f" {keyword} ({', '.join(sample(rng, GROUP_LABELS, 1, 2))})"

    def _range_aggregation(self, rng: random.Random) -> str:
        window = rng.choice(RANGES)
        offset = (
            f" offset {rng.choice(('30s', '1m', '5m'))}" if chance(rng, 0.1) else ""
        )
        if not chance(rng, 0.35):
            function = weighted(
                rng,
                [
                    (4, "count_over_time"),
                    (3, "rate"),
                    (1, "bytes_over_time"),
                    (1, "bytes_rate"),
                    (0.5, "absent_over_time"),
                ],
            )
            query, _ = self._log_query(rng)
            return f"{function}({query} [{window}]{offset})"
        parser = rng.choice(("json", "logfmt"))
        parts = [self._selector(rng)]
        if chance(rng, 0.4):
            parts.append(self._line_filter(rng))
        parts.append(f"| {parser}")
        if chance(rng, 0.4):
            parts.append(self._label_filter(rng, FIELDS[parser]))
        unwrap = rng.choice(FIELDS[parser]["numeric"])
        if parser == "logfmt" and chance(rng, 0.3):
            unwrap = rng.choice(
                ("duration(elapsed)", "duration_seconds(elapsed)", "bytes(size)")
            )
        parts.append(f"| unwrap {unwrap}")
        if chance(rng, 0.5):
            parts.append('| __error__=""')
        pipeline = " ".join(parts)
        function = weighted(
            rng,
            [
                (2, "sum_over_time"),
                (2, "avg_over_time"),
                (1, "min_over_time"),
                (1, "max_over_time"),
                (1, "first_over_time"),
                (1, "last_over_time"),
                (0.5, "stddev_over_time"),
                (0.5, "stdvar_over_time"),
                (1, "quantile_over_time"),
                (1, "rate"),
                (0.3, "rate_counter"),
            ],
        )
        inner = f"{pipeline} [{window}]{offset}"
        if function == "quantile_over_time":
            expression = f"quantile_over_time({rng.choice((0.5, 0.9, 0.99))}, {inner})"
        else:
            expression = f"{function}({inner})"
        grouped = function not in ("sum_over_time", "rate", "rate_counter")
        if grouped and chance(rng, 0.4):
            expression += f" by ({', '.join(sample(rng, GROUP_LABELS, 1, 2))})"
        return expression

    def _metric_query(self, rng: random.Random, depth: int) -> str:
        if depth >= 2:
            return self._range_aggregation(rng)
        form = weighted(
            rng,
            [
                (5, "range"),
                (4, "aggregate"),
                # Only outermost, where ties are checked by value (`_ranked`).
                (1.5 if depth == 0 else 0, "topk"),
                (1.5, "binary"),
                (0.5, "label_replace"),
                (0.5, "sort"),
            ],
        )
        if form == "range":
            return self._range_aggregation(rng)
        inner = self._metric_query(rng, depth + 1)
        if form == "aggregate":
            operator = weighted(
                rng,
                [
                    (4, "sum"),
                    (1, "avg"),
                    (1, "min"),
                    (1, "max"),
                    (1, "count"),
                    (0.5, "stddev"),
                    (0.5, "stdvar"),
                ],
            )
            return f"{operator}{self._grouping(rng)} ({inner})"
        if form == "topk":
            operator = rng.choice(("topk", "bottomk"))
            return f"{operator}({rng.randint(1, 5)}, {inner})"
        if form == "label_replace":
            return f'label_replace({inner}, "copied", "$1", "app", "(.*)")'
        if form == "sort":
            return f"{rng.choice(('sort', 'sort_desc'))}({inner})"
        operator = weighted(
            rng,
            [
                (2, "+"),
                (1, "-"),
                (2, "*"),
                (2, "/"),
                (0.5, "%"),
                (0.3, "^"),
                (1, ">"),
                (0.5, "<"),
                (0.5, ">="),
                (0.5, "=="),
                (0.5, "!="),
            ],
        )
        if operator == "%" and any(name in inner for name in INEXACT):
            # A remainder of a large computed float keeps only its noisiest
            # bits, which no tolerance can compare.
            operator = "*"
        comparison = operator in (">", "<", ">=", "==", "!=")
        modifier = " bool" if comparison and chance(rng, 0.4) else ""
        if chance(rng, 0.5):
            scalar = rng.choice((0, 1, 2, 0.5, 10, 100))
            return f"({inner}) {operator}{modifier} {scalar}"
        return f"({inner}) {operator}{modifier} ({self._metric_query(rng, depth + 1)})"
