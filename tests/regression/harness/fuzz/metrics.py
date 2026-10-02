"""Differential PromQL fuzzing of Metrics against Prometheus."""

from __future__ import annotations

import functools
import math
import os
import platform
import random
import re
import struct
import time
import urllib.request
from collections import defaultdict
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..compose import ComposeProject
from ..metrics.auth import basic, bearer
from ..metrics.fixture import CUSTOM_BUCKETS_SCHEMA, NativeHistogram, Sample, Series
from ..metrics.normalize import (
    assert_json_data_equivalent,
    assert_query_equivalent,
    float_close,
)
from ..metrics.wire import remote_write_body
from ..process import wait_http
from .config import Endpoint, FuzzConfig, endpoint_from_env
from .rand import chance, quote, regex_escape, sample, scaled, weighted, zipf
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

PRODUCT_DIR = Path(__file__).resolve().parents[2] / "products" / "metrics"
# `prometheus` is a local-disk TSDB. `mimir` serves recent data from ingester
# memory like a stock deployment; `mimir-blocks` flushes every round to MinIO
# and reads only through the store-gateway, matching Metrics' object-store
# read path.
ORACLES = ("prometheus", "mimir", "mimir-blocks")
MIMIR_URL = "http://127.0.0.1:19009"
MIMIR_BLOCKS_ENVIRONMENT = {
    "MIMIR_QUERY_INGESTERS_WITHIN": "1s",
    "MIMIR_QUERY_STORE_AFTER": "0s",
    "MIMIR_IGNORE_BLOCKS_WITHIN": "0s",
}
SENTINELS = 16
MAX_POINTS = 10_000
BATCH_SAMPLES = 20_000
STALE_NAN = struct.unpack("<d", struct.pack("<Q", 0x7FF0000000000002))[0]
SPECIAL_VALUES = (
    math.nan,
    math.inf,
    -math.inf,
    1e308,
    -1e308,
    5e-324,
    -0.0,
    0.0,
    STALE_NAN,
)

GAUGES = ("fuzz_gauge", "fuzz_temperature_celsius", "fuzz_queue_depth")
COUNTERS = ("fuzz_requests_total", "fuzz_bytes_total")
CLASSIC = "fuzz_latency_seconds"
NATIVE = "fuzz_native_seconds"
SERVICES = ("api", "web", "db", "cache", "queue", "auth", "billing")
REGIONS = ("us-east-1", "us-west-2", "eu-west-1", "ap-south-1")
CODES = ("200", "404", "500", "503")
BOUNDS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
GROUP_LABELS = ("instance", "region", "service", "code", "fuzz_round", "job")
RANGES = ("30s", "47s", "1m", "2m", "3m17s", "5m", "10m", "15m")
STEPS_S = (1, 5, 7, 10, 13, 15, 30, 60, 90)
# `round(v, to_nearest)` as `_vector` generates it.
FUSED_ROUND = re.compile(r", (0\.5|10|0\.1)\)")

PROFILES = (
    (3.0, "mixed"),
    (2.0, "wide"),
    (2.0, "deep"),
    (1.5, "irregular"),
    (1.5, "counters"),
    (1.0, "special"),
    (1.0, "classic"),
    (1.0, "native"),
)


@dataclass
class Catalog:
    names: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    labels: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    latest_end_ms: int = 0

    def add(self, kind: str, value: Series) -> None:
        labels = dict(value.labels)
        self.names[kind].add(labels["__name__"])
        for name, label in labels.items():
            if name != "__name__":
                self.labels[name].add(label)


def prometheus_comparator(
    oracle: Exchange, impl: Exchange, *, ranked: bool = False, fused: bool = False
) -> None:
    try:
        _compare(oracle, impl, ranked=ranked)
    except (AssertionError, Mismatch) as error:
        if fused and platform.machine() in ("arm64", "aarch64"):
            raise Inconclusive(
                "Go may fuse multiply-adds on arm64, moving quantile weights and "
                f"round's to_nearest by an ulp: {error}"
            ) from error
        raise


def _compare(oracle: Exchange, impl: Exchange, *, ranked: bool) -> None:
    expected = json_body(oracle)
    actual = json_body(impl)
    for side, value in (("oracle", expected), ("impl", actual)):
        if value.get("status") != "success":
            raise Mismatch(f"{side} returned status {value.get('status')!r}")
    left = expected["data"].get("resultType")
    right = actual["data"].get("resultType")
    if left != right:
        raise Mismatch(f"resultType {left!r} != {right!r}")
    if left in ("scalar", "string"):
        expected_value = expected["data"]["result"]
        actual_value = actual["data"]["result"]
        same_time = float_close(float(expected_value[0]), float(actual_value[0]))
        if left == "string":
            same_value = expected_value[1] == actual_value[1]
        else:
            same_value = float_close(float(expected_value[1]), float(actual_value[1]))
        if not (same_time and same_value):
            raise Mismatch(f"{left} {expected_value!r} != {actual_value!r}")
        return
    ignored = _ignored_labels(expected)
    if not ignored and not _strict_names():
        actual = _drop_unmatched_names(expected, actual)
    try:
        assert_query_equivalent("query", expected, actual, ignored_labels=ignored)
    except AssertionError:
        if not ranked:
            raise
        assert_query_equivalent("query", _ranked(expected), _ranked(actual))


def _comparator(query: str):
    ranked = query.startswith(("topk", "bottomk"))
    fused = "quantile" in query or FUSED_ROUND.search(query) is not None
    if not (ranked or fused):
        return prometheus_comparator
    return functools.partial(prometheus_comparator, ranked=ranked, fused=fused)


def _ranked(response: dict[str, Any]) -> dict[str, Any]:
    """topk/bottomk may keep any of the series tied at the cut, and Prometheus
    meets them in its head's series-creation order, so only each step's
    selected values are comparable."""
    steps: dict[str, list[str]] = defaultdict(list)
    for series in response["data"].get("result") or ():
        samples = series["values"] if "values" in series else [series["value"]]
        for timestamp, value in samples:
            steps[str(float(timestamp))].append(value)

    def order(value: str) -> tuple[bool, float]:
        number = float(value)
        return (math.isnan(number), 0.0 if math.isnan(number) else number)

    result = [
        {
            "metric": {"step": step},
            "values": [
                [index, value] for index, value in enumerate(sorted(values, key=order))
            ],
        }
        for step, values in steps.items()
    ]
    return {"status": "success", "data": {"resultType": "matrix", "result": result}}


def _strict_names() -> bool:
    return os.environ.get("FUZZ_METRICS_STRICT_NAME", "") not in ("", "0")


def _drop_unmatched_names(
    expected: dict[str, Any], actual: dict[str, Any]
) -> dict[str, Any]:
    """Where the oracle keeps `__name__` on some series only (say both sides of
    an `or`), drop it from implementation series the oracle has without it."""
    oracle = {
        tuple(sorted(series["metric"].items()))
        for series in expected["data"].get("result") or ()
    }
    result = []
    for series in actual["data"].get("result") or ():
        metric = series["metric"]
        if "__name__" in metric and tuple(sorted(metric.items())) not in oracle:
            unnamed = {
                name: value for name, value in metric.items() if name != "__name__"
            }
            if tuple(sorted(unnamed.items())) in oracle:
                series = {**series, "metric": unnamed}
        result.append(series)
    return {**actual, "data": {**actual["data"], "result": result}}


def _ignored_labels(expected: dict[str, Any]) -> tuple[str, ...]:
    """Metrics keeps `__name__` through functions that Prometheus strips it from,
    a divergence the live suite also ignores. Only skip it when the oracle has
    dropped it everywhere, so selector results still compare names strictly."""
    if _strict_names():
        return ()
    result = expected["data"].get("result") or []
    if any("__name__" in series.get("metric", {}) for series in result):
        return ()
    return ("__name__",)


def discovery_comparator(oracle: Exchange, impl: Exchange) -> None:
    assert_json_data_equivalent("discovery", json_body(oracle), json_body(impl))


class MetricsFuzz(FuzzProduct):
    name = "metrics"
    resource_roles = {
        "prometheus": "oracle",
        "mimir": "oracle",
        "metrics-writer-0": "impl",
        "metrics-writer-1": "impl",
        "metrics-reader": "impl",
    }
    # Prometheus rejects samples older than its head max time minus 1h.
    default_window = "30m"

    def __init__(self, config: FuzzConfig) -> None:
        super().__init__(config)
        # Tokens must outlive the run, not just the regression suite's hour.
        expires_from = int(time.time() + config.duration_s)
        self.oracle = os.environ.get("FUZZ_METRICS_ORACLE") or "prometheus"
        if self.oracle not in ORACLES:
            raise ValueError(f"FUZZ_METRICS_ORACLE must be one of {ORACLES}")
        if self.oracle == "prometheus":
            oracle_read = oracle_write = Endpoint("http://127.0.0.1:19090")
        else:
            oracle_read = Endpoint(f"{MIMIR_URL}/prometheus")
            oracle_write = Endpoint(MIMIR_URL)
        self.oracle_read = endpoint_from_env("metrics", "oracle_read", oracle_read)
        self.oracle_write = endpoint_from_env("metrics", "oracle_write", oracle_write)
        self.impl_write = endpoint_from_env(
            "metrics",
            "impl_write",
            Endpoint(
                "http://127.0.0.1:18080/write/ns/regression",
                bearer("regression-write", now=expires_from),
            ),
        )
        self.impl_read = endpoint_from_env(
            "metrics",
            "impl_read",
            Endpoint(
                "http://127.0.0.1:18082/read/ns/regression",
                basic("regression-reader", "regression-read"),
            ),
        )
        self.window_ms = int(config.window_s * 1000)
        self.catalog = Catalog()

    def stack(self) -> AbstractContextManager[object]:
        def start_reader(project: ComposeProject) -> None:
            # Readers require every writer-created shard manifest to exist.
            project.up("metrics-reader")
            wait_http("http://localhost:18082/-/ready")

        if self.oracle == "prometheus":
            oracle_service, oracle_ready, profiles = (
                "prometheus",
                "http://localhost:19090/-/ready",
                (),
            )
        else:
            oracle_service, oracle_ready, profiles = (
                "mimir",
                f"{MIMIR_URL}/ready",
                ("mimir",),
            )
            if self.oracle == "mimir-blocks":
                # Read by compose when interpolating the mimir service flags.
                os.environ.update(MIMIR_BLOCKS_ENVIRONMENT)
        return fuzz_stack(
            self.config,
            lambda: ComposeProject(
                file=PRODUCT_DIR / "docker-compose.yml",
                name="metrics-fuzz",
                services=(
                    oracle_service,
                    "minio",
                    "minio-init",
                    "metrics-writer-0",
                    "metrics-writer-1",
                ),
                profiles=profiles,
                readiness_urls=(
                    oracle_ready,
                    "http://localhost:18080/-/ready",
                    "http://localhost:18081/-/ready",
                ),
            ),
            after_start=start_reader,
        )

    def after_ingest(self, index: int) -> None:
        if self.oracle != "mimir-blocks":
            return
        # Compacts the ingester head into blocks and ships them; visibility
        # then waits on the compactor's bucket index and store-gateway sync.
        request = urllib.request.Request(
            f"{self.oracle_write.url}/ingester/flush?wait=true", method="POST"
        )
        with urllib.request.urlopen(request, timeout=300):
            pass

    def describe(self) -> dict[str, object]:
        return {"oracle": self.oracle, **super().describe()}

    # Data generation ---------------------------------------------------------

    def generate_round(self, index: int, rng: random.Random) -> Round:
        end = (time.time_ns() // 1_000_000_000 - 2) * 1000
        start = end - self.window_ms
        profile = weighted(rng, PROFILES)
        generated = self._series(index, rng, profile, start, end)
        base = (("fuzz_round", str(index)), ("fuzz_run", self.config.run_id))
        sentinels = [
            Series(
                labels=(("__name__", "fuzz_sentinel"), *base, ("shard", str(number))),
                samples=(Sample(end, 1.0),),
            )
            for number in range(SENTINELS)
        ]
        batches = self._batches(index, generated, "data")
        batches += self._batches(index, sentinels, "sentinel")
        self.catalog.latest_end_ms = end
        selector = (
            f"fuzz_sentinel{{fuzz_run={quote(self.config.run_id)}, "
            f'fuzz_round="{index}"}}'
        )

        def ready(exchange: Exchange) -> bool:
            if not exchange.ok():
                return False
            try:
                result = exchange.json()["data"]["result"]
                return bool(result) and float(result[0]["value"][1]) >= SENTINELS
            except (ValueError, KeyError, IndexError, TypeError):
                return False

        probe = Probe(
            "sentinel",
            Request(
                "GET",
                "/api/v1/query",
                (("query", f"count({selector})"), ("time", f"{end / 1000:.3f}")),
            ),
            ready,
        )
        probes = [probe]
        if self.oracle == "mimir-blocks":
            probes.extend(self._round_probes(index, generated, start, end))
        return Round(
            index=index,
            profile=profile,
            batches=batches,
            probes=probes,
            dataset={
                "window_ms": [start, end],
                "series": [
                    {
                        "labels": dict(item.labels),
                        "samples": [[s.timestamp_ms, s.value] for s in item.samples],
                        "histograms": [h.__dict__ for h in item.histograms],
                    }
                    for item in generated
                ],
            },
            stats={
                "series": len(generated),
                "samples": sum(len(item.samples) for item in generated),
                "histograms": sum(len(item.histograms) for item in generated),
            },
        )

    def _round_probes(
        self, index: int, generated: list[Series], start: int, end: int
    ) -> list[Probe]:
        """A flush can ship several blocks and the store-gateway may sync
        between them, so the sentinel alone does not prove the whole round is
        queryable. One probe per metric name, because range functions drop
        `__name__` and series differing only by name would collide. Series
        whose every sample is a stale marker are invisible to
        `count_over_time`."""
        stale = struct.pack("<d", STALE_NAN)
        expected: dict[str, int] = defaultdict(int)
        for item in generated:
            if item.histograms or any(
                struct.pack("<d", s.value) != stale for s in item.samples
            ):
                expected[dict(item.labels)["__name__"]] += 1
        range_s = math.ceil((end - start) / 1000) + 120
        matchers = f'fuzz_run={quote(self.config.run_id)}, fuzz_round="{index}"'

        def probe(name: str, count: int) -> Probe:
            query = f"count(count_over_time({name}{{{matchers}}}[{range_s}s]))"

            def ready(exchange: Exchange) -> bool:
                if not exchange.ok():
                    return False
                try:
                    result = exchange.json()["data"]["result"]
                    return bool(result) and float(result[0]["value"][1]) >= count
                except (ValueError, KeyError, IndexError, TypeError):
                    return False

            return Probe(
                f"round:{name}",
                Request(
                    "GET",
                    "/api/v1/query",
                    (("query", query), ("time", f"{end / 1000:.3f}")),
                ),
                ready,
            )

        return [probe(name, count) for name, count in sorted(expected.items())]

    def _batches(self, index: int, values: list[Series], prefix: str) -> list[Batch]:
        batches: list[Batch] = []
        current: list[Series] = []
        size = 0

        def flush() -> None:
            body = remote_write_body(tuple(current))
            headers = (
                ("Content-Type", "application/x-protobuf"),
                ("Content-Encoding", "snappy"),
                ("X-Prometheus-Remote-Write-Version", "0.1.0"),
                # Lets retried writes stay idempotent on the implementation.
                (
                    "X-Request-Id",
                    f"fuzz-{self.config.run_id}-{index}-{prefix}-{len(batches)}",
                ),
            )
            request = Request("POST", "/api/v1/write", body=body, headers=headers)
            oracle = request
            if self.oracle != "prometheus":
                oracle = Request("POST", "/api/v1/push", body=body, headers=headers)
            batches.append(
                Batch(f"{prefix}-{len(batches)}", oracle, request, len(current))
            )

        for item in values:
            weight = len(item.samples) + 10 * len(item.histograms) + 1
            if current and size + weight > BATCH_SAMPLES:
                flush()
                current, size = [], 0
            current.append(item)
            size += weight
        if current:
            flush()
        return batches

    def _series(
        self, index: int, rng: random.Random, profile: str, start: int, end: int
    ) -> list[Series]:
        scale = self.config.scale
        result: list[Series] = []
        seen: set[tuple[tuple[str, str], ...]] = set()

        def labels(name: str, extra: tuple[tuple[str, str], ...] = ()) -> tuple:
            values = {
                "__name__": name,
                "fuzz_run": self.config.run_id,
                "fuzz_round": str(index),
                "job": "fuzz",
                "instance": f"i-{rng.randrange(1000 if profile == 'wide' else 12)}",
                "region": rng.choice(REGIONS),
                "service": zipf(rng, SERVICES),
                **dict(extra),
            }
            if chance(rng, 0.5):
                values["code"] = zipf(rng, CODES)
            key = tuple(sorted(values.items()))
            # Prometheus rejects re-appending to a series; keep each unique.
            replica = 0
            while key in seen:
                replica += 1
                key = tuple(sorted({**values, "replica": str(replica)}.items()))
            seen.add(key)
            return key

        def add(kind: str, value: Series) -> None:
            self.catalog.add(kind, value)
            result.append(value)

        if profile == "classic":
            for _ in range(scaled(rng, 2, 8, scale)):
                for item in self._classic(rng, labels, start, end):
                    add("classic", item)
            return result
        if profile == "native":
            for _ in range(scaled(rng, 2, 6, scale)):
                add("native", self._native(rng, labels(NATIVE), start, end))
            return result
        count = {
            "mixed": (10, 60),
            "wide": (100, 800),
            "deep": (2, 10),
            "irregular": (10, 50),
            "counters": (10, 60),
            "special": (5, 20),
        }[profile]
        for _ in range(scaled(rng, *count, scale)):
            counter = profile == "counters" or (profile != "deep" and chance(rng, 0.4))
            name = rng.choice(COUNTERS if counter else GAUGES)
            timestamps = self._timestamps(rng, profile, start, end)
            if counter:
                values = self._counter(rng, len(timestamps))
            else:
                values = self._gauge(rng, len(timestamps))
            if profile == "special":
                values = [
                    rng.choice(SPECIAL_VALUES) if chance(rng, 0.15) else value
                    for value in values
                ]
            add(
                "counter" if counter else "gauge",
                Series(
                    labels=labels(name),
                    samples=tuple(
                        Sample(t, v) for t, v in zip(timestamps, values, strict=True)
                    ),
                ),
            )
        return result

    def _timestamps(
        self, rng: random.Random, profile: str, start: int, end: int
    ) -> list[int]:
        interval = {
            "deep": rng.choice((1, 2, 5)),
            "wide": rng.choice((15, 30, 60)),
        }.get(profile, rng.choice((5, 10, 15, 30, 60))) * 1000
        if profile == "wide":
            low = rng.randint(start, end - interval)
            high = min(end, low + interval * rng.randint(1, 10))
        elif profile == "deep":
            low, high = start, end
        else:
            low = rng.randint(start, end - 60_000)
            high = rng.randint(low + interval, end)
        irregular = profile in ("irregular", "special")
        timestamps = []
        current = low + rng.randrange(interval)
        while current <= high:
            timestamps.append(current)
            step = interval
            if irregular:
                step = max(1, round(interval * rng.uniform(0.1, 1.9)))
                if chance(rng, 0.05):
                    # Gaps beyond the 5m lookback delta exercise staleness.
                    step += rng.randint(2, 10) * 60_000
            current += step
        return timestamps or [high]

    def _gauge(self, rng: random.Random, count: int) -> list[float]:
        pattern = rng.choice(("walk", "sine", "constant", "step", "spiky"))
        base = rng.uniform(-100, 1000)
        values = []
        current = base
        for index in range(count):
            if pattern == "walk":
                current += rng.gauss(0, 5)
            elif pattern == "sine":
                current = base + 50 * math.sin(index / 7)
            elif pattern == "step":
                current = base if (index // 20) % 2 else base * 2
            elif pattern == "spiky":
                current = base * (100 if chance(rng, 0.05) else 1)
            values.append(round(current, rng.choice((0, 2, 6))))
        return values

    def _counter(self, rng: random.Random, count: int) -> list[float]:
        current = rng.uniform(0, 1000)
        values = []
        for _ in range(count):
            if chance(rng, 0.02):
                current = rng.uniform(0, 10)
            else:
                current += rng.expovariate(0.1)
            values.append(round(current, 3))
        return values

    def _classic(self, rng, labels, start: int, end: int) -> list[Series]:
        bounds = sorted(sample(rng, BOUNDS, 3, len(BOUNDS)))
        names = (*[str(bound) for bound in bounds], "+Inf")
        timestamps = self._timestamps(rng, "mixed", start, end)
        # Rarely emit non-monotonic buckets to probe histogram_quantile fix-ups.
        broken = chance(rng, 0.1)
        base = labels(f"{CLASSIC}_bucket", (("le", "+Inf"),))
        common = tuple(item for item in base if item[0] not in ("__name__", "le"))
        cumulative = [0.0] * len(names)
        total = 0.0
        buckets: list[list[Sample]] = [[] for _ in names]
        sums: list[Sample] = []
        counts: list[Sample] = []
        for timestamp in timestamps:
            if chance(rng, 0.02):
                cumulative = [0.0] * len(names)
                total = 0.0
            observations = [rng.randint(0, 20) for _ in names]
            running = 0.0
            for position, observed in enumerate(observations):
                running += observed
                cumulative[position] += running if not broken else observed
            total += sum(observations) * rng.uniform(0.001, 2.0)
            for position, value in enumerate(cumulative):
                buckets[position].append(Sample(timestamp, value))
            sums.append(Sample(timestamp, round(total, 6)))
            counts.append(Sample(timestamp, cumulative[-1]))

        def series(name: str, extra: tuple, samples: list[Sample]) -> Series:
            return Series(
                labels=tuple(sorted((("__name__", name), *common, *extra))),
                samples=tuple(samples),
            )

        return [
            *(
                series(f"{CLASSIC}_bucket", (("le", name),), values)
                for name, values in zip(names, buckets, strict=True)
            ),
            series(f"{CLASSIC}_sum", (), sums),
            series(f"{CLASSIC}_count", (), counts),
        ]

    def _native(
        self, rng: random.Random, labels: tuple, start: int, end: int
    ) -> Series:
        custom = chance(rng, 0.2)
        schema = CUSTOM_BUCKETS_SCHEMA if custom else rng.choice((0, 1, 2, 3))
        # Only the first span may start at a negative offset; later offsets
        # are gaps after the previous span and must not overlap it.
        layout = [
            (rng.randint(-3, 3) if index == 0 else rng.randint(0, 3), rng.randint(1, 4))
            for index in range(rng.randint(1, 3))
        ]
        if custom:
            layout = [(0, rng.randint(2, 5))]
        negative_layout = [] if custom or chance(rng, 0.6) else [(0, rng.randint(1, 3))]
        bounds = tuple(sorted(sample(rng, BOUNDS, layout[0][1] - 1, layout[0][1] - 1)))
        positive = [[0] * length for _, length in layout]
        negative = [[0] * length for _, length in negative_layout]
        zero = 0
        total = 0.0
        histograms = []
        for timestamp in self._timestamps(rng, "mixed", start, end):
            if chance(rng, 0.03):
                positive = [[0] * length for _, length in layout]
                negative = [[0] * length for _, length in negative_layout]
                zero, total = 0, 0.0
            for span in (*positive, *negative):
                for position in range(len(span)):
                    span[position] += rng.randint(0, 6)
            if not custom:
                zero += rng.randint(0, 3)
            total += rng.uniform(0, 30)
            histograms.append(
                NativeHistogram(
                    timestamp_ms=timestamp,
                    schema=schema,
                    zero_threshold=0.0 if custom else 0.001,
                    zero_count=zero,
                    sum=round(total, 6),
                    positive=tuple(
                        (offset, tuple(counts))
                        for (offset, _), counts in zip(layout, positive, strict=True)
                    ),
                    negative=tuple(
                        (offset, tuple(counts))
                        for (offset, _), counts in zip(
                            negative_layout, negative, strict=True
                        )
                    ),
                    custom_values=bounds if custom else (),
                )
            )
        return Series(labels=labels, samples=(), histograms=tuple(histograms))

    # Query generation --------------------------------------------------------

    def next_case(self, rng: random.Random) -> Case:
        kind = weighted(
            rng, [(5, "instant"), (5, "range"), (0.5, "series"), (0.5, "labels")]
        )
        end_s = self.catalog.latest_end_ms / 1000
        window_s = self.window_ms / 1000
        if kind == "instant":
            query = self._vector(rng, 0)
            at = rng.uniform(end_s - window_s * 1.2, end_s + 360)
            return Case(
                "promql.instant",
                query,
                Request(
                    "GET", "/api/v1/query", (("query", query), ("time", f"{at:.3f}"))
                ),
                _comparator(query),
            )
        if kind == "range":
            query = self._vector(rng, 0)
            start, end = self._time_range(rng, end_s, window_s)
            step = max(rng.choice(STEPS_S), math.ceil((end - start) / MAX_POINTS))
            return Case(
                "promql.range",
                query,
                Request(
                    "GET",
                    "/api/v1/query_range",
                    (
                        ("query", query),
                        ("start", f"{start:.3f}"),
                        ("end", f"{end:.3f}"),
                        ("step", str(step)),
                    ),
                ),
                _comparator(query),
            )
        # Prometheus answers series/label lookups at head-block granularity, so
        # narrow windows are not comparable; always span every round.
        window = (
            ("start", f"{end_s - window_s * 4:.3f}"),
            ("end", f"{end_s + 3600:.3f}"),
        )
        selectors = [
            self._selector(rng, rng.choice(("gauge", "counter", "any")))
            for _ in range(rng.randint(1, 2))
        ]
        matches = tuple(("match[]", selector) for selector in selectors)
        if kind == "series":
            return Case(
                "promql.series",
                " ".join(selectors),
                Request("GET", "/api/v1/series", (*matches, *window)),
                discovery_comparator,
            )
        name = rng.choice(("labels", *GROUP_LABELS, "le", "__name__"))
        path = "/api/v1/labels" if name == "labels" else f"/api/v1/label/{name}/values"
        return Case(
            "promql.labels" if name == "labels" else "promql.label_values",
            f"{name} {' '.join(selectors)}",
            Request("GET", path, (*matches, *window)),
            discovery_comparator,
        )

    def _time_range(
        self, rng: random.Random, end: float, window: float
    ) -> tuple[float, float]:
        shape = weighted(
            rng, [(4, "window"), (3, "slice"), (1, "narrow"), (1, "extended")]
        )
        if shape == "window":
            return end - window, end
        if shape == "slice":
            low = rng.uniform(end - window, end - 1)
            return low, rng.uniform(low + 1, end)
        if shape == "narrow":
            low = rng.uniform(end - window, end)
            return low, low + rng.uniform(1, 120)
        return end - window * 1.4, end + 600

    def _names(self, kind: str) -> list[str]:
        if kind == "any":
            return sorted(self.catalog.names["gauge"] | self.catalog.names["counter"])
        return sorted(self.catalog.names[kind])

    def _selector(
        self, rng: random.Random, kind: str, *, distinct: bool = False
    ) -> str:
        """`distinct` keeps one matcher per label: Prometheus derives absent()
        labels from matcher order, which it does not preserve across restarts,
        so a label matched twice comes and goes."""
        if kind == "bucket":
            name = f"{CLASSIC}_bucket"
        elif kind == "native":
            name = NATIVE
        else:
            names = self._names(kind) or list(GAUGES if kind == "gauge" else COUNTERS)
            name = rng.choice(names)
        matchers = [f"fuzz_run={quote(self.config.run_id)}"]
        for _ in range(weighted(rng, [(4, 0), (4, 1), (2, 2), (0.5, 3)])):
            matcher = self._matcher(rng)
            label = matcher.split("=", 1)[0].rstrip("!")
            if distinct and any(
                m.split("=", 1)[0].rstrip("!") == label for m in matchers
            ):
                continue
            matchers.append(matcher)
        if chance(rng, 0.05) and kind in ("gauge", "any"):
            matchers.append('__name__=~"fuzz_(gauge|queue_depth)"')
            return "{" + ", ".join(matchers) + "}"
        return f"{name}{{{', '.join(matchers)}}}"

    def _matcher(self, rng: random.Random) -> str:
        label = weighted(
            rng,
            [
                (3, "instance"),
                (2, "region"),
                (3, "service"),
                (2, "code"),
                (1, "fuzz_round"),
                (0.3, "replica"),
                (0.2, "absent"),
            ],
        )
        values = sorted(self.catalog.labels.get(label, ())) or ["none"]
        op = weighted(rng, [(5, "="), (2, "!="), (3, "=~"), (1, "!~")])
        if op in ("=", "!="):
            value = rng.choice(values) if chance(rng, 0.9) else rng.choice(("", "x"))
        else:
            value = weighted(
                rng,
                [
                    (3, "|".join(regex_escape(v) for v in sample(rng, values, 1, 3))),
                    (1, regex_escape(rng.choice(values))[:3] + ".*"),
                    (0.5, ".+"),
                ],
            )
        return f"{label}{op}{quote(value)}"

    def _range(self, rng: random.Random, kind: str, *, distinct: bool = False) -> str:
        offset = ""
        if chance(rng, 0.1):
            offset = f" offset {rng.choice(('1m', '5m', '-30s'))}"
        selector = self._selector(rng, kind, distinct=distinct)
        return f"{selector}[{rng.choice(RANGES)}]{offset}"

    def _leaf(self, rng: random.Random, *, distinct: bool = False) -> str:
        selector = self._selector(
            rng, rng.choice(("gauge", "counter", "any")), distinct=distinct
        )
        modifier = weighted(rng, [(8, ""), (1, "offset"), (0.5, "at")])
        if modifier == "offset":
            return f"{selector} offset {rng.choice(('30s', '2m', '5m', '-1m'))}"
        if modifier == "at":
            end_s = self.catalog.latest_end_ms / 1000
            at = rng.choice(
                (
                    f"{rng.uniform(end_s - self.window_ms / 1000, end_s):.3f}",
                    "start()",
                    "end()",
                )
            )
            return f"{selector} @ {at}"
        return selector

    def _scalar(self, rng: random.Random, depth: int) -> str:
        form = weighted(
            rng, [(6, "literal"), (1, "scalar"), (0.5, "time"), (0.2, "pi")]
        )
        if form == "scalar" and depth < 3:
            return f"scalar({self._vector(rng, depth + 1)})"
        if form == "time":
            return "time()"
        if form == "pi":
            return "pi()"
        return rng.choice(
            ("0", "1", "2", "0.5", "-1", "10", "100", "1e-9", "1e9", "NaN", "Inf")
        )

    def _grouping(self, rng: random.Random) -> str:
        if chance(rng, 0.3):
            return ""
        keyword = "without" if chance(rng, 0.2) else "by"
        return f" {keyword} ({', '.join(sample(rng, GROUP_LABELS, 1, 2))})"

    def _rollup(self, rng: random.Random) -> str:
        function = weighted(
            rng,
            [
                (4, "rate"),
                (2, "increase"),
                (1, "irate"),
                (1, "resets"),
                (1, "delta"),
                (0.5, "idelta"),
                (1, "deriv"),
                (0.5, "predict_linear"),
                (1, "changes"),
                (1, "avg_over_time"),
                (1, "min_over_time"),
                (1, "max_over_time"),
                (1, "sum_over_time"),
                (1, "count_over_time"),
                (1, "last_over_time"),
                (0.5, "stddev_over_time"),
                (0.5, "stdvar_over_time"),
                (0.5, "present_over_time"),
                (1, "quantile_over_time"),
                (0.3, "absent_over_time"),
            ],
        )
        kind = {
            "rate": "counter",
            "increase": "counter",
            "irate": "counter",
            "resets": "counter",
            "delta": "gauge",
            "idelta": "gauge",
            "deriv": "gauge",
            "predict_linear": "gauge",
        }.get(function, "any")
        argument = self._range(rng, kind, distinct=function == "absent_over_time")
        if function == "quantile_over_time":
            return (
                f"quantile_over_time({rng.choice((0, 0.5, 0.9, 0.99, 1))}, {argument})"
            )
        if function == "predict_linear":
            return f"predict_linear({argument}, {rng.choice((60, 300, -60))})"
        return f"{function}({argument})"

    def _histogram(self, rng: random.Random) -> str:
        window = rng.choice(RANGES)
        q = rng.choice((0, 0.5, 0.9, 0.99, 1, 1.5))
        if self.catalog.names["native"] and chance(rng, 0.5):
            native = self._selector(rng, "native")
            return weighted(
                rng,
                [
                    (3, f"histogram_quantile({q}, rate({native}[{window}]))"),
                    (1, f"histogram_count(rate({native}[{window}]))"),
                    (1, f"histogram_sum({native})"),
                    (1, f"histogram_avg(rate({native}[{window}]))"),
                    (0.5, f"histogram_stddev({native})"),
                    (0.5, f"histogram_stdvar({native})"),
                    (1, f"histogram_fraction(0, 0.5, {native})"),
                    (1, f"sum{self._grouping(rng)} (rate({native}[{window}]))"),
                ],
            )
        bucket = self._selector(rng, "bucket")
        extra = rng.choice(("", ", service", ", region"))
        aggregated = f"sum by (le{extra}) (rate({bucket}[{window}]))"
        return weighted(
            rng,
            [
                (3, f"histogram_quantile({q}, {aggregated})"),
                (1, f"histogram_quantile({q}, rate({bucket}[{window}]))"),
                (1, f"histogram_quantile({q}, {bucket})"),
            ],
        )

    def _binary(self, rng: random.Random, depth: int) -> str:
        operator = weighted(
            rng,
            [
                (3, "+"),
                (2, "-"),
                (2, "*"),
                (2, "/"),
                (0.5, "%"),
                (0.3, "^"),
                (0.3, "atan2"),
                (1, ">"),
                (0.5, "<"),
                (0.5, ">="),
                (0.5, "<="),
                (0.5, "=="),
                (0.5, "!="),
                (0.7, "and"),
                (0.7, "or"),
                (0.7, "unless"),
            ],
        )
        set_operator = operator in ("and", "or", "unless")
        comparison = operator in (">", "<", ">=", "<=", "==", "!=")
        modifier = " bool" if comparison and chance(rng, 0.4) else ""
        left = self._vector(rng, depth + 1)
        if not set_operator and chance(rng, 0.45):
            scalar = self._scalar(rng, depth)
            if chance(rng, 0.5):
                return f"({left}) {operator}{modifier} {scalar}"
            return f"{scalar} {operator}{modifier} ({left})"
        groups = sample(rng, ("region", "service", "code"), 1, 2)
        if chance(rng, 0.6):
            # Aggregate both sides identically so most joins actually match.
            by = ", ".join(groups)
            left = f"sum by ({by}) ({left})"
            right = f"sum by ({by}) ({self._vector(rng, depth + 1)})"
            matching = ""
        else:
            right = self._vector(rng, depth + 1)
            matching = weighted(
                rng,
                [
                    (3, ""),
                    (2, f" on ({', '.join(groups)})"),
                    (1, " ignoring (instance, fuzz_round)"),
                ],
            )
            if matching and not set_operator and chance(rng, 0.3):
                matching += " group_left"
        return f"({left}) {operator}{modifier}{matching} ({right})"

    def _vector(self, rng: random.Random, depth: int) -> str:
        if depth >= 3:
            return weighted(rng, [(1, self._leaf(rng)), (1, self._rollup(rng))])
        form = weighted(
            rng,
            [
                (2, "leaf"),
                (4, "rollup"),
                (3, "aggregate"),
                # Only outermost, where ties are checked by value (`_ranked`).
                (1.5 if depth == 0 else 0, "topk"),
                (0.7, "quantile"),
                (2, "binary"),
                (1, "math"),
                (1, "histogram"),
                (0.5, "label"),
                (0.3, "absent"),
                (0.3, "sort"),
                (0.6, "subquery"),
                (0.2, "count_values"),
                (0.2, "date"),
            ],
        )
        if form == "leaf":
            return self._leaf(rng)
        if form == "rollup":
            return self._rollup(rng)
        if form == "histogram":
            return self._histogram(rng)
        if form == "binary":
            return self._binary(rng, depth)
        if form == "absent":
            return rng.choice(
                (
                    f"absent({self._leaf(rng, distinct=True)})",
                    f"absent_over_time({self._range(rng, 'any', distinct=True)})",
                )
            )
        if form == "date":
            function = rng.choice(("hour", "minute", "day_of_week", "days_in_month"))
            return (
                f"{function}()"
                if chance(rng, 0.5)
                else f"{function}(timestamp({self._leaf(rng)}))"
            )
        inner = self._vector(rng, depth + 1)
        if form == "aggregate":
            operator = weighted(
                rng,
                [
                    (4, "sum"),
                    (2, "avg"),
                    (1, "min"),
                    (1, "max"),
                    (2, "count"),
                    (0.5, "stddev"),
                    (0.5, "stdvar"),
                    (0.5, "group"),
                ],
            )
            return f"{operator}{self._grouping(rng)} ({inner})"
        if form == "topk":
            operator = rng.choice(("topk", "bottomk"))
            return f"{operator}{self._grouping(rng)} ({rng.randint(1, 5)}, {inner})"
        if form == "quantile":
            q = rng.choice((0, 0.5, 0.9, 1, -0.5, 1.5))
            return f"quantile{self._grouping(rng)} ({q}, {inner})"
        if form == "math":
            function = weighted(
                rng,
                [
                    (2, "abs"),
                    (1, "ceil"),
                    (1, "floor"),
                    (1, "round"),
                    (0.5, "exp"),
                    (1, "ln"),
                    (0.5, "log2"),
                    (1, "log10"),
                    (1, "sqrt"),
                    (0.5, "sgn"),
                    (0.3, "sin"),
                    (0.3, "acos"),
                    (0.3, "deg"),
                    (1, "clamp"),
                    (1, "clamp_min"),
                    (1, "clamp_max"),
                    (0.5, "timestamp"),
                ],
            )
            if function == "clamp":
                return f"clamp({inner}, {rng.choice((-10, 0))}, {rng.choice((5, 100))})"
            if function in ("clamp_min", "clamp_max"):
                return f"{function}({inner}, {rng.choice((0, 50, -5))})"
            if function == "round" and chance(rng, 0.5):
                return f"round({inner}, {rng.choice((0.5, 10, 0.1))})"
            return f"{function}({inner})"
        if form == "label":
            if chance(rng, 0.5):
                return f'label_replace({inner}, "copy", "$1", "instance", "i-(.*)")'
            return f'label_join({inner}, "joined", "-", "region", "service")'
        if form == "sort":
            return f"{rng.choice(('sort', 'sort_desc'))}({inner})"
        if form == "count_values":
            return f'count_values("value", round({inner}))'
        step = rng.choice(("15s", "30s", "1m", ""))
        function = rng.choice(
            ("max_over_time", "avg_over_time", "min_over_time", "rate")
        )
        return f"{function}(({inner})[{rng.choice(('5m', '10m', '15m'))}:{step}])"
