"""Differential LogQL fuzzing of Logs against Loki."""

from __future__ import annotations

import json
import math
import os
import random
import re
import time
import urllib.request
from collections import defaultdict
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..canonical import assert_equivalent
from ..compose import ComposeProject
from . import cold
from .cold import ColdProduct, ColdService, ColdStack, scrape
from .config import Endpoint, FuzzConfig, endpoint_from_env
from .network import minio_floor_url, service_url
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
# `production` uses what the operator deploys (1h segments, 1 MiB pages, its
# cache sizes and buffered writes) instead of the small values that exercise
# edge cases.
IMPL_CONFIG_DIRS = {"regression": ".", "production": "production"}
SECOND = 1_000_000_000
MAX_LIMIT = 5000
MAX_POINTS = 10_000
SENTINELS = 4
PUSH_BYTES = 1_000_000
METADATA_LOOKBACK = 24 * 3600 * SECOND
FLUSH_TIMEOUT_S = 120
LOKI_CONFIGS = {
    "recent": "loki-fuzz.yaml",
    "historical": "loki-fuzz-historical.yaml",
    "cold": "loki-fuzz-cold.yaml",
}
HOUR = 3600 * SECOND
DAY = 24 * HOUR

# The cold scenario's day of logs: a fixed set of Kubernetes-style streams
# (namespace x app x pod x container) writing logfmt request logs at skewed
# rates, pushed hour by hour.
COLD_JOB = "fuzz-cold"
COLD_NAMESPACES = ("payments", "checkout", "search", "identity", "platform", "data")
COLD_APPS = ("api", "worker", "gateway")
COLD_CONTAINERS = ("app", "proxy")
COLD_PODS = 4
COLD_ENTRIES = 1_000_000
# A token in about one line in 2000: a needle search reads every chunk but
# returns few lines.
COLD_NEEDLE = "deadline_exceeded"
COLD_NEEDLE_RATE = 0.0005
COLD_LIMIT = 1000
COLD_STEPS = {1: 60, 6: 300, 24: 900}
# Logs counts no rollup passes, so the cold rollup settle reads its log: the
# rollup module logs every segment it starts and seals at info, and the
# server a failed pass at warn.
ROLLUP_LOG = "error,plural_logs_server=warn,plural_logs::db::rollup=info"
ROLLUP_STARTED = "starting logs rollup segment"
ROLLUP_SEALED = "sealed logs rollup segment"
ROLLUP_FAILED = "Logs segment rollup failed"
# Loki's compactor first runs one `compaction_interval` (10 minutes) after it
# starts; settling restarts Loki with this interval instead and waits for two
# successful runs, the second finding the first's work done.
LOKI_COMPACTION_INTERVAL = "10s"
LOKI_COMPACTIONS = "loki_boltdb_shipper_compact_tables_operation_total"
LOKI_COMPACTION_TIMEOUT_S = 600.0

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


def _gauge(exposition: str, name: str) -> float:
    """Sum of every sample of `name` in a Prometheus text exposition."""
    total = 0.0
    for line in exposition.splitlines():
        if line.startswith(name) and line[len(name) : len(name) + 1] in ("{", " "):
            total += float(line.rsplit(" ", 1)[1])
    return total


def rollup_progress(log: str) -> tuple[int, int, int]:
    """Rollup segments a Logs server's log shows started and sealed, and
    rollup passes that failed."""
    lines = log.splitlines()
    return (
        sum(ROLLUP_STARTED in line for line in lines),
        sum(ROLLUP_SEALED in line for line in lines),
        sum(ROLLUP_FAILED in line for line in lines),
    )


class RollupLogWatch:
    """Whether a Logs server started with `ROLLUP_LOG` has rolled up all it
    will: every rollup segment it started is sealed, and nothing new started
    for `quiet_s`, longer than two rollup intervals, so a whole pass found
    nothing left. The quiet time counts from the first call, made once the
    server is ready."""

    def __init__(
        self,
        read_log: Callable[[], str],
        quiet_s: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.read_log = read_log
        self.quiet_s = quiet_s
        self.clock = clock
        self.progress: tuple[int, int, int] | None = None
        self.changed = 0.0

    def __call__(self) -> bool:
        progress = rollup_progress(self.read_log())
        started, sealed, failed = progress
        if failed:
            raise RuntimeError(f"{failed} Logs rollup passes failed while settling")
        now = self.clock()
        if progress != self.progress:
            self.progress, self.changed = progress, now
        return started == sealed and now - self.changed >= self.quiet_s


def loki_compactions(scrape: str) -> float:
    """Successful Loki compactor runs in a Prometheus scrape."""
    return sum(
        float(line.rsplit(" ", 1)[1])
        for line in scrape.splitlines()
        if line.startswith(LOKI_COMPACTIONS + "{") and 'status="success"' in line
    )


def loki_compactor_config(config: Path) -> str:
    """Loki's `config` with its compactor running every
    `LOKI_COMPACTION_INTERVAL`."""
    text = config.read_text()
    if re.search(r"^compactor:", text, re.MULTILINE):
        raise ValueError(f"{config} already configures the compactor")
    return (
        f"{text.rstrip()}\ncompactor:\n"
        f"  compaction_interval: {LOKI_COMPACTION_INTERVAL}\n"
    )


class LogsFuzz(FuzzProduct, ColdProduct):
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
        if config.object_store and self.storage != "s3":
            raise ValueError(
                f"FUZZ_SCENARIO={config.scenario} requires FUZZ_LOGS_STORAGE=s3"
            )
        namespace = os.environ.get("FUZZ_LOGS_NAMESPACE", "regression")
        self.impl_service = "logs-s3" if self.storage == "s3" else "logs"
        self.impl_url = service_url(
            self.impl_service, 3100, 13111 if self.storage == "s3" else 13101
        )
        self.loki_url = service_url("loki-fuzz", 3100, 13110)
        impl = self.impl_url
        oracle = Endpoint(self.loki_url)
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
        self.cold_end = 0
        self.cold_counts: dict[str, int] = defaultdict(int)

    def stack(self) -> AbstractContextManager[object]:
        services = ["loki-fuzz", self.impl_service]
        if self.storage == "s3":
            services[1:1] = ["minio", "minio-init"]
        os.environ["LOGS_CONFIG_DIR"] = IMPL_CONFIG_DIRS[self.impl_config]
        os.environ["LOKI_FUZZ_CONFIG"] = LOKI_CONFIGS[self.config.scenario]
        return fuzz_stack(
            self.config,
            lambda: ComposeProject(
                file=PRODUCT_DIR / "docker-compose.yml",
                name="logs-fuzz",
                services=tuple(services),
                profiles=("fuzz",),
                readiness_urls=tuple(self.floor_urls().values()),
            ),
            roles=self.resource_roles,
        )

    def after_ingest(self, index: int) -> None:
        if not self.config.historical:
            return
        # Loki skips the ingesters here, so a round is only visible once every
        # chunk it touched is in MinIO.
        self._flush_loki(f"round {index}")

    def _flush_loki(self, what: str) -> None:
        """/flush is asynchronous: waits until Loki holds no chunk in memory."""
        request = urllib.request.Request(f"{self.loki_url}/flush", method="POST")
        with urllib.request.urlopen(request, timeout=30):
            pass
        deadline = time.monotonic() + FLUSH_TIMEOUT_S
        while time.monotonic() < deadline:
            with urllib.request.urlopen(f"{self.loki_url}/metrics", timeout=30) as r:
                text = r.read().decode()
            if _gauge(text, "loki_ingester_memory_chunks") == 0 and (
                _gauge(text, "loki_ingester_flush_queue_length") == 0
            ):
                return
            time.sleep(0.2)
        raise RuntimeError(f"Loki did not flush {what} within {FLUSH_TIMEOUT_S}s")

    def floor_urls(self) -> dict[str, str]:
        urls = {
            "loki-fuzz": f"{self.loki_url}/ready",
            self.impl_service: f"{self.impl_url}/-/ready",
        }
        if self.storage == "s3":
            urls.update(minio_floor_url())
        return urls

    def describe(self) -> dict[str, object]:
        return {
            **super().describe(),
            "storage": self.storage,
            "impl_config": self.impl_config,
        }

    # Cold reads --------------------------------------------------------------

    def cold_services(self) -> dict[str, ColdService]:
        return {
            "impl": ColdService(
                self.impl_service,
                f"{self.impl_url}/-/ready",
                metrics_url=f"{self.impl_url}/metrics",
            ),
            "oracle": ColdService(
                "loki-fuzz", f"{self.loki_url}/ready", "loki-fuzz-data"
            ),
        }

    def _cold_selector(self, **matchers: str) -> str:
        labels = {**matchers, "job": COLD_JOB, "fuzz_run": self.config.run_id}
        return "{" + ", ".join(f"{k}={quote(v)}" for k, v in labels.items()) + "}"

    def _cold_streams(self, rng: random.Random) -> list[tuple[dict[str, str], float]]:
        pods = max(1, round(COLD_PODS * self.config.scale))
        streams = []
        for rank, namespace in enumerate(COLD_NAMESPACES):
            for app in COLD_APPS:
                for pod in range(pods):
                    for container in COLD_CONTAINERS:
                        labels = {
                            "fuzz_run": self.config.run_id,
                            "job": COLD_JOB,
                            "namespace": namespace,
                            "app": app,
                            "pod": f"{namespace}-{app}-{pod}",
                            "container": container,
                        }
                        weight = (3.0 if container == "app" else 1.0) / (1 + rank)
                        streams.append((labels, weight * rng.uniform(0.5, 1.5)))
        return streams

    def cold_load(self, rng: random.Random, end_ns: int) -> Iterator[Batch]:
        assert self.config.cold is not None
        span = int(self.config.cold.span_s * SECOND)
        start = end_ns - span
        streams = self._cold_streams(rng)
        weights = sum(weight for _, weight in streams)
        total = round(COLD_ENTRIES * self.config.scale)
        rates = [total * weight / weights / span for _, weight in streams]
        carry = [0.0] * len(streams)
        self.cold_end = end_ns
        self.cold_counts.clear()
        self.cold_counts["streams"] = len(streams)
        for low in range(start, end_ns, HOUR):
            high = min(low + HOUR, end_ns)
            pushed = []
            for index, (labels, _) in enumerate(streams):
                exact = rates[index] * (high - low) + carry[index]
                count = int(exact)
                carry[index] = exact - count
                if not count:
                    continue
                step = (high - low) / count
                values = [
                    [str(low + int((k + rng.random()) * step)), self._cold_line(rng)]
                    for k in range(count)
                ]
                pushed.append({"stream": labels, "values": values})
                self.cold_counts["entries"] += count
                self.cold_counts["bytes"] += sum(len(value[1]) for value in values)
            yield from self._batches(pushed, f"cold-{(low - start) // HOUR:02d}")

    def _cold_line(self, rng: random.Random) -> str:
        level = weighted(
            rng, [(1, "debug"), (6, "info"), (1.5, "warn"), (0.5, "error")]
        )
        if level == "error":
            status = rng.choice((500, 502, 503))
        else:
            status = weighted(
                rng, [(10, 200), (2, 201), (1, 204), (1, 404), (0.3, 429)]
            )
        message = " ".join(rng.choices(WORDS, k=rng.randint(2, 6)))
        if chance(rng, COLD_NEEDLE_RATE):
            message += f" {COLD_NEEDLE}"
            self.cold_counts["needles"] += 1
        return (
            f"level={level} method={zipf(rng, METHODS)} path={zipf(rng, PATHS)} "
            f"status={status} latency_ms={round(rng.lognormvariate(3, 1.0), 1)} "
            f"bytes={rng.randint(0, 50_000)} trace_id={rng.getrandbits(64):016x} "
            f"msg={quote(message)}"
        )

    def cold_flush(self, stack: ColdStack) -> None:
        assert self.config.cold is not None
        self._flush_loki("the cold dataset")
        # Loki builds TSDB index files from its head of flushed series every
        # 15 minutes and when it stops, and uploads them on stop; queriers
        # without that local head find series only in uploaded files.
        settings = self.config.cold
        services = self.cold_services()
        oracle, impl = services["oracle"], services["impl"]
        stack.stop(oracle)
        listing = stack.mc("ls", "--recursive", "local/loki-fuzz/index/")
        first = (self.cold_end - int(settings.span_s * SECOND)) // DAY
        missing = [
            table
            for table in range(first, (self.cold_end - 1) // DAY + 1)
            if f"index_{table}/" not in listing
        ]
        if missing:
            raise RuntimeError(f"Loki uploaded no TSDB index for day tables {missing}")
        report: dict[str, Any] = {
            "before": self._cold_layout(stack, impl),
            "settle_s": {},
        }
        timings = report["settle_s"]
        ready_timeout_s = settings.ready_timeout_s
        if settings.compaction:
            loki_config = PRODUCT_DIR / LOKI_CONFIGS["cold"]
            loki_metrics = f"{self.loki_url}/metrics"
            timings["loki_compactor"] = stack.settle_on(
                oracle,
                config=loki_config,
                text=loki_compactor_config(loki_config),
                env="LOKI_FUZZ_CONFIG_PATH",
                settled=lambda: loki_compactions(scrape(loki_metrics)) >= 2,
                wipe=True,
                ready_timeout_s=ready_timeout_s,
                timeout_s=LOKI_COMPACTION_TIMEOUT_S,
                poll_s=2.0,
                what="its compactor",
            )
        else:
            stack.start(oracle, wipe=True, ready_timeout_s=ready_timeout_s)
        if settings.rollups:
            config_dir = PRODUCT_DIR / IMPL_CONFIG_DIRS[self.impl_config]
            timings["rollups"] = stack.settle_rollups(
                impl,
                config=config_dir / "logs-s3.yaml",
                env="LOGS_S3_CONFIG",
                settled=RollupLogWatch(
                    lambda: stack.service_log(impl),
                    quiet_s=2 * cold.ROLLUP_SETTLE_INTERVAL_S + 5,
                ),
                environment={"LOGS_RUST_LOG": ROLLUP_LOG},
                ready_timeout_s=ready_timeout_s,
            )
        if settings.compaction:
            assert impl.metrics_url is not None
            timings["impl_compaction"], report["impl_compaction"] = (
                stack.settle_compaction(impl, metrics_url=impl.metrics_url)
            )
        report["settle_s"] = {name: round(value, 1) for name, value in timings.items()}
        if timings:
            report["after"] = self._cold_layout(stack, impl)
        print(f"cold logs: settled {report['settle_s']}", flush=True)
        for when in ("before", "after"):
            if when in report:
                summary = report[when]["summary"]
                print(f"cold logs: layout {when} settling: {summary}", flush=True)
        path = self.config.output_dir / "cold_settle.json"
        path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    def _cold_layout(self, stack: ColdStack, impl: ColdService) -> dict[str, Any]:
        """Both sides' objects in MinIO, the implementation's SlateDB layout
        as its manifest has it, and a one-line summary of the two."""
        assert impl.metrics_url is not None
        objects = {
            "impl": stack.bucket_layout("local/logs-fuzz/"),
            "oracle": stack.bucket_layout("local/loki-fuzz/index/"),
        }
        manifest = cold.compaction_state(scrape(impl.metrics_url))

        def total(side: str, match: Callable[[str], bool]) -> dict[str, int]:
            entries = [v for k, v in objects[side].items() if match(k)]
            return {
                "objects": sum(e["objects"] for e in entries),
                "mib": sum(e["bytes"] for e in entries) >> 20,
                "largest_mib": max((e["largest"] for e in entries), default=0) >> 20,
            }

        return {
            "objects": objects,
            "manifest": manifest,
            "summary": {
                "impl_ssts": total("impl", lambda key: key.endswith("/compacted")),
                "impl_wal": total("impl", lambda key: key.endswith("/wal")),
                "impl_manifest": {
                    name.removeprefix("slatedb_db_"): int(manifest.get(name, 0))
                    for name in cold.SLATEDB_LAYOUT[:3]
                },
                "loki_index_files": total("oracle", lambda key: True),
            },
        }

    def cold_probes(self) -> list[Probe]:
        assert self.config.cold is not None
        expected = self.cold_counts["entries"]
        hours = math.ceil(self.config.cold.span_s / 3600) + 1
        query = f"sum(count_over_time({self._cold_selector()} [{hours}h]))"

        def ready(exchange: Exchange) -> bool:
            if not exchange.ok():
                return False
            try:
                result = exchange.json()["data"]["result"]
                return bool(result) and float(result[0]["value"][1]) == expected
            except (ValueError, KeyError, IndexError, TypeError):
                return False

        return [
            Probe(
                "entries",
                Request(
                    "GET",
                    "/loki/api/v1/query",
                    (("query", query), ("time", str(self.cold_end))),
                ),
                ready,
            )
        ]

    def cold_battery(self, rng: random.Random) -> list[Case]:
        end = self.cold_end
        busy = rng.choice(COLD_NAMESPACES[:3])
        everything = self._cold_selector()
        namespace = self._cold_selector(namespace=busy)

        def logs(family: str, query: str, hours: int) -> Case:
            params = (
                ("query", query),
                ("start", str(end - hours * HOUR)),
                ("end", str(end)),
                ("limit", str(COLD_LIMIT)),
                ("direction", "backward"),
            )
            return Case(
                f"{family}.{hours}h",
                query,
                Request("GET", "/loki/api/v1/query_range", params),
                loki_comparator(COLD_LIMIT),
            )

        def metric(family: str, template: str, hours: int) -> Case:
            step = COLD_STEPS[hours]
            query = template.replace("{range}", f"{step}s")
            high = end - end % (step * SECOND)
            params = (
                ("query", query),
                ("start", str(high - hours * HOUR)),
                ("end", str(high)),
                ("step", str(step)),
            )
            return Case(
                f"{family}.{hours}h",
                query,
                Request("GET", "/loki/api/v1/query_range", params),
                _metric_comparator(query),
            )

        day = (("start", str(end - DAY)), ("end", str(end)))
        needle = f"{everything} |= {quote(COLD_NEEDLE)}"
        errors = f'{namespace} |= "level=error"'
        by_level = (
            "sum by (level) (count_over_time("
            + everything
            + " | logfmt | keep level [{range}]))"
        )
        by_namespace = (
            "sum by (namespace) (count_over_time("
            + everything
            + ' |= "level=error" [{range}]))'
        )
        by_app = "sum by (app) (rate(" + namespace + " [{range}]))"
        return [
            logs("log.needle", needle, 1),
            logs("log.needle", needle, 24),
            *(logs("log.errors.namespace", errors, hours) for hours in (1, 6, 24)),
            *(metric("metric.count_by_level", by_level, hours) for hours in (6, 24)),
            metric("metric.errors_by_namespace", by_namespace, 24),
            *(metric("metric.rate_by_app", by_app, hours) for hours in (1, 6)),
            Case(
                "series.namespace.24h",
                namespace,
                Request("GET", "/loki/api/v1/series", (("match[]", namespace), *day)),
                loki_comparator(None),
            ),
            Case(
                "labels.24h",
                "labels",
                Request("GET", "/loki/api/v1/labels", day),
                loki_comparator(None),
            ),
            Case(
                "label_values.pod.24h",
                "pod",
                Request("GET", "/loki/api/v1/label/pod/values", day),
                loki_comparator(None),
            ),
        ]

    def cold_stats(self) -> dict[str, Any]:
        return dict(self.cold_counts)

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
        # Loki 3.5's series API misses flushed streams until the ingester ships
        # its TSDB head (every 15 minutes), so it has no historical answer.
        series = 0.0 if self.config.historical else 1.0
        kind = weighted(
            rng,
            [
                (5, "log"),
                (5, "metric_range"),
                (3, "metric_instant"),
                (series, "series"),
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
