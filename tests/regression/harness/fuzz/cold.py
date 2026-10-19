"""The `cold` scenario: cold reads from object storage.

Each product backfills a span of data (22h by default) into both sides, both
flush and compact it into object storage, and a fixed, seeded battery of
long-range queries then runs `ColdConfig.repeats` times. Before every measured
query the side about to answer it is stopped, its compose volume (if any) is
emptied, and its container is recreated, so in-memory caches, disk caches in
the container's writable layer, and local index or block copies are all gone;
the query is sent once the process reports ready, `settle_s` has passed and it
has moved less than `IDLE_BYTES` to or from object storage over `IDLE_S`.
Whatever a production process
does on its own while starting (Mimir's store-gateway building index-headers,
Tempo polling its blocklist, the implementation opening SlateDB) happens before
the query, as it would after a deploy, and is reported separately as startup
traffic. The other side sits idle meanwhile, so the two never contend.

Object-store requests are attributed per side from the MinIO latency proxy's
per-client-IP counters, read before and after each query.
"""

from __future__ import annotations

import json
import os
import random
import re
import shutil
import tempfile
import time
import urllib.request
from abc import ABC, abstractmethod
from bisect import bisect_right
from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..compose import ComposeProject
from ..process import run, wait_http
from .config import Budget, ColdConfig, Endpoint, FuzzConfig
from .rand import derive
from .recorder import (
    CaseRecord,
    IngestRecord,
    Recorder,
    RoundRecord,
    Summary,
    percentile,
)
from .resources import ResourceSampler
from .runner import Batch, Case, FuzzProduct, Probe, classify, settle_mismatch
from .transport import Exchange, Transport

SIDES = ("impl", "oracle")
PROXY_SERVICE = "minio-latency"
PROXY_STATS_URL = "http://127.0.0.1:9020/"
# The proxy's image, so wiping a volume pulls nothing.
WIPE_IMAGE = "python:3.13-slim"
# Uploads below this over a quiescence window are background polling, not
# flushes or compactions.
QUIESCE_BYTES = 1 << 20
PRIME_TIMEOUT_S = 600.0
# A process moving less than this over `IDLE_S` is only polling (SlateDB
# looks for the next manifest several times a second, each a sub-KiB 404).
IDLE_BYTES = 64 << 10
IDLE_S = 2.0
IDLE_POLL_S = 0.5
SETTLE_CAP_S = 60.0
MINUTE_NS = 60 * 1_000_000_000
_STATS_SCRIPT = (
    "import sys, urllib.request; sys.stdout.write(urllib.request.urlopen("
    f"{PROXY_STATS_URL!r}, timeout=10).read().decode())"
)
_TIMELINE_SCRIPT = (
    "import sys, urllib.request; sys.stdout.write(urllib.request.urlopen("
    f"{PROXY_STATS_URL!r} + sys.argv[1], timeout=30).read().decode())"
)
# Larger than any request number, so a timeline query returns only `next`.
_TIMELINE_END = 1 << 62
_TRAFFIC_KEYS = ("requests", "get", "get_bytes", "bytes", "sent")
# While settling (`ColdConfig.rollups`) the implementation's rollup task runs
# this often instead of every 5 minutes, which no measured process lives for.
ROLLUP_SETTLE_INTERVAL_S = 10
ROLLUP_SETTLE_TIMEOUT_S = 900.0
ROLLUP_PASSES = "telemetry_rollup_passes_total"
WARMER_RUNS = "telemetry_cache_warmer_runs_total"
WARM_TIMEOUT_S = 300.0
# While settling (`ColdConfig.compaction`) SlateDB compaction is done once the
# manifest's layout holds still, nothing is being compacted and the process
# uploads less than `QUIESCE_BYTES`, over one `COMPACTION_QUIET_S` window
# (several compactor polls and manifest refreshes).
SLATEDB_LAYOUT = (
    "slatedb_db_sst_count",
    "slatedb_db_l0_sst_count",
    "slatedb_db_sorted_run_count",
    "slatedb_db_sst_view_count",
    "storage_segment_compactions_total",
)
SLATEDB_COMPACTING = (
    "slatedb_compactor_total_bytes_being_compacted",
    "slatedb_compactor_running_compactions",
    "storage_compaction_oldest_active_seconds",
)
COMPACTION_QUIET_S = 30.0
COMPACTION_TIMEOUT_S = 900.0


def startup_warm_passes(scrape: str) -> float:
    """Finished startup cache-warming passes, whatever their outcome."""
    return sum(
        float(line.rsplit(" ", 1)[1])
        for line in scrape.splitlines()
        if line.startswith(WARMER_RUNS + "{") and 'mode="startup"' in line
    )


def warmer_off_config(config: Path) -> str:
    """`config` with startup cache warming disabled."""
    text = config.read_text()
    if re.search(r"^cache_warmer:", text, re.MULTILINE):
        raise ValueError(f"{config} already configures cache_warmer")
    return f"{text.rstrip()}\ncache_warmer:\n  enabled: false\n"


def rollup_passes(scrape: str, product: str) -> float:
    """Successful rollup passes of `product` in a Prometheus scrape."""
    return sum(
        float(line.rsplit(" ", 1)[1])
        for line in scrape.splitlines()
        if line.startswith(ROLLUP_PASSES + "{")
        and f'product="{product}"' in line
        and 'outcome="ok"' in line
    )


def settle_config(config: Path) -> str:
    """`config` with the rollup task running every `ROLLUP_SETTLE_INTERVAL_S`."""
    text = config.read_text()
    if re.search(r"^rollup:", text, re.MULTILINE):
        raise ValueError(f"{config} already configures rollups")
    return f"{text.rstrip()}\nrollup:\n  interval_seconds: {ROLLUP_SETTLE_INTERVAL_S}\n"


def scrape_sums(scrape: str, names: tuple[str, ...]) -> dict[str, float]:
    """Each of `names` in a Prometheus scrape, summed over its label sets;
    names the scrape lacks are left out."""
    sums: dict[str, float] = {}
    for line in scrape.splitlines():
        name = re.split(r"[{ ]", line, maxsplit=1)[0]
        if name in names:
            sums[name] = sums.get(name, 0.0) + float(line.rsplit(" ", 1)[1])
    return sums


def compaction_state(scrape: str) -> dict[str, float]:
    """The SlateDB layout and compaction gauges of an implementation's scrape."""
    return scrape_sums(scrape, SLATEDB_LAYOUT + SLATEDB_COMPACTING)


def compaction_quiet(before: Mapping[str, float], after: Mapping[str, float]) -> bool:
    """Whether nothing compacted between two `compaction_state`s and nothing
    is being compacted now."""
    if SLATEDB_LAYOUT[0] not in after:
        raise ValueError(f"the scrape has no {SLATEDB_LAYOUT[0]}; not a SlateDB store")
    return all(after.get(name, 0.0) == 0 for name in SLATEDB_COMPACTING) and all(
        before.get(name) == after.get(name) for name in SLATEDB_LAYOUT
    )


def object_layout(listing: str) -> dict[str, dict[str, int]]:
    """Count, total and largest bytes of the objects in an `mc ls --recursive
    --json` listing, per directory (SlateDB's `.../compacted`, `.../wal`;
    Loki's `index_<day>` and, once compacted, `index_<day>/<tenant>`)."""
    layout: dict[str, dict[str, int]] = {}
    for line in listing.splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        if item.get("type") != "file":
            continue
        directory = item["key"].rpartition("/")[0]
        size = int(item.get("size", 0))
        entry = layout.setdefault(directory, {"objects": 0, "bytes": 0, "largest": 0})
        entry["objects"] += 1
        entry["bytes"] += size
        entry["largest"] = max(entry["largest"], size)
    return dict(sorted(layout.items()))


def scrape(url: str) -> str:
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read().decode()


@dataclass(frozen=True)
class ColdService:
    """The compose service that answers one side's reads. `volume` is a
    compose volume of that service holding only local state (caches, local
    index copies, WAL), emptied on every cold restart."""

    service: str
    ready_url: str
    volume: str | None = None
    # The implementation's Prometheus endpoint, and the config file compose
    # mounts from the `config_env` variable when set (for `ColdConfig.impl_starts`).
    metrics_url: str | None = None
    config: Path | None = None
    config_env: str | None = None


def traffic(
    before: Mapping[str, Any], after: Mapping[str, Any], client: str | None
) -> dict[str, int]:
    """Object-store requests one client IP made between two proxy snapshots:
    every request, GETs (S3 reads and listings) and their response bytes,
    all response bytes, and request bytes (`sent`, mostly uploads)."""
    totals = dict.fromkeys(_TRAFFIC_KEYS, 0)
    if client is None:
        return totals
    previous = before.get(client, {})
    for method, counts in after.get(client, {}).items():
        prior = previous.get(method, {})
        requests = counts.get("requests", 0) - prior.get("requests", 0)
        received = counts.get("bytes", 0) - prior.get("bytes", 0)
        totals["requests"] += requests
        totals["bytes"] += received
        totals["sent"] += counts.get("sent", 0) - prior.get("sent", 0)
        if method == "GET":
            totals["get"] += requests
            totals["get_bytes"] += received
    return totals


def request_waves(requests: list[Mapping[str, Any]]) -> dict[str, Any]:
    """The shape of a set of timed object-store requests: how many, their
    response bytes, wall time from the first start to the last end, the time
    at least one was in flight, the most in flight at once, and `waves`, the
    longest chain of requests where each starts after the previous one ended
    (a lower bound on dependent round trips)."""
    timed = sorted(
        (float(r["start"]), float(r["end"] if r.get("end") is not None else r["start"]))
        for r in requests
    )
    if not timed:
        return {
            "requests": 0,
            "bytes": 0,
            "span_ms": 0.0,
            "busy_ms": 0.0,
            "max_inflight": 0,
            "waves": 0,
        }
    busy, current_start, current_end = 0.0, timed[0][0], timed[0][1]
    for start, end in timed[1:]:
        if start > current_end:
            busy += current_end - current_start
            current_start, current_end = start, end
        else:
            current_end = max(current_end, end)
    busy += current_end - current_start
    # At equal times ends (-1) sort first, so back-to-back requests never overlap.
    edges = [(start, 1) for start, _ in timed] + [(end, -1) for _, end in timed]
    inflight = peak = 0
    for _, step in sorted(edges):
        inflight += step
        peak = max(peak, inflight)
    by_end = sorted(range(len(timed)), key=lambda i: timed[i][1])
    ends = [timed[i][1] for i in by_end]
    best_before: list[int] = []
    chain = [0] * len(timed)
    for position, index in enumerate(by_end):
        start = timed[index][0]
        prior = bisect_right(ends, start, 0, position)
        chain[index] = 1 + (best_before[prior - 1] if prior else 0)
        best_before.append(max(chain[index], best_before[-1] if best_before else 0))
    return {
        "requests": len(timed),
        "bytes": sum(int(r.get("bytes") or 0) for r in requests),
        "span_ms": round(max(end for _, end in timed) - timed[0][0], 3),
        "busy_ms": round(busy, 3),
        "max_inflight": peak,
        "waves": max(chain),
    }


class ColdStack:
    """Restarts, volume wipes and proxy counters for one compose project."""

    def __init__(self, project: ComposeProject) -> None:
        self.project = project

    def stop(self, service: ColdService) -> None:
        self.project.execute("stop", "--timeout", "60", service.service, timeout=180)

    def start(
        self, service: ColdService, *, wipe: bool, ready_timeout_s: float
    ) -> float:
        """Recreates a stopped service's container (empty writable layer),
        optionally empties its volume first, and returns the seconds from
        start to ready."""
        if wipe and service.volume:
            run(
                (
                    "docker",
                    "run",
                    "--rm",
                    "--volume",
                    f"{self.project.name}_{service.volume}:/wipe",
                    WIPE_IMAGE,
                    "find",
                    "/wipe",
                    "-mindepth",
                    "1",
                    "-delete",
                ),
                timeout=180,
            )
        started = time.monotonic()
        self.project.execute(
            "up",
            "--detach",
            "--no-deps",
            "--force-recreate",
            service.service,
            timeout=300,
        )
        # Recreated containers lose their cpusets.
        for hook in self.project.after_up:
            hook()
        wait_http(service.ready_url, timeout=ready_timeout_s, interval=0.25)
        return time.monotonic() - started

    def restart(
        self, service: ColdService, *, wipe: bool, ready_timeout_s: float
    ) -> float:
        self.stop(service)
        return self.start(service, wipe=wipe, ready_timeout_s=ready_timeout_s)

    def settle_rollups(
        self,
        service: ColdService,
        *,
        config: Path,
        env: str,
        ready_timeout_s: float,
        product: str | None = None,
        metrics_url: str | None = None,
        settled: Callable[[], bool] | None = None,
        environment: Mapping[str, str] | None = None,
        timeout_s: float = ROLLUP_SETTLE_TIMEOUT_S,
    ) -> float:
        """Restarts `service` once on `config` with rollups every
        `ROLLUP_SETTLE_INTERVAL_S` (compose mounts the file named by `env`)
        and waits for two successful passes of `product` at `metrics_url`,
        the second finding nothing the first left to roll up, or, for a
        server that counts no passes, until `settled()`. Returns the
        seconds it took."""
        if settled is None:
            if product is None or metrics_url is None:
                raise ValueError("settle_rollups needs a product and metrics_url")
            url, name = metrics_url, product

            def settled() -> bool:
                return rollup_passes(scrape(url), name) >= 2

        return self.settle_on(
            service,
            config=config,
            text=settle_config(config),
            env=env,
            settled=settled,
            environment=environment,
            wipe=False,
            ready_timeout_s=ready_timeout_s,
            timeout_s=timeout_s,
            poll_s=ROLLUP_SETTLE_INTERVAL_S / 2,
            what="its rollups",
        )

    def settle_on(
        self,
        service: ColdService,
        *,
        config: Path,
        text: str,
        env: str,
        settled: Callable[[], bool],
        ready_timeout_s: float,
        timeout_s: float,
        poll_s: float,
        what: str,
        environment: Mapping[str, str] | None = None,
        wipe: bool = False,
    ) -> float:
        """Restarts `service` once on `text`, written to a file named like
        `config` that compose mounts from the variable `env`, with
        `environment` set for compose too, and polls `settled()` every
        `poll_s`. Unsets them all again, so later restarts use `config` as it
        is; the service keeps running on `text` until then. Returns the
        seconds it took."""
        directory = Path(tempfile.mkdtemp(prefix="cold-settle-"))
        settle = directory / config.name
        settle.write_text(text)
        variables = {env: str(settle), **(environment or {})}
        previous = {name: os.environ.get(name) for name in variables}
        started = time.monotonic()
        os.environ.update(variables)
        try:
            self.restart(service, wipe=wipe, ready_timeout_s=ready_timeout_s)
            while not settled():
                if time.monotonic() - started >= timeout_s:
                    raise RuntimeError(
                        f"{service.service} did not settle {what} "
                        f"within {timeout_s:.0f}s"
                    )
                time.sleep(poll_s)
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            shutil.rmtree(directory, ignore_errors=True)
        return time.monotonic() - started

    def settle_compaction(
        self,
        service: ColdService,
        *,
        metrics_url: str,
        window_s: float = COMPACTION_QUIET_S,
        timeout_s: float = COMPACTION_TIMEOUT_S,
    ) -> tuple[float, dict[str, float]]:
        """Waits until `service`'s SlateDB compaction is quiescent: over one
        `window_s` its manifest layout (`SLATEDB_LAYOUT`) held still, nothing
        is being compacted (`SLATEDB_COMPACTING`) and it uploaded less than
        `QUIESCE_BYTES`. Returns the seconds it took and the final state."""
        client = self.address(service.service)
        started = time.monotonic()
        before, stats = compaction_state(scrape(metrics_url)), self.proxy_stats()
        while True:
            time.sleep(window_s)
            after, latest = compaction_state(scrape(metrics_url)), self.proxy_stats()
            sent = traffic(stats, latest, client)["sent"]
            if compaction_quiet(before, after) and sent < QUIESCE_BYTES:
                return time.monotonic() - started, after
            if time.monotonic() - started >= timeout_s:
                raise RuntimeError(
                    f"{service.service} was still compacting after {timeout_s:.0f}s: "
                    f"{before} -> {after}, {sent} bytes uploaded"
                )
            before, stats = after, latest

    def service_log(self, service: ColdService) -> str:
        """Everything the service's current container has logged."""
        return self.project.execute(
            "logs", "--no-log-prefix", service.service, timeout=120
        )

    def bucket_layout(self, prefix: str) -> dict[str, dict[str, int]]:
        """`object_layout` of the objects under `prefix` (`alias/bucket/path`)."""
        return object_layout(self.mc("ls", "--recursive", "--json", prefix))

    def address(self, service: str) -> str | None:
        ids = self.project.execute("ps", "--quiet", service, timeout=60).split()
        if not ids:
            return None
        raw = run(
            (
                "docker",
                "inspect",
                "--format",
                "{{json .NetworkSettings.Networks}}",
                ids[0],
            ),
            timeout=60,
        ).stdout
        networks = json.loads(raw) or {}
        return next(
            (item["IPAddress"] for item in networks.values() if item.get("IPAddress")),
            None,
        )

    def proxy_stats(self) -> dict[str, Any]:
        output = self.project.execute(
            "exec", "-T", PROXY_SERVICE, "python", "-c", _STATS_SCRIPT, timeout=60
        )
        return json.loads(output)["clients"]

    def proxy_timeline(self, since: int, client: str | None = None) -> dict[str, Any]:
        """The proxy's recorded requests numbered `since` and later, optionally
        for one client, and `next` (see `proxy.py`; needs FUZZ_COLD_TIMELINE)."""
        query = f"timeline?since={since}" + (f"&client={client}" if client else "")
        output = self.project.execute(
            "exec",
            "-T",
            PROXY_SERVICE,
            "python",
            "-c",
            _TIMELINE_SCRIPT,
            query,
            timeout=120,
        )
        return json.loads(output)

    def mc(self, *args: str) -> str:
        """Runs the MinIO client (`minio-init`'s image and credentials)."""
        return self.project.execute(
            "run", "--rm", "--no-deps", "-T", "minio-init", *args, timeout=300
        )


class ColdProduct(ABC):
    """What a product adds to run the cold battery."""

    @abstractmethod
    def cold_services(self) -> dict[str, ColdService]:
        """`impl` and `oracle`: the services that answer reads."""

    @abstractmethod
    def cold_load(self, rng: random.Random, end_ns: int) -> Iterator[Batch]:
        """Write batches for the whole span ending at `end_ns`. Consumed
        lazily, after the previous batch reached both sides."""

    @abstractmethod
    def cold_probes(self) -> list[Probe]:
        """After loading: requests that hold only once everything written is
        readable."""

    @abstractmethod
    def cold_battery(self, rng: random.Random) -> list[Case]:
        """The fixed query battery over the loaded data."""

    @abstractmethod
    def cold_stats(self) -> dict[str, Any]:
        """Dataset shape, for the report."""

    def cold_flush(self, stack: ColdStack) -> None:
        """Move everything the oracle holds locally into object storage."""
        return None


def _stats(values: list[float], keys: tuple[str, ...]) -> dict[str, float | None]:
    fractions = {"p50": 0.5, "p90": 0.9, "p99": 0.99}
    result: dict[str, float | None] = {}
    for key in keys:
        if key == "max":
            result[key] = max(values) if values else None
        elif key == "mean":
            result[key] = sum(values) / len(values) if values else None
        else:
            result[key] = percentile(values, fractions[key])
    return result


def summarize(
    measurements: list[dict[str, Any]], startups: Mapping[str, list[dict[str, Any]]]
) -> dict[str, Any]:
    """Per-family cold and warm latency and per-query object-store traffic,
    over measurements both sides answered."""
    families: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in measurements:
        if all(item[side]["status"] is not None for side in SIDES):
            families[item["family"]].append(item)

    def side_stats(items: list[dict[str, Any]], side: str) -> dict[str, Any]:
        warm = [i[side]["warm_ms"] for i in items if i[side].get("warm_ms") is not None]
        return {
            "cold_ms": _stats(
                [i[side]["cold_ms"] for i in items], ("p50", "p90", "p99")
            ),
            "warm_ms": _stats(warm, ("p50",)),
            "get": _stats([i[side]["query"]["get"] for i in items], ("p50", "max")),
            "get_bytes": _stats(
                [i[side]["query"]["get_bytes"] for i in items], ("p50", "max")
            ),
            "requests": _stats(
                [i[side]["query"]["requests"] for i in items], ("p50", "max")
            ),
        }

    answered = [item for items in families.values() for item in items]
    starts = sorted(
        {start for item in answered for start in item.get("impl_starts", {})}
    )

    def start_stats(items: list[dict[str, Any]], start: str) -> dict[str, Any]:
        records = [
            {"impl": item["impl_starts"][start]}
            for item in items
            if item["impl_starts"].get(start, {}).get("status") is not None
        ]
        startup = [record["impl"]["startup"] for record in records]
        return {
            **side_stats(records, "impl"),
            "startup": {
                key: _stats([s[key] for s in startup if key in s], ("p50", "max"))
                for key in ("ready_s", "settle_s", "warm_s", "get", "get_bytes")
            },
        }

    by_start = {
        start: {
            "families": {
                family: start_stats(items, start)
                for family, items in sorted(families.items())
            },
            "totals": start_stats(answered, start),
        }
        for start in starts
    }
    return {
        **({"impl_starts": by_start} if by_start else {}),
        "measured": len(measurements),
        "answered": len(answered),
        "families": {
            family: {
                "cases": len(items),
                **{side: side_stats(items, side) for side in SIDES},
            }
            for family, items in sorted(families.items())
        },
        "totals": {
            side: {
                **side_stats(answered, side),
                "get_total": sum(i[side]["query"]["get"] for i in answered),
                "get_bytes_total": sum(i[side]["query"]["get_bytes"] for i in answered),
            }
            for side in SIDES
        },
        "startup": {
            side: {
                "restarts": len(startups[side]),
                "ready_s": _stats(
                    [s["ready_s"] for s in startups[side]], ("p50", "p90", "max")
                ),
                "settle_s": _stats(
                    [s.get("settle_s", 0.0) for s in startups[side]], ("p50", "max")
                ),
                "get": _stats([s["get"] for s in startups[side]], ("p50", "max")),
                "get_bytes": _stats(
                    [s["get_bytes"] for s in startups[side]], ("p50", "max")
                ),
            }
            for side in SIDES
        },
    }


class ColdRunner:
    def __init__(
        self,
        product: FuzzProduct,
        config: FuzzConfig,
        project: ComposeProject,
        sampler: ResourceSampler | None = None,
        layout: dict[str, Any] | None = None,
    ) -> None:
        if not isinstance(product, ColdProduct):
            raise ValueError(f"{product.name} has no cold battery")
        assert config.cold is not None
        self.product = product
        self.cold: ColdProduct = product
        self.config = config
        self.settings: ColdConfig = config.cold
        self.stack = ColdStack(project)
        self.sampler = sampler
        self.transport = Transport(
            timeout_s=config.request_timeout_s, handlers=product.handlers()
        )
        self.budget = Budget(config.duration_s)
        self.reserve = min(30.0, max(2.0, config.duration_s * 0.05))
        self.recorder = Recorder(config, product.describe(), layout=layout)
        self.order = derive(config.seed, "order")
        self.services = product.cold_services()
        self.endpoints: dict[str, Endpoint] = {
            "impl": product.impl_read,
            "oracle": product.oracle_read,
        }
        self.measurements: list[dict[str, Any]] = []
        self.startups: dict[str, list[dict[str, Any]]] = {side: [] for side in SIDES}
        self.batches = 0
        self.planned = 0
        self.timings: dict[str, float] = {}
        self._log = (config.output_dir / "cold.jsonl").open("w", encoding="utf-8")
        self._timeline = (
            (config.output_dir / "cold_timeline.jsonl").open("w", encoding="utf-8")
            if self.settings.timeline
            else None
        )

    def _say(self, message: str) -> None:
        print(f"cold {self.config.product}: {message}", flush=True)

    def run(self) -> Summary:
        settings = self.settings
        self._say(
            f"seed={self.config.seed} run={self.config.run_id} "
            f"budget={self.config.duration_s:.0f}s span={settings.span_s:.0f}s "
            f"repeats={settings.repeats} first_byte={settings.first_byte_ms:g}ms "
            f"output={self.config.output_dir}"
        )
        if self.sampler is not None:
            self.sampler.start()
        try:
            if (
                self._timed("load_s", self._load)
                and self._timed("settle_s", self._settle)
                and self._timed("prime_s", self._prime)
            ):
                self._timed("battery_s", self._battery)
        finally:
            self._log.close()
            if self._timeline is not None:
                self._timeline.close()
            elapsed_s = self.budget.elapsed()
            resources = self.sampler.stop() if self.sampler is not None else None
            summary = self.recorder.finish(
                elapsed_s=elapsed_s,
                resources=resources,
                extra={"cold": self._summary()},
            )
        return summary

    def _timed(self, name: str, step) -> bool:
        started = time.monotonic()
        try:
            return step()
        finally:
            self.timings[name] = round(time.monotonic() - started, 1)

    def _out_of_time(self, needed_s: float = 0.0) -> bool:
        return self.budget.expired(self.reserve + needed_s)

    # Loading -----------------------------------------------------------------

    def _load(self) -> bool:
        end_ns = (time.time_ns() // MINUTE_NS - 1) * MINUTE_NS
        rng = derive(self.config.seed, "cold-data")
        last_progress = time.monotonic()
        batches = 0
        for batch in self.cold.cold_load(rng, end_ns):
            oracle = self.transport.send_with_retry(
                self.product.oracle_write, batch.oracle, budget=self.budget
            )
            impl = self.transport.send_with_retry(
                self.product.impl_write, batch.impl, budget=self.budget
            )
            self.recorder.record_ingest(
                IngestRecord(0, batch.name, batch.items, oracle, impl)
            )
            batches += 1
            if self._out_of_time():
                self.recorder.note(
                    f"the budget ran out while loading ({batches} batches)"
                )
                return False
            if time.monotonic() - last_progress >= 30:
                last_progress = time.monotonic()
                self._say(f"loaded {batches} batches ({batch.name})")
        self.batches = batches
        self._say(f"loaded {batches} batches: {self.cold.cold_stats()}")
        return True

    def _settle(self) -> bool:
        self.cold.cold_flush(self.stack)
        waited = self._quiesce()
        self._say(f"object-store uploads quiet after {waited:.0f}s")
        return not self._out_of_time()

    def _quiesce(self) -> float:
        """Waits until neither side uploads more than `QUIESCE_BYTES` over a
        `quiesce_s` window: flushes, compactions and index uploads are done,
        so none of them runs during a measurement."""
        started = time.monotonic()
        deadline = started + self.settings.quiesce_timeout_s
        clients = {
            side: self.stack.address(service.service)
            for side, service in self.services.items()
        }
        before = self.stack.proxy_stats()
        while True:
            time.sleep(self.settings.quiesce_s)
            after = self.stack.proxy_stats()
            uploads = {
                side: traffic(before, after, client)["sent"]
                for side, client in clients.items()
            }
            if all(sent < QUIESCE_BYTES for sent in uploads.values()):
                return time.monotonic() - started
            if time.monotonic() >= deadline or self._out_of_time():
                self.recorder.note(
                    f"object-store uploads never went quiet (last window: {uploads}); "
                    "measuring anyway"
                )
                return time.monotonic() - started
            before = after

    def _prime(self) -> bool:
        """One cold restart of both sides, then the probes: the data must be
        complete in object storage alone before anything is measured."""
        visible = {}
        for side in SIDES:
            self.stack.restart(
                self.services[side],
                wipe=True,
                ready_timeout_s=self.settings.ready_timeout_s,
            )
        timeout = max(self.config.visibility_timeout_s, PRIME_TIMEOUT_S)
        probes = self.cold.cold_probes()
        for side in SIDES:
            visible[side] = self._wait(self.endpoints[side], probes, timeout)
        self.recorder.record_round(
            RoundRecord(
                index=0,
                profile="cold",
                batches=self.batches,
                stats=self.cold.cold_stats(),
                visible=visible,
                load_ms=self.timings.get("load_s", 0.0) * 1000,
            )
        )
        if not all(visible.values()):
            self.recorder.note(
                f"after a cold restart the data was not complete: {visible}; "
                "no cold query would be meaningful"
            )
        return all(visible.values())

    def _wait(self, endpoint: Endpoint, probes: list[Probe], timeout_s: float) -> bool:
        deadline = time.monotonic() + min(timeout_s, self.budget.remaining())
        pending = list(probes)
        while pending:
            pending = [
                probe
                for probe in pending
                if not probe.ready(self.transport.send(endpoint, probe.request))
            ]
            if not pending:
                return True
            if time.monotonic() >= deadline:
                self.recorder.note(
                    f"{endpoint.url}: probes {[p.name for p in pending]} never held"
                )
                return False
            time.sleep(1.0)
        return True

    # Measuring ---------------------------------------------------------------

    def _battery(self) -> bool:
        cases = [
            case
            for case in self.cold.cold_battery(derive(self.config.seed, "cold-battery"))
            if self.settings.selected(case.family)
        ]
        self.planned = len(cases) * self.settings.repeats
        cycle_s = 0.0
        done = 0
        for repeat in range(self.settings.repeats):
            for number, case in enumerate(cases):
                if self._out_of_time(cycle_s):
                    self.recorder.note(
                        f"the budget ran out after {done} of {self.planned} cold "
                        "measurements"
                    )
                    return True
                started = time.monotonic()
                self._measure(f"c{repeat}-q{number:03d}", repeat, case)
                done += 1
                cycle_s = max(cycle_s, time.monotonic() - started)
        return True

    def _measure(self, case_id: str, repeat: int, case: Case) -> None:
        first = SIDES if self.order.random() < 0.5 else SIDES[::-1]
        # The last start is the implementation's measurement of record.
        starts = self.settings.impl_starts
        sides: dict[str, dict[str, Any]] = {}
        impl_starts: dict[str, dict[str, Any]] = {}
        for side in first:
            if side == "impl" and starts:
                for start in starts:
                    impl_starts[start] = self._cold_query(
                        side, case, case_id, start, record=start == starts[-1]
                    )
                sides[side] = impl_starts[starts[-1]]
            else:
                sides[side] = self._cold_query(side, case, case_id)
        oracle, impl = sides["oracle"]["cold"], sides["impl"]["cold"]
        outcome, detail = classify(case, oracle, impl)
        warm_oracle, warm_impl = sides["oracle"]["warm"], sides["impl"]["warm"]
        oracle_rechecks: list[Exchange] = []
        if outcome == "mismatch" and case.idempotent and warm_oracle and warm_impl:
            endpoint = self.endpoints["oracle"]
            outcome, extra, oracle_rechecks = settle_mismatch(
                case,
                (impl, warm_impl),
                (oracle, warm_oracle),
                lambda: self.transport.send(endpoint, case.request),
            )
            if extra:
                detail = f"{detail}; {extra}"
        self.recorder.record_case(
            CaseRecord(
                id=case_id,
                round=repeat,
                family=case.family,
                query=case.query,
                request=case.request,
                outcome=outcome,
                detail=detail,
                oracle=oracle,
                impl=impl,
                oracle_rechecks=tuple(oracle_rechecks),
            )
        )
        line = {
            "id": case_id,
            "repeat": repeat,
            "family": case.family,
            "query": case.query,
            "request": case.request.describe(),
            "outcome": outcome,
            "first": first[0],
            **{side: _side_record(sides[side]) for side in SIDES},
        }
        if impl_starts:
            line["impl_starts"] = {
                start: {**_side_record(result), "start": start}
                for start, result in impl_starts.items()
            }
        self.measurements.append(line)
        self._log.write(json.dumps(line, ensure_ascii=False) + "\n")
        self._log.flush()
        self._say(
            f"{len(self.measurements)}/{self.planned} {case.family} {outcome}: "
            f"impl {impl.latency_ms:.0f} ms ({line['impl']['query']['get']} GETs), "
            f"oracle {oracle.latency_ms:.0f} ms ({line['oracle']['query']['get']} GETs)"
        )

    def _cold_query(
        self,
        side: str,
        case: Case,
        case_id: str = "",
        start: str = "idle",
        record: bool = True,
    ) -> dict[str, Any]:
        service = self.services[side]
        endpoint = self.endpoints[side]
        self.stack.stop(service)
        stopped = self.stack.proxy_stats()
        marks = self._timeline_mark()
        ready_s = self._start(service, start)
        client = self.stack.address(service.service)
        settled = time.monotonic()
        warm_s = None
        if start == "ready":
            before = self.stack.proxy_stats()
        else:
            if start == "warm":
                warm_s = self._wait_warm(service)
            before = self._idle(client)
        settle_s = time.monotonic() - settled
        query_mark = self._timeline_mark()
        cold = self.transport.send(endpoint, case.request)
        after = self.stack.proxy_stats()
        if marks is not None and query_mark is not None:
            self._record_timeline(
                case_id, side, case, cold, client, marks["next"], query_mark
            )
        warm = (
            self.transport.send(endpoint, case.request) if self.settings.warm else None
        )
        startup = {
            "ready_s": round(ready_s, 3),
            "settle_s": round(settle_s, 3),
            **traffic(stopped, before, client),
        }
        if warm_s is not None:
            startup["warm_s"] = round(warm_s, 3)
        if record:
            self.startups[side].append(startup)
        return {
            "cold": cold,
            "warm": warm,
            "startup": startup,
            "query": traffic(before, after, client),
        }

    def _start(self, service: ColdService, start: str) -> float:
        """Starts `service` empty; `off` on a copy of its config with startup
        cache warming disabled."""
        if start != "off":
            return self.stack.start(
                service, wipe=True, ready_timeout_s=self.settings.ready_timeout_s
            )
        if service.config is None or service.config_env is None:
            raise ValueError(f"{service.service} has no config to disable warming in")
        directory = Path(tempfile.mkdtemp(prefix="cold-warmer-off-"))
        config = directory / service.config.name
        config.write_text(warmer_off_config(service.config))
        os.environ[service.config_env] = str(config)
        try:
            return self.stack.start(
                service, wipe=True, ready_timeout_s=self.settings.ready_timeout_s
            )
        finally:
            os.environ.pop(service.config_env, None)
            shutil.rmtree(directory, ignore_errors=True)

    def _wait_warm(self, service: ColdService) -> float:
        """Seconds from now until `service` finished its startup warm pass."""
        if service.metrics_url is None:
            raise ValueError(f"{service.service} has no metrics endpoint")
        started = time.monotonic()
        while True:
            with urllib.request.urlopen(service.metrics_url, timeout=30) as response:
                if startup_warm_passes(response.read().decode()) >= 1:
                    return time.monotonic() - started
            if time.monotonic() - started >= WARM_TIMEOUT_S:
                raise RuntimeError(
                    f"{service.service} did not finish its startup warm pass "
                    f"within {WARM_TIMEOUT_S:.0f}s"
                )
            time.sleep(0.1)

    def _timeline_mark(self) -> dict[str, Any] | None:
        if not self.settings.timeline:
            return None
        return self.stack.proxy_timeline(_TIMELINE_END)

    def _record_timeline(
        self,
        case_id: str,
        side: str,
        case: Case,
        cold: Exchange,
        client: str | None,
        since: int,
        query_mark: Mapping[str, Any],
    ) -> None:
        assert self._timeline is not None
        requests = self.stack.proxy_timeline(since, client)["requests"]
        phases = {
            "startup": [r for r in requests if r["seq"] < query_mark["next"]],
            "query": [r for r in requests if r["seq"] >= query_mark["next"]],
        }
        line = {
            "id": case_id,
            "side": side,
            "family": case.family,
            "client": client,
            "cold_ms": round(cold.latency_ms, 3),
            "query_mark_ms": query_mark["now"],
            **{
                phase: {"summary": request_waves(items), "requests": items}
                for phase, items in phases.items()
            },
        }
        self._timeline.write(json.dumps(line, ensure_ascii=False) + "\n")
        self._timeline.flush()

    def _idle(self, client: str | None) -> dict[str, Any]:
        """Waits `settle_s`, then until `client` has moved less than
        `IDLE_BYTES` over the last `IDLE_S` (at most `SETTLE_CAP_S` longer),
        so work a fresh process starts on its own (index-header builds, a
        compactor pass, index downloads) is over before the query. Returns
        the last proxy snapshot."""
        time.sleep(self.settings.settle_s)
        deadline = time.monotonic() + SETTLE_CAP_S
        window = [(time.monotonic(), self.stack.proxy_stats())]
        while True:
            time.sleep(IDLE_POLL_S)
            now = time.monotonic()
            snapshot = self.stack.proxy_stats()
            window.append((now, snapshot))
            while len(window) > 1 and now - window[1][0] >= IDLE_S:
                window.pop(0)
            if now - window[0][0] >= IDLE_S:
                moved = traffic(window[0][1], snapshot, client)
                if moved["bytes"] + moved["sent"] < IDLE_BYTES:
                    return snapshot
            if now >= deadline:
                return snapshot

    def _summary(self) -> dict[str, Any]:
        settings = self.settings
        return {
            **settings.describe(),
            "dataset": self.cold.cold_stats(),
            "planned": self.planned,
            "timings_s": self.timings,
            **summarize(self.measurements, self.startups),
        }


def _side_record(result: dict[str, Any]) -> dict[str, Any]:
    cold: Exchange = result["cold"]
    warm: Exchange | None = result["warm"]
    return {
        "status": cold.status,
        "error": cold.error,
        "bytes": len(cold.body),
        "cold_ms": round(cold.latency_ms, 3),
        "warm_ms": round(warm.latency_ms, 3) if warm and warm.status else None,
        "startup": result["startup"],
        "query": result["query"],
    }
