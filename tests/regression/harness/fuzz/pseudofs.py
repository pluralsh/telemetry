"""Model-checked fuzzing of PseudoFS over gRPC.

The oracle is an in-process reference model (`harness.pseudofs.model`), so
reports carry implementation latency only. Each round loads or churns a
randomized tree and checks every mutation's result against the model; some
rounds also run concurrent race scenarios in a scratch tenant and check that
readers never see torn, lost, or corrupt state, and every few rounds the
server is SIGKILLed and restarted to check that acknowledged writes survive.
Queries then compare reads, ranges, stat, listing, glob, grep, and single
timed mutations at each durability level against the model.
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any

from ..compose import ComposeProject
from ..pseudofs.client import Client, ClientTimeout, ClientUnavailable
from ..pseudofs.content import (
    BINARY_KINDS,
    TEXT_KINDS,
    Spec,
    check_race_payload,
    race_payload,
)
from ..pseudofs.model import PATH_KINDS, Dir, File, Limits, Model, join
from .config import Endpoint, FuzzConfig, endpoint_from_env
from .network import minio_floor_url, service_url
from .rand import ODD_STRINGS, WORDS, chance, weighted
from .runner import Batch, Case, FuzzProduct, Mismatch, Probe, Round
from .stack import fuzz_stack
from .transport import Exchange, Request

PRODUCT_DIR = Path(__file__).resolve().parents[2] / "products" / "pseudofs"
GRPC_PORT = 9093
HOST_PORT = 19093
MIB = 1024 * 1024
OPS_PER_BATCH = 200
SYNC_DIR = "/.sync"
RACE_FAMILIES = ("overwrite", "append", "unlink", "rename")

# Must match products/pseudofs/<config>.yaml.
IMPL_CONFIGS: dict[str, dict[str, Any]] = {
    "regression": {
        "inline_threshold": 4096,
        "max_file_mib": 24,
        "limits": Limits(
            max_unary_bytes=8 * MIB,
            max_file_size_bytes=256 * MIB,
            default_grep_matches=100,
            max_grep_matches=1000,
            max_grep_context_lines=100,
            default_grep_file_size_bytes=16 * MIB,
            max_grep_file_size_bytes=64 * MIB,
            max_scanned_entries=50_000,
            grep_concurrency=4,
        ),
        "max_data_mib": 1536,
    },
    "production": {
        "inline_threshold": 16 * 1024,
        "max_file_mib": 96,
        "limits": Limits(max_unary_bytes=8 * MIB, grep_concurrency=8),
        "max_data_mib": 3072,
    },
}
MAX_FILES = 40_000
IMPL_CONFIGS["local"] = IMPL_CONFIGS["production"]
# PseudoFS has no sharded topology; the benchmark's sharded runs use production.
CONFIG_ALIASES = {"production-sharded": "production"}

PROFILES = (
    (3.0, "tree"),
    (3.0, "churn"),
    (1.0, "wide"),
    (1.0, "deep"),
    (1.5, "large"),
    (1.5, "appendlog"),
    (0.5, "tenant"),
)

EXTENSIONS = (
    (6.0, ".txt"),
    (3.0, ".log"),
    (2.0, ".md"),
    (2.0, ".json"),
    (1.0, ".csv"),
    (1.0, ".rs"),
    (2.0, ""),
    (1.0, ".png"),
    (0.5, ".JPG"),
    (0.5, ".gif"),
    (0.5, ".pdf"),
    (0.5, ".bin"),
    (0.5, ".tar.gz"),
    (0.3, ".mp4"),
    (0.3, ".sqlite"),
    (0.3, ".bin.txt"),
)
BINARY_EXTENSION_KIND = {
    ".png": "png",
    ".JPG": "jpeg",
    ".gif": "gif",
    ".pdf": "pdf",
    ".bin": "binary",
    ".tar.gz": "gzip",
    ".mp4": "mp4",
    ".sqlite": "sqlite",
}
ODD_NAMES = (
    "résumé",
    "日本語",
    "emoji-🚀",
    "with space",
    ".hidden",
    "a=b",
    "#hash",
    "plus+sign",
    "star*",
    "q?mark",
    "[bracket]",
    "{brace}",
    "UPPER",
    "dot.dot",
)
DURABILITIES = (
    (3.0, "applied"),
    (4.0, "written"),
    (1.0, "durable"),
    (0.5, "unspecified"),
)


def _json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True).encode()


def _error_key(error: dict[str, Any]) -> tuple[str, str, str]:
    kind = error.get("kind", "")
    return (
        error.get("code", ""),
        kind,
        error.get("path", "") if kind in PATH_KINDS else "",
    )


def difference(
    op: dict[str, Any], expected: dict[str, Any], actual: dict[str, Any]
) -> str | None:
    """Why `actual` (implementation) differs from `expected` (model), or None."""
    expected_error, actual_error = expected.get("error"), actual.get("error")
    if (expected_error is None) != (actual_error is None):
        return f"expected {_brief(expected)}, got {_brief(actual)}"
    if expected_error is not None and _error_key(expected_error) != _error_key(
        actual_error
    ):
        wanted, got = _error_key(expected_error), _error_key(actual_error)
        return f"expected error {wanted}, got {got}"
    if op["op"] == "grep":
        return _grep_difference(expected, actual)
    for key in sorted(set(expected) | set(actual)):
        if key == "error":
            continue
        # A failed stream carries whatever items arrived before the error.
        if (expected.get(key) or None) != (actual.get(key) or None):
            wanted, got = _brief(expected.get(key)), _brief(actual.get(key))
            return f"{key}: expected {wanted}, got {got}"
    return None


def _brief(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, default=str)
    return text if len(text) <= 400 else text[:400] + "…"


def _grep_difference(expected: dict[str, Any], actual: dict[str, Any]) -> str | None:
    wanted, got = expected.get("matches") or [], actual.get("matches") or []
    # Only model results carry the limit; stability rechecks compare two
    # results from the same side, which must agree exactly.
    maximum = expected.get("limit")
    if (
        maximum is None
        or "limit" in actual
        or len(wanted) <= maximum
        or expected.get("error")
    ):
        if wanted != got:
            return _first_list_difference(wanted, got)
        return None
    # More matches than the limit: concurrent file searches may claim the
    # budget out of order, so accept any `maximum` matches that keep walk
    # order and are a prefix of each file's own matches.
    if len(got) != maximum:
        return f"expected exactly {maximum} matches (limit hit), got {len(got)}"
    position = 0
    for item in got:
        while position < len(wanted) and wanted[position] != item:
            position += 1
        if position == len(wanted):
            return f"match not in the model's walk-ordered results: {_brief(item)}"
        position += 1
    by_file: dict[str, list[dict[str, Any]]] = {}
    for item in wanted:
        by_file.setdefault(item["path"], []).append(item)
    seen: dict[str, int] = {}
    for item in got:
        index = seen.get(item["path"], 0)
        if by_file[item["path"]][index] != item:
            return f"{item['path']}: matches are not a prefix of the file's matches"
        seen[item["path"]] = index + 1
    return None


def _first_list_difference(wanted: list[Any], got: list[Any]) -> str:
    for index, (left, right) in enumerate(zip(wanted, got, strict=False)):
        if left != right:
            return f"item {index}: expected {_brief(left)}, got {_brief(right)}"
    return f"expected {len(wanted)} items, got {len(got)}"


class Planner:
    """Generates operations against a scratch copy of the model, recording the
    results the implementation must return."""

    def __init__(self, model: Model, tenant: str, rng: random.Random) -> None:
        self.model = model
        self.tenant = tenant
        self.rng = rng
        self.ops: list[dict[str, Any]] = []
        self.expected: list[dict[str, Any]] = []
        self.groups: list[list[int]] | None = None

    def add(self, op: dict[str, Any]) -> dict[str, Any]:
        op.setdefault("tenant", self.tenant)
        result = self.model.execute(op)
        self.ops.append(op)
        self.expected.append(result)
        return result


class PseudofsFuzz(FuzzProduct):
    name = "pseudofs"
    default_window = "1m"
    resource_roles = {"pseudofs": "impl"}
    reference_latency = False

    def __init__(self, config: FuzzConfig) -> None:
        super().__init__(config)
        impl_config = os.environ.get("FUZZ_PSEUDOFS_CONFIG") or "regression"
        self.impl_config = CONFIG_ALIASES.get(impl_config, impl_config)
        if self.impl_config not in IMPL_CONFIGS:
            raise ValueError(
                f"FUZZ_PSEUDOFS_CONFIG must be one of {tuple(IMPL_CONFIGS)}"
            )
        settings = IMPL_CONFIGS[self.impl_config]
        self.inline_threshold = int(settings["inline_threshold"])
        self.max_file_bytes = int(
            float(
                os.environ.get("FUZZ_PSEUDOFS_MAX_FILE_MIB") or settings["max_file_mib"]
            )
            * MIB
        )
        limits = settings["limits"]
        self.max_data_bytes = int(
            float(
                os.environ.get("FUZZ_PSEUDOFS_MAX_DATA_MIB") or settings["max_data_mib"]
            )
            * MIB
        )
        self.model = Model(limits)
        self.limits = limits
        prefix = os.environ.get("FUZZ_PSEUDOFS_TENANT") or "fuzz"
        self.tenant = f"{prefix}-{config.seed:08x}"
        self.other_tenant = f"{self.tenant}-b"
        self.race_tenant = f"{self.tenant}-race"
        host = service_url("pseudofs", GRPC_PORT, HOST_PORT).removeprefix("http://")
        impl = endpoint_from_env("pseudofs", "impl", Endpoint(f"grpc://{host}"))
        self.impl_read = self.impl_write = impl
        self.oracle_read = self.oracle_write = Endpoint("model://reference")
        default_crash = "4" if config.stack == "compose" else "0"
        self.crash_every = int(
            os.environ.get("FUZZ_PSEUDOFS_CRASH_EVERY") or default_crash
        )
        self.race_rate = float(os.environ.get("FUZZ_PSEUDOFS_RACE_RATE") or "0.5")
        self.concurrency = int(os.environ.get("FUZZ_PSEUDOFS_CONCURRENCY") or "8")
        self.pool = ThreadPoolExecutor(max_workers=max(self.concurrency, 16))
        self._client: Client | None = None
        self._client_lock = threading.Lock()
        self._project: ComposeProject | None = None
        self._before_batch: Model | None = None
        self.crashes = 0
        self.resyncs = 0

    def describe(self) -> dict[str, object]:
        return {
            "impl_config": self.impl_config,
            "tenant": self.tenant,
            "crash_every": self.crash_every,
            "race_rate": self.race_rate,
            "concurrency": self.concurrency,
            "max_file_bytes": self.max_file_bytes,
            **super().describe(),
        }

    # Stack ---------------------------------------------------------------------

    def client(self) -> Client:
        with self._client_lock:
            if self._client is None:
                target = self.impl_read.url.partition("://")[2]
                self._client = Client(target, self.config.request_timeout_s)
            return self._client

    def stack(self) -> AbstractContextManager[object]:
        os.environ["PSEUDOFS_CONFIG"] = f"{self.impl_config}.yaml"
        services = (
            ("pseudofs",)
            if self.impl_config == "local"
            else (
                "minio",
                "minio-init",
                "pseudofs",
            )
        )

        @contextmanager
        def managed() -> Iterator[object]:
            with fuzz_stack(
                self.config,
                lambda: ComposeProject(
                    file=PRODUCT_DIR / "docker-compose.yml",
                    name="pseudofs-fuzz",
                    services=services,
                ),
                roles=self.resource_roles,
                after_start=lambda _: self.wait_ready(),
            ) as project:
                self._project = project
                if project is None:
                    self.wait_ready()
                yield project

        return managed()

    def wait_ready(self, timeout_s: float = 120.0) -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.client().health():
                return
            time.sleep(0.25)
        raise TimeoutError(f"pseudofs at {self.impl_read.url} never became healthy")

    def floor_urls(self) -> dict[str, str]:
        urls = {"pseudofs": f"{self.impl_read.url}/health"}
        if self.impl_config != "local":
            urls |= minio_floor_url()
        return urls

    def handlers(self) -> dict[str, Callable[[Endpoint, Request], Exchange]]:
        return {"model": self._send_model, "grpc": self._send_grpc}

    def after_ingest(self, index: int) -> None:
        if not self.crash_every or self._project is None:
            return
        if (index + 1) % self.crash_every:
            return
        # The round's last write was WRITTEN, so everything acknowledged
        # before it must survive a process crash.
        self._project.execute("kill", "-s", "SIGKILL", "pseudofs", timeout=60)
        self._project.execute("start", "pseudofs", timeout=120)
        self.wait_ready()
        self.crashes += 1

    # Transport -----------------------------------------------------------------

    def _send_model(self, endpoint: Endpoint, request: Request) -> Exchange:
        started = time.perf_counter()
        payload = json.loads(request.body or b"{}")
        kind = payload.get("type")
        if kind == "op":
            result: Any = self.model.execute(payload["op"])
        elif kind == "batch":
            self._before_batch = self.model.clone()
            for op in payload["ops"]:
                self.model.execute(op)
            result = {"ops": len(payload["ops"])}
        else:
            result = {"violations": []}
        return Exchange(200, _json(result), (time.perf_counter() - started) * 1000)

    def _send_grpc(self, endpoint: Endpoint, request: Request) -> Exchange:
        started = time.perf_counter()

        def elapsed() -> float:
            return (time.perf_counter() - started) * 1000

        if request.method == "GET":
            return Exchange(200 if self.client().health() else 503, b"", elapsed())
        payload = json.loads(request.body or b"{}")
        kind = payload.get("type")
        try:
            if kind == "op":
                result = self.client().execute(payload["op"])
                return Exchange(200, _json(result), elapsed())
            if kind == "batch":
                failures = self._run_batch(payload)
            else:
                failures = self._run_race(payload)
        except ClientTimeout as error:
            if kind == "op":
                return Exchange(None, b"", elapsed(), "timeout")
            failures = [f"timeout: {error}"]
        except ClientUnavailable as error:
            if kind == "op":
                return Exchange(None, b"", elapsed(), f"transport: {error}")
            failures = [f"unavailable: {error}"]
        # Batches never report status None: a retry would re-apply mutations.
        status = 500 if failures else 200
        return Exchange(status, _json({"failures": failures[:20]}), elapsed())

    def _run_batch(self, payload: dict[str, Any]) -> list[str]:
        ops, expected = payload["ops"], payload["expected"]
        groups = payload.get("groups") or [list(range(len(ops)))]
        rejected: set[int] = set()

        def run(group: list[int]) -> list[str]:
            failures = []
            for index in group:
                actual = self.client().execute(ops[index])
                if (
                    reason := difference(ops[index], expected[index], actual)
                ) is not None:
                    failures.append(f"op {index} {_brief(ops[index])}: {reason}")
                    if "error" in actual and "error" not in expected[index]:
                        rejected.add(index)
            return failures

        failures: list[str] = []
        for result in self.pool.map(run, groups):
            failures += result
        if rejected:
            self._resync(ops, rejected)
        return failures

    def _resync(self, ops: list[dict[str, Any]], rejected: set[int]) -> None:
        """Replays the batch on the model without the ops the implementation
        rejected, so one reported failure does not cascade into mismatches on
        every later query that touches the same paths."""
        if self._before_batch is None:
            return
        model = self._before_batch
        for index, op in enumerate(ops):
            if index not in rejected:
                model.execute(op)
        self.model.tenants = model.tenants
        self.resyncs += 1

    # Rounds --------------------------------------------------------------------

    def generate_round(self, index: int, rng: random.Random) -> Round:
        files, size = self._usage(self.tenant)
        if size > self.max_data_bytes or files > MAX_FILES:
            profile = "prune"
        else:
            profile = weighted(rng, PROFILES)
        planner = Planner(self.model.clone(), self.tenant, rng)
        getattr(self, f"_plan_{profile}")(planner, index)
        planner.add(
            {
                "op": "write",
                "path": f"{SYNC_DIR}/r{index:04d}",
                "content": Spec("text", index, 32).dump(),
                "durability": "written",
            }
        )
        batches = self._batches(index, planner)
        races = []
        if chance(rng, self.race_rate):
            family = rng.choice(RACE_FAMILIES)
            races.append(family)
            body = _json(
                {
                    "type": "race",
                    "family": family,
                    "round": index,
                    "seed": rng.getrandbits(32),
                }
            )
            request = Request("RPC", f"/race/{family}", body=body)
            batches.insert(
                len(batches) // 2, Batch(f"race-{family}", request, request, 1)
            )
        probe = Probe(
            "sync",
            self._op_request(
                {
                    "op": "exists",
                    "path": f"{SYNC_DIR}/r{index:04d}",
                    "tenant": self.tenant,
                }
            ),
            lambda exchange: bool(json.loads(exchange.body or b"{}").get("value")),
        )
        files, directories = planner.model.paths(self.tenant)
        return Round(
            index=index,
            profile=profile,
            batches=batches,
            probes=[probe],
            dataset={"profile": profile, "ops": planner.ops, "groups": planner.groups},
            stats={
                "ops": len(planner.ops),
                "files": len(files),
                "directories": len(directories),
                "races": races,
                "crash": bool(self.crash_every and (index + 1) % self.crash_every == 0),
            },
        )

    def _op_request(self, op: dict[str, Any]) -> Request:
        return Request("RPC", f"/{op['op']}", body=_json({"type": "op", "op": op}))

    def _batches(self, index: int, planner: Planner) -> list[Batch]:
        ops, expected = planner.ops, planner.expected
        batches = []
        if planner.groups:
            grouped = sorted({i for group in planner.groups for i in group})
            body = _json(
                {
                    "type": "batch",
                    "ops": [ops[i] for i in grouped],
                    "expected": [expected[i] for i in grouped],
                    "groups": [
                        [grouped.index(i) for i in group] for group in planner.groups
                    ],
                }
            )
            request = Request("RPC", "/batch/parallel", body=body)
            batches.append(Batch(f"r{index}-parallel", request, request, len(grouped)))
            rest = [i for i in range(len(ops)) if i not in set(grouped)]
        else:
            rest = list(range(len(ops)))
        for start in range(0, len(rest), OPS_PER_BATCH):
            chunk = rest[start : start + OPS_PER_BATCH]
            body = _json(
                {
                    "type": "batch",
                    "ops": [ops[i] for i in chunk],
                    "expected": [expected[i] for i in chunk],
                }
            )
            request = Request("RPC", "/batch", body=body)
            batches.append(
                Batch(f"r{index}-{len(batches)}", request, request, len(chunk))
            )
        return batches

    def _name(self, rng: random.Random, directory: bool = False) -> str:
        if chance(rng, 0.08):
            stem = rng.choice(ODD_NAMES)
        else:
            stem = f"{rng.choice(WORDS)}-{rng.randint(0, 999)}"
        return stem if directory else stem + weighted(rng, EXTENSIONS)

    def _spec(
        self, rng: random.Random, name: str, size_class: str | None = None
    ) -> Spec:
        """Content for `name`: usually matching its extension, sometimes not."""
        extension = next(
            (ext for ext in BINARY_EXTENSION_KIND if name.endswith(ext)), None
        )
        if extension and not chance(rng, 0.15):
            kind = BINARY_EXTENSION_KIND[extension]
        elif chance(rng, 0.08):
            kind = rng.choice(BINARY_KINDS)
        else:
            kind = weighted(
                rng,
                (
                    (8.0, "text"),
                    (1.0, "crlf"),
                    (1.5, "unicode"),
                    (0.7, "longlines"),
                    (0.3, "multibyte_cut"),
                    (0.3, "late_nul"),
                ),
            )
        return Spec(kind, rng.getrandbits(48), self._size(rng, size_class))

    def _size(self, rng: random.Random, size_class: str | None = None) -> int:
        threshold = self.inline_threshold
        size_class = size_class or weighted(
            rng,
            (
                (1.0, "empty"),
                (10.0, "small"),
                (2.0, "edge"),
                (4.0, "blob"),
                (0.4, "big"),
            ),
        )
        if size_class == "empty":
            return 0
        if size_class == "small":
            return rng.randint(1, threshold)
        if size_class == "edge":
            return threshold + rng.choice((-1, 0, 1))
        if size_class == "blob":
            return rng.randint(threshold + 1, max(threshold + 2, MIB))
        return rng.randint(MIB, max(MIB + 1, self.max_file_bytes))

    def _write_op(
        self, path: str, spec: Spec, rng: random.Random, durability: str | None = None
    ) -> dict[str, Any]:
        via = "stream" if spec.size > 4 * MIB or chance(rng, 0.15) else "unary"
        op: dict[str, Any] = {
            "op": "write",
            "path": path,
            "content": spec.dump(),
            "durability": durability or weighted(rng, DURABILITIES),
            "via": via,
        }
        if via == "unary" and spec.text and chance(rng, 0.3):
            op["text"] = True
        return op

    def _plan_tree(self, planner: Planner, index: int, root: str | None = None) -> None:
        rng = planner.rng
        root = root or f"/t{index:04d}"
        groups: list[list[int]] = []
        for branch in range(rng.randint(2, max(2, self.concurrency))):
            group: list[int] = []
            stack = [(f"{root}/b{branch}-{self._name(rng, True)}", 0)]
            budget = int(rng.randint(10, 60) * self.config.scale)
            while stack and budget > 0:
                path, depth = stack.pop()
                for _ in range(rng.randint(0, 8)):
                    name = self._name(rng)
                    file_path = join(path, name)
                    if planner.model.lookup(planner.tenant, file_path) is not None:
                        continue
                    group.append(len(planner.ops))
                    planner.add(
                        self._write_op(file_path, self._spec(rng, name), rng, "applied")
                    )
                    budget -= 1
                if depth < 5:
                    for _ in range(rng.randint(0, 3)):
                        stack.append((join(path, self._name(rng, True)), depth + 1))
                if chance(rng, 0.2):
                    group.append(len(planner.ops))
                    planner.add(
                        {
                            "op": "mkdir",
                            "path": join(path, "empty"),
                            "parents": True,
                            "exist_ok": True,
                        }
                    )
            groups.append(group)
        planner.groups = [group for group in groups if group]

    def _usage(self, tenant: str) -> tuple[int, int]:
        files = size = 0
        stack = [self.model.tenants.get(tenant, Dir())]
        while stack:
            for child in stack.pop().children.values():
                if isinstance(child, Dir):
                    stack.append(child)
                else:
                    files += 1
                    size += child.size
        return files, size

    def _plan_prune(self, planner: Planner, index: int) -> None:
        """Deletes the oldest half of the top-level trees, file by file, so long
        runs keep a bounded dataset while exercising deep deletes."""
        root = planner.model.tenants.get(planner.tenant, Dir())
        names = sorted(name for name in root.children if not name.startswith("."))
        for name in names[: max(1, len(names) // 2)]:
            self._remove_tree(planner, join("/", name))

    def _remove_tree(self, planner: Planner, path: str) -> None:
        node = planner.model.lookup(planner.tenant, path)
        if isinstance(node, Dir):
            for name in sorted(node.children):
                self._remove_tree(planner, join(path, name))
            planner.add({"op": "rmdir", "path": path, "durability": "applied"})
        elif node is not None:
            planner.add({"op": "unlink", "path": path, "durability": "applied"})

    def _plan_tenant(self, planner: Planner, index: int) -> None:
        other = Planner(planner.model, self.other_tenant, planner.rng)
        self._plan_tree(other, index)
        planner.ops += other.ops
        planner.expected += other.expected
        planner.groups = None

    def _plan_wide(self, planner: Planner, index: int) -> None:
        rng = planner.rng
        root = f"/w{index:04d}"
        count = int(rng.randint(200, 2000) * self.config.scale)
        groups: list[list[int]] = [[] for _ in range(self.concurrency)]
        for item in range(count):
            name = f"{rng.choice(WORDS)}-{item:05d}" + weighted(rng, EXTENSIONS)
            groups[item % len(groups)].append(len(planner.ops))
            planner.add(
                self._write_op(
                    join(root, name), self._spec(rng, name, "small"), rng, "applied"
                )
                | {"via": "unary", "text": False}
            )
        planner.groups = groups

    def _plan_deep(self, planner: Planner, index: int) -> None:
        rng = planner.rng
        path = f"/d{index:04d}"
        for depth in range(rng.randint(20, 60)):
            path = join(path, f"level{depth}")
            if chance(rng, 0.4):
                name = self._name(rng)
                planner.add(
                    self._write_op(
                        join(path, name), self._spec(rng, name, "small"), rng
                    )
                )
        planner.add({"op": "mkdir", "path": join(path, "leaf"), "parents": True})

    def _plan_large(self, planner: Planner, index: int) -> None:
        rng = planner.rng
        root = f"/l{index:04d}"
        groups = []
        for item in range(rng.randint(2, 6)):
            name = f"large-{item}" + rng.choice((".bin", ".png", ".log", ""))
            spec = self._spec(rng, name, "big")
            groups.append([len(planner.ops)])
            planner.add(
                self._write_op(join(root, name), spec, rng, weighted(rng, DURABILITIES))
            )
        unary = self.limits.max_unary_bytes
        for offset in (0, 1):
            planner.add(
                {
                    "op": "write",
                    "path": join(root, f"unary-limit+{offset}.bin"),
                    "content": Spec(
                        "binary", rng.getrandbits(32), unary + offset
                    ).dump(),
                    "durability": "written",
                    "via": "unary",
                }
            )
        planner.groups = groups

    def _plan_appendlog(self, planner: Planner, index: int) -> None:
        rng = planner.rng
        root = f"/a{index:04d}"
        groups = []
        for item in range(rng.randint(1, 4)):
            path = join(root, f"append-{item}.log")
            group = []
            for _ in range(rng.randint(5, 40)):
                size = self._size(
                    rng, weighted(rng, ((8.0, "small"), (1.0, "edge"), (1.0, "blob")))
                )
                group.append(len(planner.ops))
                planner.add(
                    {
                        "op": "append",
                        "path": path,
                        "content": Spec(
                            rng.choice(TEXT_KINDS[:3]), rng.getrandbits(48), size
                        ).dump(),
                        "durability": weighted(rng, DURABILITIES),
                        "text": chance(rng, 0.3),
                    }
                )
            groups.append(group)
        planner.groups = groups

    def _plan_churn(self, planner: Planner, index: int) -> None:
        rng = planner.rng
        model = planner.model
        for _ in range(int(rng.randint(20, 120) * self.config.scale)):
            files, directories = self._sample_paths(model, planner.tenant, rng)
            action = weighted(
                rng,
                (
                    (4.0, "overwrite"),
                    (2.0, "append"),
                    (2.0, "rename"),
                    (1.5, "unlink"),
                    (1.0, "mkdir"),
                    (1.0, "rmdir"),
                    (1.5, "invalid"),
                ),
            )
            if action == "overwrite" and files:
                path = rng.choice(files)
                planner.add(
                    self._write_op(path, self._spec(rng, path.rsplit("/", 1)[1]), rng)
                )
            elif action == "append" and files:
                path = rng.choice(files)
                # Unary appends past the gRPC message limits fail in the
                # transport, before the service's own unary limit applies.
                size_class = weighted(
                    rng, ((8.0, "small"), (1.0, "edge"), (2.0, "blob"))
                )
                spec = Spec(
                    rng.choice(TEXT_KINDS[:3]),
                    rng.getrandbits(48),
                    self._size(rng, size_class),
                )
                planner.add({"op": "append", "path": path, "content": spec.dump()})
            elif action == "rename" and (files or len(directories) > 1):
                source = rng.choice(files + directories[1:])
                parent = rng.choice(directories)
                target = join(parent, self._name(rng, source in directories))
                if chance(rng, 0.3) and files:
                    target = rng.choice(files)
                planner.add(
                    {
                        "op": "rename",
                        "source": source,
                        "target": target,
                        "durability": weighted(rng, DURABILITIES),
                    }
                )
            elif action == "unlink" and files:
                planner.add({"op": "unlink", "path": rng.choice(files)})
            elif action == "mkdir":
                parent = rng.choice(directories)
                planner.add(
                    {
                        "op": "mkdir",
                        "path": join(parent, self._name(rng, True)),
                        "parents": chance(rng, 0.5),
                        "exist_ok": chance(rng, 0.5),
                    }
                )
            elif action == "rmdir" and len(directories) > 1:
                planner.add({"op": "rmdir", "path": rng.choice(directories[1:])})
            else:
                planner.add(self._invalid_op(rng, files, directories))

    def _invalid_op(
        self, rng: random.Random, files: list[str], directories: list[str]
    ) -> dict[str, Any]:
        """A mutation the model expects to fail, exercising error paths."""
        file = rng.choice(files) if files else "/missing"
        directory = rng.choice(directories)
        choices = [
            {"op": "mkdir", "path": directory},
            {"op": "mkdir", "path": join(file, "child"), "parents": True},
            {"op": "rmdir", "path": file},
            {"op": "rmdir", "path": "/"},
            {"op": "unlink", "path": directory},
            {"op": "unlink", "path": "/nope/missing.txt"},
            {"op": "write", "path": directory, "content": Spec("text", 1, 10).dump()},
            {
                "op": "write",
                "path": join(file, "below.txt"),
                "content": Spec("text", 1, 10).dump(),
            },
            {"op": "rename", "source": directory, "target": join(directory, "inside")},
            {"op": "rename", "source": "/does/not/exist", "target": "/elsewhere"},
            {"op": "rename", "source": file, "target": file},
            {"op": "rename", "source": "/missing-self", "target": "/missing-self"},
            {"op": "append", "path": directory, "content": Spec("text", 1, 10).dump()},
            {"op": "open", "path": file, "mode": "x"},
            {"op": "open", "path": directory, "mode": "a"},
        ]
        return rng.choice(choices)

    def _sample_paths(
        self, model: Model, tenant: str, rng: random.Random, limit: int = 64
    ) -> tuple[list[str], list[str]]:
        """Random files and directories by random descent, cheap on big trees."""
        files: list[str] = []
        directories: list[str] = ["/"]
        root = model.tenants.get(tenant)
        if root is None:
            return files, directories
        for _ in range(limit):
            path, node = "/", root
            while isinstance(node, Dir) and node.children:
                name = rng.choice(list(node.children))
                path, node = join(path, name), node.children[name]
                if isinstance(node, Dir):
                    directories.append(path)
                    if chance(rng, 0.25):
                        break
                else:
                    files.append(path)
            if len(files) >= limit // 2 and len(directories) >= limit // 4:
                break
        return sorted(set(files)), sorted(set(directories))

    # Races ---------------------------------------------------------------------

    def _run_race(self, payload: dict[str, Any]) -> list[str]:
        family = payload["family"]
        root = f"/race-{payload['round']:04d}-{family}"
        rng = random.Random(payload["seed"])
        return getattr(self, f"_race_{family}")(root, rng)

    def _parallel(self, workers: list[Callable[[], list[str]]]) -> list[str]:
        violations: list[str] = []
        for result in self.pool.map(lambda worker: worker(), workers):
            violations += result
        return violations

    def _race_overwrite(self, root: str, rng: random.Random) -> list[str]:
        client, tenant, path = self.client(), self.race_tenant, f"{root}/file.bin"
        threshold = self.inline_threshold
        sizes = [64, threshold - 128, threshold + 1, 200_000, 2 * MIB]
        writers, iterations = 4, 6
        plans = [[rng.choice(sizes) for _ in range(iterations)] for _ in range(writers)]
        initial = race_payload(99, 0, 10)
        client.write_stream(tenant, path, initial, 2)
        allowed = {len(initial)} | {
            len(race_payload(w, i, plans[w][i]))
            for w in range(writers)
            for i in range(iterations)
        }
        done = threading.Event()

        def writer(number: int) -> Callable[[], list[str]]:
            def run() -> list[str]:
                for iteration, size in enumerate(plans[number]):
                    data = race_payload(number, iteration, size)
                    op = {
                        "op": "write",
                        "path": path,
                        "tenant": tenant,
                        "data": data,
                        "durability": rng.choice(("applied", "written")),
                        "via": "stream" if size > MIB else "unary",
                    }
                    if "error" in (result := client.execute(op)):
                        return [f"writer {number}: {result['error']}"]
                return []

            return run

        def reader(number: int) -> Callable[[], list[str]]:
            def run() -> list[str]:
                violations = []
                while not done.is_set() and len(violations) < 5:
                    stat = client.execute(
                        {"op": "stat", "path": path, "tenant": tenant}
                    )
                    if "error" in stat:
                        violations.append(f"reader {number} stat: {stat['error']}")
                    elif stat["size"] not in allowed:
                        violations.append(
                            f"reader {number}: stat size {stat['size']} never written"
                        )
                    try:
                        data = client.read_all(
                            {"op": "stream", "path": path, "tenant": tenant}
                        )
                    except Exception as error:
                        violations.append(f"reader {number} stream: {error}")
                        continue
                    if (reason := check_race_payload(data)) is not None:
                        violations.append(f"reader {number}: {reason}")
                return violations

            return run

        def writers_then_stop() -> list[str]:
            try:
                return self._parallel([writer(n) for n in range(writers)])
            finally:
                done.set()

        violations = self._parallel([writers_then_stop, *(reader(n) for n in range(4))])
        final = client.read_all({"op": "stream", "path": path, "tenant": tenant})
        if (reason := check_race_payload(final)) is not None:
            violations.append(f"final content: {reason}")
        elif int(final.split(b" ", 3)[2]) != iterations - 1:
            violations.append("final content is not some writer's last write")
        return violations

    def _race_append(self, root: str, rng: random.Random) -> list[str]:
        client, tenant, path = self.client(), self.race_tenant, f"{root}/log.txt"
        writers, records = 4, 25
        width = rng.choice((48, self.inline_threshold + 100))
        done = threading.Event()

        def record(number: int, sequence: int) -> bytes:
            head = f"{number:02d}:{sequence:05d}:".encode()
            return head + b"." * (width - len(head) - 1) + b"\n"

        def check(data: bytes) -> str | None:
            if len(data) % width:
                return f"torn append: size {len(data)} is not a multiple of {width}"
            next_sequence = [0] * writers
            for offset in range(0, len(data), width):
                line = data[offset : offset + width]
                try:
                    number, sequence = int(line[:2]), int(line[3:8])
                except ValueError:
                    return f"malformed record at {offset}: {line[:16]!r}"
                if line != record(number, sequence):
                    return f"corrupt record at {offset}"
                if sequence != next_sequence[number]:
                    due = next_sequence[number]
                    return f"writer {number}: record {sequence} where {due} was due"
                next_sequence[number] += 1
            return None

        def writer(number: int) -> Callable[[], list[str]]:
            def run() -> list[str]:
                for sequence in range(records):
                    op = {
                        "op": "append",
                        "path": path,
                        "tenant": tenant,
                        "data": record(number, sequence),
                        "durability": "applied",
                    }
                    if "error" in (result := client.execute(op)):
                        return [f"appender {number}: {result['error']}"]
                return []

            return run

        def reader() -> list[str]:
            violations = []
            while not done.is_set() and len(violations) < 5:
                try:
                    data = client.read_all(
                        {"op": "stream", "path": path, "tenant": tenant}
                    )
                except Exception as error:
                    if "NOT_FOUND" not in str(error):
                        violations.append(f"reader: {error}")
                    continue
                if (reason := check(data)) is not None:
                    violations.append(f"reader: {reason}")
            return violations

        def writers_then_stop() -> list[str]:
            try:
                return self._parallel([writer(n) for n in range(writers)])
            finally:
                done.set()

        violations = self._parallel([writers_then_stop, reader, reader])
        final = client.read_all({"op": "stream", "path": path, "tenant": tenant})
        if len(final) != (total := writers * records * width):
            violations.append(f"lost appends: {len(final)} bytes, expected {total}")
        elif (reason := check(final)) is not None:
            violations.append(f"final content: {reason}")
        return violations

    def _churn_race(
        self,
        churn: Callable[[], None],
        probes: list[dict[str, Any]],
        content: dict[str, bytes],
    ) -> list[str]:
        """Runs `churn` while readers issue `probes`, which may only succeed
        (with known content) or fail with NOT_FOUND."""
        client = self.client()
        done = threading.Event()
        allowed = {"NOT_FOUND"}

        def reader(seed: int) -> list[str]:
            local = random.Random(seed)
            violations = []
            while not done.is_set() and len(violations) < 5:
                op = local.choice(probes)
                if op["op"] == "read_all":
                    try:
                        data = client.read_all(op)
                    except Exception as error:
                        if "NOT_FOUND" not in str(error) and "not found" not in str(
                            error
                        ):
                            violations.append(f"stream {op['path']}: {error}")
                        continue
                    if data != content.get(op["path"].rsplit("/", 1)[1]):
                        violations.append(f"stream {op['path']}: unexpected content")
                    continue
                result = client.execute(op)
                error = result.get("error")
                if error is not None and error["code"] not in allowed:
                    violations.append(
                        f"{op['op']} {op.get('path', op.get('pattern'))}: {error}"
                    )
            return violations

        def run_churn() -> list[str]:
            try:
                churn()
                return []
            except Exception as error:
                return [f"churn: {error}"]
            finally:
                done.set()

        return self._parallel([run_churn, *(lambda s=s: reader(s) for s in range(4))])

    def _race_unlink(self, root: str, rng: random.Random) -> list[str]:
        client, tenant = self.client(), self.race_tenant
        data = {"f.txt": b"churned content\n" * 3}
        path = f"{root}/f.txt"
        client.execute({"op": "mkdir", "path": root, "parents": True, "tenant": tenant})

        def churn() -> None:
            for _ in range(int(150 * self.config.scale)):
                for op in (
                    {
                        "op": "write",
                        "path": path,
                        "data": data["f.txt"],
                        "durability": "applied",
                    },
                    {"op": "unlink", "path": path, "durability": "applied"},
                ):
                    result = client.execute({**op, "tenant": tenant})
                    if "error" in result:
                        raise RuntimeError(f"{op['op']}: {result['error']}")

        probes = [
            {"op": name, "path": path, "tenant": tenant}
            for name in ("stat", "exists", "is_file", "read_bytes", "read_text")
        ] + [
            {"op": "read_all", "path": path, "tenant": tenant},
            {"op": "open", "path": path, "mode": "r", "tenant": tenant},
            {"op": "iterdir", "path": root, "tenant": tenant},
            {"op": "glob", "pattern": f"{root}/*", "tenant": tenant},
            {"op": "grep", "path": root, "pattern": "churned", "tenant": tenant},
        ]
        return self._churn_race(churn, probes, data)

    def _race_rename(self, root: str, rng: random.Random) -> list[str]:
        client, tenant = self.client(), self.race_tenant
        content = {f"f{i}.txt": f"file {i}\n".encode() * (i + 1) for i in range(3)}
        content["y.txt"] = b"replacement\n"
        for name, data in content.items():
            folder = "y" if name == "y.txt" else "d1"
            client.execute(
                {
                    "op": "write",
                    "path": f"{root}/{folder}/{name}",
                    "data": data,
                    "tenant": tenant,
                }
            )

        def churn() -> None:
            for step in range(int(100 * self.config.scale)):
                source, target = (
                    (f"{root}/d1", f"{root}/d2")
                    if step % 2 == 0
                    else (f"{root}/d2", f"{root}/d1")
                )
                ops = [
                    {
                        "op": "rename",
                        "source": source,
                        "target": target,
                        "durability": "applied",
                    },
                    {
                        "op": "write",
                        "path": f"{root}/x/y.txt",
                        "data": content["y.txt"],
                        "durability": "applied",
                    },
                    # Replaces the existing target, deleting its inode.
                    {
                        "op": "rename",
                        "source": f"{root}/x/y.txt",
                        "target": f"{root}/y/y.txt",
                        "durability": "applied",
                    },
                ]
                for op in ops:
                    result = client.execute({**op, "tenant": tenant})
                    if "error" in result:
                        raise RuntimeError(f"{op['op']}: {result['error']}")

        probes = []
        for folder in ("d1", "d2"):
            for name in ("f0.txt", "f2.txt"):
                file = f"{root}/{folder}/{name}"
                probes += [
                    {"op": "stat", "path": file, "tenant": tenant},
                    {"op": "read_all", "path": file, "tenant": tenant},
                    {"op": "exists", "path": file, "tenant": tenant},
                ]
            probes.append(
                {"op": "iterdir", "path": f"{root}/{folder}", "tenant": tenant}
            )
        probes += [
            {"op": "stat", "path": f"{root}/y/y.txt", "tenant": tenant},
            {"op": "read_bytes", "path": f"{root}/y/y.txt", "tenant": tenant},
            {"op": "read_all", "path": f"{root}/y/y.txt", "tenant": tenant},
            {"op": "glob", "pattern": f"{root}/**", "tenant": tenant},
            {
                "op": "grep",
                "path": root,
                "pattern": "file|replacement",
                "tenant": tenant,
            },
        ]
        return self._churn_race(churn, probes, content)

    # Queries -------------------------------------------------------------------

    def next_case(self, rng: random.Random) -> Case:
        family = weighted(
            rng,
            (
                (8.0, "stat"),
                (4.0, "exists"),
                (6.0, "read.bytes"),
                (2.0, "read.text"),
                (8.0, "read.range"),
                (3.0, "read.stream"),
                (5.0, "iterdir"),
                (10.0, "glob"),
                (10.0, "grep"),
                (2.0, "resolve"),
                (2.0, "edge"),
                (8.0, "write"),
                (3.0, "append"),
                (1.0, "mkdir"),
                (2.0, "rename"),
                (1.5, "unlink"),
                (1.0, "rmdir"),
                (1.0, "open"),
            ),
        )
        tenant = self.other_tenant if chance(rng, 0.05) else self.tenant
        files, directories = self._sample_paths(self.model, tenant, rng, 24)
        op, family = getattr(self, f"_case_{family.replace('.', '_')}")(
            rng, tenant, files, directories
        )
        op.setdefault("tenant", tenant)
        node = (
            self.model.lookup(op["tenant"], op.get("path", "/"))
            if "path" in op
            else None
        )
        if isinstance(node, File):
            op["size_hint"] = node.size
        mutating = op["op"] in (
            "write",
            "append",
            "mkdir",
            "rename",
            "unlink",
            "rmdir",
            "open",
        )
        return Case(
            family=family,
            query=_brief({key: value for key, value in op.items() if key != "tenant"}),
            request=self._op_request(op),
            compare=self._comparator(op),
            idempotent=not mutating,
        )

    def _comparator(self, op: dict[str, Any]) -> Callable[[Exchange, Exchange], None]:
        def compare(oracle: Exchange, impl: Exchange) -> None:
            if (
                reason := difference(op, json.loads(oracle.body), json.loads(impl.body))
            ) is not None:
                raise Mismatch(reason)

        return compare

    def _pick_file(self, rng: random.Random, files: list[str]) -> str:
        if files and not chance(rng, 0.05):
            return rng.choice(files)
        return rng.choice(("/missing.txt", "/t0000/nope/file.txt", "/"))

    def _case_stat(self, rng, tenant, files, directories):
        path = (
            self._pick_file(rng, files) if chance(rng, 0.7) else rng.choice(directories)
        )
        return {"op": "stat", "path": path}, "stat"

    def _case_exists(self, rng, tenant, files, directories):
        name = rng.choice(("exists", "is_file", "is_dir", "is_symlink"))
        path = rng.choice(files + directories) if files else "/"
        variant = rng.random()
        if variant < 0.2 and files:
            path = join(rng.choice(files), "below")
        elif variant < 0.3:
            path += "-missing"
        return {"op": name, "path": path}, "exists"

    def _case_read_bytes(self, rng, tenant, files, directories):
        path = self._pick_file(rng, files)
        return {
            "op": rng.choice(("read_bytes", "read_bytes", "read_text")),
            "path": path,
        }, "read.bytes"

    def _case_read_text(self, rng, tenant, files, directories):
        return {"op": "read_text", "path": self._pick_file(rng, files)}, "read.text"

    def _case_read_range(self, rng, tenant, files, directories):
        path = self._pick_file(rng, files)
        node = self.model.lookup(tenant, path)
        size = node.size if isinstance(node, File) else 0
        offset = rng.choice(
            (0, rng.randint(0, max(0, size)), size, size + 10, max(0, size - 1))
        )
        length = rng.choice(
            (None, 0, 1, rng.randint(1, max(1, size)), self.inline_threshold, 3 * MIB)
        )
        return {
            "op": "stream",
            "path": path,
            "offset": offset,
            "length": length,
        }, "read.range"

    def _case_read_stream(self, rng, tenant, files, directories):
        large = [
            p
            for p in files
            if (n := self.model.lookup(tenant, p)) is not None and n.size > MIB
        ]
        return {
            "op": "stream",
            "path": rng.choice(large or files or ["/missing"]),
        }, "read.stream"

    def _case_iterdir(self, rng, tenant, files, directories):
        path = (
            rng.choice(directories)
            if not chance(rng, 0.1)
            else self._pick_file(rng, files)
        )
        return {"op": "iterdir", "path": path}, "iterdir"

    def _glob_pattern(
        self, rng: random.Random, files: list[str], directories: list[str]
    ) -> str:
        source = (
            rng.choice(files + directories[1:])
            if len(files) + len(directories) > 1
            else "/x"
        )
        parts = [part for part in source.split("/") if part]
        out = []

        def escape(value: str) -> str:
            return "".join(f"\\{c}" if c in "*?[]{}" else c for c in value)

        for index, part in enumerate(parts):
            roll = rng.random()
            escaped = escape(part)
            if roll < 0.15:
                out.append("*")
            elif roll < 0.25 and len(part) > 1:
                out.append(escape(part[:-1]) + "?")
            elif roll < 0.3 and part[0].isascii() and part[0].isalnum():
                out.append(
                    f"[{part[0]}{rng.choice('0123456789abcxyz')}]" + escape(part[1:])
                )
            elif roll < 0.35 and not any(c in part for c in "{},"):
                out.append("{" + ",".join((part, rng.choice(WORDS))) + "}")
            elif roll < 0.42:
                out.append("**")
                out.append(escaped)
            elif roll < 0.45 and part.rpartition(".")[2] != part:
                out.append("*." + part.rpartition(".")[2])
            else:
                out.append(escaped)
            if index == 0 and chance(rng, 0.1):
                out.insert(0, "**")
        pattern = "/" + "/".join(out)
        roll = rng.random()
        if roll < 0.05:
            pattern = "/**"
        elif roll < 0.1:
            pattern = f"/**/*{weighted(rng, EXTENSIONS) or '.txt'}"
        elif roll < 0.12:
            pattern = rng.choice(("/a/{b", "/x/[z-a]", "/../x", "/**/sub{,"))
        return pattern

    def _case_glob(self, rng, tenant, files, directories):
        op = {"op": "glob", "pattern": self._glob_pattern(rng, files, directories)}
        if chance(rng, 0.3):
            op["max_results"] = rng.choice((1, 2, 5, 50, 20000))
        return op, "glob"

    def _grep_pattern(
        self, rng: random.Random, files: list[str], tenant: str
    ) -> tuple[str, bool]:
        """A pattern both Rust's `regex` and Python's `re` read the same way."""
        roll = rng.random()
        if roll < 0.3 and files:
            node = self.model.lookup(tenant, rng.choice(files))
            if (
                isinstance(node, File)
                and node.parts
                and node.parts[0].text
                and node.size
            ):
                lines = node.prefix(4096).decode("utf-8", "replace").splitlines()
                words = [w for line in lines[:50] for w in line.split() if len(w) > 2]
                if words:
                    return rng.choice(words), True
        choices = (
            (rng.choice(WORDS), False),
            (rng.choice(WORDS), True),
            (rf"needle-{rng.randint(0, 50)}\b", False),
            (r"ERROR code=[0-9]{3}", False),
            (r"^\d+ ERROR", False),
            (r"done$", False),
            (f"(?:{rng.choice(WORDS)}|{rng.choice(WORDS)})", False),
            (r"user.\w+", False),
            ("ünïcødé", True),
            (rng.choice([s for s in ODD_STRINGS if s]), True),
            ("needle-block", True),
            ("x{4000}", False),
            ("(unclosed", False),
        )
        return rng.choice(choices)

    def _case_grep(self, rng, tenant, files, directories):
        pattern, fixed = self._grep_pattern(rng, files, tenant)
        roll = rng.random()
        if roll < 0.25:
            path = rng.choice(directories)
        elif roll < 0.45 and files:
            path = rng.choice(files)
        elif roll < 0.75:
            path = self._glob_pattern(rng, files, directories)
        else:
            path = "/"
        op = {
            "op": "grep",
            "path": path,
            "pattern": pattern,
            "fixed_string": fixed,
            "case_insensitive": chance(rng, 0.25),
            "before_context": rng.choice((0, 0, 1, 2, 5, 150)),
            "after_context": rng.choice((0, 0, 1, 3, 150)),
            "max_matches": rng.choice((0, 0, 1, 7, 50, 5000)),
        }
        if chance(rng, 0.1):
            op["max_file_size_bytes"] = rng.choice(
                (100, self.inline_threshold, 100 * MIB)
            )
        return op, "grep"

    def _case_resolve(self, rng, tenant, files, directories):
        base = rng.choice(files + directories) if files else "/a/b"
        variants = (
            base,
            base.lstrip("/"),
            base + "/.",
            base.replace("/", "//"),
            base + "/../x",
            "/" + "/".join([".."] * 2) + base,
            "",
            ".",
            base + "\x00",
        )
        return {
            "op": rng.choice(("resolve", "absolute")),
            "path": rng.choice(variants),
        }, "resolve"

    def _case_edge(self, rng, tenant, files, directories):
        path = rng.choice(files + directories) if files else "/x"
        choices = [
            {
                "op": "stat",
                "path": path,
                "tenant": rng.choice(("", "-bad", "a b", "x" * 129)),
            },
            {"op": "stat", "path": "/a/../../b"},
            {"op": "stat", "path": path + "\x00"},
            {"op": "unlink", "path": "/"},
            {"op": "rmdir", "path": "/"},
            {"op": "rename", "source": "/", "target": "/x"},
            {"op": "iterdir", "path": "/../.."},
            {"op": "glob", "pattern": "/x", "tenant": "bad tenant"},
            {"op": "grep", "path": "/", "pattern": "(", "fixed_string": False},
            {"op": "read_text", "path": "/"},
            {"op": "is_file", "path": path + "\x00"},
            {"op": "exists", "path": "/a/b", "tenant": "-"},
        ]
        return rng.choice(choices), "edge"

    def _case_write(self, rng, tenant, files, directories):
        cls = weighted(rng, ((5.0, "inline"), (3.0, "blob"), (1.0, "stream")))
        durability = weighted(rng, DURABILITIES[:3])
        if files and chance(rng, 0.4):
            path = rng.choice(files)
        else:
            path = join(rng.choice(directories), self._name(rng))
        size = {
            "inline": self._size(rng, "small"),
            "blob": self._size(rng, "blob"),
            "stream": rng.randint(
                MIB, max(MIB + 1, min(self.max_file_bytes, 16 * MIB))
            ),
        }[cls]
        spec = self._spec(rng, path.rsplit("/", 1)[1])
        spec = Spec(spec.kind, spec.seed, size)
        op = {
            "op": "write",
            "path": path,
            "content": spec.dump(),
            "durability": durability,
            "via": "stream" if cls == "stream" else "unary",
        }
        if cls != "stream" and spec.text and chance(rng, 0.3):
            op["text"] = True
        return op, f"write.{cls}.{durability}"

    def _case_append(self, rng, tenant, files, directories):
        path = (
            rng.choice(files)
            if files and chance(rng, 0.8)
            else join(rng.choice(directories), "new.log")
        )
        spec = Spec(
            rng.choice(TEXT_KINDS[:3]),
            rng.getrandbits(48),
            self._size(
                rng, weighted(rng, ((6.0, "small"), (1.0, "blob"), (0.5, "empty")))
            ),
        )
        return {
            "op": "append",
            "path": path,
            "content": spec.dump(),
            "durability": weighted(rng, DURABILITIES[:3]),
            "text": chance(rng, 0.3),
        }, "append"

    def _case_mkdir(self, rng, tenant, files, directories):
        parent = rng.choice(directories)
        path = join(parent, self._name(rng, True)) if chance(rng, 0.7) else parent
        if chance(rng, 0.2):
            path = join(path, "nested/deeper")
        return {
            "op": "mkdir",
            "path": path,
            "parents": chance(rng, 0.5),
            "exist_ok": chance(rng, 0.5),
        }, "mkdir"

    def _case_rename(self, rng, tenant, files, directories):
        candidates = files + directories[1:]
        source = rng.choice(candidates) if candidates else "/missing"
        target = join(rng.choice(directories), self._name(rng, source in directories))
        if chance(rng, 0.2) and candidates:
            target = rng.choice(candidates)
        return {
            "op": "rename",
            "source": source,
            "target": target,
            "durability": weighted(rng, DURABILITIES[:3]),
        }, "rename"

    def _case_unlink(self, rng, tenant, files, directories):
        return {
            "op": "unlink",
            "path": self._pick_file(rng, files),
            "durability": weighted(rng, DURABILITIES[:3]),
        }, "unlink"

    def _case_rmdir(self, rng, tenant, files, directories):
        empty = [
            d
            for d in directories
            if isinstance(n := self.model.lookup(tenant, d), Dir) and not n.children
        ]
        path = (
            rng.choice(empty) if empty and chance(rng, 0.7) else rng.choice(directories)
        )
        return {"op": "rmdir", "path": path}, "rmdir"

    def _case_open(self, rng, tenant, files, directories):
        path = (
            self._pick_file(rng, files)
            if chance(rng, 0.7)
            else join(rng.choice(directories), "opened.txt")
        )
        return {
            "op": "open",
            "path": path,
            "mode": rng.choice(("r", "rb", "w", "a", "ab", "rw")),
        }, "open"
