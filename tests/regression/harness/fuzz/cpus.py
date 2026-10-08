"""CPU pinning, so the implementation, the oracle, object storage and the
runner never compete for the same cores.

Cores are split by role: one for shared object storage, one for the runner
when it is in the compose network, and the rest in equal halves for `impl`
and `oracle` (an odd core is left idle rather than favouring a side). Pins are
applied with live container updates after every `up`, so services started
mid-run, such as readers, are covered too. `FUZZ_CPU_RANGE` (such as `6-11`)
confines the layout to those cores, so concurrent benchmark lanes never share
one.
"""

from __future__ import annotations

import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .network import in_network
from .resources import DockerApi, docker_socket

PIN_ENV = "FUZZ_PIN_CPUS"
RANGE_ENV = "FUZZ_CPU_RANGE"
ROLES = ("shared", "runner", "impl", "oracle")
MIN_CPUS = 4


def pinning_enabled() -> bool:
    return os.environ.get(PIN_ENV, "1").strip().lower() not in ("0", "false", "no")


def parse_range(value: str) -> tuple[int, int]:
    """`first-last` (inclusive) or a single core, as `(first, count)`."""
    first, _, last = value.strip().partition("-")
    start, end = int(first), int(last or first)
    if start < 0 or end < start:
        raise ValueError(f"{RANGE_ENV} must be <first>-<last>, got {value!r}")
    return start, end - start + 1


def lane_ranges(cpus: int, lanes: int) -> list[str]:
    """Equal, disjoint core ranges for `lanes` concurrent runs."""
    per = cpus // lanes
    if per < MIN_CPUS:
        raise ValueError(
            f"{lanes} lanes over {cpus} Docker CPUs leaves {per} per lane; "
            f"each needs at least {MIN_CPUS}"
        )
    return [_span(lane * per, per) for lane in range(lanes)]


def _span(start: int, count: int) -> str:
    return str(start) if count == 1 else f"{start}-{start + count - 1}"


@dataclass
class CpuLayout:
    """`cpusets` maps a role to its cpuset string; empty when not pinned."""

    cpus: int | None
    cpusets: dict[str, str] = field(default_factory=dict)
    cores: dict[str, int] = field(default_factory=dict)
    reason: str | None = None
    lane: str | None = None

    @classmethod
    def plan(
        cls, cpus: int | None, *, runner: bool, lane: str | None = None
    ) -> CpuLayout:
        if not pinning_enabled():
            return cls(cpus, reason=f"disabled by {PIN_ENV}")
        if cpus is None:
            return cls(cpus, reason="Docker CPU count unknown")
        first, available = 0, cpus
        if lane is not None:
            first, available = parse_range(lane)
            if first + available > cpus:
                raise ValueError(f"{RANGE_ENV}={lane} exceeds {cpus} Docker CPUs")
        reserved = 2 if runner else 1
        if available < reserved + 2 or available < MIN_CPUS:
            return cls(cpus, reason=f"only {available} CPUs to pin", lane=lane)
        side = (available - reserved) // 2
        counts = {"shared": 1, "impl": side, "oracle": side}
        if runner:
            counts["runner"] = 1
        layout = cls(cpus, lane=lane)
        start = first
        for role in ROLES:
            if role in counts:
                layout.cpusets[role] = _span(start, counts[role])
                layout.cores[role] = counts[role]
                start += counts[role]
        return layout

    def describe(self) -> dict[str, Any]:
        described: dict[str, Any] = {"docker_cpus": self.cpus}
        if self.lane is not None:
            described["lane"] = self.lane
        if self.reason:
            described["pinned"] = False
            described["reason"] = self.reason
        else:
            described["pinned"] = True
            described["cpusets"] = dict(self.cpusets)
        return described


class CpuPinner:
    """Applies a `CpuLayout` to every running container of a compose project."""

    def __init__(self, project: str, roles: Mapping[str, str]) -> None:
        self.project = project
        self.roles = dict(roles)
        path = docker_socket()
        self.docker = DockerApi(path) if path else None
        cpus = docker_cpus(self.docker) if self.docker else None
        self.layout = CpuLayout.plan(
            cpus, runner=in_network(), lane=os.environ.get(RANGE_ENV) or None
        )
        if self.docker is None and not self.layout.reason:
            self.layout.reason = "Docker socket unavailable"
            self.layout.cpusets.clear()
        self._pinned: dict[str, str] = {}

    def apply(self) -> None:
        if self.layout.reason or self.docker is None:
            return
        targets = {
            container: self.roles.get(service, "shared")
            for container, service in self.docker.containers(self.project).items()
        }
        if "runner" in self.layout.cpusets:
            targets[socket.gethostname()] = "runner"
        for container, role in targets.items():
            if self._pinned.get(container) == role:
                continue
            self.docker.update_cpus(
                container, self.layout.cpusets[role], self.layout.cores[role]
            )
            self._pinned[container] = role


def docker_cpus(docker: DockerApi) -> int | None:
    try:
        cpus = docker.info().get("NCPU")
    except Exception:
        return None
    return cpus if isinstance(cpus, int) and cpus > 0 else None
