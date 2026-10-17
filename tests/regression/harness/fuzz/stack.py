"""Compose-managed or externally managed fuzz stacks."""

from __future__ import annotations

import fcntl
import os
import tempfile
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

from ..compose import ComposeProject
from .config import FIRST_BYTE_MS, HISTORICAL_ENVIRONMENT, FuzzConfig
from .cpus import CpuPinner


@contextmanager
def fuzz_stack(
    config: FuzzConfig,
    project: Callable[[], ComposeProject],
    *,
    roles: Mapping[str, str],
    after_start: Callable[[ComposeProject], None] | None = None,
) -> Iterator[ComposeProject | None]:
    """External stacks (remote, S3-backed, or started by hand) are never
    started or torn down; the first round's visibility probe doubles as their
    readiness check."""
    if config.stack == "external":
        yield None
        return
    compose = project()
    with _exclusive(compose.name):
        yield from _managed(config, compose, roles, after_start)


@contextmanager
def _exclusive(name: str) -> Iterator[None]:
    """Two runs sharing a compose project restart and wipe each other's
    containers and volumes, which reads as data loss in the target."""
    path = Path(tempfile.gettempdir()) / f"fuzz-{name}.lock"
    with path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(
                f"another fuzz run is using compose project {name!r} ({path})"
            ) from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _managed(
    config: FuzzConfig,
    compose: ComposeProject,
    roles: Mapping[str, str],
    after_start: Callable[[ComposeProject], None] | None,
) -> Iterator[ComposeProject]:
    if config.object_store:
        os.environ.update(HISTORICAL_ENVIRONMENT)
        os.environ.setdefault(
            "MINIO_FIRST_BYTE_MS", f"{FIRST_BYTE_MS[config.scenario]:g}"
        )
        compose.services += ("minio-latency",)
        compose.profiles += (config.scenario,)
    else:
        for name in HISTORICAL_ENVIRONMENT:
            os.environ.pop(name, None)
    pinner = CpuPinner(compose.name, roles)
    compose.after_up.append(pinner.apply)
    compose.cpu_layout = pinner.layout
    if os.environ.get("FUZZ_KEEP_STACK", "") not in ("", "0"):
        # Left running so failing cases can be re-queried by hand; a later
        # run of the same product replaces it.
        compose.start()
        if after_start is not None:
            after_start(compose)
        yield compose
        print(f"fuzz: keeping compose project {compose.name!r} running")
        return
    with compose:
        if after_start is not None:
            after_start(compose)
        yield compose
