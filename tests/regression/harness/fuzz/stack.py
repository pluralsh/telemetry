"""Compose-managed or externally managed fuzz stacks."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager

from ..compose import ComposeProject
from .config import HISTORICAL_ENVIRONMENT, FuzzConfig
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
    if config.historical:
        os.environ.update(HISTORICAL_ENVIRONMENT)
        compose.services += ("minio-latency",)
        compose.profiles += ("historical",)
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
