"""Compose-managed or externally managed fuzz stacks."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from ..compose import ComposeProject
from .config import FuzzConfig


@contextmanager
def fuzz_stack(
    config: FuzzConfig,
    project: Callable[[], ComposeProject],
    *,
    after_start: Callable[[ComposeProject], None] | None = None,
) -> Iterator[ComposeProject | None]:
    """External stacks (remote, S3-backed, or started by hand) are never
    started or torn down; the first round's visibility probe doubles as their
    readiness check."""
    if config.stack == "external":
        yield None
        return
    compose = project()
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
