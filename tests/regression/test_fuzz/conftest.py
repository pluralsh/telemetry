from __future__ import annotations

import os
from collections.abc import Callable

import pytest
from harness.compose import docker_available
from harness.fuzz.recorder import Summary
from harness.fuzz.runner import FuzzProduct, run_fuzz


@pytest.fixture
def fuzz() -> Callable[[type[FuzzProduct]], Summary]:
    def run(product: type[FuzzProduct]) -> Summary:
        if os.environ.get("FUZZ_STACK", "compose") == "compose":
            available, reason = docker_available()
            if not available:
                pytest.skip(reason)
        summary = run_fuzz(product)
        assert not summary.failed, (
            f"{'; '.join(summary.reasons)}; report: {summary.output_dir}/summary.md"
        )
        return summary

    return run
