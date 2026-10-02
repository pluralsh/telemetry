from __future__ import annotations

from pathlib import Path

import pytest
from harness.compose import docker_available

REGRESSION_ROOT = Path(__file__).resolve().parent


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--extended",
        action="store_true",
        help="run slower retention and restart regression scenarios",
    )
    parser.addoption(
        "--fuzz",
        action="store_true",
        help="run long differential fuzz sessions (see FUZZ_* variables)",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "docker: requires a working Docker daemon")
    config.addinivalue_line("markers", "extended: slower durability/lifecycle coverage")
    config.addinivalue_line("markers", "fuzz: long-running differential fuzz session")


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    available, reason = docker_available()
    docker_skip = pytest.mark.skip(reason=reason)
    extended_skip = pytest.mark.skip(
        reason="pass --extended to run lifecycle scenarios"
    )
    fuzz_skip = pytest.mark.skip(reason="pass --fuzz to run differential fuzzing")
    for item in items:
        if "docker" in item.keywords and not available:
            item.add_marker(docker_skip)
        if "extended" in item.keywords and not config.getoption("--extended"):
            item.add_marker(extended_skip)
        if "fuzz" in item.keywords and not config.getoption("--fuzz"):
            item.add_marker(fuzz_skip)
