"""Small, testable process and readiness primitives for regression products."""

from __future__ import annotations

import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CommandResult:
    args: tuple[str, ...]
    stdout: str
    stderr: str


class CommandFailed(RuntimeError):
    """A subprocess failed and its diagnostics are safe to show in CI."""


def run(
    args: Sequence[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float | None = None,
) -> CommandResult:
    completed = subprocess.run(
        args,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    result = CommandResult(tuple(args), completed.stdout, completed.stderr)
    if completed.returncode:
        command = " ".join(args)
        raise CommandFailed(
            f"{command} exited with {completed.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def wait_http(url: str, *, timeout: float = 180, interval: float = 0.5) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=min(interval, 2)) as response:
                if 200 <= response.status < 300:
                    return
        except (OSError, urllib.error.URLError) as error:
            last_error = error
        time.sleep(interval)
    raise TimeoutError(f"{url} was not ready after {timeout}s: {last_error}")
