"""Docker Compose lifecycle with failure diagnostics and unconditional cleanup."""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .fuzz.network import join_project_network, leave_project_network
from .process import CommandFailed, run, wait_http


def build_images_default() -> bool:
    """CI prebuilds product images with a layer cache and sets
    `REGRESSION_BUILD=0` so Compose reuses them instead of rebuilding."""
    return os.getenv("REGRESSION_BUILD", "1") != "0"


def docker_available() -> tuple[bool, str]:
    if shutil.which("docker") is None:
        return False, "docker executable is unavailable"
    try:
        run(("docker", "info"), timeout=10)
        run(("docker", "compose", "version"), timeout=10)
    except (CommandFailed, OSError) as error:
        return False, f"Docker is unavailable: {error}"
    return True, ""


@dataclass
class ComposeProject:
    file: Path
    name: str
    readiness_urls: tuple[str, ...] = ()
    services: tuple[str, ...] = ()
    profiles: tuple[str, ...] = ()
    build: bool = field(default_factory=build_images_default)
    # Run after every `up`, including services started after `start`.
    after_up: list[Callable[[], None]] = field(default_factory=list)
    # The fuzz harness's `CpuLayout`, recorded in `run.json`.
    cpu_layout: Any = field(default=None, init=False)
    _started: bool = field(default=False, init=False)

    @property
    def command(self) -> tuple[str, ...]:
        command = [
            "docker",
            "compose",
            "--project-name",
            self.name,
            "--file",
            str(self.file),
        ]
        for profile in self.profiles:
            command.extend(("--profile", profile))
        return tuple(command)

    def execute(self, *args: str, timeout: float | None = None) -> str:
        return run((*self.command, *args), cwd=self.file.parent, timeout=timeout).stdout

    def up(self, *services: str) -> None:
        args = ["up", "--detach", "--remove-orphans"]
        if self.build:
            args.append("--build")
        self.execute(*args, *services, timeout=900)
        join_project_network(self.name)
        for hook in self.after_up:
            hook()

    def start(self) -> None:
        try:
            self.up(*self.services)
            self._started = True
            for url in self.readiness_urls:
                wait_http(url)
        except Exception:
            self.print_diagnostics()
            self.stop()
            raise

    def restart(self, service: str) -> None:
        self.execute("restart", service, timeout=180)
        for url in self.readiness_urls:
            wait_http(url)

    def print_diagnostics(self) -> None:
        for args in (("ps",), ("logs", "--no-color")):
            try:
                output = self.execute(*args, timeout=60)
            except Exception as error:
                print(f"could not collect {' '.join(args)}: {error}")
            else:
                print(output)

    def save_logs(self, path: Path) -> None:
        try:
            path.write_text(
                self.execute("logs", "--no-color", "--timestamps", timeout=120)
            )
        except Exception as error:
            print(f"could not save compose logs to {path}: {error}")

    def stop(self) -> None:
        if not self._started:
            return
        try:
            if logs := os.getenv("REGRESSION_COMPOSE_LOGS"):
                self.save_logs(Path(logs))
            # Compose cannot remove a network the runner is still attached to.
            leave_project_network(self.name)
            self.execute("down", "--volumes", "--remove-orphans", timeout=180)
        finally:
            self._started = False

    def __enter__(self) -> ComposeProject:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if exc is not None:
            self.print_diagnostics()
        self.stop()
