"""Where a run happened: git revision, host hardware, Docker, CI, and settings."""

from __future__ import annotations

import json
import os
import platform
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ..resources import DockerApi, docker_socket

# Variables that change what a run measures; credentials and endpoints are
# deliberately absent so entries are safe to commit.
SETTINGS_ENV = (
    "FUZZ_IN_NETWORK",
    "FUZZ_LOGS_CONFIG",
    "FUZZ_LOGS_DUPLICATE_RATE",
    "FUZZ_LOGS_STORAGE",
    "FUZZ_LOGS_UNALIGNED_RATE",
    "FUZZ_METRICS_CONFIG",
    "FUZZ_METRICS_ORACLE",
    "FUZZ_METRICS_STORAGE",
    "FUZZ_METRICS_STRICT_NAME",
    "FUZZ_QUERIES_PER_ROUND",
    "FUZZ_SCENARIO",
    "MINIO_FIRST_BYTE_MS",
    "FUZZ_TRACES_CONFIG",
    "FUZZ_TRACES_DUPLICATE_RATE",
    "FUZZ_TRACES_ORACLE",
    "MIMIR_QUERY_ENGINE",
    "REGRESSION_BUILD",
    "TEMPO_IMAGE",
)
HOST_LABEL_ENV = "FUZZ_HISTORY_HOST_LABEL"
_GIB = 1024**3


def _run(*command: str, cwd: Path | None = None) -> str | None:
    try:
        result = subprocess.run(
            command, cwd=cwd, capture_output=True, text=True, timeout=15, check=True
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip()


def collect_git(repo: Path) -> dict[str, Any]:
    commit = _run("git", "rev-parse", "HEAD", cwd=repo) or os.environ.get("GITHUB_SHA")
    branch = _run("git", "rev-parse", "--abbrev-ref", "HEAD", cwd=repo)
    if branch in (None, "HEAD"):
        branch = os.environ.get("GITHUB_HEAD_REF") or os.environ.get(
            "GITHUB_REF_NAME", branch
        )
    status = _run("git", "status", "--porcelain", "--untracked-files=no", cwd=repo)
    return {
        "commit": commit,
        "short": commit[:10] if commit else None,
        "branch": branch,
        "dirty": bool(status) if status is not None else None,
    }


def _cpu_model() -> str | None:
    if platform.system() == "Darwin":
        return _run("sysctl", "-n", "machdep.cpu.brand_string")
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.lower().startswith(("model name", "hardware")):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or None


def _memory_bytes() -> int | None:
    if platform.system() == "Darwin":
        value = _run("sysctl", "-n", "hw.memsize")
        return int(value) if value and value.isdigit() else None
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def _gib(value: Any) -> float | None:
    return round(int(value) / _GIB, 1) if isinstance(value, int) and value else None


def collect_docker() -> dict[str, Any] | None:
    raw = _run("docker", "info", "--format", "{{json .}}")
    if not raw:
        return None
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return {
        "version": info.get("ServerVersion"),
        "os": info.get("OperatingSystem"),
        "arch": info.get("Architecture"),
        "cpus": info.get("NCPU"),
        "memory_gib": _gib(info.get("MemTotal")),
    }


def collect_host() -> dict[str, Any]:
    """Hardware the numbers came from. On macOS the Docker VM limits matter
    more than the host's, so both are recorded."""
    return {
        "label": os.environ.get(HOST_LABEL_ENV) or None,
        "os": platform.system(),
        "os_release": platform.release(),
        "arch": platform.machine(),
        "cpu": _cpu_model(),
        "cores": os.cpu_count(),
        "memory_gib": _gib(_memory_bytes()),
        "python": platform.python_version(),
        "docker": collect_docker(),
    }


def collect_ci(environ: Mapping[str, str] | None = None) -> dict[str, Any] | None:
    environ = os.environ if environ is None else environ
    if not environ.get("GITHUB_ACTIONS"):
        return None
    return {
        "provider": "github-actions",
        "workflow": environ.get("GITHUB_WORKFLOW"),
        "run_id": environ.get("GITHUB_RUN_ID"),
        "runner": environ.get("RUNNER_NAME"),
    }


def collect_environment(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    environ = os.environ if environ is None else environ
    return {name: environ[name] for name in SETTINGS_ENV if environ.get(name)}


def collect_images(project: str) -> dict[str, Any]:
    """Image name and ID per running compose service, so oracle versions and
    local implementation builds can be told apart later."""
    path = docker_socket()
    if path is None:
        return {}
    try:
        return DockerApi(path).images(project)
    except Exception:
        return {}
