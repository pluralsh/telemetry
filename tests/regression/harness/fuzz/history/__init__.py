"""Durable benchmark history for fuzz runs, enabled with `FUZZ_HISTORY_DIR`."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from ..config import REPO_ROOT
from .entry import SCHEMA_VERSION, build_entry, entry_name
from .environment import (
    collect_ci,
    collect_environment,
    collect_git,
    collect_host,
    collect_images,
)
from .store import PRODUCTS, load_entries, rebuild, write_entry

HISTORY_ENV = "FUZZ_HISTORY_DIR"
BATCH_ENV = "FUZZ_HISTORY_BATCH"
DEFAULT_DIR = REPO_ROOT / "documentation" / "benchmarks" / "fuzz"

__all__ = [
    "BATCH_ENV",
    "DEFAULT_DIR",
    "HISTORY_ENV",
    "PRODUCTS",
    "SCHEMA_VERSION",
    "build_entry",
    "collect_images",
    "entry_name",
    "history_dir",
    "load_entries",
    "rebuild",
    "record_output",
    "write_entry",
]


def history_dir() -> Path | None:
    value = os.environ.get(HISTORY_ENV, "").strip()
    if not value:
        return None
    path = Path(value)
    return path if path.is_absolute() else REPO_ROOT / path


def record_output(
    output_dir: Path,
    root: Path,
    *,
    images: dict[str, Any] | None = None,
) -> Path:
    """Turn a finished run's `run.json` and `summary.json` into a history
    entry under `root`, then rebuild the generated index files."""
    run = json.loads((output_dir / "run.json").read_text(encoding="utf-8"))
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    entry = build_entry(
        run=run,
        summary=summary,
        git=collect_git(REPO_ROOT),
        host=collect_host(),
        environment=collect_environment(),
        images=images,
        ci=collect_ci(),
        batch=os.environ.get(BATCH_ENV) or None,
    )
    path = write_entry(root, entry)
    rebuild(root)
    return path
