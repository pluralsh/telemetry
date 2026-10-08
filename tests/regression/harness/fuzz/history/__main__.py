"""Maintain the fuzz benchmark history:

PYTHONPATH=tests/regression python -m harness.fuzz.history rebuild [DIR]
PYTHONPATH=tests/regression python -m harness.fuzz.history record OUTPUT_DIR [DIR]

`DIR` defaults to `FUZZ_HISTORY_DIR`, then `documentation/benchmarks/fuzz`.
`record` imports an existing run's output directory (for example a CI
artifact); git and host details are taken from where it is imported.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

from . import DEFAULT_DIR, history_dir, rebuild, record_output
from .store import load_entries


def _root(value: str | None) -> Path:
    if value:
        return Path(value).resolve()
    return history_dir() or DEFAULT_DIR


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="python -m harness.fuzz.history")
    commands = parser.add_subparsers(dest="command", required=True)
    rebuild_parser = commands.add_parser("rebuild", help="regenerate index files")
    rebuild_parser.add_argument("dir", nargs="?")
    rebuild_parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 instead of writing when generated files are out of date",
    )
    record_parser = commands.add_parser("record", help="import a run output dir")
    record_parser.add_argument("output_dir")
    record_parser.add_argument("dir", nargs="?")
    args = parser.parse_args(argv)
    root = _root(args.dir)
    if args.command == "record":
        path = record_output(Path(args.output_dir), root)
        print(f"recorded {path}")
        return 0
    if args.check:
        return _check(root)
    changed = rebuild(root)
    entries = len(load_entries(root))
    print(f"{root}: {entries} entries, {len(changed)} file(s) updated")
    for path in changed:
        print(f"  {path}")
    return 0


def _check(root: Path) -> int:
    with tempfile.TemporaryDirectory() as scratch:
        copy = Path(scratch) / "history"
        if root.exists():
            shutil.copytree(root, copy)
        changed = rebuild(copy)
    for path in changed:
        print(f"out of date: {root / path.relative_to(copy)}", file=sys.stderr)
    return 1 if changed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
