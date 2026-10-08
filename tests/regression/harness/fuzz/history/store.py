"""History directory layout, entry persistence, and deterministic index rebuilds.

```
<root>/README.md                       latest results and recent runs (generated)
<root>/index.json                      every entry's key metrics (generated)
<root>/history/<product>/README.md     full per-product table (generated)
<root>/history/<product>/<name>.json   the entry itself (source of truth)
<root>/history/<product>/<name>.md     the entry rendered (generated)
```
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .entry import SCHEMA_VERSION
from .render import index_json, render_entry, render_index, render_product

PRODUCTS = ("logs", "metrics", "traces")


def dumps(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False) + "\n"


def _write(path: Path, text: str) -> bool:
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return True


def load_entries(root: Path) -> list[dict[str, Any]]:
    entries = []
    for path in sorted((root / "history").glob("*/*.json")):
        entry = json.loads(path.read_text(encoding="utf-8"))
        if entry.get("schema_version") != SCHEMA_VERSION:
            raise ValueError(
                f"{path}: unsupported schema_version {entry.get('schema_version')!r}"
            )
        if path.stem != entry["name"] or path.parent.name != entry["product"]:
            raise ValueError(f"{path}: name or product does not match its location")
        entries.append(entry)
    return entries


def write_entry(root: Path, entry: dict[str, Any]) -> Path:
    """Store an entry without overwriting an existing one of the same name."""
    directory = root / "history" / entry["product"]
    directory.mkdir(parents=True, exist_ok=True)
    base, number = entry["name"], 1
    while (directory / f"{entry['name']}.json").exists():
        number += 1
        entry = {**entry, "name": f"{base}-{number}"}
    path = directory / f"{entry['name']}.json"
    path.write_text(dumps(entry), encoding="utf-8")
    return path


def rebuild(root: Path) -> list[Path]:
    """Regenerate every derived file from the JSON entries; returns changed paths."""
    entries = load_entries(root)
    products = sorted(set(PRODUCTS) | {entry["product"] for entry in entries})
    changed = []

    def write(path: Path, text: str) -> None:
        if _write(path, text):
            changed.append(path)

    expected = set()
    for product in products:
        directory = root / "history" / product
        runs = [entry for entry in entries if entry["product"] == product]
        for entry in runs:
            path = directory / f"{entry['name']}.md"
            expected.add(path)
            write(path, render_entry(entry))
        write(directory / "README.md", render_product(product, runs))
        for stale in sorted(directory.glob("*.md")):
            if stale.name != "README.md" and stale not in expected:
                stale.unlink()
                changed.append(stale)
    write(root / "README.md", render_index(products, entries))
    write(root / "index.json", dumps(index_json(products, entries)))
    return changed
