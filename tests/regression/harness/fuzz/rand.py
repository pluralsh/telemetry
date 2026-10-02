"""Seeded randomness helpers and query-literal escaping shared by generators."""

from __future__ import annotations

import hashlib
import json
import random
from collections.abc import Sequence

WORDS = (
    "request",
    "response",
    "timeout",
    "connection",
    "refused",
    "retry",
    "cache",
    "miss",
    "hit",
    "user",
    "order",
    "payment",
    "declined",
    "accepted",
    "queue",
    "worker",
    "started",
    "finished",
    "panic",
    "deadline",
    "exceeded",
    "upstream",
    "downstream",
    "shard",
    "compaction",
    "flush",
    "error",
    "warning",
    "needle",
    "haystack",
)

# Values that stress escaping, Unicode handling, and regex metacharacters.
ODD_STRINGS = (
    "ünïcødé",
    "空白 スペース",
    'with "quotes"',
    "back\\slash",
    "emoji-🚀",
    "trailing ",
    " leading",
    "a=b c=d",
    "{brace}",
    "$dollar",
    ".dot.",
    "*star*",
    "pipe|pipe",
    "",
)

_REGEX_META = set("\\.+*?()|[]{}^$")


def derive(seed: int, *parts: object) -> random.Random:
    """Independent, reproducible stream for one purpose (`data`, round 3, ...)."""
    key = ":".join(str(part) for part in (seed, *parts)).encode()
    digest = hashlib.blake2b(key, digest_size=8).digest()
    return random.Random(int.from_bytes(digest, "big"))


def chance(rng: random.Random, probability: float) -> bool:
    return rng.random() < probability


def weighted[T](rng: random.Random, choices: Sequence[tuple[float, T]]) -> T:
    total = sum(weight for weight, _ in choices)
    point = rng.random() * total
    for weight, value in choices:
        point -= weight
        if point < 0:
            return value
    return choices[-1][1]


def zipf[T](rng: random.Random, items: Sequence[T], exponent: float = 1.1) -> T:
    """Skewed choice so a few values dominate, as in real label distributions."""
    return weighted(
        rng,
        [(1.0 / (index + 1) ** exponent, item) for index, item in enumerate(items)],
    )


def scaled(rng: random.Random, low: int, high: int, scale: float) -> int:
    upper = max(low, round(high * scale))
    return rng.randint(max(1, round(low * min(scale, 1.0))), upper)


def quote(value: str) -> str:
    """Double-quoted literal accepted by LogQL, PromQL, and TraceQL lexers."""
    return json.dumps(value, ensure_ascii=False)


def regex_escape(value: str) -> str:
    """Escape only RE2/Rust-regex metacharacters (Go rejects `\\ ` and friends)."""
    return "".join(f"\\{char}" if char in _REGEX_META else char for char in value)


def sample[T](rng: random.Random, items: Sequence[T], low: int, high: int) -> list[T]:
    count = min(len(items), rng.randint(low, high))
    return rng.sample(list(items), count)
