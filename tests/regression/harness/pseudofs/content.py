"""Deterministic file contents described by small, JSON-friendly specs.

Operations carry a `Spec` rather than bytes, so recorded datasets stay small
and both the reference model and the client can materialize identical bytes.
"""

from __future__ import annotations

import random
import threading
from collections import OrderedDict
from dataclasses import asdict, dataclass
from typing import Any

import blake3

from ..fuzz.rand import ODD_STRINGS, WORDS

# Leading bytes for formats the server must skip by signature.
SIGNATURES = {
    "png": b"\x89PNG\r\n\x1a\n",
    "jpeg": b"\xff\xd8\xff\xe0",
    "gif": b"GIF89a",
    "pdf": b"%PDF-1.7\n",
    "zip": b"PK\x03\x04",
    "gzip": b"\x1f\x8b\x08\x00",
    "elf": b"\x7fELF\x02\x01\x01",
    "sqlite": b"SQLite format 3\x00",
    "mp4": b"\x00\x00\x00\x20ftypisom",
    "wasm": b"\x00asm\x01\x00\x00\x00",
}
TEXT_KINDS = ("text", "crlf", "unicode", "longlines", "multibyte_cut", "late_nul")
BINARY_KINDS = ("binary", "zeros", *SIGNATURES)
KINDS = (*TEXT_KINDS, *BINARY_KINDS, "race")

# A line over this many bytes is truncated in grep output.
DISPLAY_BYTES = 4096
BLOCK_BYTES = 64 * 1024


@dataclass(frozen=True)
class Spec:
    kind: str
    seed: int
    size: int

    @classmethod
    def load(cls, value: dict[str, Any]) -> Spec:
        return cls(str(value["kind"]), int(value["seed"]), int(value["size"]))

    def dump(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def text(self) -> bool:
        return self.kind in TEXT_KINDS


def _words(rng: random.Random, count: int) -> str:
    return " ".join(rng.choice(WORDS) for _ in range(count))


def _line(rng: random.Random, kind: str, number: int) -> str:
    roll = rng.random()
    if kind == "unicode" and roll < 0.4:
        return f"{number} {rng.choice(ODD_STRINGS)} {_words(rng, 3)} Ünïcødé É é"
    if kind == "longlines" and roll < 0.05:
        return f"{number} long " + "x" * rng.randint(
            DISPLAY_BYTES - 64, DISPLAY_BYTES * 3
        )
    if roll < 0.05:
        return ""
    if roll < 0.15:
        return f"{number} ERROR code={rng.randint(100, 999)} {_words(rng, 4)}"
    if roll < 0.2:
        return f"{number} needle-{rng.randint(0, 50)} {_words(rng, 2)} done"
    return f"{number} {_words(rng, rng.randint(1, 12))}"


def _text_block(seed: int, kind: str, size: int) -> bytes:
    rng = random.Random(seed)
    newline = "\r\n" if kind == "crlf" else "\n"
    lines: list[str] = []
    total = 0
    number = 0
    if kind == "multibyte_cut":
        # A 3-byte character straddles the display limit of the first line.
        lines.append("a" * (DISPLAY_BYTES - 1) + "€ tail needle")
        total += len(lines[0].encode()) + 1
    while total < size:
        number += 1
        line = _line(rng, kind, number)
        lines.append(line)
        total += len(line.encode()) + len(newline)
    return (newline.join(lines) + newline).encode()


def _fit(data: bytes, size: int) -> bytes:
    """Cut UTF-8 text to `size` bytes without splitting a character."""
    if len(data) <= size:
        return data + b"z" * (size - len(data))
    cut = size
    while cut > 0 and data[cut] & 0xC0 == 0x80:
        cut -= 1
    return data[:cut] + b"z" * (size - cut)


def _text(spec: Spec) -> bytes:
    if spec.size <= BLOCK_BYTES * 2:
        return _fit(_text_block(spec.seed, spec.kind, spec.size), spec.size)
    # Large text repeats one block with a distinct header, so generation stays
    # cheap while every block still has unique line content.
    block = _fit(_text_block(spec.seed, spec.kind, BLOCK_BYTES), BLOCK_BYTES)
    parts = []
    total = 0
    index = 0
    while total < spec.size:
        header = f"## block {index} needle-block\n".encode()
        parts.append(header)
        parts.append(block)
        total += len(header) + len(block)
        index += 1
    return _fit(b"".join(parts), spec.size)


def _materialize(spec: Spec) -> bytes:
    if spec_size(spec) == 0:
        return b""
    if spec.kind == "late_nul":
        # Text whose first NUL is past the 8 KiB binary probe, so it is searched.
        body = _text(Spec("text", spec.seed, spec_size(spec)))
        position = 8192 + random.Random(spec.seed).randint(1, 600)
        return body[:position] + b"\x00" + body[position + 1 :]
    if spec.text:
        return _text(spec)
    rng = random.Random(spec.seed)
    if spec.kind == "zeros":
        return b"\x00" * spec.size
    if spec.kind == "binary":
        return rng.randbytes(spec.size)
    if spec.kind == "race":
        raise ValueError("race payloads are built by `race_payload`")
    signature = SIGNATURES[spec.kind]
    # NUL-free body, so only the signature marks these as binary.
    body = bytes(
        byte or 1 for byte in rng.randbytes(max(0, spec.size - len(signature)))
    )
    return (signature + body)[: spec.size]


class _Cache:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.size = 0
        self.items: OrderedDict[Spec, bytes] = OrderedDict()
        self.lock = threading.Lock()

    def get(self, spec: Spec) -> bytes:
        with self.lock:
            data = self.items.get(spec)
            if data is not None:
                self.items.move_to_end(spec)
                return data
        data = _materialize(spec)
        with self.lock:
            if spec not in self.items:
                self.items[spec] = data
                self.size += len(data)
            while self.size > self.capacity and len(self.items) > 1:
                _, evicted = self.items.popitem(last=False)
                self.size -= len(evicted)
        return data


_CACHE = _Cache(768 * 1024 * 1024)


def materialize(spec: Spec) -> bytes:
    return _CACHE.get(spec)


def spec_size(spec: Spec) -> int:
    """Actual byte length; `late_nul` content is at least 9000 bytes."""
    if spec.kind == "late_nul":
        return max(spec.size, 9000)
    return spec.size


def digest(data: bytes | memoryview) -> str:
    return blake3.blake3(data).hexdigest()


def race_payload(writer: int, iteration: int, size: int) -> bytes:
    """Self-describing content: a header naming the body's length and digest."""
    body = random.Random(writer * 1_000_003 + iteration).randbytes(max(0, size))
    header = f"RACE {writer} {iteration} {len(body)} {digest(body)}\n".encode()
    return header + body


def check_race_payload(data: bytes) -> str | None:
    """Returns why `data` is not one complete race payload, or None."""
    newline = data.find(b"\n")
    if newline < 0:
        return f"no header in {len(data)} bytes"
    parts = data[:newline].split(b" ")
    if len(parts) != 5 or parts[0] != b"RACE":
        return f"malformed header {data[:80]!r}"
    body = data[newline + 1 :]
    if int(parts[3]) != len(body):
        return f"torn read: header says {int(parts[3])} bytes, body has {len(body)}"
    if digest(body) != parts[4].decode():
        return "torn read: body digest does not match its header"
    return None
