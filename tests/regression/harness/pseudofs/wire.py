"""Minimal proto3 codec for `pseudofs.v1`, so the harness needs no generated stubs.

Messages are plain dicts keyed by field name. Only the field types the
PseudoFS API uses are supported.
"""

from __future__ import annotations

import struct
from typing import Any

# name -> (number, type, label); label is "", "repeated", or "optional".
Field = tuple[int, str, str]

SCHEMAS: dict[str, dict[str, Field]] = {
    "Empty": {},
    "PathRequest": {"path": (1, "string", ""), "tenant": (2, "string", "")},
    "PathResponse": {"path": (1, "string", "")},
    "BoolResponse": {"value": (1, "bool", "")},
    "TextResponse": {"value": (1, "string", "")},
    "BytesResponse": {"value": (1, "bytes", "")},
    "FileChunk": {"data": (1, "bytes", "")},
    "WriteTextRequest": {
        "path": (1, "string", ""),
        "value": (2, "string", ""),
        "durability": (3, "enum", ""),
        "tenant": (4, "string", ""),
    },
    "WriteBytesRequest": {
        "path": (1, "string", ""),
        "value": (2, "bytes", ""),
        "durability": (3, "enum", ""),
        "tenant": (4, "string", ""),
    },
    "MutationPathRequest": {
        "path": (1, "string", ""),
        "durability": (2, "enum", ""),
        "tenant": (3, "string", ""),
    },
    "WriteResponse": {"size": (1, "uint64", "")},
    "OpenRequest": {
        "path": (1, "string", ""),
        "mode": (2, "string", ""),
        "durability": (3, "enum", ""),
        "tenant": (4, "string", ""),
    },
    "FileHandle": {
        "path": (1, "string", ""),
        "mode": (2, "string", ""),
        "position": (3, "uint64", ""),
        "tenant": (4, "string", ""),
    },
    "MkdirRequest": {
        "path": (1, "string", ""),
        "parents": (2, "bool", ""),
        "exist_ok": (3, "bool", ""),
        "tenant": (4, "string", ""),
    },
    "RenameRequest": {
        "source": (1, "string", ""),
        "target": (2, "string", ""),
        "durability": (3, "enum", ""),
        "tenant": (4, "string", ""),
    },
    "DirectoryEntry": {
        "path": (1, "string", ""),
        "name": (2, "string", ""),
        "kind": (3, "enum", ""),
    },
    "StatResponse": {
        "kind": (1, "enum", ""),
        "mode": (2, "uint32", ""),
        "size": (3, "uint64", ""),
        "atime": (4, "double", ""),
        "mtime": (5, "double", ""),
        "ctime": (6, "double", ""),
    },
    "WriteFileStart": {
        "path": (1, "string", ""),
        "durability": (2, "enum", ""),
        "tenant": (3, "string", ""),
    },
    "WriteFileRequest": {
        "start": (1, "WriteFileStart", "optional"),
        "chunk": (2, "bytes", "optional"),
    },
    "StreamFileRequest": {
        "path": (1, "string", ""),
        "tenant": (2, "string", ""),
        "offset": (3, "uint64", ""),
        "length": (4, "uint64", "optional"),
    },
    "GlobRequest": {
        "tenant": (1, "string", ""),
        "pattern": (2, "string", ""),
        "max_results": (3, "uint32", ""),
    },
    "GrepRequest": {
        "tenant": (1, "string", ""),
        "path": (2, "string", ""),
        "pattern": (3, "string", ""),
        "fixed_string": (4, "bool", ""),
        "case_insensitive": (5, "bool", ""),
        "before_context": (6, "uint32", ""),
        "after_context": (7, "uint32", ""),
        "max_matches": (8, "uint32", ""),
        "max_file_size_bytes": (9, "uint64", ""),
    },
    "GrepLine": {"line_number": (1, "uint64", ""), "text": (2, "string", "")},
    "GrepMatch": {
        "path": (1, "string", ""),
        "line_number": (2, "uint64", ""),
        "line": (3, "string", ""),
        "before": (4, "GrepLine", "repeated"),
        "after": (5, "GrepLine", "repeated"),
    },
    "ErrorDetail": {"kind": (1, "string", ""), "path": (2, "string", "")},
    # grpc.health.v1
    "HealthCheckRequest": {"service": (1, "string", "")},
    "HealthCheckResponse": {"status": (1, "enum", "")},
}

DURABILITY = {"unspecified": 0, "applied": 1, "written": 2, "durable": 3}
FILE_KINDS = {0: "unspecified", 1: "file", 2: "directory"}

_VARINT = {"bool", "enum", "uint32", "uint64"}
_LENGTH = {"string", "bytes"}
_DEFAULTS: dict[str, Any] = {
    "string": "",
    "bytes": b"",
    "bool": False,
    "enum": 0,
    "uint32": 0,
    "uint64": 0,
    "double": 0.0,
}


def _varint(value: int) -> bytes:
    if value < 0:
        value += 1 << 64
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _read_varint(data: bytes | memoryview, position: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if position >= len(data):
            raise ValueError("truncated varint")
        byte = data[position]
        position += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, position
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")


def _key(number: int, wire_type: int) -> bytes:
    return _varint(number << 3 | wire_type)


def encode(message: str, value: dict[str, Any]) -> bytes:
    schema = SCHEMAS[message]
    out = bytearray()
    for name, item in value.items():
        if name not in schema:
            raise KeyError(f"{message} has no field {name!r}")
        number, kind, label = schema[name]
        if item is None:
            continue
        items = item if label == "repeated" else [item]
        for element in items:
            if label == "" and element == _DEFAULTS.get(kind):
                continue
            if kind in _VARINT:
                out += _key(number, 0) + _varint(int(element))
            elif kind == "double":
                out += _key(number, 1) + struct.pack("<d", element)
            elif kind in _LENGTH:
                raw = element.encode() if kind == "string" else bytes(element)
                out += _key(number, 2) + _varint(len(raw)) + raw
            else:
                raw = encode(kind, element)
                out += _key(number, 2) + _varint(len(raw)) + raw
    return bytes(out)


def decode(message: str, data: bytes) -> dict[str, Any]:
    schema = SCHEMAS[message]
    by_number = {
        number: (name, kind, label) for name, (number, kind, label) in schema.items()
    }
    value: dict[str, Any] = {}
    for name, (_, kind, label) in schema.items():
        if label == "repeated":
            value[name] = []
        elif label == "optional":
            value[name] = None
        else:
            value[name] = _DEFAULTS.get(kind)
    view = memoryview(data)
    position = 0
    while position < len(view):
        key, position = _read_varint(view, position)
        number, wire_type = key >> 3, key & 7
        if wire_type == 0:
            raw, position = _read_varint(view, position)
        elif wire_type == 1:
            raw = bytes(view[position : position + 8])
            position += 8
        elif wire_type == 2:
            length, position = _read_varint(view, position)
            raw = bytes(view[position : position + length])
            position += length
        elif wire_type == 5:
            raw = bytes(view[position : position + 4])
            position += 4
        else:
            raise ValueError(f"unsupported wire type {wire_type} in {message}")
        field = by_number.get(number)
        if field is None:
            continue
        name, kind, label = field
        if kind == "bool":
            parsed: Any = bool(raw)
        elif kind in ("enum", "uint32", "uint64"):
            parsed = raw
        elif kind == "double":
            parsed = struct.unpack("<d", raw)[0]
        elif kind == "string":
            parsed = raw.decode()
        elif kind == "bytes":
            parsed = raw
        else:
            parsed = decode(kind, raw)
        if label == "repeated":
            value[name].append(parsed)
        else:
            value[name] = parsed
    return value
