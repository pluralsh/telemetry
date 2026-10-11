"""In-memory reference model of PseudoFS semantics.

Every operation returns the same normalized result dict the gRPC client
produces, so the fuzzer compares them directly. The model encodes intended
behaviour: path normalization, implicit parent creation on writes and renames,
error kinds, directory-entry order (UTF-8 byte order), glob walk order, and
grep's line scanning, context, binary skipping, and limits.
"""

from __future__ import annotations

import re
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from .content import Spec, digest, materialize, spec_size

KIND_CODE = {
    "not_found": "NOT_FOUND",
    "already_exists": "ALREADY_EXISTS",
    "not_directory": "FAILED_PRECONDITION",
    "is_directory": "FAILED_PRECONDITION",
    "directory_not_empty": "FAILED_PRECONDITION",
    "invalid_path": "INVALID_ARGUMENT",
    "invalid_pattern": "INVALID_ARGUMENT",
    "invalid_mode": "INVALID_ARGUMENT",
    "resource_exhausted": "RESOURCE_EXHAUSTED",
    "invalid_utf8": "DATA_LOSS",
    "root_operation": "PERMISSION_DENIED",
    "conflict": "ABORTED",
}
# Error kinds whose detail path is a filesystem path rather than a message.
PATH_KINDS = frozenset(
    {
        "not_found",
        "already_exists",
        "not_directory",
        "is_directory",
        "directory_not_empty",
        "invalid_utf8",
        "root_operation",
    }
)
DIRECTORY_MODE = 0o040755
FILE_MODE = 0o100644
DIRECTORY_SIZE = 4096
BINARY_PROBE_BYTES = 8 * 1024
DISPLAY_BYTES = 4096
GLOB_META = frozenset("*?[{")

BINARY_EXTENSIONS = frozenset(
    [
        "png",
        "jpg",
        "jpeg",
        "gif",
        "webp",
        "bmp",
        "ico",
        "tif",
        "tiff",
        "heic",
        "heif",
        "avif",
        "psd",
        "raw",
        "cr2",
        "nef",
        "pdf",
        "zip",
        "gz",
        "tgz",
        "bz2",
        "xz",
        "zst",
        "lz4",
        "7z",
        "rar",
        "tar",
        "jar",
        "war",
        "whl",
        "apk",
        "dmg",
        "iso",
        "docx",
        "xlsx",
        "pptx",
        "odt",
        "ods",
        "odp",
        "exe",
        "dll",
        "so",
        "dylib",
        "o",
        "a",
        "lib",
        "obj",
        "class",
        "pyc",
        "pyo",
        "wasm",
        "mp3",
        "mp4",
        "m4a",
        "m4v",
        "mov",
        "avi",
        "mkv",
        "webm",
        "wav",
        "flac",
        "ogg",
        "opus",
        "aac",
        "wma",
        "wmv",
        "woff",
        "woff2",
        "ttf",
        "otf",
        "eot",
        "sqlite",
        "sqlite3",
        "parquet",
        "avro",
        "orc",
        "arrow",
        "feather",
        "npy",
        "npz",
        "pkl",
        "pickle",
        "h5",
        "hdf5",
        "onnx",
        "pt",
        "pth",
        "safetensors",
    ]
)
BINARY_SIGNATURES = (
    b"\x89PNG\r\n\x1a\n",
    b"\xff\xd8\xff",
    b"GIF87a",
    b"GIF89a",
    b"RIFF",
    b"II*\x00",
    b"MM\x00*",
    b"%PDF-",
    b"PK\x03\x04",
    b"PK\x05\x06",
    b"\x1f\x8b",
    b"\xfd7zXZ\x00",
    b"\x28\xb5\x2f\xfd",
    b"7z\xbc\xaf\x27\x1c",
    b"Rar!\x1a\x07",
    b"\x7fELF",
    b"\xcf\xfa\xed\xfe",
    b"\xce\xfa\xed\xfe",
    b"\xca\xfe\xba\xbe",
    b"\x00asm",
    b"OggS",
    b"fLaC",
    b"SQLite format 3\x00",
    b"PAR1",
    b"wOFF",
    b"wOF2",
)


class FsError(Exception):
    def __init__(self, kind: str, path: str = "", code: str | None = None) -> None:
        super().__init__(f"{kind}: {path}")
        self.kind = kind
        self.path = path
        self.code = code or KIND_CODE[kind]

    def describe(self) -> dict[str, str]:
        return {"code": self.code, "kind": self.kind, "path": self.path}


def unary_limit_error() -> FsError:
    return FsError("resource_exhausted", "")


@dataclass
class Limits:
    max_unary_bytes: int = 8 * 1024 * 1024
    max_file_size_bytes: int = 1024 * 1024 * 1024
    default_glob_results: int = 1000
    max_glob_results: int = 10000
    default_grep_matches: int = 100
    max_grep_matches: int = 1000
    max_grep_context_lines: int = 100
    default_grep_file_size_bytes: int = 16 * 1024 * 1024
    max_grep_file_size_bytes: int = 64 * 1024 * 1024
    max_scanned_entries: int = 1_000_000
    grep_concurrency: int = 8


@dataclass
class File:
    parts: tuple[Spec, ...] = ()
    size: int = 0

    def content(self) -> bytes:
        if len(self.parts) == 1:
            return materialize(self.parts[0])
        return b"".join(materialize(part) for part in self.parts)

    def prefix(self, length: int) -> bytes:
        out = bytearray()
        for part in self.parts:
            if len(out) >= length:
                break
            out += materialize(part)[: length - len(out)]
        return bytes(out)


@dataclass
class Dir:
    children: dict[str, File | Dir] = field(default_factory=dict)

    def clone(self) -> Dir:
        return Dir(
            {
                name: child.clone() if isinstance(child, Dir) else child
                for name, child in self.children.items()
            }
        )


def kind_name(node: File | Dir) -> str:
    return "directory" if isinstance(node, Dir) else "file"


def normalize(path: str) -> str:
    if "\x00" in path:
        raise FsError("invalid_path", "paths cannot contain NUL")
    absolute = path.startswith("/")
    parts: list[str] = []
    for component in path.split("/"):
        if component in ("", "."):
            continue
        if component == "..":
            if not parts:
                raise FsError(
                    "invalid_path", "path traversal cannot escape the filesystem root"
                )
            parts.pop()
        else:
            parts.append(component)
    if not absolute and path == "":
        return "/"
    return "/" + "/".join(parts)


def components(path: str) -> list[str]:
    return [part for part in path.split("/") if part]


def join(parent: str, name: str) -> str:
    return f"/{name}" if parent == "/" else f"{parent}/{name}"


def parent_and_name(path: str) -> tuple[str, str]:
    index = path.rfind("/")
    return (path[:index] or "/", path[index + 1 :])


def validate_tenant(tenant: str) -> None:
    raw = tenant.encode()
    message = "tenant must start and end with an ASCII letter or digit"
    if not 1 <= len(raw) <= 128:
        raise FsError("invalid_path", "tenant must contain between 1 and 128 bytes")
    if not (chr(raw[0]).isalnum() and raw[0] < 128) or not (
        chr(raw[-1]).isalnum() and raw[-1] < 128
    ):
        raise FsError("invalid_path", message)
    for byte in raw:
        if not (byte < 128 and (chr(byte).isalnum() or chr(byte) in "-_.")):
            raise FsError("invalid_path", message)


def limit(requested: int, default: int, maximum: int) -> int:
    return default if requested == 0 else min(requested, maximum)


# Glob ------------------------------------------------------------------------

LITERAL, PATTERN, RECURSIVE = "literal", "pattern", "recursive"


def has_glob_meta(value: str) -> bool:
    return any(char in GLOB_META for char in value)


def unescape(component: str) -> str:
    out = []
    chars = iter(component)
    for char in chars:
        if char == "\\":
            following = next(chars, None)
            if following is not None:
                out.append(following)
        else:
            out.append(char)
    return "".join(out)


def translate(glob: str, in_braces: bool = False) -> str:
    chars = list(glob)
    out: list[str] = []
    index = 0
    while index < len(chars):
        char = chars[index]
        if char == "*":
            out.append(".*")
        elif char == "?":
            out.append(".")
        elif char == "\\":
            index += 1
            if index < len(chars):
                out.append(re.escape(chars[index]))
        elif char == "[":
            rest = chars[index + 1 :]
            offset = rest.index("]") if "]" in rest else -1
            if offset > 0:
                body = "".join(rest[:offset])
                if body[0] in "!^":
                    body = "^" + body[1:].replace("\\", "\\\\")
                else:
                    body = body.replace("\\", "\\\\")
                out.append(f"[{body}]")
                index += offset + 1
            else:
                out.append("\\[")
        elif char == "{" and not in_braces:
            rest = chars[index + 1 :]
            if "}" not in rest:
                raise FsError("invalid_pattern", "")
            close = rest.index("}")
            inner = "".join(rest[:close])
            alternatives = [translate(item, True) for item in inner.split(",")]
            out.append("(?:" + "|".join(alternatives) + ")")
            index += close + 1
        else:
            out.append(re.escape(char))
        index += 1
    return "".join(out)


class GlobPattern:
    def __init__(self, segments: list[tuple[str, Any]], base: str, start: int) -> None:
        self.segments = segments
        self.base = base
        self.start = start

    @classmethod
    def compile(cls, pattern: str) -> GlobPattern:
        normalized = normalize(pattern)
        segments: list[tuple[str, Any]] = []
        for component in components(normalized):
            if component == "**":
                if segments and segments[-1][0] == RECURSIVE:
                    continue
                segments.append((RECURSIVE, None))
            elif has_glob_meta(component):
                try:
                    regex = re.compile(translate(component), re.S)
                except re.error as error:
                    raise FsError("invalid_pattern", "") from error
                segments.append((PATTERN, regex))
            else:
                segments.append((LITERAL, unescape(component)))
        start = next(
            (i for i, (kind, _) in enumerate(segments) if kind != LITERAL),
            len(segments),
        )
        base = "/" + "/".join(value for _, value in segments[:start])
        return cls(segments, base, start)

    @classmethod
    def recursive(cls, base: str) -> GlobPattern:
        return cls([(RECURSIVE, None)], base, 0)

    def closure(self, states: list[int]) -> list[int]:
        states = list(states)
        index = 0
        while index < len(states):
            state = states[index]
            if (
                state < len(self.segments)
                and self.segments[state][0] == RECURSIVE
                and state + 1 not in states
            ):
                states.append(state + 1)
            index += 1
        return sorted(set(states))

    def initial(self) -> list[int]:
        return self.closure([self.start])

    def step(self, states: list[int], name: str) -> list[int]:
        following = []
        for state in states:
            if state >= len(self.segments):
                continue
            kind, value = self.segments[state]
            if kind == RECURSIVE:
                following.append(state)
            elif (
                kind == LITERAL
                and value == name
                or kind == PATTERN
                and value.fullmatch(name)
            ):
                following.append(state + 1)
        return self.closure(following)

    def accepts(self, states: list[int]) -> bool:
        return len(self.segments) in states

    def can_descend(self, states: list[int]) -> bool:
        return any(state < len(self.segments) for state in states)

    def literal_names(self, states: list[int]) -> list[str] | None:
        names = set()
        for state in states:
            if state >= len(self.segments):
                continue
            kind, value = self.segments[state]
            if kind != LITERAL:
                return None
            names.add(value)
        return sorted(names)


# Grep ------------------------------------------------------------------------


def has_binary_extension(name: str) -> bool:
    stem, dot, extension = name.rpartition(".")
    if not dot or not stem:
        return False
    lowered = "".join(c.lower() if c.isascii() else c for c in extension)
    return lowered in BINARY_EXTENSIONS


def looks_binary(probe: bytes) -> bool:
    return (
        b"\x00" in probe
        or any(probe.startswith(signature) for signature in BINARY_SIGNATURES)
        or probe[4:8] == b"ftyp"
    )


def display(raw: bytes) -> str:
    return raw[:DISPLAY_BYTES].decode("utf-8", errors="replace")


def scan_lines(
    content: bytes,
    path: str,
    regex: re.Pattern[str],
    before_context: int,
    after_context: int,
    budget: list[int],
) -> list[dict[str, Any]]:
    """Mirrors the server's line scanner; `budget` is a one-element counter."""
    output: list[dict[str, Any]] = []
    before: deque[dict[str, Any]] = deque()
    pending: list[list[Any]] = []
    exhausted = False
    lines = content.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    for number, raw in enumerate(lines, start=1):
        if raw.endswith(b"\r"):
            raw = raw[:-1]
        line = {"line_number": number, "text": display(raw)}
        for item in pending:
            item[1] -= 1
            item[0]["after"].append(line)
        while pending and pending[0][1] == 0:
            output.append(pending.pop(0)[0])
        if not exhausted and regex.search(raw.decode("utf-8", "surrogateescape")):
            if budget[0] > 0:
                budget[0] -= 1
                found = {
                    "path": path,
                    "line_number": number,
                    "line": line["text"],
                    "before": list(before),
                    "after": [],
                }
                if after_context == 0:
                    output.append(found)
                else:
                    pending.append([found, after_context])
            else:
                exhausted = True
        if before_context > 0:
            if len(before) == before_context:
                before.popleft()
            before.append(line)
        if (exhausted or budget[0] == 0) and not pending:
            break
    output.extend(item[0] for item in pending)
    return output


# Model -----------------------------------------------------------------------


@dataclass
class Found:
    path: str
    name: str
    node: File | Dir

    def entry(self) -> list[str]:
        return [self.path, self.name, kind_name(self.node)]


class Model:
    def __init__(self, limits: Limits | None = None) -> None:
        self.limits = limits or Limits()
        self.tenants: dict[str, Dir] = {}

    def clone(self) -> Model:
        copy = Model(self.limits)
        copy.tenants = {tenant: root.clone() for tenant, root in self.tenants.items()}
        return copy

    # Helpers -----------------------------------------------------------------

    def root(self, tenant: str) -> Dir:
        validate_tenant(tenant)
        return self.tenants.setdefault(tenant, Dir())

    def _resolve(self, root: Dir, normalized: str) -> File | Dir:
        node: File | Dir = root
        for component in components(normalized):
            if not isinstance(node, Dir):
                raise FsError("not_directory", normalized)
            child = node.children.get(component)
            if child is None:
                raise FsError("not_found", normalized)
            node = child
        return node

    def lookup(self, tenant: str, path: str) -> File | Dir | None:
        try:
            return self._resolve(self.root(tenant), normalize(path))
        except FsError:
            return None

    def _ensure_directories(self, root: Dir, path: str, create: bool) -> Dir:
        """Walks `path`, creating missing directories when `create` is set."""
        node = root
        for component in components(path):
            child = node.children.get(component)
            if child is None:
                child = Dir()
                if create:
                    node.children[component] = child
            elif not isinstance(child, Dir):
                raise FsError("not_directory", path)
            node = child
        return node

    def walk(self, root: Dir, pattern: GlobPattern) -> Iterator[Found]:
        try:
            base = self._resolve(root, pattern.base)
        except FsError as error:
            if error.kind in ("not_found", "not_directory"):
                return
            raise
        if pattern.start == len(pattern.segments):
            if pattern.base != "/":
                yield Found(pattern.base, pattern.base.rsplit("/", 1)[1], base)
            return
        if not isinstance(base, Dir):
            return
        stack = [(base, pattern.base, pattern.initial())]
        scanned = 0
        maximum = self.limits.max_scanned_entries
        while stack:
            directory, path, states = stack.pop()
            subdirectories = []
            names = pattern.literal_names(states)
            if names is not None:
                children = []
                for name in names:
                    scanned += 1
                    if scanned > maximum:
                        raise FsError("resource_exhausted")
                    if (child := directory.children.get(name)) is not None:
                        children.append((name, child))
            else:
                children = None
            iterator = (
                children if children is not None else sorted(directory.children.items())
            )
            for name, child in iterator:
                if children is None:
                    scanned += 1
                    if scanned > maximum:
                        raise FsError("resource_exhausted")
                following = pattern.step(states, name)
                if not following:
                    continue
                child_path = join(path, name)
                if pattern.accepts(following):
                    yield Found(child_path, name, child)
                if isinstance(child, Dir) and pattern.can_descend(following):
                    subdirectories.append((child, child_path, following))
            stack.extend(reversed(subdirectories))

    # Operations ----------------------------------------------------------------

    def execute(self, op: dict[str, Any]) -> dict[str, Any]:
        handler = getattr(self, f"op_{op['op']}")
        try:
            return handler(op)
        except FsError as error:
            return {"error": error.describe()}

    def op_exists(self, op: dict[str, Any]) -> dict[str, Any]:
        root = self.root(op["tenant"])
        normalized = normalize(op["path"])
        try:
            self._resolve(root, normalized)
        except FsError as error:
            if error.kind in ("not_found", "not_directory"):
                return {"value": False}
            raise
        return {"value": True}

    def _is_kind(self, op: dict[str, Any], kind: type) -> dict[str, Any]:
        root = self.root(op["tenant"])
        normalized = normalize(op["path"])
        try:
            node = self._resolve(root, normalized)
        except FsError as error:
            if error.kind in ("not_found", "not_directory"):
                return {"value": False}
            raise
        return {"value": isinstance(node, kind)}

    def op_is_file(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._is_kind(op, File)

    def op_is_dir(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._is_kind(op, Dir)

    def op_is_symlink(self, op: dict[str, Any]) -> dict[str, Any]:
        validate_tenant(op["tenant"])
        normalize(op["path"])
        return {"value": False}

    def op_stat(self, op: dict[str, Any]) -> dict[str, Any]:
        root = self.root(op["tenant"])
        node = self._resolve(root, normalize(op["path"]))
        if isinstance(node, Dir):
            return {"kind": "directory", "mode": DIRECTORY_MODE, "size": DIRECTORY_SIZE}
        return {"kind": "file", "mode": FILE_MODE, "size": node.size}

    def _read(self, op: dict[str, Any], text: bool) -> dict[str, Any]:
        normalized = normalize(op["path"])
        node = self._resolve(self.root(op["tenant"]), normalized)
        if isinstance(node, Dir):
            raise FsError("is_directory", normalized)
        if node.size > self.limits.max_unary_bytes:
            raise unary_limit_error()
        data = node.content()
        if text:
            try:
                data.decode("utf-8")
            except UnicodeDecodeError:
                raise FsError("invalid_utf8", normalized) from None
        return {"size": len(data), "blake3": digest(data)}

    def op_read_bytes(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._read(op, text=False)

    def op_read_text(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._read(op, text=True)

    def op_stream(self, op: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize(op["path"])
        node = self._resolve(self.root(op["tenant"]), normalized)
        if isinstance(node, Dir):
            raise FsError("is_directory", normalized)
        offset = op.get("offset", 0)
        length = op.get("length")
        end = node.size if length is None else min(node.size, offset + length)
        if offset >= end:
            data = b""
        elif offset == 0 and end == node.size:
            data = node.content()
        else:
            data = node.content()[offset:end]
        return {"size": len(data), "blake3": digest(data)}

    def _upload(
        self, op: dict[str, Any], append: bool, spec: Spec | None
    ) -> dict[str, Any]:
        size = spec_size(spec) if spec is not None else 0
        if op.get("via", "unary") == "unary" and size > self.limits.max_unary_bytes:
            raise unary_limit_error()
        normalized = normalize(op["path"])
        root = self.root(op["tenant"])
        if normalized == "/":
            raise FsError("is_directory", "/")
        try:
            current = self._resolve(root, normalized)
        except FsError as error:
            if error.kind != "not_found":
                raise
            current = None
        if isinstance(current, Dir):
            raise FsError("is_directory", normalized)
        base = current.size if append and current is not None else 0
        if base + size > self.limits.max_file_size_bytes:
            raise FsError("resource_exhausted")
        if append and size == 0 and current is not None:
            return {"size": current.size}
        parent_path, name = parent_and_name(normalized)
        parent = self._ensure_directories(root, parent_path, create=True)
        parts = () if spec is None or size == 0 else (spec,)
        if append and current is not None:
            parent.children[name] = File(current.parts + parts, current.size + size)
        else:
            parent.children[name] = File(parts, size)
        return {"size": parent.children[name].size}

    def op_write(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._upload(op, False, Spec.load(op["content"]))

    def op_append(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._upload(op, True, Spec.load(op["content"]))

    def op_open(self, op: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize(op["path"])
        mode = op["mode"]
        if mode in ("r", "rb"):
            node = self._resolve(self.root(op["tenant"]), normalized)
            if isinstance(node, Dir):
                raise FsError("is_directory", normalized)
        elif mode in ("w", "wb"):
            self._upload({**op, "via": "unary"}, False, None)
        elif mode in ("a", "ab"):
            if not self.op_exists(op)["value"]:
                self._upload({**op, "via": "unary"}, False, None)
            elif self._is_kind(op, Dir)["value"]:
                raise FsError("is_directory", normalized)
        else:
            raise FsError("invalid_mode", mode)
        return {"path": normalized, "mode": mode, "position": 0, "tenant": op["tenant"]}

    def op_mkdir(self, op: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize(op["path"])
        root = self.root(op["tenant"])
        exist_ok = op.get("exist_ok", False)
        if normalized == "/":
            if exist_ok:
                return {}
            raise FsError("already_exists", "/")
        try:
            existing = self._resolve(root, normalized)
        except FsError:
            existing = None
        if existing is not None:
            if isinstance(existing, Dir) and exist_ok:
                return {}
            raise FsError("already_exists", normalized)
        parent_path, name = parent_and_name(normalized)
        if op.get("parents", False):
            parent = self._ensure_directories(root, parent_path, create=True)
        else:
            parent = self._resolve(root, parent_path)
            if not isinstance(parent, Dir):
                raise FsError("not_directory", parent_path)
        parent.children[name] = Dir()
        return {}

    def op_unlink(self, op: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize(op["path"])
        root = self.root(op["tenant"])
        if normalized == "/":
            raise FsError("root_operation", "/")
        node = self._resolve(root, normalized)
        if isinstance(node, Dir):
            raise FsError("is_directory", normalized)
        parent_path, name = parent_and_name(normalized)
        parent = self._resolve(root, parent_path)
        assert isinstance(parent, Dir)
        del parent.children[name]
        return {}

    def op_rmdir(self, op: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize(op["path"])
        root = self.root(op["tenant"])
        if normalized == "/":
            raise FsError("root_operation", "/")
        node = self._resolve(root, normalized)
        if not isinstance(node, Dir):
            raise FsError("not_directory", normalized)
        if node.children:
            raise FsError("directory_not_empty", normalized)
        parent_path, name = parent_and_name(normalized)
        parent = self._resolve(root, parent_path)
        assert isinstance(parent, Dir)
        del parent.children[name]
        return {}

    def op_iterdir(self, op: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize(op["path"])
        root = self.root(op["tenant"])
        node = self._resolve(root, normalized)
        if not isinstance(node, Dir):
            raise FsError("not_directory", normalized)
        return {
            "entries": [
                [join(normalized, name), name, kind_name(child)]
                for name, child in sorted(node.children.items())
            ]
        }

    def op_rename(self, op: dict[str, Any]) -> dict[str, Any]:
        source = normalize(op["source"])
        target = normalize(op["target"])
        root = self.root(op["tenant"])
        if source == "/" or target == "/":
            raise FsError("root_operation", "/")
        if source == target:
            self._resolve(root, source)
            return {"path": target}
        if target.startswith(source + "/"):
            raise FsError("invalid_path", "cannot move a directory inside itself")
        node = self._resolve(root, source)
        source_parent_path, source_name = parent_and_name(source)
        source_parent = self._resolve(root, source_parent_path)
        assert isinstance(source_parent, Dir)
        target_parent_path, target_name = parent_and_name(target)
        self._ensure_directories(root, target_parent_path, create=False)
        try:
            existing = self._resolve(root, target)
        except FsError:
            existing = None
        if existing is not None:
            if isinstance(existing, Dir) and existing.children:
                raise FsError("directory_not_empty", target)
            if type(existing) is not type(node):
                raise FsError(
                    "is_directory" if isinstance(existing, Dir) else "not_directory",
                    target,
                )
        del source_parent.children[source_name]
        target_parent = self._ensure_directories(root, target_parent_path, create=True)
        target_parent.children[target_name] = node
        return {"path": target}

    def op_resolve(self, op: dict[str, Any]) -> dict[str, Any]:
        validate_tenant(op["tenant"])
        return {"path": normalize(op["path"])}

    def op_absolute(self, op: dict[str, Any]) -> dict[str, Any]:
        return self.op_resolve(op)

    def op_glob(self, op: dict[str, Any]) -> dict[str, Any]:
        limits = self.limits
        maximum = limit(
            op.get("max_results", 0),
            limits.default_glob_results,
            limits.max_glob_results,
        )
        try:
            pattern = GlobPattern.compile(op["pattern"])
            root = self.root(op["tenant"])
        except FsError as error:
            return {"entries": [], "error": error.describe()}
        entries: list[list[str]] = []
        try:
            for found in self.walk(root, pattern):
                if len(entries) >= maximum:
                    break
                entries.append(found.entry())
        except FsError as error:
            return {"entries": entries, "error": error.describe()}
        return {"entries": entries}

    def op_grep(self, op: dict[str, Any]) -> dict[str, Any]:
        """Returns matches past `max_matches`, plus the effective limit; the
        comparator applies the limit because the server searches files
        concurrently and may fill the budget from later files.

        With `grep_concurrency` files in flight, a file starts only after every
        file `grep_concurrency` places earlier has finished, so once the files
        so far hold `limit` matches, no file beyond the next
        `grep_concurrency` can contribute; scanning stops there.
        """
        limits = self.limits
        context = limits.max_grep_context_lines
        before = min(op.get("before_context", 0), context)
        after = min(op.get("after_context", 0), context)
        maximum = limit(
            op.get("max_matches", 0),
            limits.default_grep_matches,
            limits.max_grep_matches,
        )
        max_size = limit(
            op.get("max_file_size_bytes", 0),
            limits.default_grep_file_size_bytes,
            limits.max_grep_file_size_bytes,
        )
        matches: list[dict[str, Any]] = []
        try:
            source = (
                re.escape(op["pattern"]) if op.get("fixed_string") else op["pattern"]
            )
            try:
                regex = re.compile(source, re.I if op.get("case_insensitive") else 0)
            except re.error:
                raise FsError("invalid_pattern") from None
            root = self.root(op["tenant"])
            path = op["path"]
            if has_glob_meta(path):
                found = self.walk(root, GlobPattern.compile(path))
            else:
                normalized = normalize(path)
                node = self._resolve(root, normalized)
                if isinstance(node, File):
                    found = iter(
                        [Found(normalized, normalized.rsplit("/", 1)[1], node)]
                    )
                else:
                    found = self.walk(root, GlobPattern.recursive(normalized))
            budget = [1 << 62]
            files_past_limit: int | None = None
            for item in found:
                if not isinstance(item.node, File):
                    continue
                if files_past_limit is not None:
                    if files_past_limit > limits.grep_concurrency:
                        break
                    files_past_limit += 1
                matches += self._grep_file(item, regex, before, after, max_size, budget)
                if files_past_limit is None and len(matches) >= maximum:
                    files_past_limit = 0
        except FsError as error:
            # The server stops once `limit` matches are sent, before a walk
            # error further on.
            if len(matches) >= maximum:
                return {"matches": matches, "limit": maximum}
            return {"matches": matches, "limit": maximum, "error": error.describe()}
        return {"matches": matches, "limit": maximum}

    def _grep_file(
        self,
        item: Found,
        regex: re.Pattern[str],
        before: int,
        after: int,
        max_size: int,
        budget: list[int],
    ) -> list[dict[str, Any]]:
        node = item.node
        assert isinstance(node, File)
        if has_binary_extension(item.name) or node.size == 0 or node.size > max_size:
            return []
        if looks_binary(node.prefix(BINARY_PROBE_BYTES)):
            return []
        return scan_lines(node.content(), item.path, regex, before, after, budget)

    # Introspection used by generators -------------------------------------------

    def paths(self, tenant: str) -> tuple[list[str], list[str]]:
        """All file and directory paths (directories include `/`)."""
        files: list[str] = []
        directories: list[str] = ["/"]
        stack = [("/", self.tenants.get(tenant, Dir()))]
        while stack:
            path, directory = stack.pop()
            for name, child in directory.children.items():
                child_path = join(path, name)
                if isinstance(child, Dir):
                    directories.append(child_path)
                    stack.append((child_path, child))
                else:
                    files.append(child_path)
        return files, directories
