"""PseudoFS gRPC client that returns results normalized like the reference model."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import blake3
import grpc

from .content import Spec, materialize
from .wire import DURABILITY, FILE_KINDS, decode, encode

SERVICE = "/pseudofs.v1.PseudoFS/"
HEALTH = "/grpc.health.v1.Health/Check"
WRITE_CHUNK_BYTES = 1024 * 1024
MESSAGE_BYTES = 64 * 1024 * 1024
# Throughput floor for timeouts on large transfers.
MIN_BYTES_PER_SECOND = 8 * 1024 * 1024


class ClientTimeout(Exception):
    pass


class ClientUnavailable(Exception):
    """The server could not be reached, as opposed to an UNAVAILABLE reply."""


def _error(error: grpc.RpcError) -> dict[str, Any]:
    code = error.code()
    kind = path = ""
    for key, value in error.trailing_metadata() or ():
        if key == "grpc-status-details-bin":
            detail = decode("ErrorDetail", value)
            kind, path = detail["kind"], detail["path"]
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        raise ClientTimeout(error.details())
    if code == grpc.StatusCode.UNAVAILABLE and not kind:
        raise ClientUnavailable(error.details())
    return {"code": code.name, "kind": kind, "path": path, "message": error.details()}


class Client:
    def __init__(self, target: str, timeout_s: float) -> None:
        self.target = target
        self.timeout_s = timeout_s
        self.channel = grpc.insecure_channel(
            target,
            options=[
                ("grpc.max_receive_message_length", MESSAGE_BYTES),
                ("grpc.max_send_message_length", MESSAGE_BYTES),
            ],
        )

    def close(self) -> None:
        self.channel.close()

    def _timeout(self, size: int = 0) -> float:
        return self.timeout_s + size / MIN_BYTES_PER_SECOND

    def _unary(
        self,
        method: str,
        request: str,
        response: str,
        value: dict[str, Any],
        size: int = 0,
    ) -> dict[str, Any]:
        call = self.channel.unary_unary(
            SERVICE + method,
            request_serializer=lambda item: encode(request, item),
            response_deserializer=lambda raw: decode(response, raw),
        )
        return call(value, timeout=self._timeout(size))

    def _stream(
        self,
        method: str,
        request: str,
        response: str,
        value: dict[str, Any],
        size: int = 0,
    ) -> Iterator[dict[str, Any]]:
        call = self.channel.unary_stream(
            SERVICE + method,
            request_serializer=lambda item: encode(request, item),
            response_deserializer=lambda raw: decode(response, raw),
        )
        return call(value, timeout=self._timeout(size))

    def health(self) -> bool:
        call = self.channel.unary_unary(
            HEALTH,
            request_serializer=lambda item: encode("HealthCheckRequest", item),
            response_deserializer=lambda raw: decode("HealthCheckResponse", raw),
        )
        try:
            return call({}, timeout=5)["status"] == 1
        except grpc.RpcError:
            return False

    def execute(self, op: dict[str, Any]) -> dict[str, Any]:
        handler: Callable[[dict[str, Any]], dict[str, Any]] = getattr(
            self, f"op_{op['op']}"
        )
        try:
            return handler(op)
        except grpc.RpcError as error:
            return {"error": _error(error)}

    # Operations ----------------------------------------------------------------

    def _bool(self, method: str, op: dict[str, Any]) -> dict[str, Any]:
        reply = self._unary(
            method,
            "PathRequest",
            "BoolResponse",
            {"path": op["path"], "tenant": op["tenant"]},
        )
        return {"value": reply["value"]}

    def op_exists(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._bool("Exists", op)

    def op_is_file(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._bool("IsFile", op)

    def op_is_dir(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._bool("IsDir", op)

    def op_is_symlink(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._bool("IsSymlink", op)

    def op_stat(self, op: dict[str, Any]) -> dict[str, Any]:
        reply = self._unary(
            "Stat",
            "PathRequest",
            "StatResponse",
            {"path": op["path"], "tenant": op["tenant"]},
        )
        return {
            "kind": FILE_KINDS[reply["kind"]],
            "mode": reply["mode"],
            "size": reply["size"],
        }

    def op_read_bytes(self, op: dict[str, Any]) -> dict[str, Any]:
        reply = self._unary(
            "ReadBytes",
            "PathRequest",
            "BytesResponse",
            {"path": op["path"], "tenant": op["tenant"]},
            op.get("size_hint", 0),
        )
        data = reply["value"]
        return {"size": len(data), "blake3": blake3.blake3(data).hexdigest()}

    def op_read_text(self, op: dict[str, Any]) -> dict[str, Any]:
        reply = self._unary(
            "ReadText",
            "PathRequest",
            "TextResponse",
            {"path": op["path"], "tenant": op["tenant"]},
            op.get("size_hint", 0),
        )
        data = reply["value"].encode()
        return {"size": len(data), "blake3": blake3.blake3(data).hexdigest()}

    def read_all(self, op: dict[str, Any]) -> bytes:
        """Whole-file stream read returning the bytes, for race checks."""
        chunks = self._stream(
            "StreamFile",
            "StreamFileRequest",
            "FileChunk",
            {"path": op["path"], "tenant": op["tenant"]},
            op.get("size_hint", 0),
        )
        return b"".join(chunk["data"] for chunk in chunks)

    def op_stream(self, op: dict[str, Any]) -> dict[str, Any]:
        request = {
            "path": op["path"],
            "tenant": op["tenant"],
            "offset": op.get("offset", 0),
            "length": op.get("length"),
        }
        hasher = blake3.blake3()
        size = 0
        for chunk in self._stream(
            "StreamFile",
            "StreamFileRequest",
            "FileChunk",
            request,
            op.get("size_hint", 0),
        ):
            hasher.update(chunk["data"])
            size += len(chunk["data"])
        return {"size": size, "blake3": hasher.hexdigest()}

    def _content(self, op: dict[str, Any]) -> bytes:
        return materialize(Spec.load(op["content"])) if op.get("content") else b""

    def _put(self, op: dict[str, Any], append: bool) -> dict[str, Any]:
        data = op["data"] if "data" in op else self._content(op)
        durability = DURABILITY[op.get("durability", "written")]
        if op.get("via") == "stream" and not append:
            return {
                "size": self.write_stream(op["tenant"], op["path"], data, durability)
            }
        if op.get("text"):
            method = "AppendText" if append else "WriteText"
            value: dict[str, Any] = {"value": data.decode()}
            request = "WriteTextRequest"
        else:
            method = "AppendBytes" if append else "WriteBytes"
            value = {"value": data}
            request = "WriteBytesRequest"
        value.update(path=op["path"], tenant=op["tenant"], durability=durability)
        reply = self._unary(method, request, "WriteResponse", value, len(data))
        return {"size": reply["size"]}

    def write_stream(self, tenant: str, path: str, data: bytes, durability: int) -> int:
        def messages() -> Iterator[dict[str, Any]]:
            yield {"start": {"path": path, "tenant": tenant, "durability": durability}}
            view = memoryview(data)
            for offset in range(0, len(data), WRITE_CHUNK_BYTES):
                yield {"chunk": bytes(view[offset : offset + WRITE_CHUNK_BYTES])}

        call = self.channel.stream_unary(
            SERVICE + "WriteFile",
            request_serializer=lambda item: encode("WriteFileRequest", item),
            response_deserializer=lambda raw: decode("WriteResponse", raw),
        )
        return call(messages(), timeout=self._timeout(len(data)))["size"]

    def op_write(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._put(op, append=False)

    def op_append(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._put(op, append=True)

    def op_open(self, op: dict[str, Any]) -> dict[str, Any]:
        reply = self._unary(
            "Open",
            "OpenRequest",
            "FileHandle",
            {
                "path": op["path"],
                "mode": op["mode"],
                "durability": DURABILITY[op.get("durability", "written")],
                "tenant": op["tenant"],
            },
        )
        return {
            "path": reply["path"],
            "mode": reply["mode"],
            "position": reply["position"],
            "tenant": reply["tenant"],
        }

    def op_mkdir(self, op: dict[str, Any]) -> dict[str, Any]:
        self._unary(
            "Mkdir",
            "MkdirRequest",
            "Empty",
            {
                "path": op["path"],
                "parents": op.get("parents", False),
                "exist_ok": op.get("exist_ok", False),
                "tenant": op["tenant"],
            },
        )
        return {}

    def _mutation(self, method: str, op: dict[str, Any]) -> dict[str, Any]:
        self._unary(
            method,
            "MutationPathRequest",
            "Empty",
            {
                "path": op["path"],
                "durability": DURABILITY[op.get("durability", "written")],
                "tenant": op["tenant"],
            },
        )
        return {}

    def op_unlink(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._mutation("Unlink", op)

    def op_rmdir(self, op: dict[str, Any]) -> dict[str, Any]:
        return self._mutation("Rmdir", op)

    def op_rename(self, op: dict[str, Any]) -> dict[str, Any]:
        reply = self._unary(
            "Rename",
            "RenameRequest",
            "PathResponse",
            {
                "source": op["source"],
                "target": op["target"],
                "durability": DURABILITY[op.get("durability", "written")],
                "tenant": op["tenant"],
            },
        )
        return {"path": reply["path"]}

    def op_resolve(self, op: dict[str, Any]) -> dict[str, Any]:
        reply = self._unary(
            "Resolve",
            "PathRequest",
            "PathResponse",
            {"path": op["path"], "tenant": op["tenant"]},
        )
        return {"path": reply["path"]}

    def op_absolute(self, op: dict[str, Any]) -> dict[str, Any]:
        reply = self._unary(
            "Absolute",
            "PathRequest",
            "PathResponse",
            {"path": op["path"], "tenant": op["tenant"]},
        )
        return {"path": reply["path"]}

    def _entries(self, stream: Iterator[dict[str, Any]], key: str) -> dict[str, Any]:
        items: list[Any] = []
        try:
            for item in stream:
                if key == "entries":
                    items.append([item["path"], item["name"], FILE_KINDS[item["kind"]]])
                else:
                    items.append(
                        {
                            "path": item["path"],
                            "line_number": item["line_number"],
                            "line": item["line"],
                            "before": item["before"],
                            "after": item["after"],
                        }
                    )
        except grpc.RpcError as error:
            return {key: items, "error": _error(error)}
        return {key: items}

    def op_iterdir(self, op: dict[str, Any]) -> dict[str, Any]:
        stream = self._stream(
            "Iterdir",
            "PathRequest",
            "DirectoryEntry",
            {"path": op["path"], "tenant": op["tenant"]},
        )
        return self._entries(stream, "entries")

    def op_glob(self, op: dict[str, Any]) -> dict[str, Any]:
        stream = self._stream(
            "Glob",
            "GlobRequest",
            "DirectoryEntry",
            {
                "tenant": op["tenant"],
                "pattern": op["pattern"],
                "max_results": op.get("max_results", 0),
            },
        )
        return self._entries(stream, "entries")

    def op_grep(self, op: dict[str, Any]) -> dict[str, Any]:
        request = {
            key: op.get(key, default)
            for key, default in (
                ("tenant", ""),
                ("path", ""),
                ("pattern", ""),
                ("fixed_string", False),
                ("case_insensitive", False),
                ("before_context", 0),
                ("after_context", 0),
                ("max_matches", 0),
                ("max_file_size_bytes", 0),
            )
        }
        return self._entries(
            self._stream("Grep", "GrepRequest", "GrepMatch", request), "matches"
        )
