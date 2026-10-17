"""TCP proxy that delays the first response byte of every request.

Used by the `historical` and `cold` fuzz scenarios in front of MinIO so
object-store reads cost roughly what they do against S3 for both the
implementation and the oracle. A "request" is any client data that arrives
after the previous response started, which matches HTTP/1.1 keep-alive without
pipelining. Only the first byte waits; the rest of a response streams at full
speed.

With `--stats-port`, a plain HTTP endpoint on that port answers any request
with cumulative counters per client IP and HTTP method: requests, response
bytes (`bytes`), and request bytes (`sent`). Containers keep one IP while they
run, so a client's object-store traffic over an interval is the difference of
two snapshots.

With `--timeline` (or `PROXY_TIMELINE=1`), the proxy also keeps the last
`TIMELINE_CAP` requests, each with its client, method, path, `Range` header,
response status, response bytes, and times in milliseconds on the proxy's
monotonic clock: `start` (request line received), `first_byte` (first response
byte forwarded, after the delay) and `end` (last response byte forwarded so
far). `GET /timeline?since=<seq>&client=<ip>` on the stats port returns the
requests numbered `since` and later (optionally for one client) and `next`, the
number the next request will get.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import time
from collections import defaultdict, deque
from typing import Any
from urllib.parse import parse_qs, urlsplit

CHUNK = 256 * 1024
METHODS = (b"GET", b"PUT", b"POST", b"HEAD", b"DELETE")
TIMELINE_CAP = 500_000
TIMELINE_ENV = "PROXY_TIMELINE"


def _now_ms() -> float:
    return time.monotonic() * 1000


class Stats:
    def __init__(self, timeline: bool = False) -> None:
        self.clients: dict[str, dict[str, dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: {"requests": 0, "bytes": 0, "sent": 0})
        )
        self.timeline: deque[dict[str, Any]] | None = (
            deque(maxlen=TIMELINE_CAP) if timeline else None
        )
        self.next_seq = 0

    def snapshot(self, first_byte_ms: float) -> dict[str, object]:
        return {
            "first_byte_ms": first_byte_ms,
            "clients": {
                client: {method: dict(counts) for method, counts in methods.items()}
                for client, methods in self.clients.items()
            },
        }

    def begin(self, client: str, method: str, head: bytes) -> dict[str, Any] | None:
        """Starts a timeline entry for a request whose first chunk is `head`."""
        if self.timeline is None:
            return None
        path, range_ = request_target(head)
        entry: dict[str, Any] = {
            "seq": self.next_seq,
            "client": client,
            "method": method,
            "path": path,
            "range": range_,
            "status": None,
            "bytes": 0,
            "start": round(_now_ms(), 3),
            "first_byte": None,
            "end": None,
        }
        self.next_seq += 1
        self.timeline.append(entry)
        return entry

    def timeline_since(self, since: int, client: str | None) -> dict[str, Any]:
        entries = [
            dict(entry)
            for entry in self.timeline or ()
            if entry["seq"] >= since and (client is None or entry["client"] == client)
        ]
        return {
            "enabled": self.timeline is not None,
            "now": round(_now_ms(), 3),
            "next": self.next_seq,
            "requests": entries,
        }


def request_method(data: bytes) -> str | None:
    """The method of an HTTP/1.1 request line, or None for a body continuation
    (such as the payload after `Expect: 100-continue`)."""
    head, _, _ = data[:8].partition(b" ")
    return head.decode() if head in METHODS else None


def request_target(data: bytes) -> tuple[str, str | None]:
    """The path (with query) and `Range` header value of a request's head."""
    head = data.split(b"\r\n\r\n", 1)[0].decode("latin-1")
    lines = head.split("\r\n")
    parts = lines[0].split(" ")
    path = parts[1] if len(parts) > 1 else ""
    range_ = next(
        (
            line.split(":", 1)[1].strip()
            for line in lines[1:]
            if line.lower().startswith("range:")
        ),
        None,
    )
    return path, range_


def response_status(data: bytes) -> int | None:
    parts = data[:16].split(b" ")
    if len(parts) > 1 and parts[0].startswith(b"HTTP/") and parts[1].isdigit():
        return int(parts[1])
    return None


class Turn:
    def __init__(self, client: str, stats: Stats) -> None:
        self.request_pending = False
        self.client = client
        self.stats = stats
        self.method = "OTHER"
        self.entry: dict[str, Any] | None = None

    def counts(self) -> dict[str, int]:
        return self.stats.clients[self.client][self.method]


async def _pump(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    turn: Turn,
    *,
    upstream: bool,
    delay_s: float,
) -> None:
    try:
        while data := await reader.read(CHUNK):
            first_at = None
            if upstream:
                if turn.request_pending:
                    turn.request_pending = False
                    await asyncio.sleep(delay_s)
                    first_at = round(_now_ms(), 3)
                turn.counts()["bytes"] += len(data)
            else:
                if not turn.request_pending and (method := request_method(data)):
                    turn.method = method
                    turn.counts()["requests"] += 1
                    turn.entry = turn.stats.begin(turn.client, method, data)
                turn.request_pending = True
                turn.counts()["sent"] += len(data)
            writer.write(data)
            await writer.drain()
            if upstream and (entry := turn.entry) is not None:
                if first_at is not None and entry["first_byte"] is None:
                    entry["first_byte"] = first_at
                    entry["status"] = response_status(data)
                entry["bytes"] += len(data)
                entry["end"] = round(_now_ms(), 3)
        if writer.can_write_eof():
            writer.write_eof()
    except (ConnectionError, OSError):
        pass


async def _serve(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    *,
    host: str,
    port: int,
    delay_s: float,
    stats: Stats,
) -> None:
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(host, port)
    except OSError:
        client_writer.close()
        return
    peer = client_writer.get_extra_info("peername")
    turn = Turn(peer[0] if peer else "unknown", stats)
    await asyncio.gather(
        _pump(client_reader, upstream_writer, turn, upstream=False, delay_s=delay_s),
        _pump(upstream_reader, client_writer, turn, upstream=True, delay_s=delay_s),
    )
    for writer in (client_writer, upstream_writer):
        writer.close()


async def _report(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    stats: Stats,
    first_byte_ms: float,
) -> None:
    head = b""
    with contextlib.suppress(
        asyncio.IncompleteReadError, asyncio.LimitOverrunError, OSError
    ):
        head = await reader.readuntil(b"\r\n\r\n")
    target = urlsplit(request_target(head)[0])
    if target.path == "/timeline":
        query = parse_qs(target.query)
        since = int(query.get("since", ["0"])[0])
        client = query.get("client", [None])[0]
        payload: dict[str, Any] = stats.timeline_since(since, client)
    else:
        payload = stats.snapshot(first_byte_ms)
    body = json.dumps(payload).encode()
    writer.write(
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
        + body
    )
    with contextlib.suppress(OSError):
        await writer.drain()
    writer.close()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", type=int, required=True)
    parser.add_argument("--upstream-host", default="127.0.0.1")
    parser.add_argument("--upstream", type=int, required=True)
    parser.add_argument("--first-byte-ms", type=float, default=15.0)
    parser.add_argument("--stats-port", type=int)
    parser.add_argument(
        "--timeline",
        action="store_true",
        default=os.environ.get(TIMELINE_ENV, "") not in ("", "0"),
    )
    args = parser.parse_args()
    delay_s = args.first_byte_ms / 1000
    stats = Stats(timeline=args.timeline)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        await _serve(
            reader,
            writer,
            host=args.upstream_host,
            port=args.upstream,
            delay_s=delay_s,
            stats=stats,
        )

    async def report(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        await _report(reader, writer, stats=stats, first_byte_ms=args.first_byte_ms)

    server = await asyncio.start_server(handle, "0.0.0.0", args.listen)
    servers = [server]
    if args.stats_port:
        servers.append(await asyncio.start_server(report, "0.0.0.0", args.stats_port))
    print(
        f"latency proxy :{args.listen} -> {args.upstream_host}:{args.upstream}, "
        f"first byte +{args.first_byte_ms:g} ms"
        + (f", stats on :{args.stats_port}" if args.stats_port else "")
        + (", timeline on" if args.timeline else ""),
        flush=True,
    )
    await asyncio.gather(*(item.serve_forever() for item in servers))


if __name__ == "__main__":
    asyncio.run(main())
