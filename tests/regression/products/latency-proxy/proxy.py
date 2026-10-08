"""TCP proxy that delays the first response byte of every request.

Used by the `historical` fuzz scenario in front of MinIO so object-store reads
cost roughly what they do against S3 for both the implementation and the
oracle. A "request" is any client data that arrives after the previous
response started, which matches HTTP/1.1 keep-alive without pipelining. Only
the first byte waits; the rest of a response streams at full speed.
"""

from __future__ import annotations

import argparse
import asyncio

CHUNK = 256 * 1024


class Turn:
    def __init__(self) -> None:
        self.request_pending = False


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
            if upstream:
                if turn.request_pending:
                    turn.request_pending = False
                    await asyncio.sleep(delay_s)
            else:
                turn.request_pending = True
            writer.write(data)
            await writer.drain()
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
) -> None:
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(host, port)
    except OSError:
        client_writer.close()
        return
    turn = Turn()
    await asyncio.gather(
        _pump(client_reader, upstream_writer, turn, upstream=False, delay_s=delay_s),
        _pump(upstream_reader, client_writer, turn, upstream=True, delay_s=delay_s),
    )
    for writer in (client_writer, upstream_writer):
        writer.close()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", type=int, required=True)
    parser.add_argument("--upstream-host", default="127.0.0.1")
    parser.add_argument("--upstream", type=int, required=True)
    parser.add_argument("--first-byte-ms", type=float, default=15.0)
    args = parser.parse_args()
    delay_s = args.first_byte_ms / 1000

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        await _serve(
            reader,
            writer,
            host=args.upstream_host,
            port=args.upstream,
            delay_s=delay_s,
        )

    server = await asyncio.start_server(handle, "0.0.0.0", args.listen)
    print(
        f"latency proxy :{args.listen} -> {args.upstream_host}:{args.upstream}, "
        f"first byte +{args.first_byte_ms:g} ms",
        flush=True,
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
