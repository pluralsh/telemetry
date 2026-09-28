# PseudoFS gRPC API

The public service is `pseudofs.v1.PseudoFS` on port `9093`. Its source contract
is [`pseudofs.proto`](../../crates/proto/proto/pseudofs/v1/pseudofs.proto).
Server reflection and the standard gRPC health service are enabled. Every
request requires a `tenant`; the streaming `WriteFile` RPC carries it in its
first `start` message. A tenant selects an isolated filesystem root inside the
shared SlateDB instance. Tenant selection is isolation, not authentication, so
secure and authorize the service with cluster network policy or a service-mesh
transport policy.

Unary methods mirror the operations expected by embedded runtimes:
`Exists`, `IsFile`, `IsDir`, `IsSymlink`, `ReadText`, `ReadBytes`, `WriteText`,
`WriteBytes`, `AppendText`, `AppendBytes`, `Open`, `Mkdir`, `Unlink`, `Rmdir`,
`Stat`, `Rename`, `Resolve`, and `Absolute`. Unary file reads and writes are
limited by `max_unary_file_size_bytes`.

`Iterdir` streams a snapshot-consistent directory listing without buffering the
whole directory. `StreamFile` sends snapshot-consistent file chunks in order.
`WriteFile` is a client stream whose
first message must be `start`; subsequent messages contain bytes. Uploaded
chunks remain invisible until the stream completes and its new file generation
is atomically published. A failed stream is aborted and does not replace an
existing file.

Mutation requests select `APPLIED`, `WRITTEN`, or `DURABLE` acknowledgement.
Unspecified durability defaults to `WRITTEN`. Filesystem failures use canonical
gRPC status codes and include a serialized `ErrorDetail` in status details.

An adapter for Monty or another runtime should translate its OS progress events
directly to these methods. The `Open` response is a stateless path/mode handle;
it echoes the tenant so adapters can retain the complete filesystem identity.
The client remains responsible for its cursor, while reads and writes use the
path-based methods.

Tenant identifiers contain 1–128 bytes, start and end with an ASCII letter or
digit, and may contain letters, digits, `-`, `_`, and `.`. Paths are always
resolved from that tenant's root. A path containing `..` is accepted only while
it remains inside the root; attempts to traverse above `/` return
`INVALID_ARGUMENT`.
