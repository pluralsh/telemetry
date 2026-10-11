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
Its optional `offset` and `length` select a byte range; offsets past the end
of the file return no chunks. Range reads only fetch the parts of the file
they cover, including from the remote blob store on a cache miss. `WriteFile`
is a client stream whose first message must be `start`; subsequent messages
contain bytes. Uploaded bytes remain invisible until the stream completes and
its new file generation is atomically published. A failed stream is aborted
and does not replace an existing file.

`Glob` streams the paths matching an absolute pattern, depth-first, as
`DirectoryEntry` messages. Within a path component, `*` matches any run of
characters, `?` one character, `[abc]`/`[a-z]` a character class, `[!abc]` its
complement, and `{png,jpg}` any alternative. A `**` component matches zero or
more directories, so `/dir/**/subfolder` finds every `subfolder` beneath
`/dir`. A backslash escapes the next character. The walk starts at the longest
literal prefix of the pattern and skips subtrees the pattern cannot match.
Literal components are resolved by name lookup rather than by listing the
directory, so `/*/*/subfolder` reads only
the first two levels. Wildcards also match names that begin with `.`. A walk
that examines more than the server's `max_scanned_entries` directory entries
fails with `RESOURCE_EXHAUSTED`, and a walk stops promptly once the client
cancels the stream, even while no entries are matching.

`Grep` searches files and streams matching lines with their line numbers and
up to `before_context`/`after_context` surrounding lines. `path` may be a
file, a directory (searched recursively), or a glob pattern. `pattern` uses
[Rust regex syntax](https://docs.rs/regex/latest/regex/#syntax) unless
`fixed_string` is set; `case_insensitive` applies to both. Binary files are
skipped: names with a known binary extension (images, video, audio, archives,
PDFs, executables, and similar, case-insensitive) are skipped without reading
content; other files are skipped if their first 8 KiB contain a NUL byte or
start with a known binary signature (PNG, JPEG, GIF, PDF, ZIP, gzip, ELF,
ISO media, and others). Files larger than `max_file_size_bytes` and lines
longer than 1 MiB are also skipped. Returned line text is decoded lossily as
UTF-8 and truncated to 4 KiB. Files are searched concurrently (up to the
server's `grep_concurrency`), but results stream in walk order. `max_matches`
bounds matching lines across all files; when the limit is reached, which
matches from concurrently searched files are returned can vary between runs.
The walk is subject to the same `max_scanned_entries` budget as `Glob`, and a
search stops scanning within 256 KiB of file content once the client
disconnects or its deadline passes, so set a deadline on broad searches.
Patterns run on a linear-time engine with a 16 MiB lazy DFA cache per search;
very large counted repetitions (such as `x{10000}`) can still exceed it and
scan orders of magnitude more slowly.
Zero-valued limits on `Glob` and `Grep` use
the server's `search` defaults, and larger values are capped at the server's
maximums. An invalid pattern returns `INVALID_ARGUMENT` with detail kind
`invalid_pattern`.

Mutation requests select `APPLIED`, `WRITTEN`, or `DURABLE` acknowledgement.
Unspecified durability defaults to `WRITTEN`. Blob contents are always fsynced
to the local blob directory before the request is acknowledged.

| Level | Acknowledged after |
| --- | --- |
| `APPLIED` | The metadata change is visible to readers. |
| `WRITTEN` | The metastore WAL is flushed. With `metastore_wal_path` set this is a local fsync; otherwise it is a write to the object store. |
| `DURABLE` | As `WRITTEN`, and new blob content has also been uploaded to the remote blob store. |

`APPLIED` and `WRITTEN` writes of large files return before the blob is
uploaded; the background uploader copies it to the remote store, retrying
across restarts, so the local blob directory must be persistent.

Filesystem failures use canonical gRPC status codes and include a serialized
`ErrorDetail` in status details. `RESOURCE_EXHAUSTED` means a size limit
(including the unary limits) was exceeded or the local blob directory stayed
full of blobs awaiting upload for the whole write timeout;
`UNAVAILABLE` means the local or remote blob store failed.

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
