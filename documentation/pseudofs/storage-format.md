# PseudoFS storage format

PseudoFS separates filesystem metadata from file content. SlateDB is the
metastore: it holds inodes, directory entries, generations, small inline file
contents, and blob bookkeeping. Content above `inline_threshold_bytes` is
stored as immutable blobs outside SlateDB, so large files never pass through
the memtable, the write-ahead log, or LSM compaction.

## Namespace

PseudoFS derives a distinct root inode ID from each validated tenant
identifier. Every inode stores its owning tenant, and path traversal verifies
that ownership at every step. Directory entries are keyed by parent inode plus
child name. Paths are resolved only by walking from the selected tenant root.
Moving a directory changes one parent entry, so descendants do not need to be
rewritten. Directory-entry values also carry the expected tenant root ID,
allowing listings and lookups to reject a cross-tenant link without an extra
inode read.

## File content

A file inode points to its visible head generation. Replacement creates a new
single-generation chain; append creates a generation that references the
previous head. Each generation records its byte length and where its bytes
live:

- **Inline**: the bytes are one metastore value keyed by the generation.
- **Blob**: the bytes are one immutable blob identified by a UUID.

Readers resolve the inode and its generation chain from one SlateDB snapshot,
then read generations oldest to newest. Range reads skip whole generations
outside the range and read only the overlapping part of the rest. Long append
chains are rewritten into one generation in the background; the rewrite reads
from a snapshot and commits only if the file still has the same inode and
head, so appends that race with it are never lost.

Uploads buffer up to `inline_threshold_bytes` in memory. Larger uploads stream
into a staged local file, which is fsynced and renamed into the local blob
directory before any metadata references it. Completion atomically writes the
generation, updates the inode, links the parent directory entry, and records
the blob as pending upload. Nothing is visible until that commit, and an
aborted upload deletes its staged file.

## Blob lifecycle

The local blob directory (`blobs.local_path`) contains:

- `staging/`: uploads in progress and in-flight downloads, cleared at startup.
- `blobs/<uuid>`: committed blobs, either pending upload or cached.

A background worker uploads pending blobs to the remote store under
`<blobs.path>/<uuid>` (multipart above 8 MiB) and then removes their pending
records. Pending blobs are pinned on local disk and never evicted; uploaded
blobs are an LRU cache bounded by `blobs.cache_capacity_bytes`. A full-file
read of an uncached blob downloads it into the cache; a partial read streams
the requested range from the remote store without caching.

Replacing, unlinking, or compacting a file writes a garbage marker for the old
chain in the same batch. Cleanup deletes the old generations and inline
values and queues each blob as dead. The worker deletes dead blobs locally and
remotely once they are older than `blobs.gc_grace_seconds`, which lets streams
that started before the change finish reading. Garbage markers are retried at
startup, so a crash never leaks or prematurely deletes a blob.

## Keys

Every key has the following outer layout:

```text
[pseudofs subsystem][version = 0x03][record family][tenant root UUID][suffix]
```

Record families are `i` (inode), `d` (directory entry), `g` (generation), `c`
(inline content), `p` (blob pending upload), `r` (dead blob awaiting
deletion), and `x` (garbage marker). The persisted `pseudofs/v3` SlateDB
segment extractor returns the three-byte subsystem/version/family prefix, so
each record family is its own segment and compacts independently: hot inode
and directory records are not rewritten alongside inline content. Version 3
is a hard format switch; earlier databases are not read.

Record suffixes use binary UUIDs. SlateDB write batches make directory-entry,
inode, and generation changes atomic. A process-local mutation lock prevents
conflicting read-modify-write operations inside the sole writer process; it is
never held while waiting for durability. PseudoFS is unsharded: it always runs
as a single standalone SlateDB writer.

## Paths

Paths are UTF-8 and lexically normalized. Repeated separators and `.` are
removed. `..` removes one component but is rejected if it would traverse above
the tenant root; NUL is rejected. Symlinks are not stored.
