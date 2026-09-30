# PseudoFS storage format

PseudoFS derives a distinct root inode ID from each validated tenant
identifier. Every inode stores its
owning tenant, and path traversal verifies that ownership at every step.
Directory entries are keyed by parent inode plus child name. Paths are resolved
only by walking from the selected tenant root. Moving a directory changes one
parent entry, so descendants do not need to be rewritten.
Directory-entry values also carry the expected tenant root ID, allowing
listings and lookups to reject a cross-tenant link without an extra inode read.

File bytes are immutable chunks grouped into generations. A file inode points
to its visible head generation. Replacement starts a new chain; append creates
a generation that references the previous head. Readers walk generations from
oldest to newest and then chunks by numeric index.

Streaming uploads use a fresh unpublished generation. Each accepted chunk and
its upload marker are stored together. Completion atomically writes generation
metadata, updates the file inode, links the parent directory entry, and removes
the upload marker. This prevents partial replacement from becoming visible.

Every key has the following outer layout:

```text
[pseudofs subsystem][version = 0x02][record family][tenant root UUID][suffix]
```

The persisted `pseudofs/v2` SlateDB segment extractor returns the two-byte
subsystem/version prefix. Because the record family follows it directly,
startup recovery scans all upload markers or all garbage markers with one prefix
each. Version 2 is a hard format switch: version-1 databases are not read.

Record suffixes use binary UUIDs and, for chunks, a big-endian index. SlateDB
write batches make directory-entry and inode changes atomic. A process-local
mutation lock prevents conflicting read-modify-write operations inside the sole
writer process. PseudoFS is unsharded: it always runs as a single standalone
SlateDB writer.

Paths are UTF-8 and lexically normalized. Repeated separators and `.` are
removed. `..` removes one component but is rejected if it would traverse above
the tenant root; NUL is rejected. Symlinks are not stored.
