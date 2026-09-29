# PseudoFS storage format

PseudoFS derives a distinct root inode ID and routing slot from each validated
tenant identifier. The routing slot uses the common 12-bit hash space and is
identical for every record in that tenant's filesystem. Every inode stores its
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
[pseudofs subsystem][version][routing slot:u16 BE][record family][tenant root UUID][suffix]
```

The persisted `pseudofs/v1` SlateDB segment extractor returns the two-byte
subsystem/version prefix. The routing slot therefore immediately follows the
segment prefix, and one projected key range selects all record families and
tenants in a shard-split interval. Since every record for one tenant uses the
same slot, a split moves its directories, inodes, generations, chunks, upload
markers, and garbage markers together.

Record suffixes use binary UUIDs and, for chunks, a big-endian index. SlateDB
write batches make directory-entry and inode changes atomic. A process-local
mutation lock prevents conflicting read-modify-write operations inside the sole
writer process. PseudoFS remains operationally unsharded; the slot-aware format
preserves the option to adopt projected shard splits later.

Paths are UTF-8 and lexically normalized. Repeated separators and `.` are
removed. `..` removes one component but is rejected if it would traverse above
the tenant root; NUL is rejected. Symlinks are not stored.
