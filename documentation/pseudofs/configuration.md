# PseudoFS configuration

Start from [`config/pseudofs.example.yaml`](../../config/pseudofs.example.yaml)
and run `pseudofs-server --config config/pseudofs.yaml`.

`listener` is the gRPC bind address and defaults to `0.0.0.0:9093`.
`max_unary_file_size_bytes` defaults to 8 MiB and bounds the complete file
buffer allocated by unary read, write, and append RPCs. Larger files must use
the streaming methods.
`max_decoding_message_bytes` and `max_encoding_message_bytes` bound individual
gRPC messages. Large files should use streaming methods, whose chunks are still
subject to the decoding limit.

`filesystem.chunk_size_bytes` sets the size of streamed read chunks and
defaults to 1 MiB. `filesystem.inline_threshold_bytes` defaults to 16 KiB (at
most 16 MiB): files written in one generation at or below it are stored in the
metastore, and larger ones become blobs. Inline bytes are rewritten by every
metastore compaction, so a higher threshold trades metastore write load and
block-cache space for fewer small blobs. `filesystem.max_file_size_bytes`
rejects larger writes and defaults to 1 GiB.

`filesystem.max_append_generations` defaults to 64. Once a file's append
chain reaches it, a background compaction merges the chain's recent end into
one generation, leaving at most half the threshold. Each generation it leaves
is larger than everything merged after it, so a file of `n` bytes is
rewritten O(log n) times over its life rather than in full at every
threshold, and appends that land during a compaction are kept on top of the
merged generation. Appends fail with `RESOURCE_EXHAUSTED` if compaction
falls behind by 16 times the threshold (at least 1024 generations).

`filesystem.storage` configures the SlateDB metastore with the shared storage
configuration. Local, AWS, Azure, GCP, and in-memory object stores are
supported. `storage.path` is the filesystem's root prefix, a non-empty prefix
without leading, trailing, or repeated `/`: the metastore is kept under
`<path>/metastore` and blobs under `<path>/blobs` (see the storage format
docs). Production local deployments should place the local object-store
path on a persistent volume. The metastore holds only metadata and small
files, so its block cache mainly serves inode and directory lookups; configure
`FoyerHybrid` as `block_cache` and `FoyerMemory` as `meta_cache` for indexes
and filters. Use `write_policy: WriteOnEviction` (the operator's default for
PseudoFS): blocks reach disk only when they fall out of the memory tier. The
shared default, `WriteOnInsertion`, writes every block read into the cache to
disk; in the production fuzz benchmark that cost 60% more CPU and raised p99
append latency from 111 ms to 424 ms. When the object store is remote, set
`filesystem.metastore_wal_path` to a directory on a persistent volume: the
write-ahead log is then fsynced locally, so `WRITTEN` acknowledgements no
longer wait on the bucket, while SSTs and the manifest stay in the object
store.

`filesystem.blobs` configures blob storage:

- `local_path` (required in production, default `.data/pseudofs-blobs`) holds
  staged uploads, blobs awaiting upload, and the read cache. It must be
  persistent, because writes are acknowledged before their blobs are uploaded.
- `object_store` selects the remote blob store and defaults to
  `filesystem.storage.object_store`. Blobs are always kept under
  `<storage.path>/blobs` in it.
- `cache_capacity_bytes` (default 4 GiB) bounds local blob disk usage.
  Uploaded blobs are evicted least recently used first. A whole-file read of
  an evicted blob downloads it once, however many readers miss at the same
  time, and streams it to the first reader while it is cached. A partial read
  streams its range from the remote store and caches blobs up to an eighth of
  the capacity in the background. `Grep` and append compaction neither add
  blobs to the cache nor refresh their place in it. Blobs awaiting upload
  are never evicted. Staged writes count against the budget too: while blobs
  awaiting upload and staged writes fill it, large writes wait for the
  uploader to catch up, failing with `RESOURCE_EXHAUSTED` after 30 seconds. A
  write never waits when nothing is awaiting upload, so one file larger than
  the budget, or concurrent writes alone, can still exceed it.
- `upload_concurrency` (default 4) bounds concurrent background uploads.
- `gc_grace_seconds` (default 300) is how long unreferenced blobs are kept
  before deletion, so streams that began before an overwrite can finish.
  Streams of a replaced file that run longer than this may fail.

`search` bounds `Glob` and `Grep` requests. A request value of zero uses
`default_glob_results` (1000), `default_grep_matches` (100), or
`default_grep_file_size_bytes` (16 MiB); larger values are capped at
`max_glob_results` (10000), `max_grep_matches` (1000), and
`max_grep_file_size_bytes` (64 MiB). `max_grep_context_lines` (100) caps
before and after context. `grep_concurrency` (8) is the number of files one
`Grep` request searches at once. `max_scanned_entries` (1,000,000) bounds the
directory entries one `Glob` or `Grep` walk may examine before failing with
`RESOURCE_EXHAUSTED`. `max_grep_seconds` (30) bounds how long one `Grep`
request searches; time spent waiting for the client to read matches does not
count. It then fails with `RESOURCE_EXHAUSTED` after the matches already sent.

The telemetry operator exposes these settings on the `PseudoFS` resource
under `spec.config`: `inlineThresholdBytes`; `blobs.cacheCapacityBytes`, `blobs.uploadConcurrency`, and
`blobs.gcGraceSeconds`; and `search.*` with camel-cased names of the fields
above. The operator places the blob directory and, for remote object stores,
the metastore WAL on the data volume, uses the storage object store for
blobs, and rejects a blob cache larger than 90% of a bounded data volume or a
search default above its maximum.

Only one server may open a given SlateDB path for writing. PseudoFS therefore
supports one replica and does not have reader, writer, sharding, namespace, or
authentication configuration. Tenants are supplied per gRPC request rather than
preconfigured; all tenant filesystems share this process, SlateDB writer, and
configured memory/disk caches.
