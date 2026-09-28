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

`filesystem.chunk_size_bytes` controls immutable content chunk size and defaults
to 1 MiB. `filesystem.max_file_size_bytes` rejects larger writes and defaults to
1 GiB. `filesystem.max_append_generations` defaults to 64 and bounds append
generation chains by scheduling a bounded-memory rewrite at each threshold.

`filesystem.storage` accepts the shared SlateDB storage configuration. Local,
AWS, Azure, GCP, and in-memory object stores are supported. Production local
deployments should place the local object-store path on a persistent volume.
Configure `FoyerHybrid` as `block_cache` for memory plus local-disk data caching
and `FoyerMemory` as `meta_cache` for indexes and filters.

Only one server may open a given SlateDB path for writing. PseudoFS therefore
supports one replica and does not have reader, writer, sharding, namespace, or
authentication configuration. Tenants are supplied per gRPC request rather than
preconfigured; all tenant filesystems share this process, SlateDB writer, and
configured memory/disk caches.
