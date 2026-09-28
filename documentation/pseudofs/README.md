# PseudoFS

PseudoFS is a persistent virtual filesystem for sandboxed and embedded language
runtimes. It stores multiple tenant-isolated logical filesystems in one SlateDB
instance and exposes filesystem operations through gRPC. It is not mountable
and does not access the host filesystem on behalf of clients.

- [API](apis.md)
- [Configuration](configuration.md)
- [Storage format](storage-format.md)

PseudoFS intentionally runs as one combined reader/writer process. This keeps
read-after-write behavior simple and conforms to SlateDB's single-writer
fencing model. Symlinks, environment variables, file locks, and multiple
writer processes are outside the service's scope.
