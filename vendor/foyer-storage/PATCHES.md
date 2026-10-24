# foyer-storage 0.22.6, patched

Unmodified `foyer-storage` 0.22.6 from crates.io (Apache-2.0,
https://github.com/foyer-rs/foyer) except for three open upstream fixes to
the block engine. Drop this directory and the `[patch.crates-io]` entry once
a release carries all of them.

## No writes after recovering with 0 clean blocks

foyer-rs/foyer#1373, fixed by #1374 (approved, not merged). Reclaim only
started after taking a clean block or finishing a write or reclaim, so a disk
cache that recovered with every block holding data never wrote again. Seen
on a logs reader: 0 clean and 1,152 evictable blocks, and no disk writes
after a restart.

`init` and the waiting path of `get_clean_block` start a reclaim. Test:
`test_store_write_after_recovery_without_clean_block` in `engine.rs`, which
needs tokio's `time` feature in dev-dependencies.

## Hang when the runtime shuts down during a reclaim

foyer-rs/foyer#1379, fixed by #1380 (in review). A reclaim was spawned while
holding the block manager's state lock; a runtime that is shutting down drops
the task at once, and its drop takes the same lock again.

`reclaim_if_needed` returns the reclaim, which callers spawn after releasing
the lock. Test: `test_reclaim_dropped_by_spawner_does_not_deadlock` in
`manager.rs`.

#1374's two new reclaims are applied in #1380's form, spawned after the lock
is released.

## Entries that miss the flush buffer's tail are dropped

foyer-rs/foyer#1334, fixed by #1335 (unreviewed). While a flush is in
flight the flusher kept filling its current buffer, and every entry that no
longer fit was dropped and counted as `buffer_overflow`. Seen on a logs
reader dropping about 56% of disk-cache inserts while its disk was nearly
idle.

`Buffer::push` tells "no room left" from "can never fit", and the flusher
retries entries of the first kind in order after the buffer rotates. They
stay counted in the submit queue until written, so the queue's threshold,
and our warmer's throttle on it, push back instead of losing them. Tests: the
`flusher.rs` tests module, which needs `mixtrics` in dev-dependencies.
