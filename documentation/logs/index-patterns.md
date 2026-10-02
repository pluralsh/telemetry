# Logs index patterns

Logs uses two complementary index paths: stream labels for LogQL selection and
term postings for line-content search.

```text
LogQL selector ─► label postings ─► stream IDs ─► run records
                                                    │ time/expiry pruning
line filter ─────► term directories/posting blocks ─┤
                                                    ▼
                                    object block range reads and decode
```

## Stream index

- A stream fingerprint dictionary assigns a segment-local `u32` stream ID.
- The forward index maps stream IDs to complete label sets.
- Exact `(label name, label value)` postings are Roaring bitmaps.
- Label matcher results determine which streams' runs are listed and read.

## Run and search indexes

Run records supply each stream's minimum/maximum timestamps and block range in
every object, so queries reject out-of-range runs before any block read, and
block headers reject out-of-range blocks before decompression. Runs of one
object are read together with one range scan when they lie close together.
Content search tokenizes log lines and stores:

- segment field statistics,
- per-term statistics,
- fixed-size directory pages,
- bounded posting blocks containing `(stream, leaf object, row)` addresses
  and frequencies.

Search intersects candidate runs from the stream selector with term postings,
then applies the remaining LogQL pipeline to decoded rows. BM25 statistics are
available for ranked search. Limits on read units, entries, concurrency, and
in-flight bytes bound expensive broad queries.

Only the `match` stage uses term postings. Line filters (`|=`, `|~`) must find
every substring or regex match, while postings nominate rows by whole tokens,
so line filters scan decoded rows instead.