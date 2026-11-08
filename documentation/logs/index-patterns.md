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
With `full_text_index` on (it is off by default), content search tokenizes
log lines and stores:

- segment field statistics,
- per-term directory fragments, which hold a term's postings inline when it
  has at most one block of them in a flush,
- bounded posting blocks containing `(stream, leaf object, row)` addresses
  and frequencies.

The index costs writers about a dozen storage records per entry on lines with
request IDs and similar unique tokens, so with it off writers sustain more
entries per core and every query scans like Loki.

Search intersects candidate runs from the stream selector with term postings,
then applies the remaining LogQL pipeline to decoded rows. BM25 statistics are
available for ranked search. Limits on read units, entries, concurrency, and
in-flight bytes bound expensive broad queries.

The `match` stage ranks with term postings, and single-branch `|=` filters use
the postings of whole tokens inside their needle to skip rows that cannot
match; the filter still decides every candidate. Regex filters (`|~`) scan.
Without the index both scan decoded rows.