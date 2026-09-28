# Line index patterns

Line uses two complementary index paths: stream labels for LogQL selection and
term postings for line-content search.

```text
LogQL selector ─► label postings ─► stream IDs ─► page metadata
                                                    │ time/block pruning
line filter ─────► term directories/posting blocks ─┤
                                                    ▼
                                             page block decode
```

## Stream index

- A stream fingerprint dictionary assigns a segment-local `u32` stream ID.
- The forward index maps stream IDs to complete label sets.
- Exact `(label name, label value)` postings are Roaring bitmaps.
- Label matcher results determine which stream page ranges are scanned.

## Page and search indexes

Page metadata supplies minimum/maximum timestamps and per-block bounds, so
queries reject out-of-range pages and blocks before decompression. Content
search tokenizes log lines and stores:

- segment field statistics,
- per-term statistics,
- fixed-size directory pages,
- bounded posting blocks containing page/row addresses and frequencies.

Search intersects candidate pages from the stream selector with term postings,
then applies the remaining LogQL pipeline to decoded rows. BM25 statistics are
available for ranked search. Limits on pages, entries, concurrency, and
in-flight bytes bound expensive broad queries.
