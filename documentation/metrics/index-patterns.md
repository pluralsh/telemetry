# Metrics index patterns

Metrics indexes series independently in each time bucket.

```text
label matchers
    │
    ▼
inverted postings (Roaring bitmaps)
    │ intersect / union
    ▼
series IDs ──► forward index ──► labels and metadata
    │
    ▼
metric-prefixed sample records
```

## Main indexes

- **Series dictionary** maps a stable 128-bit label-set fingerprint to a local
  `u32` series ID, avoiding repeated label sets in sample keys.
- **Forward index** maps each series ID back to labels, metric type, and unit.
- **Inverted index** maps each exact `(label name, label value)` to a Roaring
  bitmap of series IDs. Equality matchers intersect smallest postings first;
  unions support multi-value selection.
- **Sample index** sorts by bucket, metric name, then series ID.

PromQL planning first narrows buckets by time and series by labels, then loads
only matching sample records. Label-name and label-value APIs scan index key
ranges rather than sample payloads. Readers cache loaded bucket views; the
`reader_cache_capacity` limit currently counts entries, despite its historical
byte-oriented default value.

Indexes are bucket-local: long ranges repeat lookup work per bucket, while old
buckets remain isolated from active-write churn.
