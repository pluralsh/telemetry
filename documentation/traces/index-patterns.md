# Traces index patterns

Traces has a direct locator for trace IDs and typed inverted postings for
TraceQL candidate reduction.

```text
trace ID ─► locator fragment ─► page directory ─► one trace payload

TraceQL safe equality predicates
    └─► typed attribute postings
          └─► candidate page indexes
                └─► decode + evaluate complete expression
```

Attribute posting keys include:

- scope (`resource` or `span`),
- attribute name,
- value type (`string`, `bool`, `int`, or `double`),
- encoded value,
- page sequence.

Posting values are compact arrays of trace indexes within the page. Type tags
keep values such as integer `7` and string `"7"` distinct.

Full TraceQL evaluation still runs against decoded candidates, so indexing
does not change query semantics. Queries without usable predicates scan page
metadata across the requested segments and prune by time using the per-trace
summaries in metadata.
