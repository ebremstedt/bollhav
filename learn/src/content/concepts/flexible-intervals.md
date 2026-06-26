---
title: "Flexible intervals"
body: "One model, more than one grain."
---

`fixed_intervals=False` frees a model from a single fixed grid. It's an attestation that the output doesn't depend on how time is partitioned — the query is window-decomposable and the write is order-independent/idempotent. State then becomes a coverage set (the ranges that are done) rather than one chunk grid, so the same model can be sliced at more than one interval — e.g. backfill history `@daily` and run recent data `@hourly` — without forking its identity or needing a `torch` reset.

**Not for aggregations.** A `GROUP BY` (sum, max, count…) over a span isn't window-decomposable — slicing it would aggregate each piece in isolation and produce the wrong total. Aggregates should stay `TIMELESS` (whole-table rebuild) or keep a fixed grid.
