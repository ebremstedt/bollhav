[Home](index.md) › [Model](MODEL.md) › **Contract**

# Contract

The model's own time window — the historical scope its runs may target. Bollhav uses `begin` and `end` to decide what range to walk in `reload` / backfill modes. In `latest` mode they're ignored — latest reads from `now()`.

```python
from bollhav.model import Contract

Model(contract=Contract(begin=..., end=...), ...)
```

> **Not the same as an upstream contract.** This `Contract` is *this* model's own time bounds. A downstream's gating policy on an upstream is a separate thing — an [`UpstreamContract`](UPSTREAM.md) level. Both ride a `contract=` parameter (one on `Model`, one on `Source`), but they're different types.

Only a [`TEMPORAL`](TEMPORALITY.md) model has a time axis, so only a temporal model carries a `begin`/`end`. A [`TIMELESS`](TEMPORALITY.md) model has no time to reference — giving one a `begin`/`end` raises at validation.

A temporal model needn't be batched. With `batching`, the window is split into chunks (one state row per window). **Without** batching, a temporal model with a closed `begin`/`end` loads that whole range in one run and records it as a single state row spanning `[begin, end]` — so a downstream can still gate a `WINDOW` contract against it (its window must fall inside the range).

## begin

Type: `datetime` · Default: `None`

Window start. Must be UTC-aware (`tzinfo=timezone.utc`); naive datetimes raise at validation. In `reload`/backfill it's where the walk starts.

## end

Type: `datetime` · Default: `None`

Window end. Must be UTC-aware. When `None`, a reload walks up to the latest complete tick.

## See also

- [Temporality](TEMPORALITY.md) — `TEMPORAL` (has a window) vs `TIMELESS` (no time axis).
- [Upstream](UPSTREAM.md) — the separate `UpstreamContract` a downstream gates with.
