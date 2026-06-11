[Home](index.md) › [Model](MODEL.md) › **Kind**

# Kinds

Every model is one of three kinds. The kind decides its unit of work, how many state rows it has, and how a downstream [contract](UPSTREAM.md) checks it.

Kind is a required, explicit field — every model sets `kind=` from the `Kind` enum. There is no default and no inference.

```python
from bollhav.model import Kind
```

| kind | unit of work | state rows | declared by |
|---|---|---|---|
| `Kind.INTERVAL` | one time window (`@daily`, `@hourly`, …) | one per window | `kind=Kind.INTERVAL` — requires `batching` |
| `Kind.MONOLITHIC` | the whole table | one | `kind=Kind.MONOLITHIC` — no `batching` |
| `Kind.VIEW` | the view itself | one (does it exist) | `kind=Kind.VIEW` — a view |

Because kind is explicit, a forgotten `batching` can't silently turn a table into a whole-table load: `Kind.INTERVAL` without `batching` raises, and `Kind.MONOLITHIC` with `batching` raises.

`run.intervals` (on the `ModelRun` from `@load_models`) yields one window per unit for `Kind.INTERVAL` models, or a single `None` for `Kind.MONOLITHIC` / `Kind.VIEW` — so the same loop runs the unit of work the right number of times.

## See also

- [Upstream](UPSTREAM.md) — depending on another model by kind.
- [State](STATE.md) — the rows each kind records.
