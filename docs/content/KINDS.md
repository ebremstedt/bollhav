[← home](index.md)

# Kinds

Every model is one of three kinds. The kind decides its unit of work, how many state rows it has, and how a downstream [contract](UPSTREAM.md) checks it.

| kind | unit of work | state rows | declared by |
|---|---|---|---|
| `interval` | one time window (`@daily`, `@hourly`, …) | one per window | batched (default) |
| `monolithic` | the whole table | one | `monolithic=True` |
| `view` | the view itself | one (does it exist) | view target |

`interval` is the default. `monolithic` is explicit — never inferred — so a forgotten `batching` can't silently turn a table into a whole-table load. A view target is always `view`.

`model.intervals` yields one window per unit for `interval` models, or a single `None` for `monolithic` / `view` — so the same loop runs the unit of work the right number of times.

## See also

- [Upstream & contracts](UPSTREAM.md) — depending on another model by kind.
- [State](STATE.md) — the rows each kind records.
