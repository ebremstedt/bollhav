[Home](index.md) › [Model](MODEL.md) › Source › **None**

# No source

`source` is **optional** — `Model(source=...)` defaults to `None`, and most models leave it unset.

A model doesn't need a declared source. Your `read()` function is what actually produces the data (it receives the model + the interval and returns DataFrames), so where the rows come from is the read function's business, not the model's. `source` is there only as metadata/config for code that wants to introspect it.

```python
Model(
    target=Target(name="orders", ...),
    kind=Kind.INTERVAL,
    batching=Batch(...),
    # no source — read() supplies the rows
)
```

## When you *do* set it

| Use `source=` when | Type |
|---|---|
| your `read()` (or tooling) wants the source table/schema/DSN as config | [SourceTable](SOURCETABLE.md) |
| your `read()` (or tooling) wants the file path/encoding/separator as config | [SourceFile](SOURCEFILE.md) |
| the model is a [VIEW](KINDS.md) — the view's definition *is* a query | `SourceTable(query=...)` (required) |

Only the VIEW case makes `source` mandatory; everywhere else it's a convenience.
