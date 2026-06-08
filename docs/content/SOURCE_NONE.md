[Home](index.md) › [Model](MODEL.md) › Source › **None**

# No declared inputs (unknown provenance)

A model doesn't have to declare any inputs. Your `read()` function is what actually produces the data (it receives the model + the interval and returns DataFrames), so where the rows come from is the read function's business — it can read from hardcoded SQL or anything else.

```python
Model(
    target=Target(name="orders", ...),
    kind=Kind.INTERVAL,
    batching=Batch(...),
    # no upstream — read() supplies the rows
)
```

## The unknown sentinel

When a model declares an empty `upstream`, bollhav doesn't leave it empty — provenance is *total*. It auto-injects a single **typeless** `Source`:

```python
Source(name="unknown-<uuid>", type=None)
```

- `type=None` is the marker for unknown provenance. It's never SQL-addressable and never gated.
- The name is uuid-suffixed so each unknown is a **distinct** node in the [lineage](LINEAGE.md) graph (two unknown-provenance models don't collapse into one).
- It isn't counted as a declared input — `source_names` / `upstream_names` exclude it.

Two computed fields surface this for a lineage audit:

```python
model.declared_inputs   # [] — nothing real declared
model.inputs_known      # False — its only input is the unknown sentinel
```

## When you *do* declare inputs

Everything goes in one [`upstream`](UPSTREAM.md) list as a `Source`, typed by what it is:

| your input is | type |
|---|---|
| a relational table / view / managed model | [`SourceModel`](SOURCETABLE.md) |
| a file | [`SourceFile`](SOURCEFILE.md) |
| an HTTP API | `SourceApi` |

…and gated (a managed upstream the state machine waits for) iff it carries a `contract` — see [Upstream](UPSTREAM.md).
