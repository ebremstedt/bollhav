[Home](index.md) › [Model](MODEL.md) › Source › **SourceModel**

# SourceModel

The `type` of a **relational** input — a `Source` that is a database table, a view, or another bollhav-managed model. It carries the config to read it. Use it in a model's [`upstream`](UPSTREAM.md) list:

```python
upstream=[
    Source("raw.orders", type=SourceModel(schema="raw", dsn_env_var="RAW_DSN")),
]
```

A `SourceModel` is the only **SQL-addressable** type — `model.ref("raw.orders")` resolves it into a `FROM`. It's also the only type that can be **gated**: attach a `contract` to make it a managed upstream (see [Upstream](UPSTREAM.md)). For file/API inputs see [SourceFile](SOURCEFILE.md) / `SourceApi`.

A [VIEW](KINDS.md) model's definition *is* a `SourceModel` with a `query` set, in its `upstream` list — that's what `CREATE OR REPLACE VIEW` runs.

## schema

Type: `str` · Default: `None`

Source schema.

## catalog

Type: `str` · Default: `None`

Source catalog / database (3-part `catalog.schema.table` names).

## dsn_env_var

Type: `str` · Default: `None`

DSN env var for the source connection.

## query

Type: `str` · Default: `None`

Optional query override. When set on a [VIEW](KINDS.md) model's source, it *is* the view definition. Otherwise the loader may use this SQL instead of `SELECT * FROM <schema>.<name>`.

## partitioned_by

Type: `str` · Default: `None`

Partition column on the source, when relevant to the read.

## infer_schema_length

Type: `int` · Default: `None`

Passed to polars as `infer_schema_length` — the maximum number of rows to scan when inferring column types. `None` scans every row, which can be slow on large sources.

## extra

Type: `dict` · Default: `{}`

Free-form config bag for read functions that need extra knobs.
