[Home](index.md) › [Model](MODEL.md) › Source › **SourceTable**

# SourceTable

Where data is read from when the model uses a table source. For file-backed sources see [SourceFile](SOURCEFILE.md).

`source` is **optional** — `Model(source=...)` defaults to `None`. It's metadata/config for code that wants it; your `read()` function supplies the data, so a model needs no `source` at all. The one exception: a [VIEW](KINDS.md) model needs a `SourceTable` with a `query` (that's the view's definition).

## name

Type: `str` · Default: required

Source table or entity name.

## schema

Type: `str` · Default: `None`

Source schema.

## dsn_env_var

Type: `str` · Default: `None`

DSN env var for the source connection.

## query

Type: `str` · Default: `None`

Optional query override. When set, the loader uses this SQL instead of `SELECT * FROM <schema>.<name>`.

## infer_schema_length

Type: `int` · Default: `None`

Passed to polars as `infer_schema_length` — the maximum number of rows to scan when inferring column types. `None` scans every row, which can be slow on large sources.
