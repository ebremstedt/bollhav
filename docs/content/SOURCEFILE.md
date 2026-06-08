[Home](index.md) › [Model](MODEL.md) › Source › **SourceFile**

# SourceFile

The `type` of a **file** input — a `Source` that loads from a file (CSV, Parquet, JSON, …) rather than a database. For relational sources see [SourceModel](SOURCETABLE.md). Use it in a model's [`upstream`](UPSTREAM.md) list:

```python
upstream=[Source("orders.csv", type=SourceFile(path=Path("dropzone/orders.csv")))]
```

A `SourceFile` is **not** SQL-addressable (`ref()` on it raises — there's no `FROM` for a file) and can't be gated (no `contract`). Your `read()` function uses its config to load the data.

## path

Type: `Path` · Default: required

Path to the source file.

## encoding

Type: `str` · Default: `None`

Character encoding. `None` lets polars autodetect.

## separator

Type: `str` · Default: `None`

Column separator (e.g. `","`, `"\t"`, `";"`). `None` lets polars infer.

## infer_schema_length

Type: `int` · Default: `None`

Number of rows polars scans when inferring column types. `None` scans every row — slow on large files.

## remove_top_rows

Type: `int` · Default: `0`

Strip N rows from the top of the file before parsing — useful for files with cover sheets or extra header rows above the actual column row.

## archive_folder

Type: `Path` · Default: `None`

If set, the source file gets moved here after a successful load. Use to prevent the same file being processed twice.

## dateformat

Type: `str` · Default: `None`

`strftime` format string for date columns when polars can't autodetect (e.g. `"%d/%m/%Y"`).

## file_ending

Type: `str` · Default: `None`

File extension hint (e.g. `"csv"`, `"tsv"`). Only needed when `path` doesn't carry one.
