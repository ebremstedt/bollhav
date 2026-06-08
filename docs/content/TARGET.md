[Home](index.md) › [Model](MODEL.md) › **Target**

# Target

Where data lands: the destination table, its schema, the database connection, column definitions, and the write mode.

## name

Type: `str` · Default: required

Destination table name.

## suffix

Type: `str` · Default: `""`

Appended to `name` at runtime (e.g. `customers` → `customers_v2`). Off by default; set via `TABLE_SUFFIX` env var or programmatically. Used for blue/green hotswap inside a single schema — see [Schema vs table suffix](SUFFIXES.md).

## suffix_appendix

Type: `str | None` · Default: `None`

Optional `strftime` format string appended after `suffix` (e.g. `"%y%V"` → `customers_v2_2614`). Defaults to `None` because hotswap usually wants a stable predictable name; turn it on for throwaway sandbox tables.

## Computed: `name_resolved`

A derived `@property` returning the resolved table name: `name` with `suffix` (and optional `suffix_appendix`) applied. Equals `name` when `suffix` is empty. All DDL bollhav emits (`CREATE TABLE`, index/constraint names) uses `name_resolved` so a suffixed run never collides with the base table.

## schema

Type: `str` · Default: `""`

Destination schema name (just the name — `"warehouse"`). The dev/prod/PR isolation transform is two sibling fields:

- **`schema_suffix`** (`str`, default `""`) — appended as `_<suffix>`. Set per-run, usually from `SCHEMA_SUFFIX` (e.g. `$USER` in dev). Empty = bare schema.
- **`schema_suffix_appendix`** (`str | None`, default `"%y%V"`) — a `strftime` appended after the suffix (default = year+week, so suffixed schemas rotate weekly). `None` to disable.

`Target.schema_resolved` applies them: `warehouse` → `warehouse_pr123_2614_`. (It's a pure view over `schema` — the base name is never mutated, so resolution is idempotent. Note the trailing `_`, kept for back-compat; the *table*-name suffix on `name_resolved` does not add one.)

## catalog

Type: `str` · Default: `None` · **Required when `database` is set**

Destination catalog — the top of three-part addressing (`catalog.schema.table`). **Required on every database-backed model**: a model's identity is `catalog.schema.table`, and the catalog keeps names unique across databases in the shared library, so referencing by anything less risks collisions. Constructing a `Target` with a `database` but no `catalog` raises.

`Target.full_name` is then `catalog.schema_resolved.name_resolved`, and the catalog is added to the model's tags. Note that the catalog is **identity only** — `model.ref(name)` drops it when emitting SQL (it resolves to `schema.table`, since the catalog is the DSN you're already connected on). Abstract Targets (no `database`) may still omit it.

## database

Type: `Database` · Default: `None`

Target database. Required if `columns` is set.

## columns

Type: `list[PostgresColumn]` · Default: `[]`

Column definitions. Required if `database` is set.

## write_mode

Type: `WriteMode` · Default: `APPEND`

How to write data. See [Write modes](WRITEMODES.md) for the full list and trade-offs. (Whether a model is a table or a view is `kind=Kind.VIEW` on the [Model](KINDS.md), not a Target field.)

## dsn_env_var

Type: `str` · Default: `None`

DSN env var for the target connection.

## Computed: `partitioned_by`

A derived `@property` — set `partition_on=True` on the column you want to partition by; only one column may carry it.
