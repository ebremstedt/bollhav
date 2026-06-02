[← Model](MODEL.md)

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

Type: `TargetSchema` · Default: `TargetSchema()`

Destination schema. See [TargetSchema](TARGETSCHEMA.md) for the sub-fields.

## catalog

Type: `str` · Default: `None`

Destination catalog. Set for three-part addressing (`catalog.schema.table`) on warehouses like Snowflake, BigQuery, and Trino. When set, `Target.full_name` returns `catalog.schema_resolved.name_resolved` and the catalog is added to the model's tags.

## database

Type: `Database` · Default: `None`

Target database. Required if `columns` is set.

## columns

Type: `list[PostgresColumn]` · Default: `[]`

Column definitions. Required if `database` is set.

## model_type

Type: `ModelType` · Default: `TABLE`

`TABLE` or `VIEW`.

## write_mode

Type: `WriteMode` · Default: `APPEND`

How to write data. See [Write modes](MODES.md) for the full list and trade-offs.

## dsn_env_var

Type: `str` · Default: `None`

DSN env var for the target connection.

## Computed: `partitioned_by`

A derived `@property` — set `partition_on=True` on the column you want to partition by; only one column may carry it.

## actions / default_actions

Two lists of `Action` objects that drive the target's lifecycle. `default_actions` holds framework-supplied operations (CREATE SCHEMA / CREATE TABLE / DROP / TRUNCATE / CREATE INDEX / ADD UNIQUE / staging setup). `actions` holds user-added operations (GRANT / ANALYZE / COMMENT / project-specific hooks). The runner walks `default_actions ++ actions` once per pipeline run, gated by `target._applied_model_actions` so subsequent intervals short-circuit. See [Actions](ACTIONS.md).

## on_failure

Type: `OnFailure` · Default: `FAIL_FAST`

Per-target failure policy for MODEL/POST actions. `FAIL_FAST` re-raises and halts the pipeline POST sweep; `SKIP` logs a warning and continues to the next action. MODEL/PRE actions are always fail-fast — a half-failed setup cannot safely proceed to a write. See [Actions](ACTIONS.md).
