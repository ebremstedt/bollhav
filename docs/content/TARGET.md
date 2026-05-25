[← Model](MODEL.md)

# Target

Where data lands: the destination table, its schema, the database connection, column definitions, and the write mode.

## name

Type: `str` · Default: required

Destination table name.

## schema

Type: `TargetSchema` · Default: `TargetSchema()`

Destination schema. See [TargetSchema](TARGETSCHEMA.md) for the sub-fields.

## catalog

Type: `str` · Default: `None`

Destination catalog. Set for three-part addressing (`catalog.schema.table`) on warehouses like Snowflake, BigQuery, and Trino. When set, `Target.full_name` returns `catalog.schema.name` and the catalog is added to the model's tags.

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
