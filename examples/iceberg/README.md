# iceberg — the intelligence-src-entity-raw pattern, on Iceberg, locally

A schemaless raw-layer ingestion with the same file split as the
`intelligence-src-entity-raw` image, but with two swaps so it runs on a laptop
with nothing installed but Python:

* the MSSQL `viewreader` source is replaced by made-up rows, and
* the Postgres target is replaced by Iceberg tables in a local catalog.

Everything in between — the per-(hospital, view) model fan-out, the
`@model_lifecycle` / `@execute_lifecycle` hooks, the payload transform — is
unchanged.

```
src/
  main.py          @load_models        connect the catalog, loop the runs
  execute.py       @model_lifecycle    execute_model: namespace + table, then per interval
                   @execute_lifecycle  execute_interval: read -> transform -> write
  read.py                              made-up source rows (stands in for pyodbc)
  transform.py                         source columns -> _data_modified, _metadata_modified, payload
  catalog.py                           the pyiceberg Catalog (sqlite here, Hive in prod)
  blueprints.py                        which hospitals x which views become models
  models/raw.py                        create_model(table, hospital) -> Model
  peek.py                              read the tables back
  duckdb_views.py                      build warehouse/lake.duckdb for DataGrip / duckdb CLI
warehouse/                             created on first run: catalog.db + Parquet (gitignored)
```

## How it maps to the Postgres version

| intelligence-src-entity-raw | here |
|---|---|
| `Database.POSTGRES` | `Database.ICEBERG` |
| `PostgresColumn(... JSONB)` for `payload` | `IcebergColumn(... STRING)` — Iceberg has no JSON type |
| `staging=PostgresStaging()` | none — Iceberg commits are atomic already, the adapter rejects staging |
| `state=State()` | none — state lives in Postgres and this runs with no database (see below) |
| `psycopg.connect(dsn)` as `data_conn` | a pyiceberg `Catalog` as `data_conn` |
| `query_builder` building `SELECT * FROM viewreader.x WHERE TimestampRead ...` | `read_source(table, hospital, since, until)` inventing rows in that window |
| `bollhav.postgres.write(conn, run, df_gen, since, until)` | `bollhav.iceberg.write(conn, run, df_gen, since, until)`, same signature, `conn` is the Catalog |

`Target.catalog="local"` is the pyiceberg catalog **name**. `main.py` connects
one catalog per distinct name it sees on the matched models and passes it to
the lifecycle. Which backend that catalog uses is decided in `catalog.py`.

## Install

```bash
pip install 'bollhav[iceberg]' 'pyiceberg[sql-sqlite]'
```

The `iceberg` extra brings pyiceberg and pyarrow. `sql-sqlite` adds sqlalchemy
for the local `SqlCatalog`. Against a Hive Metastore you would install
`pyiceberg[hive]` instead.

## Run

From this folder. A week of backfill:

```bash
export TAGS="[all]"
export BACKFILL_ENABLED=True
export RUN_SINCE=2024-01-01
export RUN_UNTIL=2024-01-08
export TIMEZONE_OVERRIDE=UTC
export USE_SCHEMA_SUFFIX=False
export DEBUG=True
python src/main.py
```

Yesterday, the way the daily cron would run it:

```bash
export TAGS="[all]"
export LATEST_ENABLED=True
export USE_SCHEMA_SUFFIX=False
python src/main.py
```

One hospital, or one view:

```bash
export TAGS="[intelligence_raw_ste]"
export TAGS="[vCodes_Drugs]"
```

Read it back:

```bash
python src/peek.py
```

Each table prints its row count and snapshot count. `Batch(size=25)` in the
model and 50 rows per interval means every interval lands as two Iceberg
commits, so a one-week backfill shows two snapshots per table.

## What a run does

1. `@load_models` reads the env, matches models by `TAGS`, resolves the window.
2. `@model_lifecycle` sees `Database.ICEBERG` and uses `IcebergData` to create
   the namespace (`intelligence_raw_ste`) and the table from the declared
   `IcebergColumn`s. Indexes and constraints are skipped with a debug log —
   Iceberg has neither.
3. Per interval, `read_source` yields polars frames with the source columns
   plus a naive Stockholm `TimestampRead`, `transform` localises it to UTC as
   `_data_modified` and packs every column into a JSON `payload`, and
   `write_dataframes` casts each chunk to the declared arrow schema and appends
   it as one commit.

## Browsing the tables in DataGrip

DataGrip cannot read Parquet or an Iceberg catalog by itself, so the bridge is
DuckDB, whose driver DataGrip bundles. `duckdb_views.py` builds a DuckDB file
with one schema per namespace and one view per table:

```bash
pip install duckdb
python src/duckdb_views.py        # -> warehouse/lake.duckdb, 8 views
```

Each view is `iceberg_scan('<table dir>', version='?')`, which resolves the
latest Iceberg metadata at query time. So after a pipeline run you just rerun
your query; rebuild the file only when tables are added. It is written to a
temp name and renamed into place, so it can be rebuilt while DataGrip holds
the old one open (reconnect to pick up the new views).

In DataGrip:

1. **New Data Source → DuckDB.** Let it download the driver if asked. File:
   `<repo>/examples/iceberg/warehouse/lake.duckdb`.
2. **Options tab → Startup script.** DuckDB refuses to guess the latest
   metadata file unless each session opts in:

   ```sql
   INSTALL iceberg; LOAD iceberg;
   SET unsafe_enable_version_guessing = true;
   ```

3. Optional, **Advanced tab**: set `duckdb.read_only` to `true`, so DataGrip
   never takes DuckDB's single writer lock and a `duckdb` CLI can open the
   same file alongside it.
4. Connect, refresh, and the schemas `intelligence_raw_ste` and
   `intelligence_raw_sts` show up in the tree with their tables. `payload` is
   a JSON string, so DuckDB's JSON operators work on it:

   ```sql
   SELECT payload->>'DrugName' AS drug, count(*)
   FROM intelligence_raw_sts.vCodes_Drugs
   GROUP BY 1 ORDER BY 2 DESC;
   ```

The catalog itself is `warehouse/catalog.db`, a plain SQLite file. Open it as
a **SQLite** data source to see `iceberg_tables`, the list of table pointers
(`metadata_location` per table), which is all a catalog stores.

## Pointing it at Hive

Only `catalog.py` changes:

```python
from pyiceberg.catalog import load_catalog

load_catalog(
    name,
    type="hive",
    uri="thrift://metastore:9083",
    warehouse="s3://lake/warehouse",
    # s3.endpoint, s3.access-key-id, s3.secret-access-key
)
```

Models, hooks, transform and writer stay as they are: they only ever see a
`Catalog`.

## Caveats

* **No state, so no gating.** Rerun the same window and `APPEND` writes the
  rows again. The production image avoids that with `state=State()`, which
  needs a Postgres `state_conn` passed alongside the catalog `data_conn`.
* **Views are rejected.** A `Materialization.VIEW` model on an Iceberg target
  raises `IcebergViewsNotSupportedError`: pyiceberg's Sql and Hive catalogs
  cannot create views (only the REST catalog can, since pyiceberg 0.11).
* **`warehouse/` is throwaway.** Delete it to start from an empty catalog.
