# MSSQL staging

Stage an MSSQL table through the **same lifecycle hooks as Postgres**.
Each daily interval's chunks bulk-insert into a per-interval staging
table; one transaction then MERGEs that staging table into the target
and drops it — so a crash mid-stream never leaves partial rows in the
target.

| | |
|---|---|
| backend | MSSQL (`Database.MSSQL`) |
| target write mode | `UPSERT_NO_DELETE` → apply step is a `MERGE` on the PK |
| staging | `MssqlStaging()` → chunks land in staging, then one atomic apply |
| state | none — MSSQL has no state coordination (setting `State()` is a hard error) |

## How it's wired

```
main.py          run_model(sales, conn)                      # bootstrap + compute intervals
run_model.py       @model_lifecycle  run_model(model, data_conn)        # assets + staging schema (MssqlData)
run_interval.py      @execute_lifecycle  run_interval(model, unit, data_conn)  # stage table → write → MERGE → drop
mock_read.py         read(model, unit)                                  # per-interval rows in size-chunks
src/models/sales.py  the Model definition
```

The lifecycle resolves the data backend from `model.target.database`:
`Database.MSSQL` → `MssqlData`, which exposes the same staging methods as
`PostgresData`, so one set of hooks drives both backends. `@execute_lifecycle`
owns the staging table's lifecycle (create → write → apply → drop); the
body in `run_interval.py` just `read()`s and `write()`s.

`bollhav.mssql.write` in staged mode **only lands chunks** — it keys the
staging table on `model.run_id`, the same id the hook used to create it.

## Run it

Needs a reachable MSSQL with the ODBC DSN in `BOLLHAV_MSSQL_DSN`
(see `../interval_batch/README.md` for a local Docker SQL Server + the
DSN string).

```bash
export BOLLHAV_MSSQL_DSN='DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=bollhav;UID=sa;PWD=Your_password123;TrustServerCertificate=yes'
python main.py
```

Three days, 5 000 rows each → `warehouse.sales` holds 15 000 rows. Each
day streams as 2 000 + 2 000 + 1 000 (three staging inserts), then one
MERGE. Re-running is idempotent — the MERGE folds the same `sale_id`
keys instead of inserting duplicates.

### Inspect

```sql
SELECT COUNT(*) FROM warehouse.sales;
SELECT TOP 5 * FROM warehouse.sales ORDER BY sale_id;

-- staging tables self-clean; this should be empty between runs
SELECT s.name AS [schema], t.name AS [table]
FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = 'z_warehouse';
```

### See staging do its job

Set `MssqlStaging(keep_after_apply=True)` on the model to keep the
per-interval staging tables around for inspection (auto-GC is then
disabled — cleanup is on you).
