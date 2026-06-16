# MSSQL staging + Postgres state

A model whose **data** lives in MSSQL (staged through the same lifecycle
hooks as Postgres) but whose **state** lives in **Postgres**. Each daily
interval's chunks bulk-insert into a per-interval MSSQL staging table; one
transaction MERGEs it into the target and drops it; and the state machine —
on a separate Postgres connection — gates and records each interval.

| | |
|---|---|
| data backend | MSSQL (`Database.MSSQL`) — `data_conn` (pyodbc) |
| target write mode | `UPSERT_NO_DELETE` → apply step is a `MERGE` on the PK |
| staging | `MssqlStaging()` → chunks land in staging, then one atomic apply |
| state | **Postgres** — `State()` on the model, `state_conn` (psycopg) |

State always lives in Postgres (the only backend). An MSSQL-data model is
fully allowed to be state-tracked — it just keeps its state in Postgres, so
the run opens **two connections** and passes both. `@model_lifecycle`'s
`_conns` enforces it: pass only the MSSQL connection and you get a clear
error, not a driver crash.

## How it's wired

```
main.py          run_model(run, data_conn, state_conn)               # opens BOTH connections
run_model.py       @model_lifecycle  run_model(run, data_conn, state_conn)   # MSSQL assets + Postgres state bootstrap
run_interval.py      @execute_lifecycle  run_interval(run, ival, data_conn, state_conn)  # gate (PG) → stage→write→MERGE→drop (MSSQL) → mark applied (PG)
mock_read.py         read(run, ival)                                   # per-interval rows in size-chunks
src/models/sales.py  the Model definition (Database.MSSQL + MssqlStaging + State)
```

The lifecycle resolves the data backend from `model.target.database`:
`Database.MSSQL` → `MssqlData`, which exposes the same staging methods as
`PostgresData`. The state machine always runs `PostgresState` on `state_conn`.
So data/staging happen in MSSQL, gating/marking in Postgres, keyed by the
model's full name.

`bollhav.mssql.write` in staged mode **only lands chunks** — it keys the
staging table on `run.run_id`, the same id the hook used to create it.

## Run it

Needs a reachable MSSQL (`BOLLHAV_MSSQL_DSN`, ODBC) **and** a reachable
Postgres (`STATE_DSN`). Easiest is the bundled stack:

```bash
docker compose up --build      # starts MSSQL + Postgres, then runs the example
```

Or against your own databases:

```bash
export BOLLHAV_MSSQL_DSN='DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=bollhav;UID=sa;PWD=Your_password123;TrustServerCertificate=yes'
export STATE_DSN='postgresql://postgres:postgres@localhost:5432/postgres'
python main.py
```

Three days, 5 000 rows each → `warehouse.sales` holds 15 000 rows. Each
day streams as 2 000 + 2 000 + 1 000 (three staging inserts), then one
MERGE, then its Postgres state row flips to `applied`. **Re-running does
nothing** now — the applied gate (in Postgres) skips every interval. `main.py`
drops the Postgres `z_bollhav` state schema on each run so the demo always
starts fresh; remove that reset to see the gate in action.

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
