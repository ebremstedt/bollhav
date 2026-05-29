# staging_testing

End-to-end exercise of the staging feature against a real Postgres.

Demonstrates the full flow:

- 3 `@daily` intervals (2024-01-01 → 2024-01-04)
- 5000 rows per interval, yielded in **2000-row chunks** by the mock reader
- Each chunk COPYs into a per-interval staging table
  (`z_warehouse.orders_staging_<run_id_short>`)
- After the chunked stream finishes for an interval, **one transaction**
  moves staging → `warehouse.orders`, drops the staging table, and
  flips the corresponding row in `z_warehouse.orders_state` to
  `applied`. Atomic — data in target ↔ state row says applied.

Total: 9 COPYs into staging, 3 atomic flushes, 3 state-row transitions.

## What this exercises end-to-end

| Layer | What you'll see |
|---|---|
| Model config | `Target(staging=Staging())` + `state=State()` opt-in |
| `write()` dispatch | Routes to the staged path because `target.staging is not None` |
| `bollhav.postgres.staging` | DDL on UNLOGGED staging table, COPY per chunk, atomic flush, state row UPDATE |
| `@state` | Bypasses its own `mark_applied` because the staged flush already did it |

## Setup

Requires a running Postgres reachable via `$TARGET_DSN`. Use whatever
local instance you already have — the example needs a regular database
and a role that can `CREATE SCHEMA`.

## Run

```bash
cd examples/staging_testing

export TARGET_DSN="postgresql://USER:PASSWORD@localhost:5432/DBNAME"
export TAGS="[orders]"
export USE_SCHEMA_SUFFIX=false
export BACKFILL_SINCE=2024-01-01T00:00:00Z
export BACKFILL_UNTIL=2024-01-04T00:00:00Z   # 3 daily intervals

python main.py
```

Note the `TAGS` value uses bracket syntax — bollhav's tag matcher
expects `[group]` form.

Expected output (run_id will differ):

```
── runtime ── ( model ) ────
  tags              [orders]
  mode              backfill  2024-01-01T00:00:00+00:00 → 2024-01-04T00:00:00+00:00
────────────────────────────

warehouse.orders  run_id=a1b2c3d4…  3 interval(s)
  2024-01-01 → 2024-01-02
  2024-01-02 → 2024-01-03
  2024-01-03 → 2024-01-04
```

### Re-running

The model has no `recreate_table=True`, so the target accumulates
across runs. To start fresh, drop the schemas:

```bash
psql "$TARGET_DSN" -c "DROP SCHEMA IF EXISTS warehouse CASCADE; \
                       DROP SCHEMA IF EXISTS z_warehouse CASCADE;"
```

A second run without dropping should be a no-op — `@state`
gates each interval on its state row and skips applied ones.

## Verify

After a clean run, inspect Postgres:

```sql
-- Target now has 15000 rows (3 intervals × 5000).
SELECT count(*) FROM warehouse.orders;
-- 15000

-- Distribution across the window.
SELECT date(order_date) AS day, count(*) FROM warehouse.orders
GROUP BY day ORDER BY day;
--   day      | count
-- ---------- | -----
-- 2024-01-01 |  4803
-- 2024-01-02 |  4975
-- 2024-01-03 |  5033
-- 2024-01-04 |   189   (rng overflow from interval 3)

-- State table: 3 rows, all status='applied', same run_id, applied_at populated.
SELECT since AT TIME ZONE 'UTC' AS since_utc,
       until AT TIME ZONE 'UTC' AS until_utc,
       status, applied_at,
       substr(run_id::text, 1, 8) AS run_id_short
FROM z_warehouse.orders_state
ORDER BY since;

-- Staging tables: none — each was dropped inside its flush tx.
SELECT tablename FROM pg_tables
WHERE schemaname = 'z_warehouse' AND tablename LIKE 'orders_staging_%';
-- (0 rows)
```

## Resumability

Re-running `python main.py` is a no-op for already-applied intervals:

- `@state` gates each interval on its state row. `applied`
  rows short-circuit before `execute` even reads.
- The pre-fill step uses `StateMode.RESPECT` — applied rows survive,
  only fresh intervals get pending rows inserted.

To force a full rerun, drop the state table and the target table, or
add a `StateMode.DISRESPECT` toggle to your bootstrap.

## Inspecting staging mid-flight

Want to see staging tables in action? Set `keep_after_flush=True` in
`src/models/orders.py`:

```python
staging=Staging(keep_after_flush=True),
```

Then the per-interval staging tables stay around after a successful
flush — you can compare what was staged against what landed. Auto orphan
GC is disabled when `keep_after_flush=True`; clean up manually with:

```sql
DROP TABLE z_warehouse.orders_staging_<run_id_short>;
```

## What does NOT get tested here

- The exception path: crashing mid-stream leaves an orphan staging
  table and a `pending` state row. To exercise this manually, add a
  `raise RuntimeError("simulated crash")` partway through `mock_read.read`,
  re-run, and confirm the next clean run picks up the still-pending
  interval. (`gc_orphan_staging_tables(model)` would clean the orphan
  staging table.)
- Cross-DB state — not yet supported by the staging path.
- Write modes other than APPEND — staging currently rejects them with
  `NotImplementedError`.
