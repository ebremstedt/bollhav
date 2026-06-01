# staging_append

End-to-end exercise of staging with **APPEND** on both sides — chunks
bulk-insert into staging, then staging bulk-inserts into the target.
The simplest staged write pattern.

```
DataFrame chunks ──COPY──► staging table ──INSERT FROM──► target table
   (each chunk           (one per interval,              (one INSERT per
    its own small tx)     UNLOGGED by default)            interval, atomic)
```

## What this demonstrates

- 3 `@daily` intervals (2024-01-01 → 2024-01-04)
- ~500 rows per interval, yielded in **200-row chunks** by the mock reader
- Each chunk COPYs into a per-interval staging table
  (`z_warehouse.orders_staging_<run_id_short>`)
- After the chunked stream finishes for an interval, **one transaction**
  applies staging → `warehouse.orders` (INSERT), and flips the
  corresponding row in `z_warehouse.orders_state` to `applied`

The model config:

```python
Target(
    name="orders",
    write_mode=WriteMode.APPEND,     # target-side: how staging lands in target
    staging=Staging(),               # staging-side: APPEND by default
    ...
)
```

Both sides are APPEND. Each chunk just COPYs raw rows into staging
(no dedup); the final apply is a single `INSERT INTO target SELECT
FROM staging`.

## When to use this combination

- Rows are append-only by nature (events, logs, telemetry, daily snapshots)
- No duplicate keys within an interval, or duplicates are fine
- You want the cheapest possible chunk-to-staging hop (COPY beats UPSERT
  by ~2-3×)

If your data has duplicate keys within an interval and you want the
target deduped, see [`staging_upsert`](../staging_upsert/) instead.

## Setup

Requires a running Postgres reachable via `$TARGET_DSN`.

```bash
cd examples/staging_append

export TARGET_DSN="postgresql://USER:PASSWORD@localhost:5432/DBNAME"
export TAGS="[orders]"
export USE_SCHEMA_SUFFIX=false
export BACKFILL_SINCE=2024-01-01T00:00:00Z
export BACKFILL_UNTIL=2024-01-04T00:00:00Z   # 3 daily intervals

python main.py
```

`TAGS` uses bracket syntax — bollhav's tag matcher expects `[group]` form.

Expected output (run_id will differ):

```
── runtime ── ( model ) ────
  tags              [orders]
  mode              backfill  2024-01-01T00:00:00+00:00 → 2024-01-04T00:00:00+00:00
────────────────────────────

warehouse.orders  3 interval(s) to process
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

A second run without dropping should be a no-op — `@state` gates each
interval on its state row and skips applied ones.

## Verify

```sql
-- Target has ~1500 rows (3 intervals × ~500).
SELECT count(*) FROM warehouse.orders;

-- State table: 3 rows, all status='applied', same run_id, applied_at populated.
SELECT since AT TIME ZONE 'UTC' AS since_utc,
       until AT TIME ZONE 'UTC' AS until_utc,
       status, applied_at,
       substr(run_id::text, 1, 8) AS run_id_short
FROM z_warehouse.orders_state
ORDER BY since;
```

## Inspecting staging mid-flight

Set `keep_after_apply=True` in `src/models/orders.py` to keep the
staging tables around after each apply — useful when you want to
diff what was staged against what landed:

```python
staging=Staging(keep_after_apply=True),
```

Auto orphan GC is disabled when `keep_after_apply=True`; clean up
manually with:

```sql
DROP TABLE z_warehouse.orders_staging_<run_id_short>;
```

## What does NOT get tested here

- The exception path: crashing mid-stream leaves an orphan staging
  table and a `pending` state row. To exercise: add a
  `raise RuntimeError("simulated crash")` partway through `mock_read.read`,
  re-run, and confirm the next clean run picks up the still-pending
  interval. `gc_orphan_staging_tables(model)` would clean the orphan.
- Cross-DB state — not supported by the staging path.
- UPSERT_NO_DELETE and RECREATE_PARTITION target modes — see
  [`staging_upsert`](../staging_upsert/) for upsert coverage.
