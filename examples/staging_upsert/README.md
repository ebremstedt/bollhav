# staging_upsert

End-to-end exercise of staging with **UPSERT_NO_DELETE on both sides** —
chunks MERGE into staging (dedup as data arrives), then staging MERGEs
into the target.

```
chunks ──MERGE on id──► staging table ──MERGE on id──► target table
   ↑                       ↑                              ↑
   dupes within a          one row per id,                one row per id,
   single chunk also       holds the latest               applied with the
   collapse                status seen so far             latest status
```

The model config:

```python
Target(
    name="orders",
    write_mode=WriteMode.UPSERT_NO_DELETE,   # target-side: MERGE staging → target
    staging=Staging(
        write_mode=WriteMode.UPSERT_NO_DELETE,   # staging-side: MERGE chunks → staging
    ),
    ...
)
```

## When would you want to upsert to a staging table instead of appending?

This is the question this example is built to answer. There are four
distinct reasons.

### 1. Duplicate keys within the source stream

Upstream sources (CDC streams, event topics, retry-prone APIs) often
emit the same business key multiple times within a single interval —
the same order id arriving as `pending`, then `processing`, then
`shipped`. With `staging.write_mode=APPEND`, staging accumulates all
three rows. The eventual target MERGE then has to handle multiple
source rows per key.

In Postgres, `INSERT ... ON CONFLICT (id) DO UPDATE` from a source
table with N rows per id produces **arbitrary** behavior — only one
of the N rows wins, and which one depends on physical row order. If
your CDC stream is ordered (e.g. by `updated_at`), that ordering can
be lost in the COPY → staging table. You either need `ORDER BY` in
the source SELECT (extra cost) or `DISTINCT ON` (more cost), or you
push the dedup earlier — to staging.

With `staging.write_mode=UPSERT_NO_DELETE`, dedup happens **as chunks
arrive**. Staging only ever holds one row per id. The final apply is
a clean MERGE on already-deduped rows.

### 2. Staging table stays small on high-churn intervals

If an interval emits 10M rows across 100k distinct ids (100 updates
per id on average), APPEND-into-staging produces a 10M-row staging
table; the target MERGE scans 10M source rows.

UPSERT-into-staging produces a 100k-row staging table; the target
MERGE scans 100k source rows. **100× less scan work on the apply
step.** The trade-off: you paid extra work on each chunk (the MERGE
itself is more expensive than COPY), but you saved that and more on
the apply.

This is the "early dedup" optimization. Worth it when:
  - The intervals have a high duplicate rate
  - The apply lock matters (target locked during MERGE — short tx is
    better)
  - You're memory-constrained on the chunked-read side anyway

Not worth it when:
  - Duplicates are rare (paying extra MERGE cost per chunk for ~no
    dedup benefit)
  - Staging is on the same tier as the target and the COPY → staging
    hop is your bottleneck

### 3. MERGE correctness without ORDER BY tricks

In some MERGE dialects (MSSQL `MERGE`, ANSI SQL `MERGE`), having
multiple source rows that match the same target row is a **runtime
error**, not just undefined behavior. You can't even run the apply
without first deduping the source.

If your staging is deduped (via `staging.write_mode=UPSERT_NO_DELETE`),
this category of error disappears. You don't have to write `WITH
ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY
updated_at DESC) AS rn FROM staging) ... WHERE rn = 1` boilerplate
into the apply step.

### 4. Reads against staging mid-flight make sense

If you set `Staging(keep_after_apply=True)` for audit/debugging,
having a deduped staging is much more readable. "Show me what got
applied for this interval" is one row per id, not a row per CDC event.

---

That said, **APPEND-into-staging is the right default** when:
  - Rows are genuinely append-only (event logs, telemetry, daily
    snapshots) — there are no dupes by construction
  - You don't have a unique constraint on the target table at all
  - The COPY → staging hop dominates your wall-clock and you can't
    afford the per-chunk MERGE cost

See [`staging_append`](../staging_append/) for that pattern.

## What this demo shows

The mock reader emits each interval as ~300 orders × 4 updates each
= 1200 rows total, with the duplicates intentionally:
  - Spread across chunks (`id=42` seen in chunks 1 and 4)
  - And within chunks (`id=42` appearing twice in chunk 1 as `pending`
    and `processing`)

After the run:
  - Target `warehouse.orders` holds **300 rows per interval**, not 1200.
  - The `status` column reflects the latest progression step seen for
    that order id in that interval (`delivered` for most).
  - Each `s.write(chunk)` triggers `INSERT INTO staging ... ON CONFLICT
    (id) DO UPDATE` — visible in `EXPLAIN ANALYZE` if you instrument.

## Setup

```bash
cd examples/staging_upsert

export TARGET_DSN="postgresql://USER:PASSWORD@localhost:5432/DBNAME"
export TAGS="[orders]"
export USE_SCHEMA_SUFFIX=false
export BACKFILL_SINCE=2024-01-01T00:00:00Z
export BACKFILL_UNTIL=2024-01-04T00:00:00Z   # 3 daily intervals

python main.py
```

## Verify

```sql
-- Exactly 900 rows total (3 intervals × 300 distinct ids).
-- Without upsert-to-staging dedup, you'd see arbitrary/undefined behavior
-- on the final MERGE because of duplicate source rows.
SELECT count(*) FROM warehouse.orders;
-- 900

-- Each id appears exactly once.
SELECT id, count(*) FROM warehouse.orders GROUP BY id HAVING count(*) > 1;
-- (0 rows)

-- Status distribution — should be heavily 'delivered' (the last step
-- in STATUS_PROGRESSION). If you see mostly 'pending', upsert ordering
-- is broken.
SELECT status, count(*) FROM warehouse.orders GROUP BY status ORDER BY 2 DESC;

-- State table: 3 applied rows.
SELECT since AT TIME ZONE 'UTC' AS since_utc,
       until AT TIME ZONE 'UTC' AS until_utc,
       status, applied_at
FROM z_warehouse.orders_state
ORDER BY since;
```

## Comparing append-to-staging vs upsert-to-staging

Want to see the difference? Edit `src/models/orders.py`:

```python
# Switch staging-side back to APPEND
staging=Staging(write_mode=WriteMode.APPEND),
```

Then re-run after wiping the target:

```bash
psql "$TARGET_DSN" -c "DROP SCHEMA IF EXISTS warehouse CASCADE; \
                       DROP SCHEMA IF EXISTS z_warehouse CASCADE;"
python main.py
```

Compare:
- Watch staging table size (`keep_after_apply=True` lets you inspect).
- With APPEND-staging it holds 1200 rows per interval; with UPSERT it
  holds 300.
- Final target row counts should still match (Postgres `ON CONFLICT
  DO UPDATE` collapses the dupes at apply time), but at the cost of
  the apply MERGE scanning 4× more rows.

## Re-running

Same as `staging_append`: the model has no `recreate_table=True`, so
the target accumulates across runs. To start fresh:

```bash
psql "$TARGET_DSN" -c "DROP SCHEMA IF EXISTS warehouse CASCADE; \
                       DROP SCHEMA IF EXISTS z_warehouse CASCADE;"
```

A second run without dropping is a no-op — `@state` gates each
interval on its `applied` state row.
