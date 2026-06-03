[Home](index.md) › [Model](MODEL.md) › [Target](TARGET.md) › **Write modes**

# Choosing a Write Mode

`write_mode` is a property of [Target](TARGET.md) (`WriteMode`, default `APPEND`). A practical guide to picking the right one: `APPEND`, `UPSERT_NO_DELETE`, or `RECREATE_PARTITION`. Views are not a write mode — a view is `kind=Kind.VIEW` on the [Model](KINDS.md).

## Pre-load table flags

Two flags on `Target` control what happens to the table **once, before any chunks are written**. They compose with any write mode.

| Flag | Effect |
|---|---|
| `recreate_table=True` | `DROP TABLE IF EXISTS` then `CREATE TABLE` before the load. Resets schema. |
| `truncate_table=True` | `CREATE TABLE IF NOT EXISTS` then `TRUNCATE TABLE` before the load. Keeps schema, wipes rows. |

Both default to `False`. Setting both is an error (recreate already leaves the table empty).

Because the destructive step runs exactly once before the chunk loop, these flags are safe with chunked (INTERVAL or ROW) batching — earlier chunks are not overwritten by later ones.

Typical recipes:
- **Full reload, keep schema** → `WriteMode.APPEND` + `truncate_table=True`
- **Full reload, reset schema** → `WriteMode.APPEND` + `recreate_table=True`
- **Full reload with deduplication** → `WriteMode.UPSERT_NO_DELETE` + `truncate_table=True`

---

## Mode-by-mode breakdown

### APPEND

| Property | |
|---|---|
| Idempotent | ⚠️ The write always succeeds (operation is idempotent), but re-running duplicates rows (table state is not) |
| Handles source deletes | ❌ (unless paired with `truncate_table` or `recreate_table`) |
| Handles duplicates | Allowed, no check — duplicates are blindly inserted |
| Partition-aware | ✅ Recommended — partitioning the table makes it easier to query and manage data over time, even though APPEND does not act on the partition during writes |
| Backfill | ✅ Works well if duplicates are acceptable downstream — re-running the same window will append again, so deduplication must be handled elsewhere if needed |
| Data volume | ✅ Handles any volume well — blind insert with no overhead per row |
| Persisted | ✅ Written to a physical table |
| Schemaless-friendly | ✅ Best fit — no schema enforcement, any shape of data can be written as long as the target table accepts it |
| Risk of breaking target | Low |
| Effectiveness | Very fast — blind insert, no conflict checks |

**Use when:**
- You are writing to a **RAW layer** — APPEND is the natural fit here, get data in fast and let downstream layers clean it up
- Losing a run means missing data, not corrupting it
- You want maximum write throughput

**Avoid when:**
- Downstream consumers expect unique rows
- You re-run pipelines on failure (every retry appends duplicates — unless paired with `truncate_table=True`)

**Pair with:**
- `truncate_table=True` for idempotent full reloads — wipes rows once, then chunks append cleanly
- `recreate_table=True` when the model's column set has changed and you want the table reshaped before the load

---

### RECREATE_PARTITION

| Property | |
|---|---|
| Idempotent | ✅ Safe to re-run for the same `since`/`until` window |
| Handles source deletes | ✅ Within the partition window |
| Handles duplicates | Depends — duplicates from previous runs within the window are cleared, but no check on duplicates within the incoming data |
| Partition-aware | ✅ Only touches rows in the `[since, until)` range |
| Backfill | ✅ The natural choice — each batch targets its own window independently, safe to parallelise and re-run |
| Data volume | ✅ Scales well for large tables — cost is proportional to the window size, not the total table size |
| Persisted | ✅ Written to a physical table |
| Schemaless-friendly | ❌ Requires a stable schema and a reliable partition column |
| Risk of breaking target | Low — only affects the specified time window |
| Effectiveness | Efficient for incremental loads on large tables |

**Use when:**
- Your data is naturally partitioned by time (e.g. event date, created_at)
- You process data in batches (hourly, daily) and need to be able to re-run a specific window
- The table is too large to reload fully each run
- SourceTable deletes within a window must be reflected

**Avoid when:**
- Your data does not have a reliable partition column
- Rows can legitimately span or shift across partition boundaries (re-running will delete and re-insert them inconsistently)

**Requirements:** `target.partitioned_by` must be set. `since` and `until` (UTC) must be passed at write time.

---

### UPSERT_NO_DELETE

| Property | |
|---|---|
| Idempotent | ⚠️ Partially — re-running updates existing rows but does not remove rows deleted from source |
| Handles source deletes | ❌ Deleted source rows remain in the target (unless paired with `truncate_table`) |
| Handles duplicates | Not allowed, checked — conflicts on unique key are resolved via ON CONFLICT |
| Partition-aware | ❌ |
| Backfill | ⚠️ Safe to re-run but rows deleted from source during the historical period accumulate permanently — can also be slow on large key spaces due to conflict checking |
| Data volume | ⚠️ Moderate — temp table and conflict checking add overhead that grows with the number of unique keys at scale |
| Persisted | ✅ Written to a physical table |
| Schemaless-friendly | ❌ Requires a defined unique key and a stable schema |
| Risk of breaking target | Low |
| Effectiveness | Moderate — uses a temp table and ON CONFLICT, heavier than a straight insert |

**Use when:**
- Rows need to be updated in place as source data changes
- SourceTable deletes are irrelevant or handled elsewhere
- The table is too large to reload but individual rows need to stay current

**Avoid when:**
- SourceTable deletes must be reflected (use RECREATE_PARTITION, or pair with `truncate_table=True`)
- You have no reliable unique key — mode requires at least one column with `unique=True`
- You need strict idempotency (stale rows from deleted source records accumulate silently)

**Requirements:** At least one column must have `unique=True`.

---

## Summary

| Mode | Idempotent | SourceTable deletes | Partitioned | Backfill | Volume | Persisted |
|---|---|---|---|---|---|---|
| `APPEND` | ⚠️ operation yes, table state no | ❌ (pair with `truncate_table`) | ❌ | ✅ (duplicates if re-run) | Any | ✅ |
| `RECREATE_PARTITION` | ✅ | ✅ (in window) | ✅ | ✅ | Any | ✅ |
| `UPSERT_NO_DELETE` | ⚠️ rows are updated but deletes accumulate | ❌ | ❌ | ⚠️ stale deletes accumulate | Medium | ✅ |

### Pre-load flags

| Flag | When to use |
|---|---|
| `recreate_table=True` | You want a full reload AND the schema should be reset each run (early dev, column drift) |
| `truncate_table=True` | You want a full reload but the schema should stay stable (production reload) |
