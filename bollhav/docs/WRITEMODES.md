[back to README](../../README.md)

# Choosing a Write Mode

A practical guide to picking the right write mode.
## Decision trees

- [Which table mode?](WHICH_TABLE_MODE.md)
- [View or table?](VIEW_OR_TABLE.md)

---

## Mode-by-mode breakdown

### APPEND

| Property | |
|---|---|
| Idempotent | ⚠️ The write always succeeds (operation is idempotent), but re-running duplicates rows (table state is not) |
| Handles source deletes | ❌ |
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
- You re-run pipelines on failure (every retry appends duplicates)

---

### TRUNCATE_TABLE_INSERT

| Property | |
|---|---|
| Idempotent | ✅ Safe to re-run |
| Handles source deletes | ✅ Full reload erases stale rows |
| Handles duplicates | Depends — previous run duplicates are cleared by the truncate, but no check on duplicates within the incoming data |
| Partition-aware | ❌ Wipes everything, not just a window |
| Backfill | ⚠️ Safe to re-run but reloads the entire table on every batch — expensive for long historical ranges |
| Data volume | ⚠️ Fine for small-to-medium datasets — cost grows linearly with total table size since everything is reloaded every run |
| Persisted | ✅ Written to a physical table |
| Schemaless-friendly | ❌ Requires a stable, predefined schema — TRUNCATE does not fix schema drift |
| Risk of breaking target | Medium — table is briefly empty during the run |
| Effectiveness | Fast for small-to-medium datasets |

**Use when:**
- The full dataset fits comfortably in your pipeline (no need for incremental loads)
- Source deletes must be reflected in the target
- You want idempotency without complexity

**Avoid when:**
- The dataset is large — reloading everything every run is expensive
- Downstream queries run during your pipeline window (table is empty mid-run)
- You need partition-level granularity

---

### RECREATE_TABLE_INSERT

| Property | |
|---|---|
| Idempotent | ✅ Safe to re-run |
| Handles source deletes | ✅ Full reload |
| Handles duplicates | Depends — previous run duplicates are cleared by the drop, but no check on duplicates within the incoming data |
| Partition-aware | ❌ |
| Backfill | ⚠️ Safe to re-run but full reload every batch plus DDL overhead — only use if schema realignment is also needed during the backfill |
| Data volume | ❌ Poorly suited for large datasets — full reload every run plus DDL overhead makes this expensive at scale |
| Persisted | ✅ Written to a physical table |
| Schemaless-friendly | ⚠️ Better than other modes — drops and recreates the table on each run so schema drift is self-healing, but still requires a defined model |
| Risk of breaking target | High — drops the table entirely, including indexes and constraints |
| Effectiveness | Slightly slower than TRUNCATE due to schema recreation |

**Use when:**
- Your model's column definitions may have changed and the table schema needs to be realigned
- You are in early development and schema churn is expected
- A full reset is preferable to a migration

**Avoid when:**
- Other tables have foreign keys pointing at this table (drop will fail or cascade)
- Downstream consumers are sensitive to schema changes
- The dataset is large (same cost concern as TRUNCATE_TABLE_INSERT, plus DDL overhead)

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
- Source deletes within a window must be reflected

**Avoid when:**
- Your data does not have a reliable partition column
- Rows can legitimately span or shift across partition boundaries (re-running will delete and re-insert them inconsistently)

**Requirements:** `target.partitioned_by` must be set. `since` and `until` (UTC) must be passed at write time.

---

### UPSERT_NO_DELETE

| Property | |
|---|---|
| Idempotent | ⚠️ Partially — re-running updates existing rows but does not remove rows deleted from source |
| Handles source deletes | ❌ Deleted source rows remain in the target |
| Handles duplicates | Not allowed, checked — conflicts on unique key are resolved via ON CONFLICT |
| Partition-aware | ❌ |
| Backfill | ⚠️ Safe to re-run but rows deleted from source during the historical period accumulate permanently — can also be slow on large key spaces due to conflict checking |
| Data volume | ⚠️ Moderate — temp table and conflict checking add overhead that grows with the number of unique keys at scale |
| Persisted | ✅ Written to a physical table |
| Schemaless-friendly | ❌ Requires a defined unique key and a stable schema |
| Risk of breaking target | Low |
| Effectiveness | Moderate — uses a temp table and ON CONFLICT, heavier than COPY alone |

**Use when:**
- Rows need to be updated in place as source data changes
- Source deletes are irrelevant or handled elsewhere
- The table is too large to reload but individual rows need to stay current

**Avoid when:**
- Source deletes must be reflected (use RECREATE_PARTITION or TRUNCATE_TABLE_INSERT instead)
- You have no reliable unique key — mode requires at least one column with `unique=True`
- You need strict idempotency (stale rows from deleted source records accumulate silently)

**Requirements:** At least one column must have `unique=True`.

---

### VIEW

| Property | |
|---|---|
| Idempotent | ✅ |
| Handles source deletes | ✅ — query is re-evaluated on every read |
| Handles duplicates | Depends — entirely determined by the source query |
| Partition-aware | ✅ — depends on the query |
| Backfill | ✅ No backfill needed — the view always reflects the current state of the underlying data |
| Data volume | ⚠️ No write cost, but query cost scales with underlying data volume on every read |
| Persisted | ❌ No data stored — the view definition is saved but data is computed on every read |
| Schemaless-friendly | ❌ Requires a well-defined source query |
| Risk of breaking target | Low — only replaces the view definition |
| Effectiveness | No write cost, but query cost paid on every read |

**Use when:**
- You are building a **CONSUME layer** — views are ideal for exposing clean, renamed, or reshaped data to end consumers without duplicating storage
- You want to rename columns or restructure a table for a specific audience without touching the underlying data
- The result is cheap to compute on demand
- You need the result to always reflect the latest source state

**Avoid when:**
- The underlying query is expensive — every consumer pays the compute cost on every read, so if users are waiting on results, switch to a materialised mode like `TRUNCATE_TABLE_INSERT` or `RECREATE_PARTITION` and query the table instead
- Downstream tools expect a materialised table

**Requirements:** `model_type` must be `ModelType.VIEW`. `model.source.query` must be set.



## Summary

| Mode | Idempotent | Source deletes | Partitioned | Schema reset | Backfill | Volume | Persisted |
|---|---|---|---|---|---|---|---|
| `APPEND` | ⚠️ operation yes, table state no | ❌ | ❌ | ❌ | ✅ (duplicates if re-run) | Any | ✅ |
| `TRUNCATE_TABLE_INSERT` | ✅ | ✅ | ❌ | ❌ | ⚠️ full reload per batch | Small–medium | ✅ |
| `RECREATE_TABLE_INSERT` | ✅ | ✅ | ❌ | ✅ | ⚠️ full reload per batch | Small | ✅ |
| `RECREATE_PARTITION` | ✅ | ✅ (in window) | ✅ | ❌ | ✅ | Any | ✅ |
| `UPSERT_NO_DELETE` | ⚠️ rows are updated but deletes accumulate | ❌ | ❌ | ❌ | ⚠️ stale deletes accumulate | Medium | ✅ |
| `VIEW` | ✅ | ✅ | ✅ | ❌ | ✅ | N/A | ❌ |
