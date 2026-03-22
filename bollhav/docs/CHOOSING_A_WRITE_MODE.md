[back to README](..README.md)

# Choosing a Write Mode

A practical guide to picking the right write mode. The wrong choice won't always break things immediately — but it will eventually cause silent data issues, duplicate rows, or unnecessary load on your database.

## The decision tree

```
Is your target a view (no data stored)?
└── Yes → VIEW

Does your data arrive in time-based partitions (e.g. daily batches)?
└── Yes → RECREATE_PARTITION

Does your dataset fit entirely in memory and change completely each run?
├── Yes, and schema may drift → RECREATE_TABLE_INSERT
└── Yes, schema is stable → TRUNCATE_TABLE_INSERT

Do rows need to be updated in place (no full reload)?
├── Yes, and source deletes don't matter → UPSERT_NO_DELETE
└── No, just append and duplicates are fine → APPEND
```

---

## Mode-by-mode breakdown

### APPEND

| Property | |
|---|---|
| Idempotent | ❌ Re-running duplicates rows |
| Handles source deletes | ❌ |
| Handles duplicates | ❌ |
| Partition-aware | ❌ |
| Risk of breaking target | Low |
| Effectiveness | Very fast — blind insert, no conflict checks |

**Use when:**
- You are writing to a raw/landing layer where duplicates are acceptable or deduplicated downstream
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
| Handles duplicates | ✅ Table is wiped first |
| Partition-aware | ❌ Wipes everything, not just a window |
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
| Handles duplicates | ✅ Table is dropped and recreated |
| Partition-aware | ❌ |
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
| Handles duplicates | ✅ Within the partition window |
| Partition-aware | ✅ Only touches rows in the `[since, until)` range |
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
| Handles duplicates | ✅ Conflicts on unique key are resolved by updating |
| Partition-aware | ❌ |
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
| Handles duplicates | ✅ — depends on the query |
| Partition-aware | ✅ — depends on the query |
| Risk of breaking target | Low — only replaces the view definition |
| Effectiveness | No write cost, but query cost paid on every read |

**Use when:**
- You want a logical rename or reshape of an existing table without persisting data
- The result is cheap to compute on demand
- You need the result to always reflect the latest source state

**Avoid when:**
- The underlying query is expensive — every consumer pays the compute cost
- Downstream tools expect a materialised table

**Requirements:** `model_type` must be `ModelType.VIEW`. `model.source.query` must be set.

---

## Summary

| Mode | Idempotent | Source deletes | Duplicates | Partitioned | Schema reset |
|---|---|---|---|---|---|
| `APPEND` | ❌ | ❌ | ❌ | ❌ | ❌ |
| `TRUNCATE_TABLE_INSERT` | ✅ | ✅ | ✅ | ❌ | ❌ |
| `RECREATE_TABLE_INSERT` | ✅ | ✅ | ✅ | ❌ | ✅ |
| `RECREATE_PARTITION` | ✅ | ✅ (in window) | ✅ (in window) | ✅ | ❌ |
| `UPSERT_NO_DELETE` | ⚠️ | ❌ | ✅ | ❌ | ❌ |
| `VIEW` | ✅ | ✅ | ✅ | ✅ | ❌ |
