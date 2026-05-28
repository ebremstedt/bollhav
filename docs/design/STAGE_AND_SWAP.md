# Stage-and-swap: durable sub-batched writes per interval

Status: **proposal / not implemented**

## Problem

A state-enabled model runs one interval at a time, but a single interval can produce more rows than fit in memory (or one transaction). We want to commit in sub-batches (e.g. 10k rows at a time) **and** keep the interval as the atomic unit of state — partial writes from a crashed interval must not be visible in the target, and reruns must not duplicate previously committed sub-batches.

Out of scope:
- Tracking sub-batch progress in the state table. Sub-batches are an implementation detail of the write path; state granularity stays at `(since, until)`.

## Decision

Use stage-and-swap when the target's write mode can't make sub-batched writes naturally idempotent. Dispatch by write mode:

| Write mode | Strategy | Why |
|---|---|---|
| Upsert on PK (e.g. `UPSERT_NO_DELETE`) | None — write directly | Reruns overwrite prior sub-batches; idempotent for free |
| Append/insert with interval columns | Delete-by-tag, then stream-insert | `DELETE FROM target WHERE since=$1 AND until=$2` cleans partials in one small tx |
| Anything else (append without tag, multi-table, complex) | Stage-and-swap | General fallback |

This doc focuses on stage-and-swap. The other two paths are simpler and may land first.

## Mechanism

For each (model, interval) that uses staging:

1. **Stream into staging.** Sub-batches are committed to a per-interval staging table: `<state_schema>.<target_name>_staging_<run_id_short>`. Memory stays bounded — never hold more than one sub-batch in process.
2. **Final swap.** Once the interval's streaming finishes, run one small transaction:
   ```sql
   BEGIN;
     INSERT INTO target SELECT * FROM staging;
     DROP TABLE staging;
     UPDATE <name>_state
        SET status='applied', applied_at=now()
        WHERE since=$1 AND until=$2;
   COMMIT;
   ```
   The `INSERT...SELECT` runs server-side and is not constrained by our 10k chunk size. State flip and target write commit together — they cannot diverge.
3. **Crash recovery.** A crash mid-stream leaves an orphan staging table and a `pending` state row. The next invocation:
   - On entry, GC orphan staging tables for this model by name pattern (`<target>_staging_*` not matching any current `run_id`).
   - The pending interval is picked up normally; a fresh staging table is created under the new `run_id`.

## Staging table

- **Name:** `<state_schema>.<target_name>_staging_<run_id_short>` (use the first 8 chars of the run_id for readability; full run_id available on state row if disambiguation needed).
- **Schema:** mirror target columns, no indexes, no constraints. UNLOGGED on Postgres (cheap inserts, fine to lose on crash since they're not yet "committed" to the model's interval).
- **Lifecycle:** created lazily on first sub-batch write of an interval; dropped inside the final swap tx; orphan-GC'd on next invocation entry.

## Where staging lives

Two options, pick later:

- **Same DB as target.** Cheapest data movement (`INSERT...SELECT` is intra-DB). Staging table sits next to the target — needs a name convention that won't collide with user tables. Default to `z_*` schema if target is in the user's schema, same trick as the state-shares-target case.
- **State DB.** Keeps bollhav-owned tables in one place but requires cross-DB transfer for the swap, which breaks the "one small transaction" property — final commit becomes "copy from state DB to target DB" + "update state row," and those can no longer share an atomic boundary.

**Recommendation:** staging always co-locates with the target. The "atomic swap" property is load-bearing; without it we're back to a partial-write window.

## Public API sketch

Probably a context manager on the model:

```python
@state_tracker
@progress_bar
def execute(model, since, until):
    with model.stage(since=since, until=until) as stage:
        for chunk in read_source_in_chunks(model, since, until):
            stage.write(chunk)        # commits chunk to staging
        # context exit triggers the swap tx + state flip
```

`@state_tracker` would need to know that the swap tx already flipped state, so the decorator's normal `mark_applied` should become a no-op when a `stage` context was used. Two options:

- The `stage` context sets a flag on the model that `@state_tracker` checks.
- The decorator is restructured so the gate runs at entry (as today), but the apply step is delegated to whichever inner mechanism committed.

The second is cleaner but a bigger refactor.

## Interaction with `STATE_MODE` / `DISCOVER`

- `STATE_MODE=disrespect` — orphan staging tables get GC'd as usual on entry. State rows reset to pending. Reruns produce fresh staging tables. No change.
- `DISCOVER=true` — intervals come from state. Each pending row's interval gets a fresh staging table under the new run_id. No change.

## Concurrency

Two pipeline runs writing the same interval would collide. Options:

- **Advisory lock** on `(model_name, since, until)` at staging-table creation time.
- **Make staging-table creation a unique gate:** `CREATE TABLE ... IF NOT EXISTS` or fail-on-conflict. If two runs race, one wins and the other backs off.

Defer. Single-run-at-a-time is the assumed deployment for now.

## Open questions

- **Backend dispatch.** Postgres-first is fine; MSSQL implementation lands later under the same `_backend(model)` shim already in [bollhav/model/state.py](../../bollhav/model/state.py).
- **Auto-detect vs. opt-in.** Should bollhav pick stage-and-swap automatically based on `Model.target.write_mode`, or does the user opt in via `State(strategy="staging")`? Auto is friendlier; opt-in is explicit.
- **Per-interval swap vs. batched swap.** Swap after every interval (simple, more txs) or batch swaps at end-of-loop (one large final commit, longer at-risk window)? Per-interval is the default; batched is a future optimization.
- **DDL ownership.** Staging-table DDL needs to mirror target columns. Where does that introspection live? Probably reuse whatever already builds the target table for `ensure_tables`.
- **What does `stage.write(chunk)` actually do?** Probably a `COPY` on Postgres for speed. Need to make sure it composes with whatever the rest of bollhav uses for bulk insert.

## Why not sub-batch state rows

Tracking sub-batch progress in state would couple state granularity to the source's pagination semantics (offsets, watermarks). Pagination is fragile across reruns — sources change, rows shift, ordering isn't always stable. Keep state at the interval grain; let idempotency or stage-and-swap make the write path itself crash-safe.

## Related

- [State tracking](../content/STATE.md) — current state-tracking docs (interval-grain, no sub-batching).
- [bollhav/model/state.py](../../bollhav/model/state.py) — `State`, `StateMode`, `@state_tracker`.
- [bollhav/postgres/state.py](../../bollhav/postgres/state.py) — Postgres backend; staging implementation would land here as a sibling to `ensure_tables`, `prefill`, etc.
