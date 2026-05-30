[← Target](TARGET.md)

# Staging

Per-target opt-in for memory-bounded chunked writes plus atomic per-interval finalization. Set `Target(staging=Staging(...))` and the `write()` dispatcher routes through the staged path: sub-batches `COPY` into a staging table; one transaction at the end moves the rows into the target and (when `state=State(...)` is also set) flips the state row to `applied`.

## What you get

- **Memory bounding.** Sub-batches go straight to a staging table via `COPY`; nothing accumulates in Python.
- **Atomic per-interval finalization.** The flush commits `INSERT INTO target SELECT * FROM staging` + (optional) state flip together. Either the target has every row of the interval *and* the state row says `applied`, or neither.
- **Resumability** (when paired with state). A crash mid-stream leaves the staging table behind and the state row stays non-`applied`; the next run picks up that interval.
- **Concurrency-safe naming.** The staging table name embeds the bootstrap-minted `run_id`, so two workers running on the same model don't collide on one shared table.

## Opting in

```python
from bollhav.model import Model, Target, Staging, State

Model(
    target=Target(
        name="orders",
        schema=TargetSchema(name="warehouse"),
        dsn_env_var="TARGET_DSN",
        staging=Staging(),       # ← opt-in
        columns=[...],
    ),
    state=State(),               # optional — see "Without state" below
    batching=Batch(...),
)
```

## `StagingMode` — table lifecycle

The staging table can live for the whole pipeline run or be recreated each interval. Configure via `Staging(mode=...)`.

| Mode | DDL per interval | Total DDL on a 365-interval backfill | Use it when |
|---|---|---|---|
| `REUSED` (**default**) | `TRUNCATE TABLE` (only from interval 2 onward) | 1 × `CREATE SCHEMA` + 1 × `CREATE TABLE` + 364 × `TRUNCATE` | almost always — minimal catalog churn |
| `INTERVAL` | `CREATE TABLE` + `DROP TABLE` (in flush) | 1 × `CREATE SCHEMA` + 365 × `CREATE` + 365 × `DROP` | you want a forensic snapshot of the crashed-interval staging table on failure |

`REUSED` records two PRE_MODEL actions in `target._applied_model_actions` — `staging_schema_created` and `staging_table_created` — so subsequent intervals short-circuit before issuing the `CREATE`s. See [Actions](ACTIONS.md).

`INTERVAL` doesn't flip `staging_table_created` (the table is genuinely new each interval, so the "did this DDL fire?" flag has nothing to gate).

Both modes share the same table-name shape (`<prefix><run_id_short>`), so parallel workers on the same model don't collide regardless of mode.

## Staging without state

`state=State(...)` is optional. With staging-only:

- Memory bounding and per-interval atomic finalization still work — `flush` issues `INSERT INTO target SELECT * FROM staging` atomically, just **without** flipping a state row that doesn't exist.
- The user's loop re-runs every interval every time. There's no `applied` gate to skip already-completed work. Often what you want for a pure memory-bounding play.
- Re-runs are safe as long as your write is idempotent (e.g. `WriteMode.UPSERT_NO_DELETE`) or you accept the duplicate-write behaviour of `APPEND`.

```python
Target(
    name="big_dump",
    dsn_env_var="TARGET_DSN",
    staging=Staging(),           # no state= on the model
    columns=[...],
)
```

## Orphan staging tables

A crash mid-stream leaves the per-pipeline-run staging table behind. The next pipeline's bootstrap **auto-invokes `gc_orphan_staging_tables(model)`** for every staging model, dropping any staging table matching the prefix from a prior run. Logs at `debug` per drop, `warning` on connection failure.

You can opt out of auto-GC by setting `Staging(keep_after_flush=True)` — the operator is then responsible for manual cleanup (`DROP TABLE z_<schema>.<prefix>_<run_id>`). `keep_after_flush=True` is meaningful in `INTERVAL` mode only; in `REUSED` mode the staging table always stays until next-run GC drops it.

## Fields

| Field | Default | Purpose |
|---|---|---|
| `mode` | `StagingMode.REUSED` | Lifecycle — see the table above. |
| `schema` | `None` | Override the default `z_<target_schema>` staging schema. |
| `table_prefix` | `None` | Override the default `<target_name>_staging_` prefix. The `<run_id>` short-hex is always appended. |
| `logged` | `False` | UNLOGGED staging tables by default — writes skip WAL (~2-3× faster `COPY`), crash truncates them harmlessly. Set `True` for environments that mandate WAL on every write. |
| `keep_after_flush` | `False` | `INTERVAL`-only. When `True`, flush skips its `DROP TABLE`. Disables auto orphan-GC for the model. |

## Co-location with state

When state is set, the staging schema **must** live in the same database as the state schema (and therefore the target). Atomicity depends on `INSERT INTO target` and `UPDATE state` committing in the same transaction; a cross-database transaction is not supported. Setting `State(dsn_env_var=...)` together with staging raises `NotImplementedError` at `stage()` time.

Without state, the schema-and-target-must-share-a-DB constraint doesn't apply — but `Staging.schema` still defaults to `z_<target_schema>` for consistency.

## Limitations

- `WriteMode.APPEND` only. Other modes will route their final-move SQL through the same staged pattern later.
- See [State](STATE.md) for the resumability story when state is enabled.
