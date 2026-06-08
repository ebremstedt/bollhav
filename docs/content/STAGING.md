[Home](index.md) › [Model](MODEL.md) › [Target](TARGET.md) › **Staging**

# Staging

Per-target opt-in for memory-bounded chunked writes plus atomic per-interval finalization. Set `Target(staging=Staging(...))` and the staging-table lifecycle (create → write chunks → apply → drop) is owned by [`@execute_lifecycle`](EXECUTE_LIFECYCLE.md) via `PostgresData` / `MssqlData`: sub-batches land in a staging table keyed on `model.run_id`, then one transaction applies the staged content to the target. With state, the apply and the `applied` flip commit together.

A `kind=Kind.VIEW` model can't have staging — a view has nothing to stage, so `Model(...)` rejects `staging` on a VIEW.

Supported on both backends:

- **Postgres** — `bollhav.postgres.staging` (chunks via `COPY`, apply via `INSERT ... SELECT` / `... ON CONFLICT` / `DELETE + INSERT`).
- **MSSQL** — `bollhav.mssql.staging` (chunks via `fast_executemany`, apply via `INSERT ... SELECT` / `MERGE` / `DELETE + INSERT`). State coordination is Postgres-only for now.

## What you get

- **Memory bounding.** Sub-batches go straight to a staging table; nothing accumulates in Python.
- **Atomic per-interval finalization.** The apply step commits the staging-to-target move + (optional) state flip together. Either the target has every row of the interval *and* the state row says `applied`, or neither.
- **Resumability** (when paired with state). A crash mid-stream leaves the staging table behind and the state row stays non-`applied`; the next run picks up that interval.
- **Concurrency-safe naming.** The staging table name embeds the bootstrap-minted `run_id`, so two workers running on the same model don't collide on one shared table.

## Per-interval flow

The schema/staging-schema setup is done once per model by [`@model_lifecycle`](MODEL_LIFECYCLE.md) via discrete idempotent `PostgresData` methods (`create_schema`, `create_table`, `create_staging_schema`, …) — not per interval. Inside the loop, [`@execute_lifecycle`](EXECUTE_LIFECYCLE.md) drives each interval's staging table through create → write → apply → drop. Your execute calls `write()`, which lands chunks in the staging table keyed on `model.run_id`; it does **not** create or finalize the table.

```mermaid
flowchart TD
    A[execute_lifecycle: interval starts] --> C[create_staging_table<br>fresh table for this run_id]
    C --> F[execute calls write chunk -<br>dispatches on staging.write_mode]
    F --> G{more chunks?}
    G -- yes --> F
    G -- no --> H[apply_staging_to_target<br>in a single transaction]
    H --> I{target.write_mode}
    I -- APPEND --> J[INSERT INTO target<br>SELECT FROM staging]
    I -- UPSERT_NO_DELETE --> K[INSERT ... ON CONFLICT<br>DO UPDATE - Postgres<br>or MERGE - MSSQL]
    I -- RECREATE_PARTITION --> L[DELETE target window<br>+ INSERT FROM staging]
    J --> D[drop_staging_table<br>unless keep_after_apply]
    K --> D
    L --> D
    D --> M{state set?}
    M -- yes --> N[mark state row applied]
    M -- no --> O[skip]
    N --> P[interval done]
    O --> P
```

The staging table is **created fresh each interval and dropped after that interval's apply**, so staging always self-cleans on the write connection. The schema-level setup (target schema/table, staging schema) is a once-per-model step run by `@model_lifecycle` before the loop — subsequent intervals don't re-issue those CREATEs.

## Per-run flow

The pipeline run as seen from `@load_models`:

```mermaid
flowchart TD
    A[main called via @load_models] --> B[bootstrap: mint run_id,<br>GC orphan staging tables from prior runs]
    B --> C{state set?}
    C -- yes --> D[ensure state schema/table,<br>prefill pending rows,<br>filter intervals to non-applied]
    C -- no --> E[no filtering: user loop runs<br>every contract interval every time]
    D --> F[user loop iterates model.intervals]
    E --> F
    F --> G[per interval: see per-interval flow<br>each interval drops its own staging table]
    G --> H{more intervals?}
    H -- yes --> G
    H -- no --> I[run POST actions<br>user-defined, if any]
    I --> K[done]
```

Without state, the bootstrap still mints a `run_id` (the staging table name uses it) and still GCs orphans — staging-only models benefit from cleanup just like state-tracked ones.

## Opting in

```python
from bollhav.model import Model, Target, Staging, State, WriteMode

Model(
    target=Target(
        name="orders",
        schema="warehouse",
        dsn_env_var="TARGET_DSN",
        write_mode=WriteMode.APPEND,    # how staging lands in target
        staging=Staging(),              # ← opt-in
        columns=[...],
    ),
    state=State(),                      # optional - see "Without state"
    batching=Batch(...),
)
```

## `Staging.write_mode` — how chunks land *in* staging

| `staging.write_mode` | What `write(chunk)` does | Picks when |
|---|---|---|
| `APPEND` *(default)* | bulk-insert / COPY the chunk into staging — no dedup, dupes accumulate | rows are append-only, or you don't care about dupes |
| `UPSERT_NO_DELETE` | MERGE / `ON CONFLICT DO UPDATE` the chunk into staging on `target.unique_columns` — staging stays deduped as data arrives | source has duplicate keys (CDC streams, retried events) and you want them collapsed before the final apply |

`RECREATE_PARTITION` and `VIEW` are rejected on the staging side — staging is per-interval scratch space, not a long-lived partitioned table.

## `target.write_mode` — how staging lands *in* target

The same `WriteMode` enum drives the apply step. All three table modes are supported:

| `target.write_mode` | SQL the apply runs |
|---|---|
| `APPEND` | `INSERT INTO target SELECT FROM staging` |
| `UPSERT_NO_DELETE` | `INSERT INTO target SELECT FROM staging ON CONFLICT (...) DO UPDATE` (Postgres) / `MERGE target USING staging` (MSSQL) |
| `RECREATE_PARTITION` | `DELETE FROM target WHERE partition_col BETWEEN since AND until` + `INSERT INTO target SELECT FROM staging` |

The two write_modes compose orthogonally — four interesting cells:

| staging | target | When to pick |
|---|---|---|
| `APPEND` | `APPEND` | the default; raw streaming into an append-only target |
| `APPEND` | `UPSERT_NO_DELETE` | collect raw rows in staging, MERGE once at the end (one big tx, one target-side lock window) |
| `UPSERT_NO_DELETE` | `UPSERT_NO_DELETE` | dedup early as chunks arrive (staging stays small), MERGE small set at the end |
| `APPEND` | `RECREATE_PARTITION` | replace a time-window's contents atomically |

See [examples/staging_state_contracts/](https://github.com/ebremstedt/bollhav/tree/main/examples/staging_state_contracts) (Postgres staging + state + contracts) and [examples/mssql/staging/](https://github.com/ebremstedt/bollhav/tree/main/examples/mssql/staging) (MSSQL staging, no state) for runnable demos.

## Table lifecycle across intervals

Each interval gets a **fresh staging table**: `@execute_lifecycle` calls `create_staging_table(run_id)` at the start of the interval and `drop_staging_table(run_id)` after a successful apply (unless `keep_after_apply=True`). On a 365-interval backfill that's `1 × CREATE SCHEMA + 365 × CREATE + 365 × DROP`. Staging self-cleans on the write connection — no end-of-run cleanup pass and no separate DSN required.

The staging schema is created once per model by `@model_lifecycle` (`create_staging_schema`); the per-interval table CREATE is genuinely new each interval, so it isn't short-circuited.

The table name is `<prefix><run_id_short>`, so parallel workers on different runs of the same model don't collide.

## Staging without state

`state=State(...)` is optional. With staging-only:

- Memory bounding and per-interval atomic finalization still work — the apply step commits the staging-to-target move atomically, just **without** flipping a state row that doesn't exist.
- The user's loop re-runs every interval every time. There's no `applied` gate to skip already-completed work. Often what you want for a pure memory-bounding play.
- Re-runs are safe as long as your write is idempotent (`UPSERT_NO_DELETE`, `RECREATE_PARTITION`) or you accept the duplicate-write behaviour of `APPEND`.

```python
Target(
    name="big_dump",
    dsn_env_var="TARGET_DSN",
    write_mode=WriteMode.UPSERT_NO_DELETE,
    staging=Staging(),               # no state= on the model
    columns=[...],
)
```

## Orphan staging tables

A crash *mid-interval* (before the apply) leaves that interval's staging table behind. For models bollhav manages (a `dsn_env_var` is set), the next pipeline's bootstrap **auto-invokes `gc_orphan_staging_tables(conn, model)`**, dropping any staging table matching the prefix from a prior run. When you own the connection (no `dsn_env_var`), call `gc_orphan_staging_tables(conn, model)` yourself with your connection — it's exported from both `bollhav.postgres` and `bollhav.mssql`.

You can opt out of dropping entirely with `Staging(keep_after_apply=True)` — the apply skips its `DROP TABLE` and auto orphan-GC is disabled for the model; manual cleanup is then the operator's responsibility.

## Fields

| Field | Default | Purpose |
|---|---|---|
| `write_mode` | `WriteMode.APPEND` | How chunks land in staging — APPEND or UPSERT_NO_DELETE. See "Staging.write_mode" above. |
| `schema` | `None` | Override the default `z_<target_schema>` staging schema. |
| `table_prefix` | `None` | Override the default `<target_name>_staging_` prefix. The `<run_id>` short-hex is always appended. |
| `keep_after_apply` | `False` | When `True`, the apply tx skips its `DROP TABLE` (tables persist for audit). Also disables auto orphan-GC for the model. |

### Postgres-specific (`PostgresStaging`)

| Field | Default | Purpose |
|---|---|---|
| `logged` | `False` | UNLOGGED staging tables by default — writes skip WAL (~2-3× faster `COPY`), crash truncates them harmlessly. Set `True` for environments that mandate WAL on every write. |

```python
from bollhav.postgres.staging import PostgresStaging

Target(staging=PostgresStaging(logged=True, ...))
```

### MSSQL-specific (`MssqlStaging`)

Placeholder subclass — currently carries no MSSQL-only fields. Future home for knobs like `WITH (DURABILITY = SCHEMA_ONLY)` for memory-optimized tables.

```python
from bollhav.mssql.staging import MssqlStaging

Target(staging=MssqlStaging(...))   # same fields as Staging for now
```

## Co-location with state

When state is set, the staging schema **must** live in the same database as the state schema (and therefore the target). Atomicity depends on the apply tx and the state flip; a cross-database transaction is not supported. Setting `State(dsn_env_var=...)` together with staging raises `NotImplementedError`.

Without state, the schema-and-target-must-share-a-DB constraint doesn't apply — but `Staging.schema` still defaults to `z_<target_schema>` for consistency.

## Limitations

- **MSSQL + state**: state coordination isn't wired into the MSSQL apply yet — set `State()` only on Postgres targets if you want the atomic state flip. MSSQL staging without state works fine (chunked atomic apply, just no resumability gate).
- **Cross-DB state**: not supported with staging — see "Co-location with state" above.
- See [State](STATE.md) for the resumability story when state is enabled.
