[← home](index.md)

# State tracking

Per-model interval state. Opt in by adding `state=State(...)` to a model. When set, bollhav records every interval's lifecycle (`pending` → `applied`) in a per-model state table, and re-runs become resumable.

State tracking is **interval-only** in v1 — models with `batching=None`, `ChunkMode.ROW`, or view targets are not tracked.

## Opt in

```python
from bollhav.model import Model, Target, Batch, IntervalChunks, State

model = Model(
    target=Target(name="orders", schema=..., dsn_env_var="TARGET_DSN"),
    batching=Batch(interval=IntervalChunks(expression="@hourly")),
    state=State(),                          # ← that's it
)
```

Decorate `execute` with `@state_tracker` (in addition to `@progress_bar` if you use it):

```python
from bollhav.model import state_tracker, progress_bar

@state_tracker        # outer — gates on applied rows, marks applied, logs errors
@progress_bar         # inner — timing/output unchanged
def execute(model, since, until):
    ...
```

## Env vars

State tracking adds **two** bollhav-defined env vars on top of the standard runtime overrides:

| Variable | Type | Required | Default | Description |
|---|---|---|---|---|
| **STATE_MODE** | string | no | `respect` | `respect` skips applied rows on re-run; `disrespect` resets every row back to pending |
| **DISCOVER** | bool | no | `false` | When `true`, intervals come from each state-enabled model's state table instead of from bounds/backfill. Requires state to already exist from a previous run |

Plus **one user-named DSN env var** — its name is whatever you choose, but it must be set in the environment when you run the pipeline:

| Where the name is configured | Required when |
|---|---|
| `State(dsn_env_var="MY_NAME")` on the model — name is yours to pick | State should live in a separate Postgres from the target |
| Falls back to `Target(dsn_env_var=...)` when `State.dsn_env_var` is `None` | State should live in the same Postgres as the target (auto-prefixed `z_` schema) |

Minimal runnable env:

```bash
export TAGS=orders                                  # standard
export USE_SCHEMA_SUFFIX=false                      # standard
export BACKFILL_ENABLED=true                        # standard
export BACKFILL_SINCE=2024-01-01T00:00:00Z          # standard
export TARGET_DSN=postgresql://localhost/warehouse  # target DSN — name comes from Target.dsn_env_var

# Optional — only if state.dsn_env_var is set:
export STATE_DSN=postgresql://localhost/ops         # state DSN — name comes from State.dsn_env_var

# Optional — controls re-run behavior:
export STATE_MODE=respect                           # respect (default) | disrespect
```

## State config

| Field | Type | Default | Description |
|---|---|---|---|
| `dsn_env_var` | `str \| None` | `None` | Env var for the state DB DSN. When unset, falls back to the model's `target.dsn_env_var` |
| `log_errors` | `bool` | `True` | If True, exceptions from `execute` are written to a sibling errors table before being re-raised |

## Where state lives

For each state-enabled model, two tables are created (per-model — no centralized state table):

| Table | Purpose |
|---|---|
| `<state_schema>.<target_name>_state`  | one row per `(since, until)`, status `pending` or `applied` |
| `<state_schema>.<target_name>_errors` | one row per exception, with traceback |

`<state_schema>` resolves as follows:

| Configuration | State schema (model in `public.orders`) |
|---|---|
| `State(dsn_env_var="STATE_DSN")` and `STATE_DSN` is a separate Postgres | `public.orders_state` |
| `State()` — falls back to target DSN | `z_public.orders_state` |
| `SCHEMA_SUFFIX=dev`, separate state DSN | `public_dev.orders_state` |
| `SCHEMA_SUFFIX=dev`, fallback to target DSN | `z_public_dev.orders_state` |

The `z_` prefix is automatic when state shares a DB with your target — it keeps bollhav-owned tables at the bottom of a database editor's schema list and out of your application's schemas.

## State table

```
<target_name>_state
─────────────────────────────────────────────────────────
id                    BIGSERIAL PRIMARY KEY
run_id                UUID NOT NULL              -- last invocation to touch this row
since                 TIMESTAMPTZ NOT NULL
until                 TIMESTAMPTZ NOT NULL
status                TEXT NOT NULL              -- pending | applied
directive_mode        TEXT NOT NULL              -- latest | backfill | reload
interval_expression   TEXT NOT NULL              -- cron at pre-fill time
applied_at            TIMESTAMPTZ

UNIQUE (since, until)
INDEX  (status)
```

## Errors table

```
<target_name>_errors
─────────────────────────────────────────────────────────
id                   BIGSERIAL PRIMARY KEY
run_id               UUID NOT NULL              -- groups with state table
since                TIMESTAMPTZ NOT NULL
until                TIMESTAMPTZ NOT NULL
error_type           TEXT NOT NULL              -- exception class name
error_message        TEXT NOT NULL
traceback            TEXT
created_at           TIMESTAMPTZ NOT NULL DEFAULT now()

INDEX (run_id)
INDEX (created_at DESC)
```

Errors join back to the state table over `run_id` (pipeline-level) or `(since, until)` (interval-level — denormalized for ad-hoc queries).

## Lifecycle

1. **`@load_models`** computes intervals once per matched model and stashes them on `model.intervals`. For state-enabled models, it ensures the `_state` and `_errors` tables exist, then inserts `pending` rows for every interval.
2. **User loop** iterates `model.intervals` and calls the decorated `execute`.
3. **`@state_tracker`** on each call:
   - SELECT the state row by `(since, until)`. If `status='applied'` → skip.
   - Call the wrapped function.
   - On success → UPDATE row to `status='applied'`, `applied_at = now()`.
   - On exception → INSERT into `_errors` (if `log_errors=True`), then re-raise.
4. If the pipeline crashes mid-loop, surviving `pending` rows are picked up by the next invocation under `STATE_MODE=respect`.

## Re-run semantics

The two state-aware env vars combine. `STATE_MODE` controls how existing rows are treated; `DISCOVER` controls whether intervals come from bounds/backfill (default) or from the state table.

### Without DISCOVER (normal flow)

Intervals are computed from bounds/backfill. `STATE_MODE` controls the pre-fill:

| `STATE_MODE` | Pre-fill behavior | Gate behavior |
|---|---|---|
| `respect` (default) | Insert pending rows for new intervals only — applied rows are preserved | `@state_tracker` skips intervals where `status='applied'` |
| `disrespect` | Reset every interval in the computed window back to `pending`, clear `applied_at` | Same gate; but nothing is applied after a reset, so the whole window runs |

### With DISCOVER

`DISCOVER=true` means intervals come from each state-enabled model's state table — bounds and backfill are ignored. A previous run must have populated the state table first; on a fresh DB, DISCOVER finds nothing and the pipeline does no work.

| `DISCOVER` | `STATE_MODE` | What happens |
|---|---|---|
| `true` | `respect` | Read pending rows from each state table, run them. Applied rows untouched |
| `true` | `disrespect` | Reset every row in each state table back to pending (clearing applied), then read & run them all |

Non-state-enabled models get an empty interval list under `DISCOVER` — they still appear in the user's loop but do no work.

```bash
# default — picks up where you left off using the backfill window
python main.py

# force-rerun the configured backfill window
STATE_MODE=disrespect python main.py

# complete only the pending rows in state, ignoring the backfill window
DISCOVER=true python main.py

# rerun every row in state, regardless of prior applied status
DISCOVER=true STATE_MODE=disrespect python main.py
```

## Failed runs

Failed `execute` calls raise out of the decorator. The state row stays `pending`, an `_errors` row is recorded (if `log_errors=True`), and the pipeline crashes. The next invocation under `respect` retries the pending intervals.

## What does not get state-tracked

| Model shape | Why not |
|---|---|
| `batching=None` | No intervals to track. Constructing `Model(state=State(), batching=None)` raises. |
| `batching=Batch(mode=ChunkMode.ROW)` | Row chunks are sized by row count and the chunk count isn't known until read — no clean unit of state to pre-fill. |
| `ModelType.VIEW` targets | Views don't read data; there's nothing to gate. |

## Database backends

v1 ships a Postgres implementation. MSSQL is planned — same table shape, same decorator, dispatched by the model's target database.
