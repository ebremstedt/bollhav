[← home](index.md)

# Lifecycle redesign (proposed)

**Status:** design note — not yet implemented. Captures the decisions from the
lifecycle/state/connection design discussion so they're in one place before
building.

## Goal

Decompose today's monolithic `@load_models` (discovery + state bootstrap +
banner + post-actions) and the `@state` decorator into a clean, layered set of
hooks, and replace the single atomic data+state transaction with a simpler
two-connection model. The payoff: clearer separation of concerns, and **state
that can live in a different database than the data** (e.g. MSSQL data +
Postgres state).

## The three hooks

| Hook | Wraps | Owns |
|---|---|---|
| `@load_models` | `main` | discovery + runtime overrides (tags, suffixes, interval-expression override) + dry-run short-circuit + inject `models`. **No bootstrap.** |
| `@model_lifecycle` | `execute_model(model)` | model-level actions (assets, state-table ensure, prefill), the model advisory lock (held across the interval loop), model-level error handling |
| `@interval_lifecycle` | `execute_interval(model, interval)` | interval-level actions, the interval advisory lock (try-acquire/skip), `mark_applied` on success / `record_failure` on exception |

`@load_models` stays a **decorator** — it injects `models` and can skip `main`
on dry-run, both of which only a decorator can do. The two lifecycle hooks are
**context managers** (or decorators on `execute_model` / `execute_interval` if
those are functions): they need a runtime connection, bracket a specific block,
and must catch that block's exception — all natural for a context manager.

A hook is a generic runner:

```
enter:  acquire lock (state_conn)
        run actions where (level == mine, phase == PRE), routed by connection
yield → user's execute
exit:   on success    → run actions where (level == mine, phase == POST)
        on exception  → record_failure (state_conn)
        release lock
```

## The `Action` object (revised)

`Phase` today flattens two independent things into one 4-value enum. Split it,
and add the connection axis the two-connection model forces:

```python
Action(
    name:        str,
    level:       Level,        # MODEL | INTERVAL    — which bracket runs it
    phase:       Phase,        # PRE | POST          — enter vs exit
    connection:  Conn,         # DATA | STATE        — which conn it runs on
    run:         Callable,     # run(conn, model) — gets the routed conn
    should_run:  Callable = lambda m: True,
)
```

The old values map 1:1, so migration is mechanical:

```
PRE_MODEL     → (MODEL, PRE)       POST_MODEL     → (MODEL, POST)
PRE_INTERVAL  → (INTERVAL, PRE)    POST_INTERVAL  → (INTERVAL, POST)
```

Default actions, re-tagged:

| Action | Level | Phase | Conn |
|---|---|---|---|
| `schema_created` | MODEL | PRE | DATA |
| `recreated` (if `recreate_table`) | MODEL | PRE | DATA |
| `table_created` | MODEL | PRE | DATA |
| `truncated` (if `truncate_table`) | MODEL | PRE | DATA |
| `indexes_created` (if partitioned) | MODEL | PRE | DATA |
| `uniques_added` (if unique cols) | MODEL | PRE | DATA |
| `staging_schema_created` (if staging) | MODEL | PRE | DATA |
| `state_table_ensured` | MODEL | PRE | STATE |
| `prefill` | MODEL | PRE | STATE |
| `mark_running` (if state) | INTERVAL | PRE | STATE |
| `mark_applied` (if state) | INTERVAL | POST | STATE |

## What is an action vs owned by the hook

Not everything is an action. The hooks own things that **span the bracket** or
are **conditional on the outcome**:

- **Advisory locks** — acquired on enter, released on exit (they span the whole
  bracket). Hook-owned, `state_conn`.
- **`record_failure`** — runs only on exception, so it's the hook's exception
  path, not a POST action (POST actions fire on success). Hook-owned,
  `state_conn`.
- **Staging table create / apply / drop** — lives in the write path
  (`execute → write → stage`), on `data_conn`. Not actions.

## Two connections

- **`data_conn`** (target DB): chunk writes, staging apply/drop, all
  `connection=DATA` actions.
- **`state_conn`** (state DB — *may be a different DB/engine*): prefill,
  state-table ensure, `mark_running` / `mark_applied`, advisory locks, all
  `connection=STATE` actions.

Provisioned once and passed down to `execute_model` / `execute_interval`. The
runner hands each action the connection its `connection` field names. State
functions are refactored to take an **injected `conn`** instead of opening
their own via DSN — which also removes today's per-op connection churn
(`mark_running` / `mark_applied` each open a fresh connection per interval).

## State machine — non-atomic, two writes

Per interval the order is **state(running) → data(write) → state(applied)** —
three separate commits, *not* one transaction.

```
pending ──mark_running──▶ running ──(data committed)──▶ applied
                              │
                              └────────(exception)─────▶ error
```

- **Gate:** actionable = "not applied" (pending / running / error all rerun).
- **`running` is NOT terminal** — a crashed interval reruns on the next pass.
- The lock is the concurrency guard; the `running` *value* is just a progress
  record. Keep them separate.
- No `_state_applied_via_staging` marker: the "applied, then raised" edge is
  handled by rerun + idempotency, not a marker on the model.

## Locking — advisory, on `state_conn`

- **Model lock** — held `@model_lifecycle` enter → exit (across all the model's
  intervals).
- **Interval lock** — held `@interval_lifecycle` enter → exit, **try-acquire**
  (already held → skip the interval, never double-write).
- Both stack on the one `state_conn` session (session-level advisory locks
  persist across the commits in between).

## Staging

REUSED is already removed, so staging is per-interval: create + apply + drop in
one `data_conn` transaction (per-interval **data** atomicity preserved). The
state flip is **decoupled** onto `state_conn` afterward.

## Invariants to bake in now (cheap, non-deferrable)

1. **`running` is re-runnable, never terminal** → process crashes self-heal on
   the next run with no crash-specific code.
2. **Lock = concurrency guard; `running` = progress record** → orphaned locks
   cause a safe *skip*, not a double-write or a permanent block.
3. **Idempotent writes** (`UPSERT_NO_DELETE` / `RECREATE_PARTITION`) for
   state-tracked models → reruns converge. `APPEND` accepts dup-on-crash.
4. **Order data → state** (mark applied only after data commits) → the worst
   case is a recoverable duplicate, never a silent gap.

## Why non-atomic is acceptable

Dropping the single data+state transaction means a crash between the data commit
and the `applied` write leaves data committed but the row still `running`; the
rerun re-writes. That is safe given idempotent writes (invariant 3). The
decorator handles *soft* failures (catchable exceptions, including retrying the
idempotent `mark_applied`); only a *hard* crash (process/host death) opens the
window, and idempotency closes it on rerun.

**The payoff:** atomicity required state to co-locate in the target DB. Without
it, `state_conn` and `data_conn` can point at different databases/engines —
which makes **MSSQL data + Postgres state** implementable.

## Deferred — hard-crash recovery

A hard crash leaves a stale `running` row.

- **Process death** (kill -9, OOM, segfault): the OS closes the socket →
  Postgres releases the advisory lock in seconds → the interval reruns next
  pass (because `running` is re-runnable). Self-heals, no code.
- **Host / network death** (power loss, panic, partition): no socket close →
  the advisory lock is orphaned until TCP keepalive fires (default ~2h) or
  `pg_terminate_backend`. The interval is safely *skipped* (lock held) until
  then.

Deferred mechanism: a stale-`running` timeout sweep + shorter Postgres
keepalives (`tcp_keepalives_idle` etc.) for faster orphan detection. Interim
stopgap: manual state-table edit / `pg_terminate_backend`. Additive later —
nothing in this design blocks it.

## Removed / changed from current code

- `@state` → `@interval_lifecycle` (non-atomic flip, interval lock, error
  handling).
- `_bootstrap_state_for_staged_models` + `model_lock` → folded into
  `@model_lifecycle`.
- `@load_models` slimmed to discovery + overrides + dry-run + inject.
- Staging apply stops flipping state inside the data transaction.
- Remove the `_state_applied_via_staging` marker.
- Remove the "state must co-locate with target / cross-DB banned" assertion in
  `_assert_supported`.
- State functions take an injected `conn` (no self-connect via DSN).
- `Phase` enum → `Level` + `Phase` (+ `connection`) on `Action`; re-tag default
  actions; runners filter on `(level, phase)` and route by `connection`.

## Build order

1. **`Action`**: add `level` + `connection`, split `Phase`; re-tag defaults;
   update the runners to filter on `(level, phase)` and route by `connection`.
2. **Inject `conn`** into the state functions (kills self-connect / DSN churn).
3. **`@model_lifecycle`**: assets + state-table ensure + prefill + model lock
   (split out of the bootstrap).
4. **`@interval_lifecycle`**: replace `@state` (non-atomic flip + interval lock
   + error handling).
5. **Decouple** the staging apply from the state flip.
6. **Drop the co-location assertion** → enable cross-DB state.
