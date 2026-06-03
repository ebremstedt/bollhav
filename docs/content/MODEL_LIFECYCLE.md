[Home](index.md) › [Decorators](DECORATORS.md) › **Model lifecycle**

# Model lifecycle

`@model_lifecycle` brackets **one model's run**: it sets up the assets and state, calls your function (which loops the units of work), then tears down. Wrap the function that loops `model.intervals`.

```python
@model_lifecycle
def run_model(model, data_conn, state_conn=None):
    for interval in model.intervals:
        run_interval(model, interval, data_conn, state_conn)
```

`data_conn` is required (autocommit). `state_conn` defaults to `data_conn` — pass a separate one only for cross-DB state.

## What it does, in order

1. **Model lock** — if stateful with `exclusive_run`, acquire it (released in a `finally`).
2. **Assets** on `data_conn`:
   - `view` → `CREATE OR REPLACE VIEW` (that *is* the asset; no table DDL).
   - otherwise → create schema + table, then optional recreate / truncate / indexes / unique constraint / staging schema + orphan GC.
3. **State** on `state_conn`, when stateful: ensure the library + state tables, register the model, then seed rows — one singleton for `monolithic` / `view`, one per window for `interval`.
4. **Filter** — `model.intervals` is set to the *actionable* units only (skips already-`applied`), so your loop runs just the outstanding work.
5. **Run** your function.
6. **POST actions** on a clean return; **release** the model lock.

A stateless model skips steps 1, 3, 4 — just asset DDL, then your function.

## See also

- [Execute lifecycle](EXECUTE_LIFECYCLE.md) — what wraps each unit *inside* the loop.
- [State](STATE.md) · [Kinds](KINDS.md)
