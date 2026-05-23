# state_discover

End-to-end demo of state tracking against a real Postgres. Walks through the **partial failure → DISCOVER → finalize** flow that motivates the feature.

The `warehouse_clean.orders` model has 10 daily intervals (2024-01-01 → 2024-01-11). Execute is a mock — prints what it would do and (optionally) raises on a day you pick via `FAIL_ON_DAY`.

State lives in `z_warehouse_clean.orders_state` and `z_warehouse_clean.orders_errors`. The `z_` prefix is automatic because we don't configure a separate `STATE_DSN` — state shares the target DB.

## Pre-reqs

```bash
createdb warehouse                                # or use your own DB name
```

## Run the scenarios in order

The first command sets up the shell environment and runs scenario 1. The next three are one-liners that reuse the exported env — run them in the same shell.

### 1. Crash mid-pipeline

```bash
cd /Users/erbr/github/ebremstedt/bollhav/examples/state_discover && \
export TARGET_DSN=postgresql://localhost/warehouse && \
export TAGS='[orders]' && \
export USE_SCHEMA_SUFFIX=false && \
export BACKFILL_ENABLED=true && \
export BACKFILL_SINCE=2024-01-01T00:00:00Z && \
export BACKFILL_UNTIL=2024-01-11T00:00:00Z && \
NUKE_STATE=true FAIL_ON_DAY=5 python main.py
```

`NUKE_STATE=true` drops any state from prior runs. `FAIL_ON_DAY=5` raises on the 5th interval. Days 1–4 flip to `applied`, day 5 stays `pending` with a row in `orders_errors`, days 6–10 stay `pending`. Pipeline exits non-zero.

### 2. `DISCOVER` — finalize what's left

```bash
DISCOVER=true python main.py
```

Reads the 6 pending rows (days 5–10) from the state table and runs them. `BACKFILL_SINCE`/`BACKFILL_UNTIL` are ignored under DISCOVER. All 10 days are now `applied`.

### 3. `DISCOVER` + `disrespect` — rerun the entire state table

```bash
DISCOVER=true STATE_MODE=disrespect python main.py
```

Resets every row in `orders_state` to `pending` (clearing `applied_at`), then runs all 10 again. Useful when you want to repeat the whole tracked window without remembering the original bounds.

### 4. `STATE_MODE=disrespect` (no DISCOVER) — rerun the backfill window

```bash
STATE_MODE=disrespect python main.py
```

Resets every row in the *backfill window* to pending, then runs them. Same outcome as scenario 3 here (window == state table), but the source-of-truth differs: bounds/backfill vs the state table.

## Inspect at any point

```bash
psql "$TARGET_DSN" -c "SELECT since::date, status, applied_at FROM z_warehouse_clean.orders_state ORDER BY since"
psql "$TARGET_DSN" -c "SELECT since::date, error_type, error_message FROM z_warehouse_clean.orders_errors ORDER BY created_at"
```

## Env vars used here

| Variable | Effect |
|---|---|
| `TARGET_DSN` | Target DB (also state DB, since no separate state DSN is set) |
| `TAGS` | Selects the `orders` model — bracket syntax required, see [docs/MATCHING.md](../../bollhav/docs/MATCHING.md) |
| `USE_SCHEMA_SUFFIX=false` | Skips the dev/prod suffix so the schema is just `warehouse_clean` |
| `BACKFILL_ENABLED=true`, `BACKFILL_SINCE`, `BACKFILL_UNTIL` | Normal backfill window. `BACKFILL_UNTIL` is required here — `bounds.end` on the model is not consulted in backfill mode |
| `STATE_MODE` | `respect` (default) preserves applied; `disrespect` resets to pending |
| `DISCOVER` | When `true`, intervals come from the state table, not from bounds/backfill |
| `FAIL_ON_DAY` | (Example-only) Raise on the Nth day to simulate a partial run |
| `NUKE_STATE=true` | (Example-only) Drop the `z_warehouse_clean` schema before the pipeline runs — resets state without `psql` |
| `DEBUG=true` | (Optional) Enables debug logging so you see every `state: …` line as rows mutate |
