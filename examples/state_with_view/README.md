# state_with_view

State tracking with a **view** in the middle of the dependency chain.

```
warehouse.orders             (TABLE,  state + staging)   @daily ×3
        ↓
warehouse.v_high_value_orders (VIEW,   library-only)     ──── one CREATE OR REPLACE
        ↓
warehouse.high_value_sums    (TABLE,  state + staging)   @daily ×3
```

The point of the example: **a view registers itself in the library, and a downstream that declares it as upstream is satisfied by the view's mere presence.** No applied-row check against a non-existent state table — views are time-agnostic, so presence is the satisfaction proof.

## What you'll see end-to-end

| Step | Model | What happens |
|---|---|---|
| 1 | `warehouse.orders` | Bootstrap creates state table, prefills 3 pending intervals. Loop writes 3 batches via staging; each interval's row flips to `applied`. |
| 2 | `warehouse.v_high_value_orders` | Bootstrap registers it in the library (`model_type=VIEW`, `state_schema=NULL`). Loop runs once: `CREATE OR REPLACE VIEW`. No state row — there isn't a state table. |
| 3 | `warehouse.high_value_sums` | Bootstrap finds the view in the library → satisfied by presence → 3 intervals pending. Loop writes them via staging like orders. |

## Setup

Requires a running Postgres reachable via `$TARGET_DSN`. The example needs a regular database with a role that can `CREATE SCHEMA`, `CREATE VIEW`, and write tables.

## Run

```bash
cd examples/state_with_view

export TARGET_DSN="postgresql://USER:PASSWORD@localhost:5432/DBNAME"
export TAGS="[orders | view | sums]"
export USE_SCHEMA_SUFFIX=false
export BACKFILL_SINCE=2024-01-01T00:00:00Z
export BACKFILL_UNTIL=2024-01-04T00:00:00Z

python main.py
```

Expected output (truncated):

```
── runtime ── ( model ) ────
  tags              [orders | view | sums]
  mode              backfill  2024-01-01… → 2024-01-04…
────────────────────────────

warehouse.orders             run_id=…  3 interval(s) pending
warehouse.v_high_value_orders             (no state — view)
warehouse.high_value_sums    run_id=…  3 interval(s) pending

warehouse.orders  3 step(s) to process
  2024-01-01 → 2024-01-02
  2024-01-02 → 2024-01-03
  2024-01-03 → 2024-01-04

warehouse.v_high_value_orders  1 step(s) to process
  (single shot — view)

warehouse.high_value_sums  3 step(s) to process
  2024-01-01 → 2024-01-02
  2024-01-02 → 2024-01-03
  2024-01-03 → 2024-01-04
```

## Verify

```sql
-- orders + the view + the downstream all exist.
\dt warehouse.*
\dv warehouse.*

-- The view is in the library.
SELECT full_name, model_type, state_schema, state_table
FROM z_bollhav.model_library
ORDER BY full_name;
-- full_name                             | model_type | state_schema | state_table
-- ------------------------------------- | ---------- | ------------ | -----------
-- warehouse.high_value_sums             | TABLE      | z_warehouse  | high_value_sums_state
-- warehouse.orders                      | TABLE      | z_warehouse  | orders_state
-- warehouse.v_high_value_orders         | VIEW       |              |

-- High-value orders projected through the view.
SELECT count(*) AS high_value_rows FROM warehouse.v_high_value_orders;

-- Downstream state — all applied.
SELECT since AT TIME ZONE 'UTC' AS since_utc, status, applied_at
FROM z_warehouse.high_value_sums_state
ORDER BY since;
```

## What it demonstrates about the design

- **Views opt in to the library implicitly** — there's no `library=True` on `v_high_value_orders`. The bootstrap detects `target.is_view` and adds the row.
- **Satisfaction is presence-based for view upstreams** — `is_satisfied` returns True the moment the library row exists, with `state_schema` and `state_table` both NULL. No SQL is issued against a non-existent state table.
- **The block reasons distinguish the cases** — if you run `TAGS=[sums]` *without ever* running the view first, you'll see `STATE_001: upstream 'warehouse.v_high_value_orders' not registered` on every interval. After one `TAGS=[view]` run, the library row exists permanently and `sums` unblocks.

## What is NOT exercised here

- `high_value_sums` reads its **own mock data**, not from the view. This example focuses on the **satisfaction mechanism**; actual data flow through the view would require swapping `mock_read.py` for something that issues `SELECT … FROM warehouse.v_high_value_orders` against `TARGET_DSN`.
- Cross-DB layouts — view, state, library, target all live in the same database (the common case).
