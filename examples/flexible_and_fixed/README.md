# flexible + fixed intervals

A Postgres pipeline that mixes **fixed-grid** and **flexible (coverage-based)**
interval models in one dependency graph, to show which **upstream contracts**
are valid across the `fixed_intervals` boundary.

The four models (`src/models/`):

| model          | `fixed_intervals` | write mode         | gates upstream                         |
|----------------|:-----------------:|--------------------|----------------------------------------|
| `raw_events`   | `True` (fixed)    | `APPEND`           | —                                      |
| `clean_events` | **`False`** (flexible) | `UPSERT_NO_DELETE` | `raw_events` · **ENCAPSULATE** ✓        |
| `daily_report` | `True` (fixed)    | `APPEND`           | `clean_events` · **ENCAPSULATE** ✓ (on a *flexible* upstream) |
| `audit`        | `True` (fixed)    | `APPEND`           | `raw_events` · **EXACT** ✓ (on a *fixed* upstream) |

```
raw_events (fixed) ──ENCAPSULATE──▶ clean_events (FLEXIBLE) ──ENCAPSULATE──▶ daily_report (fixed)
        └────────────────EXACT────────────────────────────────────────────▶ audit (fixed)
```

`@load_models` topologically sorts these, so each upstream is `applied` before
its downstream gates on it.

## What it demonstrates

- **`fixed_intervals` is stored and queryable.** `register_model` writes it to
  `z_bollhav.library`; after a run, `clean_events` is `False`, the rest `True`.
- **`ENCAPSULATE` works against a flexible upstream** (`daily_report` →
  `clean_events`). Coverage doesn't care about grain, so a flexible upstream is
  a fine thing to build on.
- **`EXACT` works against a fixed upstream** (`audit` → `raw_events`). A fixed
  grid keeps its exact-grain rows, so the per-day match resolves.
- **A flexible model uses an idempotent write** (`clean_events` is
  `UPSERT_NO_DELETE`, keyed on `id`) — re-covering a range can't duplicate.

### The forbidden combo (not runnable on purpose)

`EXACT` on a **flexible** upstream is a hard error — a flexible upstream
coalesces away its exact-grain rows, so the gate could never match and the
downstream would block forever. Adding, say, `Source("demo.lake.clean_events",
type=SourceModel(), contract=UpstreamContract.EXACT)` to a model would raise
`ExactContractOnFlexibleUpstreamError` at gate-check. See
[`docs/content/UPSTREAM.md`](../../docs/content/UPSTREAM.md) for the full
fixed-vs-flexible matrix.

> **Status:** the flexible *execution* engine (coverage gap-filling + range
> coalescing) is still being built — see `design/flexible-intervals.md`. Today
> `clean_events` registers as flexible and the **contract rules above are
> enforced**, but its own execution still runs the fixed grid path. So this
> example exercises the *registration + lineage* half of flexibility, which is
> what's wired.

## Run it

Needs a running Postgres (you supply the DSN — no Docker here).

```bash
export TARGET_DSN='postgresql://postgres:postgres@localhost:5432/postgres'
export TAGS='[demo]'
export USE_SCHEMA_SUFFIX=false       # keep schema = "lake" so contract names match
export BACKFILL_SINCE='2024-01-01T00:00:00+00:00'
export BACKFILL_UNTIL='2024-01-04T00:00:00+00:00'

python main.py                       # add DEBUG=true for the full state:/stage: trail
```

First run: all four models run 3 intervals each (`raw_events` → `audit` →
`clean_events` → `daily_report`). Second run: everything is already `applied`,
so each model's `run.intervals` is empty and the loop does no work
(resumable).

### Inspect

```sql
-- the data
SELECT count(*) FROM lake.raw_events;       -- 30 (10/interval × 3)
SELECT count(*) FROM lake.clean_events;     -- 30 (idempotent upsert)
SELECT * FROM lake.daily_report ORDER BY day;
SELECT * FROM lake.audit ORDER BY day;

-- the per-model shape, recorded by register_model
SELECT full_name, fixed_intervals, upstream
FROM z_bollhav.library
WHERE full_name LIKE 'demo.lake.%'
ORDER BY full_name;
-- clean_events → fixed_intervals = false; the rest → true
```
