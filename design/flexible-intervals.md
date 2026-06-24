# Flexible intervals / coverage-based state — design note

Status: **partially built.** The attestation field `TimeChunking.fixed_intervals`
(default `True`) is declared and plumbed through the runtime rebuild. Nothing
reads it yet — only the `True` (grid) path exists. This note captures the rest
so it isn't lost.

## The problem it solves

State rows are keyed by `(since, until)`. The **chunk** is what produces those
boundaries, so the chunk is effectively part of a model's state *identity*. The
moment the chunk can vary per run (`INTERVAL_OVERRIDE`), state can fork into
incompatible granularities: an hourly run plants hourly rows, a later monthly
run plants monthly rows, and because the executor drains *every* non-applied row
(`get_actionable_intervals` is `WHERE status <> 'applied'`, unscoped), the old
partitioning leaks into the new run. That is the root of the "ran 14,200 hourly
windows under a monthly override" surprise.

Two coherent answers:

- **FIXED (today):** the chunk is identity. One state row per chunk. Changing
  the chunk is a migration → `STATE_MODE=torch`. Downstreams may gate `EXACT`.
- **FLEXIBLE (this note):** state is a *coverage set* (covered time ranges). The
  chunk is purely how you slice work, not identity. Re-chunk freely; fill gaps.

`fixed_intervals` is the per-model attestation choosing between them. It lives on
`TimeChunking` because the concept only exists when a model chunks time, and it
sits with the other interval-structure fields (`chunk`/`window`/`lookback`/`tz`)
rather than the execution fields on `Batch` (`size`/`retries`). Subsystems
*read* it from there; location ≠ readership.

## The attestation `fixed_intervals=False` signs

The model's output for a time range is **invariant to how that range is
partitioned**. That requires BOTH:

1. **Query is window-decomposable** — `[a,c)` ≡ `[a,b) ∪ [b,c)`. Pure
   `WHERE _data_modified ∈ window` filter/map: yes. `ROW_NUMBER() … latest
   within window`, `GROUP BY`, cumulative `THROUGH`: no.
2. **Write is order-independent** — re-partitioning changes the order units
   land, so the final table can't depend on it. Even a plain filter+`UPSERT`
   fails this if the MERGE is "last writer wins"; it's safe only if it breaks
   ties deterministically (`WHERE incoming._data_modified > existing._data_modified`)
   or uses `RECREATE_PARTITION`.

The framework can only *partially* verify this (see Guards). A wrong
`False` produces **silently divergent data**, not a crash — hence default
`True` and you must type the dangerous value yourself.

## The engine (step 2 — the bulk, NOT built)

The three `STATE_MODE`s collapse into "how much coverage to uncover before
computing gaps": `discover` = nothing, `bulldozer` = the run window, `torch` =
everything. One path.

**Storage** — barely changes. An `applied` row *is* a covered interval. Add a
GiST index; lean on PG14+ multirange arithmetic.

```sql
CREATE INDEX ix_state_applied_range ON {state}
  USING gist (tstzrange(since, until, '[)')) WHERE status = 'applied';
```

**Gap query** — replaces `get_actionable_intervals` for flexible models:

```sql
WITH covered AS (
  SELECT COALESCE(range_agg(tstzrange(since, until, '[)')), '{}'::tstzmultirange) AS rng
  FROM {state} WHERE status = 'applied' AND since IS NOT NULL
)
SELECT lower(gap) AS since, upper(gap) AS until
FROM covered,
     unnest( multirange(tstzrange(%(begin)s, %(horizon)s, '[)')) - covered.rng ) AS gap
ORDER BY since;
```

`horizon` = latest complete tick (same rule WHOLE uses, so no permanent
in-progress tail).

**Slice gaps by the run's chunk** (operational only, never stored):

```python
def compute_gap_units(run):
    for gap in state.uncovered_gaps(run.contract.begin, run.horizon):
        yield from generate_intervals_for_range(
            gap.since, gap.until, run.model.batching.time.chunk, tz)
```

**On success** — insert applied `[unit.since, unit.until)` then coalesce
touching/overlapping applied rows into maximal ranges (compact; **this is what
erases fine-grained provenance** — see landmines).

**Invalidation = range subtraction.** `uncover(span)` = `applied_mr - span`,
which may split a range in two. The three modes are then just: `discover` →
uncover nothing; `bulldozer` → `uncover(run.window)`; `torch` → uncover all.

## Guards (step 3 — small but mandatory, NOT built)

Each turns a silent severe failure into a loud definition/registration error.
They enforce the statically-checkable half of the attestation.

**Guard A — `EXACT` downstream on a flexible upstream → error.** A flexible
upstream coalesces away its exact-grain rows, so `EXACT` ("an applied row whose
`(since,until)` == my window") can never resolve → the downstream sits `blocked`
forever. Lives in `is_upstream_satisfied_live`, next to the existing TIMELESS
guard; needs `register_model` to store the upstream's `fixed_intervals`.
`ENCAPSULATE`/`THROUGH` against a flexible upstream are fine (coverage queries).

```python
if level == "exact" and match.fixed_intervals is False:
    raise ValueError("EXACT needs a fixed-grain upstream; this one is "
                     "fixed_intervals=False — use ENCAPSULATE.")
```

**Guard B — `APPEND` on a flexible model → error.** Flexibility re-processes
ranges; `APPEND` only adds → duplicate rows. Definition-time (both flags known
at construction). `UPSERT_NO_DELETE` / `RECREATE_PARTITION` allowed.

```python
if (self.batching and not self.batching.time.fixed_intervals
        and self.target.write_mode is WriteMode.APPEND):
    raise ValueError("fixed_intervals=False requires an idempotent write "
                     "(UPSERT_NO_DELETE / RECREATE_PARTITION); APPEND duplicates.")
```

Not caught (unprovable statically, stays the dev's attestation): an
order-dependent `UPSERT`, and non-window-decomposable queries.

## Concurrency (step 4 — NOT built)

Per-`(since,until)` advisory locks can't express overlap. Replace with an
exclusion constraint; a worker claims a unit by inserting a `running` row, and
the DB rejects overlaps:

```sql
ALTER TABLE {state} ADD CONSTRAINT no_inflight_overlap
  EXCLUDE USING gist (tstzrange(since, until, '[)') WITH &&) WHERE (status = 'running');
```

## Landmines engineering does NOT remove

1. **Partition-sensitivity** — the big one. Coverage treats partitions as
   interchangeable; FactCase's `ROW_NUMBER … latest per CaseKey within window`,
   aggregates, and `THROUGH` cumulative models are not. Unsolvable in general;
   it's a property of the transform. This is exactly what the attestation guards
   against by being opt-in.
2. **Coalescing erases per-range metadata** — a merged `[Jan,Jul)` has one
   `applied_at`/`run_id`; "when was Feb loaded?" and "re-run just Feb" both
   degrade. Tunable: coalesce aggressively (few rows, lossy) vs keep one row per
   load-event and `range_agg` at query time (faithful, more rows). Middle
   ground: coalesce within a run, keep islands across runs.
3. **`EXACT` is meaningless** on a flexible model (no exact-grain rows) — Guard A.

## Row count

A flexible model is multi-row but **one row per contiguous covered island**, not
per chunk: fully loaded → 1 row; N holes → N+1 islands. Count breathes (filling a
gap merges islands; uncovering splits one). Contrast the grid's fixed
"#chunks" rows. Plus transient `running`/`error` rows.

## Lineage interaction (settled)

`fixed_intervals` is per-model and does **not** propagate. A FLEXIBLE RAW with a
FIXED CLEAN is fine — CLEAN reads RAW's *data* (by watermark), not RAW's
partitioning, and FLEXIBLE *means* that data is partition-invariant (so a
flexible upstream is the *safest* to build on). The only cross-edge rule is the
contract: `EXACT` against a flexible upstream is forbidden (Guard A).

## Migration

Existing `applied` rows *are* valid coverage. So: keep them, drop pending/blocked
(recompute as gaps), add the GiST index + EXCLUDE constraint, branch the executor
on `fixed_intervals`. Roughly backward-compatible.

## Build order from here

1. ✅ `TimeChunking.fixed_intervals: bool = True` declared + plumbed (carries
   through the runtime rebuild).
2. ☐ Store it in the library (`register_model`) so downstreams can read it.
3. ☐ Branch `prefill` + actionable selection on it: grid (today) vs coverage.
4. ☐ Guards A (`EXACT`-on-flexible) + B (`APPEND`-on-flexible).
5. ☐ Range-lock exclusion constraint for the flexible path.
