# bollhav — library audit

**Date:** 2026-06-27
**Code audited:** merged `main` (`d0c0fec`, "Merge #13 from typing_fix"), via a worktree of `typing_fix` whose `bollhav/` is byte-identical to merged `main`.
**Method:** five parallel reviewers (state machine, time/window, backends, model/validation, DE feature gaps). Every claim verified against the real code with `file:line`; the time bugs were reproduced standalone against `icron.croniter`; dead code confirmed by grep. Line numbers are approximate and may drift — re-confirm before fixing.

## Caveats / context
- **roskarl pin:** bollhav imports `INTERVAL_EXPRESSION_SHORTCUTS`, but the roskarl installed in the working venv still exports the **old** `BATCH_EXPRESSION_SHORTCUTS`, so bollhav won't import there. This is a **stale dependency pin / venv refresh** issue, not a bollhav bug — bump roskarl. ("interval expression" is the current name; "batch expression" is stale.)
- **Phantom "Actions" system:** `changelog/2.1.0.md` advertises PRE/POST_INTERVAL hooks, `Target.on_failure`, and per-interval metrics/Prometheus/notify. **None of it exists in the merged code** (no `actions.py`, no `Phase`, no `on_failure`; `execute_lifecycle` is hardcoded inline). It was reverted or never landed — anything that would let you bolt on metrics/alerting/logging is therefore absent (see Tier 3 #3/#5/#7).

---

## Tier 1 — correctness bugs that bite in production (HIGH)

### 1. DST breaks `split_window` — annual crash + silent miscoverage
`model/window.py:202-218` (reproduced)
On the spring-forward day, `icron` emits a duplicate tick (e.g. `23:00+02:00` twice). The loop builds `TZInterval(23:00, 23:00)` → `SinceAfterUntilError`. **Any sub-daily model in a DST timezone hard-crashes once a year.** At `@daily` it doesn't crash but splits the DST day into a 23h chunk + a spurious 1h tail (`[00:00→23:00]` + `[23:00→00:00]`) → two state rows where the contract declares one → downstream EXACT/WINDOW gates never match the canonical midnight boundary.
**Fix:** skip non-advancing ticks (`if tick <= current: continue`); compute boundaries in UTC and render in the model tz, or snap to canonical cron instants and never emit a remainder tail when the last tick lands short.

### 2. State↔data write is non-atomic; APPEND not idempotent; `update_state=False` is dead
`lifecycle.py:452-467`, `postgres/staging.py:441-470`, `state_table.py:701-744`
Staged apply commits data on `data_conn`, then `mark_applied` commits separately on `state_conn` (different DBs in the MSSQL-data/PG-state case — atomicity structurally impossible). Crash between → data landed, row still `pending`/`running` → rerun **double-appends** (APPEND is a bare COPY/INSERT, no dedup key). Compounding: `record_failure(update_state=False)` — the safety valve documented to avoid downgrading an already-applied staged interval — **has no caller** (the single call site hardcodes `update_state=True`), so a post-merge failure flips an applied interval to `error` and it re-appends.
**Fix:** when `state_conn is data_conn`, run `mark_applied` inside the apply transaction. For cross-engine, give APPEND an idempotency key (run_id/interval) → `INSERT … WHERE NOT EXISTS`. Wire `update_state=False` for post-merge failures.

### 3. TOCTOU: applied-gate not re-checked under the interval lock
`lifecycle.py:425-467`
`is_applied(interval)` is checked **before** `try_acquire_interval_lock` and never again after. Two workers (default `allow_concurrent_runs=True`) both pass, serialize on the lock; the second proceeds to `mark_running`→`execute`→`mark_applied` on an already-applied interval → double APPEND.
**Fix:** re-check `is_applied` (and/or `mark_running … WHERE status <> 'applied'` checking rowcount) inside the lock.

### 4. `torch`/`bulldozer` invalidation runs lock-free by default
`lifecycle.py:248-302`, `locks.py:84-97`, `state.py:55`
`acquire_model_lock` only locks when `allow_concurrent_runs=False` (default is `True`). So `torch_rows`/`clear_window`/`reset_window` run with **no model lock**, racing a concurrent worker holding only a per-interval lock: torch deletes the row mid-run → that worker's `mark_applied` updates 0 rows (silently no applied row); `reset_window` flips a just-applied interval back to pending.
**Fix:** hold the model lock around the invalidation+prefill+get-actionable block whenever invalidation runs, independent of `allow_concurrent_runs`.

### 5. Postgres direct UPSERT keys off `unique_columns`, not `merge_key_columns`
`postgres/modes.py:110` (validation `model/target.py:202-206, 241`)
A PK-only model is valid (`merge_key_columns = primary_key or unique`) but the direct upsert builds the conflict target from `unique_columns` only → `ON CONFLICT ()` → SQL error. Works on MSSQL and on the PG **staged** path (both use `merge_key_columns`); only PG-direct is broken.
**Fix:** use `merge_key_columns` in `postgres/modes.py:110` (and the matching update-set exclusion). The PK constraint already exists as the conflict target.

### 6. MSSQL direct `RECREATE_PARTITION` — lying validation + dropped window
`mssql/write_modes.py:56-62` vs `113-118`
`write()` validates RECREATE_PARTITION as allowed, but `write_dataframes` only handles APPEND/UPSERT → runtime "Unhandled write mode"; `since`/`until` are accepted by `write()` and never forwarded. PG supports it directly → parity gap.
**Fix:** add a `RECREATE_PARTITION` case (DELETE window + bulk insert, mirror `mssql/staging.py` `_apply_recreate_partition`) and thread `since`/`until`; or drop it from `write()`'s allow-list with an honest "requires staging" error.

### 7. MSSQL staging GC can drop another model's *live* staging table
`mssql/staging.py:87-92, 402-417`
Staging name = bare `target.name` (no per-model digest, unlike PG's `_staging_stem`). Two same-named models in a colliding `z_<schema>` share a `LIKE` prefix; `cleanup_orphaned_staging_tables` (runs at model setup) drops everything but `keep_run_id` → drops a concurrently-running model's in-flight table.
**Fix:** port PG's per-model digest stem (blake2 of `full_name`) into the MSSQL prefix.

### 8. `no_partial_below` finer than `chunk` → uncompletable partial tail + unbounded row accretion
`window.py:113-129, 238-258`, `state_table.py:349-378`
e.g. `chunk=@monthly, no_partial_below=@daily` → contract tiling's last chunk is a stub `[06-01→06-25]` that no whole-month EXACT/WINDOW gate matches. Because the trailing edge advances daily and prefill keys on exact `(since, until)` with `ON CONFLICT DO NOTHING`, **a new stub row accretes every run, unbounded.** This is the documented use case (`@yearly` chunk + `@daily` npb).
**Fix:** exclude the partial leading/trailing interval from `contract_intervals`/prefill (only materialize whole chunks), or carry the partial as a single updatable row.

### 9. `STATE_DISABLED` override crashes legitimate gated+stateful models
`runtime.py:133-154`, `model.py:230`
The override nulls `state` but copies gated `upstream` verbatim → re-validation raises `GatedUpstreamWithoutStateError`. The operator's own escape hatch crashes any gated+stateful model.
**Fix:** when state-disabled, also strip gating from the copied upstreams (rebuild gated `Source`s with `contract=None`), or skip that validator for the state-disabled rebuild.

### 10. Lookback wrong for irregular intervals + DST; unclamped to `contract.begin`
`window.py:103-110, 195-199` (reproduced)
`_apply_lookback` samples one tick gap and multiplies → `@monthly`/`@yearly` drift by days; across DST it lands off-grid (a wall-clock instant that doesn't exist). And lookback is never clamped to `contract.begin`, producing pre-contract intervals that can never satisfy an upstream gate (wedged).
**Fix:** step backwards by real cron ticks (`get_prev`), not a sampled-duration multiply; `window_since = max(window_since, contract.begin)` after lookback.

---

## Tier 2 — medium severity / footguns

- **Curfew'd upstream → downstream hard crash.** Model-level curfew early-out returns *before* `register_model`/`ensure_tables` → a downstream's live gate hits `UnregisteredUpstreamError` (a raise, not a soft block). [`lifecycle.py:179-187`, `satisfaction.py:191`] **Fix:** register (cheap, idempotent) before the curfew early-out; only skip execution.
- **Stranded `running` on crash; no lease.** `mark_running`'s docstring claims DISCOVER auto-recovers it — **false** (prefill is `DO NOTHING`; `reset_window` skips `running`). [`state_table.py:756-759`] **Fix:** add a `running_since` lease; reclaim stale `running`; fix the docstring.
- **`error` terminal-in-run; `Batch.retries` dead.** First failure re-raises and aborts the interval loop; no mark-and-continue, no retry/backoff. `Batch.retries` is read only by `pretty()` + a test. [`lifecycle.py:454-465`, `batch.py`] **Fix:** opt-in isolate-failures (mark error, continue) + consume `retries` with backoff, or delete the field.
- **`RECREATE_PARTITION` atomicity depends on autocommit mode — opposite per backend, unenforced.** PG needs `autocommit=True` (for `conn.transaction()`); MSSQL needs `autocommit=False`. Wrong mode → empty partition on crash. [`postgres/modes.py:87`, `mssql/staging.py:358-379`] **Fix:** assert the required mode at the write boundary.
- **Empty/zero-width `Curfew`.** Bare `Curfew()` (no windows/days, deny) blocks the model 24/7 forever; a zero-width window never fires. No `__post_init__`. [`curfew.py:47-57`] **Fix:** reject the empty deny-curfew and zero-width windows.
- **Two models → one physical table.** Suffix string-concat (`name_resolved`/`resolve_schema_name`) collides (delimiter ambiguity); dedup only catches identical `full_name` within one match run. [`target.py:141-170`, `matching.py:133`] **Fix:** registration-time uniqueness check over resolved `(schema, name, catalog)` across all models; non-concatenable identity.
- **`full_name` embeds `datetime.now()` (ISO week, `%y%V`) at access** → non-deterministic identity used by matching dedup, ordering, and `ref`; a dependency edge can silently vanish across a week boundary. `canonical_full_name` exists (used for state-table naming) but wasn't adopted by the graph layer. [`target.py:146-170`] **Fix:** snapshot resolved names once at construction; migrate matching/ordering/ref to `canonical_full_name`.
- **Tag grammar silently mis-selects.** Inter-group text is discarded → `[a]&[b]` means OR (selects a model with only `a`); `not:[x]` = whole-catalog-minus; `not:not:` not honored; `[ ]`/empty candidates parse to junk. No validation. [`tagexpr.py:83-101`] **Fix:** real tokenizer that rejects un-consumed inter-group text and empty/whitespace candidates; honor or reject repeated `not:`.
- **`recreate_table`/`truncate_table` + `UPSERT_NO_DELETE`** wipes the table every run, defeating the merge — uncaught. [`target.py:208-247`] **Fix:** reject the combination (mirror the view-side guard).
- **`partitioned_by` + non-`RECREATE_PARTITION`** silently accepted, still builds an unused partition index (validator is one-directional). [`target.py:243`, `postgres/data.py:110`] **Fix:** symmetric rejection.
- **`RUN_SINCE`/`RUN_UNTIL` has no `since ≤ until` guard** → swapped dates produce an empty interval list and a silent no-op (and no tz-awareness guard at the env layer like `Contract` has). [`load_models.py` `_window_dt`, `window.py`] **Fix:** assert `since < until` (and tz-aware) at the env-parse layer with a targeted error.
- **MSSQL `#tmp` merge name = `hash()%1e7`** (per-process randomized, collision-prone; "collision-safe" claim false). [`mssql/modes.py:160-163`] **Fix:** stable wide digest (blake2 of `full_name`) or `uuid4().hex`.
- **DDL idempotency races.** PG `add_unique_constraint`'s `DO` block catches `duplicate_table` but not `duplicate_object` (42710) → concurrent ALTER escapes; MSSQL `IF NOT EXISTS … ALTER ADD CONSTRAINT` is a check-then-act TOCTOU. [`postgres/data.py:204-209`, `mssql/data.py:110-119`] **Fix:** catch both SQLSTATEs; wrap MSSQL DDL in TRY/CATCH or under the model lock.
- **Type-mapping parity gaps** (JSON/JSONB, arrays, UUID, bytea, TIMESTAMPTZ rich on PG, sparse/divergent on MSSQL) with **no "no equivalent" diagnostic** — portability fails silently. [`postgres/columns.py` vs `mssql/columns.py`] **Fix:** capability/mapping table + `NoEquivalentTypeError`, or document backend-specific types.
- **`ModelLockedError` aborts the whole pipeline** instead of skipping the contended model (the docstring says operators can "decide to skip" but there's no hook). [`lifecycle.py:198`] **Fix:** catch in the lifecycle, skip-with-log.
- **`clear_window` two-step non-atomic** (uncover commits, then leftover-DELETE) — a crash between leaves stale-grain rows. [`state_table.py`] **Fix:** one transaction.
- **MSSQL `setinputsizes`** passes precision-in-bits (53/24/1) as the ODBC column_size for FLOAT/REAL/BIT — wrong field, brittle across drivers. [`mssql/modes.py:108-111`] **Fix:** use the ODBC size (8/4/0) or 0.
- **`inputs_known`** uses "no typeless input" while `source_names`/`declared_inputs` use "type is not None" — inconsistent predicates (equivalent today, latent). [`model.py:336-340`] **Fix:** single-source off `declared_inputs`.
- **window-source × STATE_MODE combos** have no contradictory-combo guard (TORCH+LATEST silently drops latest; TORCH+explicit-window leaves a pending remainder; reload/no-dates-backfill needs `contract.begin` but only fails per-model mid-run). [`runtime.py:107-126`, `window.py:166-184`] **Fix:** pre-flight the run instruction against all matched models' contracts; warn on TORCH+window APPEND.
- **`no_partial_below` never validated** — coarser-than-chunk silently truncates the window (drops complete chunks); silently inert on models that only run `latest`/explicit-window. [`batch.py`, `window.py:115-126`] **Fix:** validate coarser-than-chunk; surface that it's only consulted on reload/no-dates-backfill.

---

## Tier 3 — gaps a data engineer will want (ranked)

1. **Output data-quality assertions** — row-count floor / not-null / unique / accepted-values / output-freshness that fail or block a run. Today freshness is only checked on **upstream inputs** (`upstream.py`, `satisfaction.py is_fresh`); a model can write 0 rows or all-NULL a key and still mark `applied`. *Highest value.* Direction: declarative `expectations=[...]`, evaluated post-write in the interval transaction, failure → `record_failure` (plumbing already exists).
2. **Target-table schema evolution** — bollhav additively self-migrates its own `z_bollhav` tables (`_migrate_state_additively`, `ensure_library`) but user targets are `CREATE TABLE IF NOT EXISTS` only; adding a column needs a destructive `recreate_table`. Direction: opt-in additive `ADD COLUMN` + drift detection vs `information_schema`, refuse type-narrowing/drops.
3. **Run metrics** — the state row has only `applied_at`; no `started_at`/`rows_written`/`duration`/bytes. Can't answer "how many rows / how slow." Direction: additive state-row columns, populated at `mark_applied` (the write path already knows the row count).
4. **Retry/backoff** — `Batch.retries` is dead config; no in-run retry. Direction: consume it with backoff, or remove.
5. **Alerting / SLA / timeout** — none. (And the changelog's "Actions"/`on_failure` surface isn't in the code.) Direction: an `on_failure` callback in the `except` block; a per-model `timeout` around `execute()`; output-SLA = inverse of #1.
6. **Backfill replay** — much improved by the run-modes redesign + `reset_interval`/`reset_model`/`reset_models`. Residual gaps: a **rerun-errors** primitive (flip only `status='error'` → pending), a **per-model run selector** (the run path is tag-expression only — no CLI), and an **APPEND-replay guard/warning** when torch/bulldozer meets APPEND without staging.
7. **Pipeline-level `run_id`** + structured logging — `run_id` is per-model (`ModelRun`); nothing correlates one `python main.py` invocation; logs are plain `%`-templates. Direction: mint a parent run_id in `@load_models`, thread it; optional structured formatter.
8. **SCD2 / MERGE-with-delete** — three write modes only; MERGE is documented "wait for PG15," not implemented; no history dimensions; cross-batch full-sync only via time-windowed `recreate_partition`. Direction: a `MERGE` (delete-not-in-source) mode + SCD2 driven by `unique_columns` + validity columns.
9. **`sensitive`/PII flag is dead** — `DatabaseColumn.sensitive` is never read and **not even persisted** to the library catalog. Direction: persist it (and column `description`) into the library `columns` metadata; optionally redact in logs.
10. **Catalog/docs generator** — the library table has rich metadata (columns, types, tags, contract, upstreams+freshness) but no generator turns it into a browsable catalog (`LINEAGE.md`: "isn't a built-in command yet"). Lineage is model-level only. Direction: render library JSON → static docs. Column-level lineage is genuinely hard (opaque Python transforms) — fine to skip.
11. **CI dry-execute** — model defs are pure/testable offline, but write/state paths need a live DB; there's a dry-*state* planner but no data dry-run that checks the produced frame against `target.columns`. Direction: dry-execute against a fixture, assert the output frame matches the declared columns.

---

## What's solid (credit — don't over-build)
- **Union-coverage satisfaction is real** — `THROUGH`/`ENCAPSULATE` use a proper coverage CTE + Python mirror (`_windows_cover`); an early "single-row containment" worry was **refuted**. `EXACT` on a flexible upstream is guarded.
- **Flexible-coverage set math** (`uncovered_gaps`, `uncover_span`, `clear_window`) has no gap/overlap/off-by-one; half-open `[)` throughout.
- **Contract-reload prefill converges and doesn't churn** (`DO NOTHING` + separate, window-scoped invalidation).
- **Model is genuinely frozen** post-construction; **topological sort** handles self-loops/diamonds/partial subgraphs; **`Source` validation** catches contract/freshness misuse.
- **Identifier quoting is injection-safe** on both backends; `RECREATE_PARTITION` window is half-open `[since, until)` consistently.
- **Run-modes redesign** is genuinely good and honest about deferred items; `reset_interval`/`reset_model` operator API is a real improvement.
- **Deliberately user-owned connection/secrets boundary** (bollhav never opens/stores DSNs) is good design, not a gap. Additive self-migration of `z_bollhav`, the errors table, and curfew-as-wall-clock-gate are well done.

---

## Refuted (don't re-chase)
- THROUGH-degenerates-to-WHOLE for a windowless downstream — **intended/documented**, the only coherent semantics.
- Window satisfaction is "single-row containment only" — **false**, union coverage is implemented.
- Model lock acquired/released on different `PostgresState` instances — **sound**, both share the same `state_conn` (pg advisory locks are session-scoped).
- "Zero/inverted resolved window silently yields no intervals" — **refuted**, `TZInterval.__post_init__` raises `SinceAfterUntilError` (but deep/generic; a `RUN_SINCE/UNTIL`-level message would be friendlier — see Tier 2).

## Top 5 to fix first
1 (DST `split_window`), 2 (non-atomic state/data + dead `update_state=False`), 3 (applied-gate TOCTOU), 4 (lock-free torch/bulldozer), 5 (PG PK-only UPSERT) — silent data corruption or crashes on normal configs.

## Planned API ergonomics (bool → enum, not bugs)
Same upgrade as `Materialization` (done on `feat/query-on-model`): replace a `bool` with a named enum so the choice reads as intent and can grow.
- **`TimeChunking.fixed_intervals: bool` → `rigidity: Rigidity` enum** — `Rigidity.RIGID` (default, was `True` — the chunk grid is the state identity) / `Rigidity.FLUID` (was `False` — freely re-sliceable coverage). Touches `window.py`, the coverage/state engine, run-modes, tests, docs. The learn site already describes it as `rigidity` (acting as if implemented).
