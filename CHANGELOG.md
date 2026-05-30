# Changelog

## [2.0.138] - 2026-05-29

Replaces the closed-shape `Mutations` struct from `2.0.137` with a pluggable **Actions** system. Same target-setup behaviour as before for built-in operations; users can now extend it.

### Added — Pluggable actions

- `bollhav.model.actions` introduces `Action`, `Phase`, and `OnFailure`. Each lifecycle operation (CREATE SCHEMA, CREATE TABLE, TRUNCATE, ADD UNIQUE, staging setup, user `GRANT` / `ANALYZE` / `COMMENT`, …) is now a callable wrapped in an `Action(name, phase, run, should_run)`. The runner walks the list, calls `should_run(target)` to gate, runs the action, records it in `target._applied_model_actions[name] = True`. See [Actions](docs/content/ACTIONS.md).
- **Four phases** on `Phase` — a 2×2 grid of (model-level vs interval-level) × (pre vs post):
  - `PRE_MODEL` — once per pipeline run, before the user's loop (current home for CREATE TABLE, indexes, staging setup).
  - `POST_MODEL` — once per pipeline run, after the user's loop returns cleanly (current home for staging cleanup; future home for user `ANALYZE` / `GRANT`).
  - `PRE_INTERVAL` and `POST_INTERVAL` — placeholder values for the eventual collapse of `@state`'s `mark_running` / `mark_applied` / `record_failure` into the action system. Enum values exist; runners aren't shipped yet.
- **Two lists on `Target`** so framework defaults and user-added actions stay separately addressable:
  - `default_actions: list[Action] | None = None` — framework's; `None` means "resolve lazily from the backend's `default_actions()` factory." Set to `[]` to opt out of every framework default. Set to a filtered list to opt out selectively.
  - `actions: list[Action] = []` — user-added. Always runs after defaults.
  - `effective_actions` property returns `default_actions ++ actions`.
- `Target.on_failure: OnFailure = FAIL_FAST` — per-target policy for `POST_MODEL` action failures. `FAIL_FAST` halts the pipeline POST sweep; `SKIP` logs and continues. `PRE_MODEL` is always fail-fast.
- Backend-specific defaults live in `bollhav.postgres.actions.default_actions()`, which returns the canonical 10-action list (8 PRE_MODEL + 2 POST_MODEL).
- Public runners: `run_pre_model_actions(conn, model)` and `run_post_model_actions(conn, model)`. Called from `write()` (PRE) and `@load_models` after `main()` returns cleanly (POST).

### Changed — `Mutations` removed; user-extension story

- `Target.mutations` (struct of 8 bools) is gone. Same information now lives in `target._applied_model_actions: dict[str, bool]` keyed by action name. Field is `init=False`; the runner owns the dict.
- `target.setup_complete` now walks `effective_actions` filtered to `Phase.PRE_MODEL` against `_applied_model_actions` — semantically identical to the old reconciliation, mechanically generalised.
- `ensure_schema_and_table(conn, model)` and `ensure_table(conn, model)` retained as backwards-compatible shims that forward to `run_pre_model_actions`. New code should call the runner directly.
- `staging.ensure_staging_schema` removed (now `staging_schema_created` action). `staging.ensure_staging_table` split into `ensure_staging_table_per_interval` (for `StagingMode.INTERVAL`, which doesn't fit the one-shot pattern) plus the `staging_table_created` action (for `StagingMode.REUSED`).

### Migration

- Replace `target.mutations.X` reads with `target._applied_model_actions.get("X")` — same key, same boolean.
- Replace `target.mutations.X = True` writes with `target._applied_model_actions["X"] = True` (rarely done outside the runner).
- For custom DDL that should run once per pipeline run, define an `Action(name, Phase.PRE_MODEL or Phase.POST_MODEL, run_callable)` and append to `Target(actions=[...])`. The framework defaults still fire because they live in `default_actions`.
- Test fixtures that set `mutations = Mutations()` should now set `_applied_model_actions = {}` and `actions = []` (and `default_actions = None` to trigger lazy resolution, or `default_actions = default_actions()` to pin them eagerly).

## [2.0.137] - 2026-05-29

Large release. Four previously-coupled feature surfaces — **state, staging, library, and target setup** — are decoupled so they can be opted into independently along orthogonal axes, plus a runtime mutation tracker on `Target` that skips redundant DDL on every interval after the first.

### Added — State: per-model interval state tables

State tracking is opt-in via `Model(state=State(...))`. When set, bollhav creates and maintains per-model state in two tables alongside the target:

| Table | Schema (default) | Contents |
|---|---|---|
| `<target_name>_state` | `z_<target_schema>` | One row per `(since, until)` interval the model says should exist. Columns: `id`, `run_id`, `since`, `until`, `status`, `blocked_reason`, `applied_at`. `(since, until)` is unique. |
| `<target_name>_errors` | `z_<target_schema>` | One row per execute exception across all runs. Columns: `id`, `run_id`, `full_name`, `error_type`, `error_message`, `traceback`, `created_at`. Joinable with the state table on `(since, until)` for per-interval inspection or on `run_id` for per-invocation lookups. |

The state schema defaults to `z_<target_schema>` so bollhav-owned tables stay out of the user's schemas. Override via `State(schema_prefix=...)` / `State(table_suffix=...)`.

The status column is one of:

| Status | Set by | Meaning |
|---|---|---|
| `pending` | bootstrap (prefill) | queued to run; the user's loop iterates these |
| `running` | `@state` decorator (immediately before invoking `execute`) | currently being processed — visible in live dashboards |
| `applied` | `@state` after a clean run, **or** the staged flush in the same transaction as the data move | completed successfully |
| `blocked` | bootstrap or live re-check | an out-of-pipeline upstream isn't satisfied; reason carries a `STATE_NNN` block code |
| `error` | `@state` when execute raises | full details written to the sibling `_errors` table, then re-raised; auto-retried on the next run under `STATE_MODE=discover` |

`STATE_MODE=discover` (default) preserves `applied` rows on re-evaluation and recomputes everything else against the current upstream state; `STATE_MODE=bulldozer` resets every row to the freshly-computed status (`applied_at` cleared too). `STATE_DISABLED=true` forces a pipeline to run with no state tracking even when models declare it.

Per-interval Postgres advisory locks (keyed by `(model.full_name, since, until)`) let multiple workers on the same model split intervals safely — same-interval collisions silently skip; different-interval workers don't conflict. An optional model-wide `model_lock` is still available for stricter one-pipeline-at-a-time semantics.

See [State](docs/content/STATE.md) for the full status lifecycle, re-evaluation rules, locking, and env-var reference.

### Added — `Target.mutations`: runtime tracker for one-shot setup DDL

- `Target.mutations` field (type `Mutations`, `init=False`) — per-pipeline-run record of which setup statements have already fired. Eight flags so far: `schema_created`, `table_created`, `recreated`, `truncated`, `indexes_created`, `uniques_added`, `staging_schema_created`, `staging_table_created`. Each one flips the first time its DDL fires in `ensure_schema_and_table` or in the staging path; subsequent intervals short-circuit at the flag check. Each flag is "did that DDL fire?", and the access path `target.mutations.<flag>` is deliberately verbose so it always reads as runtime state, never config.
- `Target.setup_complete` `@property` — reconciles every flag against its corresponding directive (`m.recreated OR not target.recreate_table`, `m.indexes_created OR target.partitioned_by is None`, etc.) so `ensure_schema_and_table` can early-return when there's nothing left to do. On a 365-interval backfill, this means **one** setup transaction instead of 365 — every subsequent interval pays zero `BEGIN`/`COMMIT` roundtrips for table setup.
- New docs page [Mutating targets](docs/content/MUTATIONS.md) explaining the access-path-as-mutability-signal naming, the two-gate (directive + flag) pattern, and the per-pipeline-run lifetime. Linked from the Target page under `Computed: mutations`.

### Added — Views and library-only tables as upstreams

- `Model(library=True)` opt-in — register the model in `z_bollhav.model_library` so downstreams can claim it as upstream. One mechanism, three use cases:
    - Static lookup tables / externally-loaded data with no state of their own.
    - VIEW models that are intended to be claimed as upstream. A view without `library=True` is still a perfectly valid bollhav model (the `CREATE OR REPLACE VIEW` runs in the user's execute), it just won't appear in the library.
    - Any other state-less model that should be discoverable cross-pipeline.
- The library gained a `model_type` column (`TABLE` / `VIEW`) and made `state_schema` / `state_table` nullable so view rows and library-only TABLE rows can store NULL for state pointers.
- `is_satisfied(entry=...)` dispatches by entry shape: NULL state pointers ⇒ presence in the library is the satisfaction (zero SQL, no `pg_views` lookup); set state pointers ⇒ the existing applied-row encapsulation check. Block reasons name the upstream's `model_type` so operators can tell at a glance which kind they're waiting on.
- `lookup` now returns a `LibraryEntry` `NamedTuple` (`upstream`, `model_type`, `state_schema`, `state_table`) instead of a positional tuple — `is_upstream_satisfied_live` and `_resolve_interval_status` updated to pass it through.
- Documented in [STATE.md → Upstreams: views and library-only tables](docs/content/STATE.md#upstreams-views-and-library-only-tables) with worked examples.

### Added — Staging without state, and a two-mode staging lifecycle

- Staging no longer requires `state=State(...)`. The previous `Model.__init__` validation is gone, `staging._assert_supported` no longer rejects, and `flush` only fires the state-row `UPDATE` when `model.state is not None`. Without state you keep both useful staging properties — memory-bounded chunked writes and atomic per-interval finalization (INSERT + UPDATE committed together) — and accept that re-runs re-process every interval because there's no `applied` gate.
- `StagingMode` enum on `Staging(mode=...)` controls the staging-table lifecycle:
    - `REUSED` (default) — one staging table per pipeline run. `CREATE TABLE` once (gated by `mutations.staging_table_created`), `TRUNCATE` at the start of every interval after the first, never `DROP` in flush. Cheapest on long backfills: ~4× less catalog churn (1 CREATE + N TRUNCATEs vs N CREATEs + N DROPs).
    - `INTERVAL` — the previous behaviour. `CREATE TABLE` every interval, `DROP TABLE` inside `flush`'s tx (unless `keep_after_flush=True`). Use when you want each interval's staging artifact to be inspectable on crash.
- `ensure_staging_schema` (`CREATE SCHEMA IF NOT EXISTS z_<schema>`) — new helper, gated by `mutations.staging_schema_created`. Previously the staging schema was created only as a side-effect of `pg_state.ensure_tables`; staging-without-state needs its own path.
- `gc_orphan_staging_tables` is now **auto-invoked at bootstrap** for every staging model (previously defined and exported but never called). A crashed prior run's staging table is dropped on the next pipeline start, matching the long-standing docstring promise. Logs at `debug` per drop, `warning` on connection failure.

### Added — Examples and tests

- New runnable example [examples/state_with_view](examples/state_with_view/) — `warehouse.orders` (table, state + staging) → `warehouse.v_high_value_orders` (VIEW) → `warehouse.high_value_sums` (table, state + staging). Walks the topo-sorted chain end-to-end and demonstrates view auto-registration and the downstream's satisfaction-by-presence path.
- ~25 new tests across `test_library.py`, `test_staging.py`, `test_state.py`, `test_write_modes.py` — covering both staging modes (REUSED 1-CREATE + N-TRUNCATEs across two intervals; INTERVAL still drops on flush), staging without state (both modes), library register/lookup for the new `model_type` + nullable-state-pointers shape, satisfaction-by-presence for view and library-only entries, `library=True` register-only bootstrap path, and the `Mutations` one-shot semantics for the two new staging flags.

### Changed — Schema migrations on `z_bollhav.model_library` are additive

- `ensure_library` migrates older library tables with **additive, idempotent** ALTERs (sentinel-gated via `information_schema`) instead of `DROP TABLE`. The library is shared across multiple pipelines run by potentially-different bollhav images at the same time — dropping would brick concurrent old-image writers. The new shape uses `ADD COLUMN model_type TEXT NOT NULL DEFAULT 'TABLE'` (old-image inserts that omit `model_type` land as `TABLE`) and `ALTER COLUMN state_schema/state_table DROP NOT NULL` (so the new code can write NULLs for view and library-only rows). Same rule going forward for any bollhav-owned shared table: never `DROP`, never tighten `NOT NULL`.

### Changed — Validation

- Removed `Model.__init__`'s "`target.staging` requires `state=State(...)`" check. Both combinations (with state, without state) are supported; see the staging section above for the trade-offs.
- `model.intervals` in **backfill mode** now raises `ValueError` when `directives.until` (i.e. `BACKFILL_UNTIL`) is unset, instead of silently falling back to `latest_complete_interval()` ("today"). Backfill means a specific window — both ends must be pinned. For "to the latest complete tick" use `LATEST_ENABLED=true` (latest mode); for "to `bounds.end`" use reload mode. This was found via E2E: a programmatic caller that bypassed `@load_models` got 880 intervals instead of the 3 the model's `bounds` declared, because the silent fallback ignored `bounds.end`. Production callers driven by `@load_models` with `BACKFILL_UNTIL` set are unaffected.

### Changed — `StateMode` rename + new default

- `StateMode.RESPECT` → `StateMode.DISCOVER` (default). Same behaviour: prefill the contract intervals into the state table, preserve existing `applied` rows, re-evaluate everything else. `discover` is a clearer name for "the prefill + run-what's-left mode."
- `StateMode.DISRESPECT` → `StateMode.BULLDOZER`. Same behaviour: reset every row to the freshly-computed status, regardless of prior value (`applied_at` cleared too).
- Env var values follow: `STATE_MODE=discover|bulldozer`. Default is `discover`.

### Changed — `StagingMode.PER_INTERVAL` → `StagingMode.INTERVAL`

- Just a rename for symmetry (no behavioural change). `REUSED` is still the default; `INTERVAL` is the opt-in mode that creates+drops a staging table per interval.

### Changed — Views need `library=True`, no auto-register

- A `ModelType.VIEW` model no longer auto-registers in the library. Opt in via `Model(..., library=True)`, same mechanism as a state-less TABLE that wants to be claimable as upstream. A view without `library=True` is still a valid model (gets `CREATE OR REPLACE VIEW`d each run) — it just won't appear in `z_bollhav.model_library` and therefore can't be claimed as an upstream.

### Note on coupling

After this release the four feature surfaces compose along independent axes — pick any subset of `state=State(...)`, `target.staging=Staging(...)`, `library=True`, and (for state-tracked tables) `Mutations` runtime gating, and the cross-cutting concerns (upstream satisfaction, schema migration, DDL gating, orphan cleanup) all keep working. The view path is the one mandatory auto-registration; everything else is opt-in.

## [2.0.136] - 2026-05-28

### Added

- `Target.suffix` and `Target.suffix_appendix` fields, plus a `name_resolved` `@property` that returns the base `name` with the suffix (and optional `strftime` appendix) applied. Mirrors what `TargetSchema.resolved` already does for the schema — same three-field triple, same resolution rule — but lives flat on `Target` because there's not enough table-specific structure to justify a separate `TargetTable` wrapper. `suffix_appendix` defaults to `None` (unlike `TargetSchema`'s `"%y%V"` default) because the typical use case — blue/green table hotswap — wants a stable predictable name (`customers_v2`), not a time-varying one. Opt in by setting an appendix when you want throwaway sandbox names.
- `TABLE_SUFFIX` and `USE_TABLE_SUFFIX` env vars on `@load_models`, parallel to `SCHEMA_SUFFIX` / `USE_SCHEMA_SUFFIX`. `USE_TABLE_SUFFIX` defaults to `false` (off) — most pipelines don't want a per-table rename. When `true`, `TABLE_SUFFIX` must be set; the value is baked into every matched model's `target.suffix`. `apply_runtime_overrides` gained a matching `table_suffix=""` kwarg for programmatic use. The two suffixes compose cleanly: `SCHEMA_SUFFIX=pr123 TABLE_SUFFIX=v2` lands tables at `warehouse_pr123_2614_.customers_v2`.
- New docs page [Schema vs table suffix](docs/content/SUFFIXES.md) — when to use which, the limitations of each (Postgres' 63-char truncation tightens, cross-table SQL doesn't follow renames, etc.), and how the two compose. `TABLE_SUFFIX` / `USE_TABLE_SUFFIX` added to the env-vars table in [RUNTIME_OVERRIDES.md](docs/content/RUNTIME_OVERRIDES.md) and to [ENV.md](docs/content/ENV.md). [TARGET.md](docs/content/TARGET.md) gained sections for the new `suffix`, `suffix_appendix`, and `name_resolved` fields.

### Changed

- All DDL bollhav emits — Postgres `CREATE TABLE` / `TRUNCATE` / index name / unique-constraint name, and MSSQL `CREATE TABLE` / PK / UQ / index — now uses `target.name_resolved` instead of `target.name`. Existing models with no `suffix` set are unaffected because `name_resolved == name` in that case. `Target.full_name` now composes as `catalog.schema.resolved.name_resolved` so the fully-qualified identifier always reflects the resolved form. `Model.pretty()` and the dry-run summary render `name_resolved` for the same reason — what you see in the printout is what'll hit the database.

## [2.0.135] - 2026-05-28

### Changed

- Removed the `Source` base class. `SourceFile` and `SourceTable` no longer inherit from it — each declares `name: str` directly. `Model.source` is now annotated `SourceFile | SourceTable | None`, so pyright narrows on `isinstance` / `match` and reveals kind-specific fields (`path`, `dsn_env_var`, etc.) without the user having to fight the type-checker. Previously the annotation was `Source | None`, where `Source` only exposed `name`, forcing users to narrow with `isinstance` before reading any useful field — and pyright wouldn't even hint at which subclass to narrow to. Breaking: `from bollhav.model import Source` no longer works, and the base class can no longer be subclassed for custom source kinds (users wanting a custom source can leave `Model(source=None)` and handle it themselves, which was already the supported escape hatch).
- `SourceTable.extra` changed from `dict | None = None` to `dict` with a `field(default_factory=dict)` default. Read sites no longer need an `is not None` guard — `source.extra.get("foo")` just works. Soft-breaking for callers that branched on `extra is None` (now always `False`); behaviour is unchanged for the common `source.extra["key"]` / `source.extra.get("key")` patterns.

### Added

- `SourceFile.extra: dict` field, mirroring `SourceTable.extra`. Same `field(default_factory=dict)` semantics — fresh empty dict per instance, no `None` guards needed. Lets file-source users attach arbitrary metadata (`source.extra["my_field"] = ...`) without subclassing.

## [2.0.134] - 2026-05-27

### Changed

- `Model.infer_intervals()` → `Model.intervals` (`@property`). Iterating `for interval in model.intervals:` reads cleanly as a derived field instead of a method call. Recomputes on every access — no caching — because the result depends on `datetime.now()` via `latest_complete_interval` and a cached value would freeze the first answer across cron-tick boundaries. Snapshot it (`intervals = model.intervals`) before iterating if you also need `len()`. Behavior is otherwise identical: same return shape (`list[TZInterval] | [None]`), same `ValueError` paths for ROW mode and missing `bounds.begin`. Breaking — call sites lose the parens.

## [2.0.133] - 2026-05-25

### Added

- `Target.catalog` field (default `None`) — three-part namespacing for warehouses that use `catalog.schema.table` addressing (Snowflake, BigQuery, Trino, etc.). When set, the catalog name is added to the model's tags, and `catalog.schema.name` joins the existing `schema.name` and `name` auto-tags. `Tags` gained four new flags (`catalog_add_to_tags`, `fully_qualified_name_add_to_tags`, `unsnake_catalog_for_tags`, `unpascal_catalog_for_tags`) to control catalog-derived tag behavior — all default to a sensible truthy/falsy matching their schema/name counterparts. `Target.full_name` now returns `catalog.schema.name` when catalog is set, falling back to `schema.name` or just `name`. `apply_runtime_overrides` carries `catalog` through when rebuilding targets with a schema suffix (catalog itself is not suffixed).

## [2.0.132] - 2026-05-24

### Added

- `DRY_RUN` env var on `@load_models` — when `true` prints a concise summary of every matched model (cron and interval count, grouped by schema, table names alphabetized within each schema) and exits without invoking the wrapped `main()`. Models are listed in alphabetical order across the whole summary. Strictly read-only — no DB or filesystem side effects.
- `DRY_RUN_EXTRA` env var — same short-circuit as `DRY_RUN`, but renders an exhaustive per-model block (schema, write mode, cron/window/intervals, bounds, tags, upstream, source, description). Setting just `DRY_RUN_EXTRA=true` implies `DRY_RUN=true`.
- Both dry-run modes render a per-group **tag explanation table** above the model list. Each group of the `TAGS` expression is shown alongside its plain-English translation (e.g. `r_interval_@daily:[clean & not:views]` → `(clean and not views) (reload, daily)`).
- `explain(expression)` and `explain_groups(expression)` functions in `bollhav.model.tagexpr` — render a tag expression in plain English, either as a single string or as per-group `(raw, english)` pairs. Importable for use outside the dry-run printer.

### Changed

- The `── runtime ──` summary now embeds the progress level in the title (e.g. `── runtime ── ( batch ) ────`) instead of taking up a row. Override rows (`tz override`, `interval override`, `lookback override`, `window override`) are hidden when unset, and `debug` is hidden when off — only meaningful values appear.
- ROW-mode models render their batch size in dry-run output (`<name>   <N> rows/chunk`) instead of trying to read `model.intervals` on them, which raises by design.

### Documentation

- New `## Dry run` section in [RUNTIME_OVERRIDES.md](docs/content/RUNTIME_OVERRIDES.md) with sample output and field descriptions. `DRY_RUN` and `DRY_RUN_EXTRA` added to the env-vars table.

## [2.0.131] - 2026-05-22

### Fixed

- `TargetSchema.full_name` produced the wrong week number around year boundaries. The default `suffix_appendix` was `%y%W`, which uses Python's "Monday-starts-the-week, week 0 before the first Monday" numbering — that disagrees with ISO 8601 and produced off-by-one weeks (most visibly in early January, where ISO week 1 was rendered as week 0 or 52). Switched the default to `%y%V`, which is the ISO 8601 week number. Thanks to [@koutakis](https://github.com/koutakis) for the fix (PR #8).
- `TargetSchema.full_name` resolved the suffix timestamp against the host's local time. That meant the week-suffix could flip a day early or late depending on the runner's timezone — two pipelines on different machines could write to differently suffixed schemas for the same logical run. Now uses `datetime.now(tz=timezone.utc)`, so the suffix is timezone-stable. Also via [@koutakis](https://github.com/koutakis) in PR #8.

### Internal

- Cleaned up ruff E402 ("module level import not at top of file") warnings across the `examples/` directory. The example entry points called `os.chdir(__file__)` before importing, so the relative `src/models` lookup inside `@load_models` would resolve. Moved the `chdir` into the `if __name__ == "__main__":` block — same behaviour at runtime (it still runs before `main()` is invoked), imports now sit at the top where ruff expects them.

## [2.0.130] - 2026-05-20

### Added

- `LOOKBACK_OVERRIDE` env var and `lookback_override` parameter on `apply_runtime_overrides`, completing the runtime override set alongside `INTERVAL_EXPRESSION_OVERRIDE` and `WINDOW_EXPRESSION_OVERRIDE`. Lets a job widen every matched model's `lookback` without editing model definitions — useful for daily incrementals that need to re-check the last N intervals for late-arriving rows. Applies in latest, backfill, and reload modes. Validates non-negative at startup. See [RUNTIME_OVERRIDES.md](docs/content/RUNTIME_OVERRIDES.md#lookback) for the units gotcha (it's cron-ticks of the interval expression, not calendar days).

### Documentation

- New `## Lookback` section in [RUNTIME_OVERRIDES.md](docs/content/RUNTIME_OVERRIDES.md) with a worked-examples table covering the common interval/lookback combinations, since the cron-tick unit is the most common footgun.
- Clarified the `lookback` parameter row in [MODEL.md](docs/content/MODEL.md) with the same caveat and a link to the runtime-overrides reference.
