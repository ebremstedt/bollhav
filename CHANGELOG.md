# Changelog

## [2.0.137] - 2026-05-29

### Added

- `Target.mutations` — per-pipeline-run tracker for one-shot setup DDL. Six flags (`schema_created`, `table_created`, `recreated`, `truncated`, `indexes_created`, `uniques_added`) flip the first time the corresponding statement fires in `ensure_schema_and_table`; subsequent intervals skip the work. `Target.setup_complete` property gates the whole function — on a 365-interval backfill, one setup transaction instead of 365. Each flag is "did that DDL fire?", and `setup_complete` reconciles them against the directive (e.g. `m.recreated OR not target.recreate_table`). Field is `init=False` so the verbose access path `target.mutations.X` always reads as runtime state, never config. See [Mutating targets](docs/content/MUTATIONS.md).
- Views and library-only tables as upstreams in `z_bollhav.model_library`. View-models auto-register on every bootstrap; static / no-state tables opt in via `Model(library=True)`. The library gained a `model_type` column and made `state_schema` / `state_table` nullable; `is_satisfied` returns `True` on mere library presence when state pointers are NULL (zero SQL), otherwise runs the existing applied-row check. `lookup` now returns a `LibraryEntry` `NamedTuple`. Block reasons name the upstream's `model_type`. Documented in [STATE.md](docs/content/STATE.md#upstreams-views-and-library-only-tables).
- New example [examples/state_with_view](examples/state_with_view/) — `warehouse.orders` (table, state + staging) → `warehouse.v_high_value_orders` (VIEW) → `warehouse.high_value_sums` (table, state + staging). Demonstrates view auto-registration and the downstream's satisfaction-by-presence path end-to-end.

### Changed

- `ensure_library` migrates older `z_bollhav.model_library` tables with **additive, idempotent** ALTERs (sentinel-gated via `information_schema`) instead of `DROP TABLE`. The library is shared across multiple pipelines run by potentially-different bollhav images at the same time, so dropping would brick concurrent old-image writers — `ADD COLUMN model_type TEXT NOT NULL DEFAULT 'TABLE'` and `ALTER COLUMN state_schema/state_table DROP NOT NULL` let both versions coexist. Same constraint going forward: never drop, never tighten `NOT NULL`.

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
