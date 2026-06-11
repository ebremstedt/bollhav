[Home](index.md) › **Environment**

# Environment variables

Reference for every env var bollhav reads, in alphabetical order. The right-side TOC indexes each variable individually so you can jump straight to one. For the *narrative* on how the run modes / overrides combine, see [Runtime overrides](DECORATORS.md#runtime-overrides).

## BACKFILL_ENABLED

Bool, default `true` (when `LATEST_ENABLED=false`). Run in backfill mode — walk every interval from `BACKFILL_SINCE` (or model `bounds.begin`) up to `BACKFILL_UNTIL` (or `now()`).

## BACKFILL_SINCE

ISO 8601 datetime, optional. Overrides each model's `bounds.begin` for this run.

Example: `BACKFILL_SINCE=2024-01-01T00:00:00Z`

## BACKFILL_UNTIL

ISO 8601 datetime, optional. Overrides each model's `bounds.end` for this run.

## DEBUG

Bool, default `false`. Pretty-prints every matched model after `apply_runtime_overrides` runs — the fully-resolved version after env vars and tag overrides have been baked in.

## DRY_RUN

Bool, default `false`. Prints a concise summary of every matched model (cron + interval count, grouped by schema, alphabetized) and exits without invoking your `main()`. Strictly read-only — no DB or filesystem side effects.

## DRY_RUN_EXTRA

Bool, default `false`. Same short-circuit as `DRY_RUN` but prints an exhaustive per-model block (schema, write mode, cron/window/intervals, bounds, tags, upstream, sources, description). Setting just `DRY_RUN_EXTRA=true` implies `DRY_RUN=true`.

## DRY_STATE

Bool, default `false`. The **state-level** dry run (vs `DRY_RUN`, which is model-level). For each model it runs the state bootstrap and prints the resolved plan — how many units **would run**, plus the `applied` / `blocked` (with reasons) breakdown — then exits **without** creating target assets, writing data, or running your model logic.

Because it resolves against the real state DB, it **initializes/refreshes the state bookkeeping** (idempotent — the same setup a real run does), so it is *not* purely read-only the way `DRY_RUN` is: it writes the per-model state tables / library rows (no target-schema DDL, no data).

It accounts for the **cascade**: a model gated on an upstream that would itself run earlier in this pass shows `pending after <upstream>` (counted as pending), not `blocked`; only gates on upstreams that would *not* run are `blocked`. This relies on models being processed in dependency order — which a real `@load_models` run guarantees (it topologically sorts).

## DRY_STATE_EXTRA

Bool, default `false`. Same short-circuit as `DRY_STATE`, but lists **every actionable interval individually** (its window + `pending` / `blocked: <reason>`) instead of just the per-model counts. Setting just `DRY_STATE_EXTRA=true` implies `DRY_STATE=true`. Mirrors `DRY_RUN_EXTRA` over `DRY_RUN`.

## DSN env vars

User-named. Whatever string you pass to `Target(dsn_env_var="MY_DB")` or `SourceModel(dsn_env_var="MY_DB")` must be set in the environment at runtime with the connection string (e.g. `MY_DB=postgresql://host/db`).

## INTERVAL_OVERRIDE

Cron expression or `@alias`, optional. Replaces every matched model's `batching.time.chunk` for this run. Useful for forcing a coarser/finer cadence without editing model code.

## LATEST_ENABLED

Bool, default `false`. Run in latest mode — process the most recent complete window. Mutually exclusive with `BACKFILL_ENABLED`.

## LOOKBACK_OVERRIDE

Non-negative int, optional. Sets every model's `batching.time.lookback`. Units are cron-ticks of the model's `chunk`, not calendar days — see [Lookback](DECORATORS.md#lookback) for the worked examples.

## PROGRESS_BAR

One of `minimal` / `model` / `batch`, default `model`. Controls the verbosity of the [`@progress_bar`](DECORATORS.md#progress-bar) execution decorator.

## SCHEMA_SUFFIX

String, optional. Overrides every model's `schema_suffix` for this run — typically used in dev (`SCHEMA_SUFFIX=$USER`) to isolate writes per developer.

## STATE_DISABLED

Bool, default `false`. When `true`, forces no-state behavior on every matched model — `state` and `target.staging` are cleared, the state bootstrap and banner are skipped, and `write()` uses the direct (non-staged) path.

Full reference: [State](STATE.md).

## STATE_MODE

One of `discover` / `bulldozer`, default `discover`. Controls re-evaluation on rerun. `discover` preserves `applied` rows and re-evaluates the rest against current upstream state; `bulldozer` resets every row to the freshly-computed status (`applied_at` cleared too).

## TABLE_SUFFIX

String, required when `USE_TABLE_SUFFIX=true`. Appended to every matched model's `Target.name` for this run (`customers` → `customers_v2`). Use for blue/green hotswap inside a single schema. See [Schema vs table suffix](TARGET.md#schema-vs-table-suffix).

## TAGS

Required. Tag expression that selects which models run.

Example: `TAGS="[clean & not:views]"`

See [Tags](TAGS.md) for the full expression grammar.

## TIMEZONE_OVERRIDE

IANA timezone string (e.g. `Europe/Stockholm`), optional. Replaces every model's `batching.time.tz` for this run.

## USE_SCHEMA_SUFFIX

Bool, default `true`. When `true`, each model's `schema_suffix` / `schema_suffix_appendix` is honored. Set `false` in production to write to the bare schema name.

## USE_TABLE_SUFFIX

Bool, default `false`. When `true`, each matched model's `Target.suffix` is set to `TABLE_SUFFIX`. Off by default because most pipelines don't need it. See [Schema vs table suffix](TARGET.md#schema-vs-table-suffix) for when you would.

## WINDOW_OVERRIDE

Cron expression or `@alias`, optional. Same idea as `INTERVAL_OVERRIDE` for `batching.time.window`. **Latest mode only** — in backfill, since/until are explicit and no window is inferred. Raises if set under backfill.
