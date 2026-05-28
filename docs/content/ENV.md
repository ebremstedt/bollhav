[← home](index.md)

# Environment variables

Reference for every env var bollhav reads. The right-side TOC indexes each variable individually so you can jump straight to one. For the *narrative* on how the run modes / overrides combine, see [Runtime overrides](RUNTIME_OVERRIDES.md).

## TAGS

Required. Tag expression that selects which models run.

Example: `TAGS="[clean & not:views]"`

See [Tags](TAGS.md) for the full expression grammar.

## LATEST_ENABLED

Bool, default `false`. Run in latest mode — process the most recent complete window. Mutually exclusive with `BACKFILL_ENABLED`.

## BACKFILL_ENABLED

Bool, default `true` (when `LATEST_ENABLED=false`). Run in backfill mode — walk every interval from `BACKFILL_SINCE` (or model `bounds.begin`) up to `BACKFILL_UNTIL` (or `now()`).

## BACKFILL_SINCE

ISO 8601 datetime, optional. Overrides each model's `bounds.begin` for this run.

Example: `BACKFILL_SINCE=2024-01-01T00:00:00Z`

## BACKFILL_UNTIL

ISO 8601 datetime, optional. Overrides each model's `bounds.end` for this run.

## INTERVAL_EXPRESSION_OVERRIDE

Cron expression or `@alias`, optional. Replaces every matched model's `batching.interval_expression` for this run. Useful for forcing a coarser/finer cadence without editing model code.

## WINDOW_EXPRESSION_OVERRIDE

Cron expression or `@alias`, optional. Same idea as above for `batching.window_expression`. **Latest mode only** — in backfill, since/until are explicit and no window is inferred. Raises if set under backfill.

## LOOKBACK_OVERRIDE

Non-negative int, optional. Sets every model's `batching.lookback`. Units are cron-ticks of the model's `interval_expression`, not calendar days — see [Lookback](RUNTIME_OVERRIDES.md#lookback) for the worked examples.

## TIMEZONE_OVERRIDE

IANA timezone string (e.g. `Europe/Stockholm`), optional. Replaces every model's `batching.tz` for this run.

## UPSTREAM

One of `enforce` / `ignore_views` / `ignore_completely`, default `enforce`. Controls how upstream dependencies are enforced. See [Upstream mode](MODEL.md#upstream-mode).

## USE_SCHEMA_SUFFIX

Bool, default `true`. When `true`, each model's `TargetSchema` honors its `suffix` / `suffix_appendix` config. Set `false` in production to write to the bare schema name.

## SCHEMA_SUFFIX

String, optional. Overrides every model's `TargetSchema.suffix` for this run — typically used in dev (`SCHEMA_SUFFIX=$USER`) to isolate writes per developer.

## USE_TABLE_SUFFIX

Bool, default `false`. When `true`, each matched model's `Target.suffix` is set to `TABLE_SUFFIX`. Off by default because most pipelines don't need it. See [Schema vs table suffix](SUFFIXES.md) for when you would.

## TABLE_SUFFIX

String, required when `USE_TABLE_SUFFIX=true`. Appended to every matched model's `Target.name` for this run (`customers` → `customers_v2`). Use for blue/green hotswap inside a single schema. See [Schema vs table suffix](SUFFIXES.md).

## DEBUG

Bool, default `false`. Pretty-prints every matched model after `apply_runtime_overrides` runs — the fully-resolved version after env vars and tag overrides have been baked in.

## DRY_RUN

Bool, default `false`. Prints a concise summary of every matched model (cron + interval count, grouped by schema, alphabetized) and exits without invoking your `main()`. Strictly read-only — no DB or filesystem side effects.

## DRY_RUN_EXTRA

Bool, default `false`. Same short-circuit as `DRY_RUN` but prints an exhaustive per-model block (schema, write mode, cron/window/intervals, bounds, tags, upstream, source, description). Setting just `DRY_RUN_EXTRA=true` implies `DRY_RUN=true`.

## PROGRESS_BAR

One of `minimal` / `model` / `batch`, default `model`. Controls the verbosity of the [`@progress_bar`](PROGRESS_BAR.md) execution decorator.

## DSN env vars

User-named. Whatever string you pass to `Target(dsn_env_var="MY_DB")` or `SourceTable(dsn_env_var="MY_DB")` must be set in the environment at runtime with the connection string (e.g. `MY_DB=postgresql://host/db`).

## State-tracking env vars (state branch)

The following are introduced by the `state` branch and not yet on `main`:

- `STATE_MODE` — `respect` (default) or `disrespect`
- `DISCOVER` — bool; read intervals from each state-enabled model's state table instead of bounds/backfill
- `NUKE_STATE` — bool; drop each state-enabled model's state and errors tables before pre-fill (dev/CI use)

Full reference: [State](STATE.md).
