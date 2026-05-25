[← back to Tags](TAGS.md)

# Advanced tag prefixes

!!! note "TODO"
    This page needs a proper rewrite — the content below was lifted from the original TAGS.md and reads more like reference dump than an explainer. Restructure as a guided walkthrough of when to reach for these prefixes and what trade-offs they imply.

## Controlling how reload chunks its work

Plain `r:` reloads using whatever the model is statically configured with (the `mode` / `row.batch_size` / `interval.expression` fields on the model's `Batch`). Two extended prefixes let you override that *at match time* without touching the model code — `apply_runtime_overrides` bakes them into the returned model's `batching`:

| Prefix | Forces | Notes |
|--------|--------|-------|
| `r_row_<N>:` | ROW-mode reload, `batch_size=<N>` | `<N>` is the row count per chunk. Capped at 10000. Compatible with `WriteMode.APPEND` and `WriteMode.UPSERT_NO_DELETE`. |
| `r_interval_<@alias>:` | INTERVAL-mode reload, `interval_expression=<alias>` | `<alias>` is one of `@minutely`/`@minute`, `@hourly`/`@hour`, `@daily`/`@day`, `@weekly`/`@week`, `@monthly`/`@month` (sourced from roskarl). |

For a cadence that doesn't have a named alias, set it statically on the model (`Batch(interval_expression="*/15 * * * *")`) and reload with plain `r:`, or override globally with the `INTERVAL_EXPRESSION_OVERRIDE` env var — arbitrary cron expressions are intentionally not accepted inside tags.

Both extended prefixes accept the `reload_` long form (`reload_row_100:`, `reload_interval_@daily:`), and both work at tag-level and group-level:

| Syntax | Meaning |
|--------|---------|
| `[r_row_100:vPAS]` | reload `vPAS` in ROW mode, 100 rows/chunk |
| `reload_row_500:[foo & bar]` | group-level — ROW mode, 500 rows/chunk for both |
| `[r_interval_@daily:sales]` | reload `sales` in INTERVAL mode, one chunk per day |
| `r_interval_@hourly:[facts]` | group-level — hourly chunks for every matched model |
| `[r_row_100:foo][r_interval_@daily:bar]` | mix — `foo` in ROW/100, `bar` in INTERVAL/@daily |

Tag overrides trump the model's static `Batch`, so the same model can be run ROW one day and INTERVAL the next without code changes. Validation fires at parse/match time — an unknown cron alias or a row-batch over the cap raises immediately.

## Combining `r:` and `not:`

The prefixes can be combined:

| Syntax | Meaning |
|--------|---------|
| `[r:sales & not:foo]` | match `sales`, exclude `foo`, reload matched |
| `r:not:[foo]` | match everything without `foo`, reload all |
| `r_row_100:not:[views]` | reload everything except `views` in ROW mode |
