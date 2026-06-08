[Home](index.md) › [Decorators](DECORATORS.md) › **@load_models step by step**

# What `@load_models` does

`@load_models` is the standard entry point — it wraps your `main(models, debug)` function and does everything needed to hand you a ready-to-run list of models.

```python
from bollhav.model import Model, load_models

@load_models
def main(models: list[Model], debug: bool) -> None:
    for model in models:
        for interval in model.intervals:
            execute(model, interval, ...)


if __name__ == "__main__":
    main()
```

## Step by step

When you call `main()`, the decorator runs the following steps **before** your function body executes:

1. **Read env vars.** Pulls every runtime override from the environment — `TAGS`, `SCHEMA_SUFFIX`, `LATEST_ENABLED`, `BACKFILL_ENABLED`, `BACKFILL_SINCE`, `BACKFILL_UNTIL`, `INTERVAL_EXPRESSION_OVERRIDE`, `WINDOW_EXPRESSION_OVERRIDE`, `LOOKBACK_OVERRIDE`, `TIMEZONE_OVERRIDE`, `UPSTREAM`, `DRY_RUN`, `DEBUG`. Full list: [Runtime overrides](RUNTIME_OVERRIDES.md).
2. **Validate the combination.** Rejects illegal mixes — e.g. `LATEST_ENABLED` and `BACKFILL_ENABLED` both `true`, a `WINDOW_EXPRESSION_OVERRIDE` without `LATEST_ENABLED`, a negative `LOOKBACK_OVERRIDE`, an unknown `TIMEZONE_OVERRIDE`.
3. **Print the runtime summary.** A short banner showing the resolved mode, tags, suffix, and any overrides in effect — so the first lines of stdout tell you exactly what this run will do.
4. **Discover models.** Imports every `Model` instance found under `folder` (defaults to `src/models`; pass `@load_models(folder="...")` to change).
5. **Match by tags.** Filters the discovered models against the `TAGS` expression and topologically sorts them with the chosen `UPSTREAM` policy. See [Matching](MATCHING.md) and [Tags](TAGS.md).
6. **Bake in the overrides.** Each matched model is **copied** (the source models are not mutated) with:
    - `target.schema_suffix` set to `SCHEMA_SUFFIX`
    - `target.suffix` set to `TABLE_SUFFIX` (when `USE_TABLE_SUFFIX=true`) — see [Schema vs table suffix](SUFFIXES.md)
    - `batching.interval` updated by `INTERVAL_EXPRESSION_OVERRIDE` / `WINDOW_EXPRESSION_OVERRIDE` / `LOOKBACK_OVERRIDE` / `TIMEZONE_OVERRIDE`
    - `directives.latest` / `directives.since` / `directives.until` set from the chosen mode
7. **If `DRY_RUN` (or `DRY_RUN_EXTRA`):** print the matched-model summary and **return without calling your `main()`**.
8. **Otherwise: call your function** as `main(models=<resolved list>, debug=<DEBUG>)`.

## Calculating intervals

`@load_models` doesn't pre-compute time chunks — it just bakes in the directives. `model.intervals` calculates them lazily on access: it resolves `[since, until)` from the directives + `batching.interval`, then splits it into `TZInterval` chunks. See [Chunking](CHUNKING.md) and [Modes](MODES.md).

## Mental model

> Env vars in → matched-and-overridden model list out → your code runs.

You write models declaratively and a plain `main()` that loops over them. `@load_models` is the glue that turns the env into a concrete plan and gives it to you.

## See also

- [Decorators](DECORATORS.md) — the three decorators and how they compose
- [Runtime overrides](RUNTIME_OVERRIDES.md) — full env-var reference and the programmatic `apply_runtime_overrides()` form
- [Matching](MATCHING.md) — how tag filtering and topological sorting work
- [Modes](MODES.md) — latest vs. backfill vs. reload
