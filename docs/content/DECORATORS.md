[← home](index.md)

# Decorators

Bollhav ships three decorators. One wraps your `main()` (the entry-point), two wrap your `execute()` (the per-interval worker). The right-side TOC indexes each individually.

## `@load_models`

Wraps your `main(models, debug)` entry-point. Reads every bollhav env var ([Env](ENV.md)), discovers `Model` instances under the configured folder, runs tag matching, applies runtime overrides, and hands the resolved model list to your function.

```python
from bollhav.model import load_models

@load_models
def main(models, debug):
    for model in models:
        for interval in model.intervals:
            execute(model, interval.since, interval.until)

if __name__ == "__main__":
    main()
```

Without this decorator you would have to call `apply_runtime_overrides()` yourself, parse the env vars manually, and propagate `debug` into your loop. See [Runtime overrides](RUNTIME_OVERRIDES.md) for the full env var flow and the programmatic `apply_runtime_overrides()` form when you need finer control.

## `@progress_bar`

Wraps your `execute(model, since, until)` worker to show timing and progress. Verbosity is controlled by the `PROGRESS_BAR` env var:

- `minimal` — one summary line per model
- `model` (default) — per-model header and bar
- `batch` — per-interval rows showing each chunk as it runs

```python
from bollhav.model import progress_bar

@progress_bar
def execute(model, since, until):
    ...
```

See [Progress bar](PROGRESS_BAR.md) for the rendering details and the example outputs at each level.

## `@state`

Wraps your `execute(model, since, until)` worker so each (since, until) interval gets recorded in the model's state table. On entry the decorator skips intervals already marked `applied`; on success it marks the row `applied`; on exception it writes to a sibling errors table (when `log_errors=True`) before re-raising.

```python
from bollhav.model import state, progress_bar

@state   # outer — gates + marks + logs
@progress_bar    # inner — timing/display
def execute(model, since, until):
    ...
```

State tracking is opt-in per model via `Model(state=State(...))` and is a no-op when `model.state is None`. See [State](STATE.md) for the full lifecycle, table shapes, and re-run semantics.

!!! note
    `@state` lives on the `state` feature branch and is not yet on `main` — the page above is a forward reference.
