[Home](index.md) › **Decorators**

# Decorators

Bollhav ships three decorators. One wraps your `main()` entry-point; two bracket the work inside it — one per model, one per unit of work. The right-side TOC indexes each individually.

| Decorator | Wraps | What it brackets |
|---|---|---|
| `@load_models` | `main(models, debug)` | env vars + model discovery + tag matching + runtime overrides |
| `@model_lifecycle` | one model's run | asset DDL + state bootstrap, then your interval loop |
| `@execute_lifecycle` | one unit of work | the state machine + staging around a single execute |

## `@load_models`

Wraps your `main(models, debug)` entry-point. Reads every bollhav env var ([Env](ENV.md)), discovers `Model` instances under the configured folder, runs tag matching, applies runtime overrides, and hands the resolved model list to your function. See [@load_models](LOAD_MODELS.md).

## `@model_lifecycle`

Brackets one model's run: sets up assets and state, calls your function (which loops `model.intervals`), then tears down. See [Model lifecycle](MODEL_LIFECYCLE.md).

## `@execute_lifecycle`

Brackets one unit of work: read, transform, write, with the state machine and staging handled around it. See [Execute lifecycle](EXECUTE_LIFECYCLE.md).

State is **not** a decorator. It is opt-in per model via `state=State(...)` on the `Model` and handled inside the lifecycle hooks — `@model_lifecycle` seeds the state rows, `@execute_lifecycle` runs the per-unit state machine. See [State](STATE.md).

## See also

- [@load_models](LOAD_MODELS.md) · [Model lifecycle](MODEL_LIFECYCLE.md) · [Execute lifecycle](EXECUTE_LIFECYCLE.md)
- [State](STATE.md)
