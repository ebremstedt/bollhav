---
title: "Decorators"
body: "The framework entry points."
---

`@load_models` does discovery — read env, apply overrides, match models by `TAGS`, resolve each run's window. `@model_lifecycle` wraps execution with state: prefill, lock, run pending intervals, mark applied.
