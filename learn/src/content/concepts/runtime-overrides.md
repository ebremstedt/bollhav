---
title: "Runtime overrides 🎛️"
body: "Change a model's settings at runtime"
---

Environment variables can rewrite a model as it loads, without touching code: `INTERVAL_OVERRIDE` changes the chunk size, `WINDOW_OVERRIDE` the catch-up scope in latest mode, `LOOKBACK_OVERRIDE` how far back late data is re-walked, and `TIMEZONE_OVERRIDE` the interval's timezone. `USE_SCHEMA_SUFFIX` writes under a suffixed schema (`z_bollhav_<suffix>`) so a test run stays isolated from production. Overriding the chunk on a fixed-interval model forks its state identity — that's by design.
