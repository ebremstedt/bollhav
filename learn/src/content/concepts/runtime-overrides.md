---
title: "Runtime overrides"
body: "Reshape a model for one run."
---

Env vars rewrite a model at load time without touching code: `INTERVAL_OVERRIDE` swaps the chunk, `WINDOW_OVERRIDE` the latest-mode catch-up scope, `LOOKBACK_OVERRIDE` the late-data re-walk, and `TIMEZONE_OVERRIDE` the interval timezone. `USE_SCHEMA_SUFFIX` writes under a suffixed schema (`z_bollhav_<suffix>`) so a test run stays isolated from prod. Overrides on a fixed grid fork its state identity — that's by design.
