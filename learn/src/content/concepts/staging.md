---
title: "Staging"
body: "Atomic incremental loads."
---

A staging table (e.g. `MssqlStaging`) is a side table a model writes a window into first, then merges into the target in one step — so the target never shows a half-written interval. Staging is interval-only, so staged models run windowed (latest / backfill), like the facts.
