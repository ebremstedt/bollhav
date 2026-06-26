---
title: "Batching"
body: "Time chunking of a model's work."
---

A cron expression defines the chunk (`@hourly`, `@daily`) whose ticks become the `(since, until)` windows a model iterates. `window` sets the catch-up scope in latest mode, `lookback` re-walks recent ticks to catch late-arriving data, and `tz` fixes the timezone. Whether that grid is rigid or freely re-sliceable is the fixed vs flexible intervals distinction.
