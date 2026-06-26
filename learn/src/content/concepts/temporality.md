---
title: "Temporality"
body: "Whether a model has a time axis."
---

`TEMPORAL` models carry `_data_modified` and are chunked into windows. `TIMELESS` models (e.g. whole-table aggregates) rebuild in one shot and gate `WHOLE` on their inputs.
