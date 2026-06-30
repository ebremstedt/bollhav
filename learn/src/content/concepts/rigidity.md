---
title: "Rigidity 🗿"
body: "How locked a model's chunk grid is — rigid or fluid"
---

`rigidity` is how locked a temporal model's chunk grid is. `RIGID` (the default) makes the grid the model's state identity — one state row per `(since, until)` chunk, so re-slicing it needs a `torch` reset and downstreams can gate `EXACT`; it's the safe choice for any model, including aggregations and order-dependent writes. `FLUID` attests that the output doesn't depend on how time is partitioned (the query is window-decomposable, the write idempotent): state tracks coverage instead, so one model can be sliced at more than one grain — backfill history `@daily`, run recent data `@hourly` — without a reset. Not for aggregations.
