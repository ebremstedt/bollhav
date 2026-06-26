---
title: "Contract"
body: "A model's own time window."
---

`Contract(begin=, end=)` is the historical scope a model's runs may target; bollhav walks that range in reload / backfill. Ignored in latest mode (which reads from `now()`). Distinct from an `UpstreamContract` — that's a downstream's gating policy on an input.
