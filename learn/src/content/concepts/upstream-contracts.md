---
title: "Upstream contracts 📜"
body: "What a model requires of its inputs before running"
---

Before a model runs a window, it checks that its inputs are ready — and each policy sets the bar differently. `EXISTS` only asks that the upstream is registered at all. `EXACT` demands the very same window. `ENCAPSULATE` matches the upstream's shape — full window coverage for a temporal input, a single existence row for a timeless one. `THROUGH` chains that coverage across a span, and `WHOLE` waits for the entire table to be loaded.
