---
title: "Fixed intervals"
body: "The chunk grid is the state identity."
---

`fixed_intervals=True` (the default) makes the chunk grid the model's state identity — state is one row per `(since, until)` chunk. Because the grid is part of identity, changing the chunk forks it, so re-chunking needs a `torch` reset. Downstreams may gate `EXACT` on it, demanding the very same interval. It's the safe default — correct for any model, including aggregations and order-dependent writes that flexible intervals can't support.
