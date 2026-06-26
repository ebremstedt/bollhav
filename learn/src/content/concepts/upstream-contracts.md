---
title: "Upstream contracts"
body: "How a model gates on its inputs."
---

`EXISTS` just needs the upstream registered. `EXACT` demands the same interval. `ENCAPSULATE` resolves by the upstream's shape — window coverage for `TEMPORAL`, an existence row for `TIMELESS`. `THROUGH` chains coverage; `WHOLE` needs the whole table loaded.
