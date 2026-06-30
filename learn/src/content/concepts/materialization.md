---
title: "Materialization 🧱"
body: "Whether a model becomes a table or a view"
---

`materialization` chooses how a model's output shows up in the database: a `TABLE` (the default — rows written by the execute function) or a `VIEW` (built from the model's defining `query`, its SELECT body). It's a deliberate choice, separate from what the model computes — having a `query` alone doesn't make something a view.
