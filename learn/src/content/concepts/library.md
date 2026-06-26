---
title: "Library"
body: "The central model + state registry."
---

The `z_bollhav` library in Postgres holds the cross-pipeline graph, state rows, and errors. Tooling reads it to colour lineage by status — applied, blocked, stale — closer to an observability graph than a static DAG.
