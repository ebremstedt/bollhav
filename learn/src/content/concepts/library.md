---
title: "Model library 📚"
body: "The shared registry of models, state, and errors"
---

The `z_bollhav` library lives in Postgres and holds the cross-pipeline graph, the state rows, and the error history all in one place. Tooling reads it to colour lineage by status — applied, blocked, stale — which makes it feel closer to a live observability graph than a static diagram.
