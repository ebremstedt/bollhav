---
title: "GUI"
body: "Visualize the lineage graph."
---

A small web app (under `gui/`) that draws the cross-pipeline model graph from the central `z_bollhav` library with live state and errors. A FastAPI backend reads Postgres via `bollhav.postgres.registry`; a Svelte Flow frontend renders managed models vs sources, status lights, and upstream contracts + freshness on the arrows. A 🎄 toggle switches lappland (bare) and stockholm (full detail).
