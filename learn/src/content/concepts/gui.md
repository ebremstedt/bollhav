---
title: "GUI 🕸️"
body: "See the whole pipeline as a live graph"
---

A small web app (under `gui/`) that draws the cross-pipeline model graph from the central `z_bollhav` library, with live state and errors. A FastAPI backend reads Postgres; a Svelte Flow frontend renders managed models versus sources, status lights, and each arrow's upstream contract and freshness. A 🎄 toggle switches between lappland (bare) and stockholm (full detail).
