---
title: "Model 🧩"
body: "The Model object everything hangs off of"
---

A `Model` ties together where it writes (`target`), what it reads (`upstream` managed inputs plus `Source` external ones), whether it tracks `state`, how it chunks time (`batching` + `temporality`), its own time bounds (`contract`), and how it's selected (`tagging`). Discovery finds every `Model` in a folder; `TAGS` picks which ones a run actually touches.
