---
title: "Dry runs"
body: "Inspect without committing."
---

Two safety valves: a **dry run** resolves what *would* execute — the matched models and their windows — without reading or writing data, and **`DRY_STATE`** runs for real but records nothing in state (no `applied` rows). Both are one click in the TUI; handy for checking a tag expression or a backfill window before firing the real run.
