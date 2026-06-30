---
title: "Dry runs 🧪"
body: "Inspect what a model would run without committing any data"
---

Two safety valves. A **dry run** works out what *would* execute — the matched models and their windows — without reading or writing any data. **`DRY_STATE`** goes further and runs for real, but records nothing in state (no `applied` rows). Both are one click in the TUI, handy for sanity-checking a tag expression or a backfill window before you fire the real run.
