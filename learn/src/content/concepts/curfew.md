---
title: "Curfew ⏰"
body: "Forbid the model to begin work during these hours (or the inverse!)"
---

`Curfew(...)` names the wall-clock hours or weekdays when a model isn't allowed to begin a run — say, leave the source alone during business hours, on weekends, or through a maintenance window. You can flip it around too and give the hours when the model *may* run, treating everything else as off-limits. Either way, work that gets skipped stays pending and is picked up on a later run. By default a model has no curfew.
