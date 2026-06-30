---
title: "Run modes ▶️"
body: "Which slice of time a run goes after"
---

A run mode picks which window the run targets. `LATEST_ENABLED` catches up the most recent complete unit. `BACKFILL_ENABLED`, with `BACKFILL_SINCE` and `BACKFILL_UNTIL`, runs a bounded historical window. Reload (`r:[tag]`) re-runs a tag's entire contract range from the start.
