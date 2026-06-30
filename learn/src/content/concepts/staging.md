---
title: "Staging 🎭"
body: "Using temporary tables to load data in order to guarantee atomicity and avoid reading a whole table into memory"
---

A staging table (like `MssqlStaging`) is a temporary side table the model writes a window into first, then merges into the target in a single step — so the target never shows a half-written interval, and the database does the merge in place without pulling the whole target table into memory. Staging only works window by window, so staged models always run windowed (latest or backfill), the same way the fact tables do.
