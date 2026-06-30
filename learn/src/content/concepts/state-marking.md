---
title: "State marking ✅"
body: "Explicitly let state know you have made your own backfilling"
---

`STATE_MARK_APPLIED` marks the run's intervals as applied without actually executing the model. Use it when you loaded the tables some other way — outside a normal state run — and just want state to catch up and reflect that the work is already done.
