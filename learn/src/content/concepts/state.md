---
title: "State"
body: "Per-interval status tracking."
---

Turn on `state=State(...)` and each model gets its own digest-named table in the central `z_bollhav` schema — one row per `(since, until)` interval. The `status` column drives everything: `pending` (not done yet), `running` (in flight, with crash recovery), `applied` (done — skipped on re-run), `blocked` (an upstream's covering window isn't `applied` yet), and `error` (execute raised — details in the errors table, auto-retried). Gating, resumable backfills, and the GUI status lights all read off these rows.
