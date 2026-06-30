---
title: "State 🚦"
body: "Keeps track of the state of the model"
---

Turn on `state=State(...)` and each model gets its own table in the central `z_bollhav` schema — one row per `(since, until)` interval, each carrying a `status`: `pending` (not done yet), `running` (in flight, with crash recovery), `applied` (done), `blocked` (an upstream's covering window isn't applied yet), or `error` (the model raised — retried automatically).

That bookkeeping buys you two things. **Nothing slips through:** any interval that's still `pending`, `blocked`, or `error` is owed, so repeated runs keep chipping away until every window is `applied` — the data is guaranteed complete. **Nothing is done twice:** an `applied` interval is skipped on the next run, so data that's already loaded is never loaded again. Gating, resumable backfills, and the GUI status lights all read off these same rows.
