---
title: "Errors table 🐛"
body: "Centralized error table"
---

When a model raises, that interval's state row flips to `error` and a full record lands in the shared `z_bollhav.errors` table — name, run id, error type, message, traceback, and timestamp. The table keeps history across runs and joins back to a model's state on `(since, until)` or `run_id`. In `discover` mode the failed interval is retried automatically on the next run.
