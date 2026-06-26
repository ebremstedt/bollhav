---
title: "Errors table"
body: "Full error history, centrally."
---

When execute raises, the interval's state row goes `error` and a row lands in the shared `z_bollhav.errors` table — `full_name`, `run_id`, `error_type`, `error_message`, `traceback`, `created_at`. It keeps history across runs, joinable with a model's state table on `(since, until)` or on `run_id`. `discover` auto-retries the failed interval on the next run.
