---
title: "Locking"
body: "Many runners, one model."
---

State rows are claimed with per-interval advisory locks, so you can run the same model from several workers at once — each grabs different `pending` intervals, and a row already `running` elsewhere is skipped (`ModelLockedError`). That's how a single heavy model scales horizontally, and how crashed `running` rows get recovered on a later run.
