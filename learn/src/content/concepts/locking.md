---
title: "Locking 🔒"
body: "Running one model from many workers safely"
---

Each interval is claimed with its own advisory lock, so you can run the same model from several workers at once — each one grabs different `pending` intervals, and any interval already `running` elsewhere is skipped. That's how a single heavy model spreads across machines, and how intervals left `running` by a crash get picked up and recovered on a later run.
