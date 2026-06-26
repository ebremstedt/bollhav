---
title: "Choreography"
body: "Ordering lives in the models."
---

With state + upstream contracts, the dependency graph lives inside the models, not a central conductor. A downstream interval stays blocked until its upstream's covering window is applied — so a dumb trigger is enough: a cron calling `python main.py` every few minutes. Each run discovers what's actionable, does it, and leaves the rest blocked for next time. No external DAG that duplicates the graph already in your models.
