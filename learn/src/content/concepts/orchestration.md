---
title: "Orchestration"
body: "An external conductor owns the DAG."
---

The stateless alternative: Airflow / Dagster / cron owns the graph — it encodes raw → clean → marts, retries, and schedules, while bollhav just runs the model you point it at (selected by `TAGS`). It's the only option where state isn't supported (e.g. MSSQL). The two compose — a thin trigger on the outside, contracts doing the real ordering inside; put Airflow in front and it degrades to a timer, not a dependency engine.
