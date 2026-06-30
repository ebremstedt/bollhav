---
title: "Orchestration 🐙"
body: "Centralized model ordering"
---

The stateless alternative: Airflow, Dagster, or cron owns the graph — it encodes raw → clean → marts, plus retries and schedules — while bollhav just runs whichever model you point it at (chosen by `TAGS`). It's the only setup where state isn't supported (for example on MSSQL). The two can also compose: a thin trigger on the outside with contracts doing the real ordering inside. Put Airflow in front of stateful models and it becomes a timer, not a dependency engine.
