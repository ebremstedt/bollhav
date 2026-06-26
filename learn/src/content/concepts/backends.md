---
title: "Backends"
body: "Where state can live."
---

State needs a Postgres store — the `z_bollhav` library, per-model status tables, and advisory locks all live there. A model can write its `target` to MSSQL while keeping state in a separate Postgres (a `STATE_DSN`). With no Postgres state store at all, MSSQL models are just stateless units of work, so an external orchestrator (Airflow / cron) has to own ordering and retries.
