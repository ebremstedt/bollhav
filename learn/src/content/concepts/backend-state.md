---
title: "Backend: State 🐘"
body: "State is only supported on Postgres"
---

State always lives in Postgres — it's the only backend that can hold it. The `z_bollhav` library, the per-model status tables, and the advisory locks that coordinate runners all need a Postgres store, pointed at with `STATE_DSN`. With no Postgres store at all there's nowhere to track progress, so models fall back to stateless units of work and an external orchestrator (Airflow or cron) has to own ordering and retries.
