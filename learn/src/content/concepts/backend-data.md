---
title: "Backend: Data 🗄️"
body: "Data can land in Postgres or MSSQL"
---

A model's `target` — where its actual data lands — can be either Postgres or MSSQL. That's a separate choice from state: a model can write its tables to MSSQL while keeping its state in a separate Postgres (set with `STATE_DSN`). So the data warehouse is yours to pick, with Postgres still doing the bookkeeping behind the scenes.
