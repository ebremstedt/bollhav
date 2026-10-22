# About

## Born to deal with a lot of models

Bollhav was born to deal with a lot of models. The model object is
standardized: every model, in every pipeline, is the same kind of object
with the same fields, declaring what a table looks like, where it goes, what
it depends on and when it runs. That is what makes data engineering easy to
reason about at scale, one model or three hundred. And because every model
says it the same way, bollhav gets great metadata for free: for execution,
for [lineage](../articles/lineage.md), and for [state](../model/state.md).

## Permissive by design

You can use the model objects as configuration, for whatever kind of
database you run against, or use the backends that come with bollhav:
[Postgres, MSSQL and Iceberg](../backends.md).

## With or without an orchestrator

You can run bollhav without an orchestrator at all: with state on, the
upstream contracts order the models and a plain cron or loop runs them. Or
you can orchestrate them with Airflow, Dagster or whatever you already have,
if you want. See [Orchestration](../articles/orchestration.md).
