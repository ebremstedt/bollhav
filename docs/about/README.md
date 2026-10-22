# About

## Born to deal with a lot of models

Bollhav was born to deal with a lot of models. The model object is
standardized and because of that bollhav generates great metadata for free: 
- for [execution](../concepts/decorators.md#progress-bar)
- for [lineage](../articles/lineage.md)
- for [state](../model/state.md)

## For ingesting and transforming alike

A model is the same shape whether it pulls a raw table in from a source
system or derives one from tables already in the warehouse. Ingestion,
cleaning, modelling, marts: one declaration and one function each, run and
tracked the same way, into the same backends (Postgres, SQL Server,
Iceberg; see [Backends](../concepts/backends.md)). There is no second tool
for the first step.

## Designed for data engineers to be efficient

Your code is the part only you can write: read, transform, write. The
[decorators](../concepts/decorators.md) carry the rest, discovery, ordering,
tables and schemas, state, locks, upstream contracts, progress, so a new
model is a declaration and a function, not a pipeline. Select what runs
with a [tag expression](../concepts/tags.md), run the whole pipeline on
your laptop against one Postgres container, and backfill faster by
starting another process; see [Testing locally](../articles/local_testing.md).

## With or without an orchestrator

You can run bollhav without an orchestrator at all: with state on, the
upstream contracts order the models and a plain cron or loop runs them. Or
you can orchestrate them with Airflow, Dagster or whatever you already have,
if you want. See [Orchestration](../articles/orchestration.md).
