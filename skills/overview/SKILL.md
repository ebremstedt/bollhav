---
name: overview
description: bollhav overview — what bollhav is and how its pieces fit. bollhav is a Python framework for building data pipelines as declarative Models (a Target + how/when it's produced), run by three decorators, with per-run virtual environments, state tracking, and tag-based selection. Use when a developer asks what bollhav is, what it can do, or where to start; it's the map that points at the other bollhav skills.
---

# bollhav — overview

bollhav is a Python framework for building **data pipelines as declarative
models**. You describe *what* a dataset is and *how/when* it's produced as a
`Model` object; bollhav handles running it, tracking coverage, enforcing
freshness contracts, and isolating environments.

This skill is the map. For a specific task, jump to the skill that owns it —
you don't have to read this first.

## The core object: `Model`

A `Model` bundles a few sub-configs (all importable from `bollhav.model`):

- **`Target`** — where the output lives: `name`, `schema`, `catalog`,
  `database` (`Database.POSTGRES` / `MSSQL`), `columns`, `write_mode`,
  optional `staging`, and the `dsn_env_var` that names its connection.
- **`Temporality`** — `TEMPORAL` (has a time axis; loaded in windows) or
  `TIMELESS` (one whole unit, no window).
- **`Materialization`** — `TABLE` (rows are written) or `VIEW` (a
  `CREATE OR REPLACE VIEW` from a `query_builder`).
- **`Batch` / `TimeChunking`** — how a temporal model is chunked
  (`chunk="@daily"`, `@hourly`, or any cron); `ChunkFix` vs `ChunkFlex`.
- **`Contract`** — the `begin`/`end` range a temporal model covers.
- **`WriteMode`** — `APPEND`, `UPSERT_NO_DELETE`, `RECREATE_PARTITION`.
- **`upstream=[Source(...)]`** — declared inputs (`SourceModel`, `SourceApi`,
  `SourceFile`, `SourceHardcoded`); `model.ref("schema.table")` turns a
  declared relational input into a suffix-aware, quoted identifier for SQL.
- **`State`** — opt-in state tracking (one row per interval / per model),
  required for staging and for gated upstream contracts.
- **`Tags` / `tags`** — how the model is selected at run time.

## How a run works — three decorators

A pipeline's entry module wires three decorated functions (see the
`pipeline-pattern` skill for the full layout):

1. `@load_models` on `main(runs)` — reads env, applies overrides, matches
   models by `TAGS=`, resolves each run's window, returns them topologically
   sorted (producers before consumers).
2. `@model_lifecycle` on `run_model(run, ...)` — per model: builds target
   assets (table, or `CREATE VIEW`), ensures state, prefills, and narrows
   `run.intervals` to what still needs doing.
3. `@execute_lifecycle` on `run_interval(run, interval, ...)` — per unit of
   work: gate → lock → check upstream contracts → run your read/write → mark
   applied.

## Virtual environments

Every run can apply a **schema suffix** so the same models write to an
isolated copy (`warehouse` → `warehouse_pr12`). `model.ref(...)` moves gated
upstream reads to the matching suffix, so a pipeline is portable across
dev / PR / prod without editing SQL. See the `env-vars` skill.

## Where to go next

- **`pipeline-pattern`** — the recommended project layout and the three
  decorators wired together.
- **`tags`** — set tags on a model and target models with the `TAGS=`
  expression syntax.
- **`env-vars`** — the env block that runs models locally (windows, modes,
  suffixes).
- **`guide`** — *interactive*: answer a few questions and get a suggested
  `Model(...)` for your pipeline. Start here if you're building something new.

Deeper reference lives in the repo: `learn/src/content/concepts/*.md` and
`docs/content/*.md`.
