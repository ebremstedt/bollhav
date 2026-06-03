[Home](index.md) › [Articles](ARTICLES.md) › **Orchestration**

# Orchestration

bollhav doesn't schedule anything. It defines models and hands you lifecycle hooks; *how* and *when* they run is yours. The fork that decides your setup: **do you use [state](STATE.md)?**

| | Without state | With state |
|---|---|---|
| Dependency ordering | the orchestrator's DAG | [upstream contracts](UPSTREAM.md), in-band |
| Skip already-done work | the orchestrator (or nothing) | automatic — the `applied` gate |
| Resumable backfills / retries | you wire them | automatic (`error`→`pending`, crash recovery) |
| What you run on | Airflow / Dagster / cron + operators | a plain cron or loop |

## Without state — bring your own orchestrator

A stateless model is just a unit of work you trigger. Use Airflow (or Dagster, Prefect, cron, a Makefile) to schedule and order them. The orchestrator owns the DAG — it encodes `raw → clean → marts`, retries, schedules, backfills. bollhav just runs the model you point it at.

Two common operator shapes:

- **Python operator** — import the model + call your execute in-process.
- **Docker / Kubernetes operator** — run the model's container, selecting models with the `TAGS` env var (`DockerOperator`, `KubernetesPodOperator`).

```python
# Airflow — the DAG IS the dependency graph
with DAG("warehouse", schedule="@hourly", ...) as dag:
    raw = DockerOperator(task_id="raw",   image="pipe", environment={"TAGS": "[raw]"})
    clean = DockerOperator(task_id="clean", image="pipe", environment={"TAGS": "[clean]"})
    raw >> clean        # ordering lives here, in Airflow — not in the models
```

This is the only option on backends without state coordination — e.g. [MSSQL](MSSQL.md), where state isn't supported, so an external orchestrator (or your own loop) drives every run.

## With state — the contracts are the DAG

Turn on `state=State(...)` and declare [upstream contracts](UPSTREAM.md). Now the dependency graph lives **in the models**, and bollhav enforces it at run time:

- **Ordering is in-band.** A downstream interval is `blocked` until its upstream's covering window is `applied`. The orchestrator doesn't need to know the graph.
- **Resumability is automatic.** A re-run skips `applied` work, retries `error`, and recovers crashed `running` rows — see [State](STATE.md).
- **So you can run on a dumb trigger.** A cron that calls `python main.py` every few minutes, or one long loop. Each run discovers what's actionable, does it, and leaves the rest `blocked` for next time.

```bash
# cron — no DAG, no operator graph; contracts gate what runs
*/5 * * * *  cd /pipe && TAGS='[all]' python main.py
```

You can still put Airflow in front — but it degrades to a **timer**, not a dependency engine: one task, "run the pipeline," on a schedule. bollhav decides what's ready. You stop maintaining a second copy of the upstream graph that already lives in your models.

## Which to pick

| Reach for an external orchestrator (no state) when… | Reach for state when… |
|---|---|
| you already run Airflow/Dagster and want its UI, retries, alerting | you want resumable backfills without bespoke bookkeeping |
| dependencies are simple or you prefer them explicit in a DAG | dependencies cross pipelines/repos (a model waits on one shipped elsewhere) |
| models run on very different cadences you'd rather schedule by hand | you want horizontal scaling (per-interval advisory locks) on one model |
| the target has no state support (MSSQL) | you'd rather not maintain a DAG that duplicates your models' upstream graph |

They compose: a thin cron/Airflow **trigger** on the outside, **state + contracts** doing the real ordering inside.

## See also

- [State](STATE.md) · [Upstream](UPSTREAM.md) — the in-band dependency + resumability machinery.
- [Model lifecycle](MODEL_LIFECYCLE.md) · [Execute lifecycle](EXECUTE_LIFECYCLE.md) — what each trigger actually runs.
