---
name: guide
description: Interactive bollhav pipeline builder. When a developer asks to "run the bollhav guide" or wants help designing a new pipeline/model, walk them through a short interview about their data and requirements, then propose a concrete Model(...) configuration by mapping their answers to the Model's keywords (temporality, materialization, batching, write_mode, staging, contract, upstream sources, tags) and tell them which read/transform/write stubs to fill in.
---

# bollhav — interactive pipeline guide

This skill is a **procedure for you (the assistant) to run an interview** and
turn the answers into a concrete `Model(...)`. Do not dump the whole thing —
ask, then propose.

## How to run it

1. Ask the questions below **using the interactive multiple-choice question
   tool**, one or two at a time, in order. Skip a question when a previous
   answer settles it (e.g. a VIEW has no `write_mode`).
2. Map each answer to Model keywords using the cheat-sheet.
3. Emit a filled-in `Model(...)` (see the output template), name the
   read/transform/write stubs to implement, and point at the `pipeline-pattern`
   skill for where the file goes and `tags` for selection.
4. Keep it conversational — confirm the target name/schema and the DSN env var
   in passing; don't over-ask.

## The interview → Model keyword mapping

**Q1. What are you producing — a physical table, or a SQL view over existing
tables?**
- Physical table → `materialization=Materialization.TABLE` (rows written by
  read/transform/write).
- SQL view → `materialization=Materialization.VIEW` + `query_builder=` (a
  SELECT string, or a callable `(model_run, since, until) -> str | Composable`
  for suffix-aware / windowed bodies). A view writes nothing.

**Q2. Does the data have a time axis you load in windows (events over time),
or is it one whole snapshot (reference / config / dimension)?**
- Windowed / time-series → `temporality=Temporality.TEMPORAL` + `batching` +
  a `contract` range.
- Whole snapshot → `temporality=Temporality.TIMELESS` (no batching, no
  contract window; unit of work is the whole thing).

**Q3 (temporal only). What's the chunk size and the covered range?**
- `batching=Batch(time=TimeChunking(chunk="@hourly" | "@daily" | "<cron>"))`.
- `contract=Contract(begin=<datetime tz-aware>, end=<datetime tz-aware>)`.
- Fixed grid vs free slicing is `ChunkFix` / `ChunkFlex` — only raise this if
  they ask; default `TimeChunking` is fine.

**Q4. Where does the input come from?**
- Another managed table you `SELECT` from → `upstream=[Source("schema.table",
  type=SourceModel())]`; build the read SQL with
  `run.model.ref("schema.table")` so it follows the env's schema suffix.
- An API → `Source(..., type=SourceApi())`; fetch it in `read.py`.
- A file → `Source(..., type=SourceFile())`; read it in `read.py`.
- Small seed/constant → `Source(..., type=SourceHardcoded())`.

**Q5. Must an upstream be fresh before this runs (enforced), or is it just a
declared dependency?**
- Enforced → make the source **gated**: `Source(..., type=SourceModel(),
  contract=UpstreamContract.ENCAPSULATE)` **and** add `state=State()` (gated
  upstreams require state).
- Declared only → plain `Source(...)`, no contract.

**Q6 (table only). How should writes behave?**
- Add new rows only → `write_mode=WriteMode.APPEND`.
- Update/insert by key, never delete → `write_mode=WriteMode.UPSERT_NO_DELETE`
  (needs unique columns on the target).
- Replace the whole window each run → `write_mode=WriteMode.RECREATE_PARTITION`
  (needs a partition column `partition_on=True` and a run window).

**Q7. Big loads, or need a crash-safe atomic swap?**
- Yes → `staging=Staging()` on the `Target` (also add `state=State()` —
  staging requires it).
- No → omit.

**Q8. How will you select this at run time?**
- Set `tagging=Tags(tags={...})`. Remember the name/schema/family tokens are
  auto-derived — see the `tags` skill; usually one or two extra tags is plenty.

## Output template

Fill this in from the answers and show it, then list the stubs:

```python
# src/models/<name>.py
from datetime import datetime, timezone
from bollhav.model import (
    Batch, Contract, Database, Materialization, Model, Source, SourceModel,
    Staging, State, Tags, Target, TimeChunking, Temporality, UpstreamContract,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType   # or bollhav.mssql

<name> = Model(
    target=Target(
        name="<name>", schema="<schema>", catalog="<catalog>",
        database=Database.POSTGRES,
        dsn_env_var="<DSN_ENV_VAR>",
        write_mode=WriteMode.<APPEND|UPSERT_NO_DELETE|RECREATE_PARTITION>,
        # staging=Staging(),                # if Q7 = yes
        columns=[
            PostgresColumn(name="...", data_type=PostgresType..., nullable=False),
        ],
    ),
    temporality=Temporality.<TEMPORAL|TIMELESS>,
    # materialization=Materialization.VIEW, query_builder="SELECT ...",  # if Q1 = view
    # batching=Batch(time=TimeChunking(chunk="@daily")),                 # if temporal
    # contract=Contract(begin=datetime(...), end=datetime(...)),         # if temporal
    # upstream=[Source("schema.table", type=SourceModel(),
    #                  contract=UpstreamContract.ENCAPSULATE)],          # if gated
    # state=State(),                        # if staging or gated upstream
    tagging=Tags(tags={"<tag>"}),
)
```

Then tell them:
- Which stubs to implement in `read.py` / `transform.py` / `write.py` (a view
  needs none — it writes nothing).
- To place the file in `src/models/` and wire nothing else (the three
  decorators in `src/main.py` pick it up) — see the `pipeline-pattern` skill.
- How to run it locally — hand off to the `env-vars` skill with a `TAGS=`
  built from the tag they chose.

Ground every suggestion in the real keyword — never invent a field. When
unsure of an enum value, check `bollhav.model` exports or the `overview` skill.
