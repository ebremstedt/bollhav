[← Target](TARGET.md)

# Actions

## What bollhav does for you out of the box

Every `Target` ships with a list of framework-supplied **default actions** — that's how bollhav creates your schema and table without you having to call DDL yourself. When you write `Model(target=Target(name="orders", ...))`, the framework's default action list runs on the first interval of every pipeline invocation:

```
CREATE SCHEMA IF NOT EXISTS <target_schema>
CREATE TABLE IF NOT EXISTS <target_schema>.<target_name> (...)
CREATE INDEX IF NOT EXISTS …           (when a column has partition_on=True)
ALTER TABLE … ADD CONSTRAINT … UNIQUE  (when columns have unique=True)
DROP TABLE IF EXISTS …                 (when target.recreate_table=True)
TRUNCATE TABLE …                       (when target.truncate_table=True)
CREATE SCHEMA z_<target_schema>        (when staging is set)
```

(The staging *table* itself isn't a model-level action — it's created fresh per interval inside `stage()` and dropped in that interval's apply transaction.)

Plus two **interval-level** actions that only fire when `state=State(...)` is set on the model:

```
PRE_INTERVAL  mark_running    state row pending → running       (when model.state is set)
POST_INTERVAL mark_applied    state row → applied               (when state set and not already
                                                                 flipped by the staging flush)
```

These operations live in `bollhav.postgres.actions.default_actions()` — a plain list of `Action` objects. Each one is gated by a `should_run` predicate so it only fires when applicable to your model. Nothing magical: the list is open for inspection, extension, or replacement.

## Why "actions" instead of hardcoded DDL

Before this layer existed, bollhav's setup code was hardcoded inside `ensure_schema_and_table`: you got CREATE TABLE and CREATE INDEX, period. If you wanted a `GRANT` or `ANALYZE` or `COMMENT ON TABLE`, you had to monkey-patch the framework or write your DDL outside it.

With actions, every framework setup step is one entry in `target.default_actions`, and your additions are a separate `target.actions` list that runs after them. Same machinery — the runner walks both lists, calls `should_run`, calls `run`, records which fired in `target._applied_model_actions` so subsequent intervals short-circuit. You add a `GRANT` action exactly the way the framework adds `CREATE TABLE`.

## The shape of an action

```python
from bollhav.model.actions import Action, Phase

@dataclass
class Action:
    name: str                                                # log key + _applied_model_actions key
    phase: Phase                                             # PRE_MODEL / POST_MODEL / PRE_INTERVAL / POST_INTERVAL
    run: Callable[[psycopg.Connection, Model], None]         # the actual work
    should_run: Callable[[Target], bool] = lambda t: True    # gate
```

The runner calls `should_run(target)` to gate, calls `run(conn, model)` to do the work, then records `target._applied_model_actions[name] = True`. Same dict gates re-runs of the same action within the pipeline.

## Four phases — a 2×2 grid

| Phase | Cardinality | Typical use |
|---|---|---|
| `PRE_MODEL` | Once per pipeline run, before user's loop | `CREATE TABLE`, `CREATE INDEX`, staging schema setup |
| `POST_MODEL` | Once per pipeline run, after user's loop returns cleanly | `ANALYZE the_whole_table`, `GRANT`, drop staging |
| `PRE_INTERVAL` | Per interval, before each `execute()` | `mark_running` (state row pending → running), per-interval metrics, lock acquire, user logging |
| `POST_INTERVAL` | Per interval, after each `execute()` returns | `mark_applied` (state row → applied), per-interval cleanup, Prometheus increment, downstream notify |

Model-level phases are recorded in `target._applied_model_actions` and short-circuit on intervals 2..N via `target.setup_complete`. Interval phases fire every interval — they're **not** recorded in the model-level dict because they're meant to repeat. Their runners (`run_pre_interval_actions(conn, model, since, until)` and `run_post_interval_actions(conn, model, since, until)`) stash the interval window on the model as `_interval_since` / `_interval_until` so action callables can read it without it being threaded through the `Action.run(conn, model)` signature.

The `@state` decorator on `execute()` is what wraps the interval runners around the user's function today. State-enabled models get the `mark_running` and `mark_applied` actions automatically — they're in `default_actions()` with `should_run` gated on `model.state is not None`, so they only fire when state is opted in.

```python
# Reading the current interval inside a custom interval action:
def emit_metric(conn, model):
    duration = (model._interval_until - model._interval_since).total_seconds()
    metrics.histogram("interval.duration", duration, tag=model.target.full_name)

orders = Model(
    target=Target(
        name="orders",
        ...,
        actions=[
            Action("emit_metric", Phase.POST_INTERVAL, emit_metric),
        ],
    ),
    state=State(),
)
```

## Two lists on Target

The framework's lifecycle and your additions are kept in separate fields so they compose cleanly:

```python
@dataclass
class Target:
    ...
    default_actions: list[Action] | None = None    # framework's; None = resolve lazily
    actions: list[Action] = field(default_factory=list)   # user-added
```

`effective_actions` returns `default_actions ++ actions` — the runner walks this concatenated list in order. Framework defaults run first, then your actions.

Four behaviours fall out:

| `default_actions` | `actions` | What runs |
|---|---|---|
| `None` (default) | `[]` (default) | The framework's setup actions only — CREATE SCHEMA, CREATE TABLE, INDEX, UNIQUE, staging schema. Every model that doesn't customise lands here. |
| `None` | `[my_action]` | The framework's defaults, then your action. Most common extension pattern — add a GRANT or ANALYZE without losing schema/table setup. |
| `[]` | `[my_action]` | **Only** your action. CREATE TABLE does not run. The model will fail unless your action handles it. Use only when you want to take full control of setup. |
| `[a for a in default_actions() if a.name != "indexes_created"]` | `[smart_indexes]` | The framework's 9 setup actions plus your smarter index action, replacing the default `indexes_created`. The most common selective-override pattern. |

Setting `actions=[]` (the field's default) is always safe — the framework defaults still cover schema and table creation. Setting `default_actions=[]` is the explicit opt-out and breaks the model unless you supply replacements for at least `schema_created` and `table_created`.

## The framework default actions — in detail

```python
from bollhav.postgres.actions import default_actions

default_actions()  # returns these, in this order:
# ── PRE_MODEL ──
#   schema_created          CREATE SCHEMA IF NOT EXISTS <target_schema>
#   recreated               DROP TABLE       (when target.recreate_table)
#   table_created           CREATE TABLE IF NOT EXISTS <target>
#   truncated               TRUNCATE         (when target.truncate_table)
#   indexes_created         CREATE INDEX     (when target has a partitioned-by col)
#   uniques_added           ADD CONSTRAINT   (when target has unique columns)
#   staging_schema_created  CREATE SCHEMA z_<target_schema> (when staging is set)
# ── POST_MODEL ──
#   (none by default — the staging table is created/dropped per interval
#    inside stage(), not via a model-level action)
# ── PRE_INTERVAL ──
#   mark_running            state row pending → running     (when model.state is set)
# ── POST_INTERVAL ──
#   mark_applied            state row → applied             (when state set + not flushed)
```

Each one's `should_run` reads the relevant attribute from Model or Target so the action skips itself when not applicable. List position is execution order. The state actions only fire when `model.state is not None` — state-less models pay zero overhead for them.

## Adding your own actions

Append to `actions`:

```python
from bollhav.model.actions import Action, Phase

def _grant_analytics(conn, model):
    conn.execute(
        f"GRANT SELECT ON {model.target.full_name} TO analytics_role"
    )

orders = Model(
    target=Target(
        name="orders",
        ...,
        actions=[
            Action("grant_analytics", Phase.POST_MODEL, _grant_analytics),
        ],
    ),
)
```

Project-wide bundles work the same way — define a list once and pass it to every Target:

```python
# my_project/actions.py
from bollhav.model.actions import Action, Phase

PROJECT_POST = [
    Action("analyzed", Phase.POST_MODEL,
           lambda c, m: c.execute(f"ANALYZE {m.target.full_name}")),
    Action("granted", Phase.POST_MODEL,
           lambda c, m: c.execute(f"GRANT SELECT ON {m.target.full_name} TO analytics")),
]
```

```python
# my_project/models/orders.py
from my_project.actions import PROJECT_POST

orders = Model(target=Target(name="orders", actions=PROJECT_POST, ...))
```

## Replacing a framework default

Filter the default list, then append your replacement:

```python
from bollhav.postgres.actions import default_actions

smart_setup = [
    a for a in default_actions() if a.name != "indexes_created"
] + [
    Action("indexes_created", Phase.PRE_MODEL, _my_smarter_indexes,
           should_run=lambda t: needs_smart_indexes(t)),
]

orders = Model(
    target=Target(name="orders", default_actions=smart_setup, ...),
)
```

## POST_MODEL failure policy

`Target.on_failure` controls what happens when a POST_MODEL action raises:

| Setting | Behavior |
|---|---|
| `OnFailure.FAIL_FAST` (default) | Re-raise. Halts the rest of this target's POST_MODEL AND the cross-model POST sweep. Operator reruns. |
| `OnFailure.SKIP` | Log warning, continue to the next action. Lets a flaky action (e.g. Slack notify) not block the pipeline. |

For per-action nuance (retry, fallback, swallow specific exceptions), wrap the failable code in the action's own `run` callable — that's clearer than a richer enum and composes with any Python error handling.

PRE_MODEL actions are always fail-fast — there's no opt-out, because continuing a write whose setup half-failed isn't recoverable.

## Runtime state — `_applied_model_actions`

The dict on `Target` that records which **model-level** actions have fired this pipeline run:

```python
target._applied_model_actions.get("table_created")    # True after the first interval
target._applied_model_actions.get("recreated")        # False if recreate_table=False (never fires)
```

Fresh dict per `apply_runtime_overrides()` invocation, so flags reset naturally between pipeline runs.

The naming is deliberate: it tracks **model-level** actions (PRE_MODEL + POST_MODEL). Future interval-level state, when those phases ship, will live separately — interval-level actions fire every interval and a per-pipeline dict can't represent that.

`target.setup_complete` reconciles `_applied_model_actions` against `effective_actions` filtered to `Phase.PRE_MODEL`. Returns True when every applicable PRE_MODEL action is in the dict, so `run_pre_model_actions` short-circuits the empty `BEGIN`/`COMMIT` on intervals after the first.

## Using `_applied_model_actions` in your own actions

The lookup pattern is `target._applied_model_actions.get(action_name)`. Three layers use it:

```python
# Layer 1 — the runner gates each action:
for action in target.effective_actions:
    if action.phase is not Phase.PRE_MODEL: continue
    if target._applied_model_actions.get(action.name): continue   # ← skip if already done
    if not action.should_run(target): continue
    action.run(conn, model)
    target._applied_model_actions[action.name] = True              # ← record

# Layer 2 — setup_complete short-circuits the whole pre-phase function
# (same logic, applied across all applicable PRE_MODEL actions)

# Layer 3 — your custom action conditions on what already ran:
def comment_on_table(conn, model):
    if not model.target._applied_model_actions.get("table_created"):
        return  # table existed already — don't re-set the comment
    conn.execute(
        f"COMMENT ON TABLE {model.target.full_name} IS 'managed by bollhav'"
    )
```

## What is NOT in the action system (yet)

- **Per-interval state lifecycle** — `mark_running` and `mark_applied` ARE actions now (PRE_INTERVAL / POST_INTERVAL). The `@state` decorator still owns the control flow around them (is-applied gate, lock acquire/release, upstream-satisfied check, exception path with `record_failure`), but the lifecycle calls themselves go through the runners.
- **The atomic flush in staged writes** — `INSERT INTO target SELECT * FROM staging` happens per-interval inside `flush_to_target`, not as an action. Per-interval data movement doesn't fit the action pattern.
- **Connection management** — runners accept an open connection; opening / closing / pooling is the caller's job.

## Limitations

- **Single-worker only** for destructive actions. Two pipelines on the same model with `recreate_table=True` each see their own `_applied_model_actions` dict and would both DROP. Use `model_lock` for cross-process safety.
- **No ordering helpers in the public API yet.** If you need to splice between two framework defaults, do it with a list comprehension. Helpers will be added when there's repeated need.
- **POST_MODEL actions don't fire if `main()` raises** — by design, so a crashed pipeline doesn't run cleanup that assumes the loop completed. Operator's recourse is to rerun.
