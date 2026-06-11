[Home](index.md) › **Library**

# Model library

Cross-pipeline registry of every bollhav model the state DB has ever seen. Lives in `z_bollhav.library`. Multiple pipelines run from possibly-different bollhav images can all write to it concurrently; downstream models in pipeline A can claim upstreams that ship in pipeline B.

## What's in a row

| Column | Type | Meaning |
|---|---|---|
| `full_name` | TEXT (PK) | `catalog.schema.name` (or `schema.name`) — the model's identity. |
| `upstream` | TEXT[] | The model's declared upstream `full_name`s. Updated on every `register` so renames propagate. |
| `model_type` | TEXT | `TABLE` or `VIEW`. Default `'TABLE'` so older bollhav images that don't write this column can still insert. |
| `state_schema` | TEXT (nullable) | Where the state table lives. `NULL` for views and library-only tables. |
| `state_table` | TEXT (nullable) | Name of the state table. `NULL` for views and library-only tables. |
| `last_seen` | TIMESTAMPTZ | `now()` on every upsert — useful for spotting models that haven't run in a while. |

## Who registers, and when

The bootstrap (run inside `@load_models` before the user's loop starts) registers each matched model in one of three paths:

| Model shape | When it registers | What it writes |
|---|---|---|
| `state=State(...)` (with or without staging) | Every bootstrap | `model_type='TABLE'`, real `state_schema` / `state_table` |
| `library=True`, no state, no staging — TABLE | Every bootstrap (opt-in via `Model(library=True)`) | `model_type='TABLE'`, both state pointers `NULL` |
| `library=True`, no state, no staging — VIEW | Every bootstrap (opt-in via `Model(library=True)`) | `model_type='VIEW'`, both state pointers `NULL` |
| Everything else (a view *without* `library=True`, a staging-only table, etc.) | Doesn't register | — |

VIEW models use the same explicit `library=True` opt-in as state-less TABLE models — there's no auto-registration for views. A view declared without `library=True` is still a perfectly valid bollhav model (bollhav will `CREATE OR REPLACE VIEW` it on every run), it just won't appear in the library and therefore can't be claimed as an upstream.

Once a model is in the library, downstreams in any pipeline can reference it by `full_name`.

## Upstream satisfaction — `is_satisfied(entry=...)`

When a downstream model declares an upstream that isn't in its own pipeline's matched set, the bootstrap (and again at runtime via `@state`'s live check) consults the library. Dispatch is by the entry's state-pointer shape:

| Entry shape | Satisfaction means |
|---|---|
| `state_schema` / `state_table` both `NULL` (VIEW or library-only TABLE) | **The row exists.** Presence in the library is satisfaction — no SQL against the upstream's state table. Views are time-agnostic and library-only tables have no temporal claim; their existence is the claim. |
| `state_schema` / `state_table` set | An `applied` row in the upstream's state table covers (matches or fully encapsulates) the downstream window. A daily-cadence upstream therefore covers an hourly-cadence downstream's intervals without coordination. |

If the upstream isn't in the library at all (i.e. lookup returns `None`), the downstream interval is blocked with `STATE_001: upstream … not registered`. If lookup returns an entry but the applied-row check fails, the block reason is `STATE_002` and names the upstream's `model_type` so an operator can tell at a glance which kind of upstream isn't ready.

See [Block codes](STATE.md#block-codes).

## Multi-image safety — only additive migrations

The library is the most-shared bollhav-owned table: multiple pipelines, multiple deployments, possibly multiple bollhav versions all writing to the same row schema concurrently. So **schema changes have to be additive**:

- New columns must have a safe default (e.g. `ADD COLUMN model_type TEXT NOT NULL DEFAULT 'TABLE'`) so older images can keep inserting without filling them in.
- `NOT NULL` may only be **relaxed** (`DROP NOT NULL`), never tightened. Tightening would block any future insert that doesn't satisfy the new constraint.
- `DROP COLUMN` / `DROP TABLE` are off-limits — they would brick concurrent old-image writers.

`ensure_library` enforces this on every bootstrap: it checks `information_schema` for the columns the current code knows about and applies any missing ALTERs in-place. Each step is sentinel-gated so the migration runs at most once and is safe to retry.

The same rule applies going forward to every bollhav-owned table that more than one pipeline touches — all of which live in the one central `z_bollhav` schema (`z_bollhav_<suffix>` for a suffixed run): the per-model digest-named state tables, the shared `errors` table, and the staging tables. If you need a destructive change, deprecate the old column over a release and remove it only after every deployed image has the new code.

## Opt-in summary

```python
# State-tracked table — auto-registers
Model(target=Target(...), state=State(), batching=Batch(...))

# View intended as upstream — opt in via library=True
Model(
    target=Target(name="v_x", ...),
    kind=Kind.VIEW,
    upstream=[Source("v_x", type=SourceModel(query="SELECT ..."))],
    library=True,
)

# Static / no-state table intended as upstream — opt in via library=True
Model(target=Target(name="countries", ...), library=True)
```

## Co-location with state

The library lives in `z_bollhav.library` **in the state DB**. For library-only models (no `state=State(...)`), `_connect(model)` falls back to `target.dsn_env_var` — fine for single-instance setups where state and target share one Postgres database. For split state/target setups, the model's target DSN needs to point at the state instance, or the library row can't be written from where the bootstrap connects.

## Related

- [State](STATE.md) — how state machinery uses the library on every interval
- [Staging](TARGET.md#staging) — how staging models interact with the library (auto-register when state is set; opt-in via `library=True` when state-less)
- [Block codes](STATE.md#block-codes) — `STATE_001` / `STATE_002` block reasons that name the library entries
