[← Target](TARGET.md)

# Mutating targets

`Target.mutations` is a per-pipeline-run tracker that records which one-shot setup operations have already fired against the destination table during the current run.

## Why this exists

A pipeline run processes many intervals. Without tracking, the table-setup code would re-evaluate the same DDL on every interval — and worse, every destructive op (`DROP TABLE`, `TRUNCATE`) would fire over and over, wiping data partway through the loop.

`mutations` solves that. Once a setup step has run during a pipeline invocation, its flag flips to `True` and every subsequent interval in the same loop skips the work. **Runs that follow the first interval do not mutate the target table again in any way** — no DROP, no TRUNCATE, no CREATE, no ALTER, no index DDL. Pure Python attribute reads.

## The fields

| Field | DDL it records | Fires when |
|---|---|---|
| `mutations.schema_created` | `CREATE SCHEMA IF NOT EXISTS` | always (every table has a schema) |
| `mutations.table_created` | `CREATE TABLE IF NOT EXISTS` | always (every table needs to exist) |
| `mutations.recreated` | `DROP TABLE IF EXISTS` | only when `target.recreate_table=True` |
| `mutations.truncated` | `TRUNCATE TABLE` | only when `target.truncate_table=True` |
| `mutations.indexes_created` | `CREATE INDEX IF NOT EXISTS` | only when a column has `partition_on=True` |
| `mutations.uniques_added` | `ALTER TABLE ... ADD CONSTRAINT ... UNIQUE` | only when columns have `unique=True` |

Each flag means exactly one thing: **"did that DDL statement actually fire on this target during this pipeline run?"** If `mutations.recreated` is `False`, it really means the DROP did not run — either because the target hasn't been processed yet, or because `recreate_table=False`.

## The two gates per DDL

Every setup operation is guarded by two independent conditions inside `ensure_table`:

```python
if target.recreate_table and not target.mutations.recreated:
    DROP TABLE...
    target.mutations.recreated = True
```

1. **Directive gate** — *should* this happen at all? (`target.recreate_table`, `target.truncate_table`, `target.partitioned_by`, `target.unique_columns`)
2. **Mutations gate** — has it *already* happened in this run? (`mutations.<flag>`)

Both must say yes before the DDL fires. The flag flips inside the same block so the gate closes for every interval that follows.

## The whole-function early-out: `setup_complete`

After interval 1, every applicable flag is `True`. To avoid even opening an empty `BEGIN`/`COMMIT` transaction for `ensure_schema_and_table`, the `setup_complete` property on Target reconciles the flags against the directives:

```python
@property
def setup_complete(self) -> bool:
    m = self.mutations
    return (
        m.schema_created
        and m.table_created
        and (m.recreated or not self.recreate_table)
        and (m.truncated or not self.truncate_table)
        and (m.indexes_created or self.partitioned_by is None)
        and (m.uniques_added or not self.unique_columns)
    )
```

Reads as: *"every required op done, and every conditional op either done or not applicable."*

`ensure_schema_and_table` returns immediately when `setup_complete` is `True`:

```python
def ensure_schema_and_table(conn, model):
    target = model.target
    if target.setup_complete:
        return                       # ← interval 2+ exits here. No BEGIN.
    with conn.transaction():
        ...
```

On a 365-interval daily backfill: **one** setup transaction instead of 365.

## Lifetime: per pipeline run

`mutations` is **not** persisted. It lives on the in-memory `Target` instance that `apply_runtime_overrides` builds at the start of every `@load_models` invocation. Run #2 starts fresh — all six flags `False` again — so the table gets created (or recreated, or truncated) once more if those directives are still set.

That is intentional. The flag is a *within-run* memo, not a *cross-run* record. If you want "this DDL has happened *ever*", look at the database itself (the schema exists, the table exists, the index exists).

## What this is **not**

A cross-process lock. Two pipelines running on the same model with `recreate_table=True` each see their own fresh `Mutations` and would both `DROP`, potentially clobbering each other. The flag isn't safety — it's an idempotency optimization within a single process. For cross-process safety, use `model_lock`. See [State](STATE.md).

## Don't pre-set these

`mutations` is `field(init=False)` — you cannot pass it to the `Target(...)` constructor. The access path `target.mutations.recreated` is deliberately verbose so every read- and write-site in the codebase signals that this is *runtime state being mutated*, not configuration.

If you find yourself reading from `target.mutations.*` in user code, that's a smell. Read the directive instead (`target.recreate_table`).
