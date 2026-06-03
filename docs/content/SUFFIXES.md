[Home](index.md) › **Schema vs table suffix**

# Schema suffix vs table suffix

bollhav lets you rewrite where your models land at run time without editing the model files. There are two knobs:

- **`SCHEMA_SUFFIX`** — pushes every matched model into a different schema (`warehouse` → `warehouse_pr123_2614_`).
- **`TABLE_SUFFIX`** — renames every matched model's table within its schema (`customers` → `customers_v2`).

They compose: you can use both at once.

| Knob | Default | What it changes | Typical use |
|---|---|---|---|
| **`SCHEMA_SUFFIX`** | required (set at runtime) | `target.schema.resolved` | Separating dev / PR / CI runs from prod |
| **`TABLE_SUFFIX`** | empty (off) | `target.name_resolved` | Blue/green hotswap inside a single schema |

## How it works

Both suffixes flow through `apply_runtime_overrides`, which **copies** each matched model and bakes the resolved identifier onto the copy. The source model files are never mutated.

```
TARGETSCHEMA(name=warehouse, suffix=pr123, suffix_appendix=%y%V)
                                        └─ resolved → "warehouse_pr123_2614_"

TARGET(name=customers,        suffix=v2,   suffix_appendix=None)
                                        └─ name_resolved → "customers_v2"

TARGET.full_name → "warehouse_pr123_2614_.customers_v2"
```

The schema-suffix appendix defaults to `%y%V` (ISO year+week) — so every PR run lands in a week-stamped namespace, easy to garbage-collect later. The table-suffix appendix defaults to **`None`** — hotswap typically wants a stable, predictable name (`customers_v2`), not a time-varying one. You can flip that on if you want.

All downstream identifiers follow:

- Postgres index name (`<table>_<col>_idx`) uses `name_resolved` — never collides between `customers_v2_payload_idx` and `customers_payload_idx`.
- Postgres unique-constraint name (`<table>_uq`) uses `name_resolved`.
- MSSQL `<table>_pk` and `<table>_uq` constraint names use `name_resolved`.
- DDL targets (`CREATE TABLE`, `TRUNCATE`, `DROP`) use `name_resolved`.

Tag matching does **not** see the suffix — it stays bound to the base `name` so the same `TAGS=[customers]` expression works regardless of whether a suffix is set.

## When to use which

### `SCHEMA_SUFFIX` — isolating an entire pipeline run

You want a whole set of models pointed at a parallel namespace. Examples:

- **Per-PR / per-branch runs.** `SCHEMA_SUFFIX=pr123` puts every table from this branch in `warehouse_pr123_2614_`, leaving prod tables untouched. Drop the schema at PR close.
- **CI / nightly canary.** Run the full pipeline against a `nightly_canary` schema; if it succeeds, swap clients over.
- **Cross-environment sandboxing.** One database, many isolated environments (`dev`, `staging`, `pr-123`) just by varying the suffix.

The mental model is **"give every model a private playground."** All cross-table references inside the pipeline (joins, FKs declared in user code) still work because they all live in the same schema — they just live in the suffixed schema instead.

### `TABLE_SUFFIX` — hotswapping one shape inside a schema

You want to keep the schema stable but build a new version of (some of) the tables alongside the live ones. Examples:

- **Blue/green table rebuild.** Build `customers_v2` from scratch, validate row counts, then in one transaction rename `customers → customers_old`, `customers_v2 → customers`. Drop `customers_old` after a grace period.
- **A/B compare against live.** Run both pipelines (`""` and `v2`) into the same schema simultaneously, point a few queries at each, compare.
- **Backfill into a side table.** Compute a heavy backfill into `events_backfill` without touching `events`, then merge or rename when done.

The mental model is **"build the new version next to the old one, swap at the end."**

## Limitations

Both suffixes share Postgres' 63-byte identifier limit and the rule that anything referencing the table by name elsewhere won't follow the rename. The two have different blast radii though.

### `SCHEMA_SUFFIX`

- **Cross-schema references break.** If a model in your pipeline reads from `other_schema.things`, the suffix doesn't rewrite that reference — your `WHERE`/`JOIN` SQL still points at the unsuffixed source. Workaround: read sources through bollhav's `SourceTable` (which can be resolved at runtime) instead of hard-coding schema names in your queries.
- **External consumers don't know about it.** Anything outside the bollhav run (dashboards, downstream ETL, ad-hoc SQL) won't find your suffixed tables. That's the *point* in dev, but be careful not to point production consumers at a suffixed schema.
- **Schema cleanup is on you.** bollhav creates schemas via `CREATE SCHEMA IF NOT EXISTS` but never drops them. Per-PR suffixes pile up.

### `TABLE_SUFFIX`

All of the schema-suffix caveats above, plus:

- **Identifier truncation is much more likely.** Index name format is `<resolved_table>_<col>_idx`. With `customer_loyalty_program_enrollments` (36) + `_v2` (3) + `_created_at_idx` (15) = 54 chars — fits. Add a date appendix (`_2614`) and you're at 59 — fits. But a long compound column or longer table name pushes past 63 and Postgres silently truncates, which can collide across two suffixed indexes that share a prefix.
- **Same-schema collisions look like real tables.** An orphaned `customers_v2` in the live schema is much harder to spot than an orphaned `warehouse_pr123_2614_` schema sitting at the top level.
- **Cross-table SQL in your `execute()` won't follow.** If your transform reads `FROM customers JOIN orders`, those names are literal — the suffix doesn't rewrite the SQL. Either keep `TABLE_SUFFIX` runs to leaf tables (no downstream joins), or use `model.target.full_name` to construct identifiers dynamically.
- **No automatic cleanup of the old version.** Renaming after blue/green is your responsibility — bollhav doesn't drop the previous shape for you.

## Combining them

The two compose cleanly. A common pattern:

```bash
# Build the v2 shape into your PR schema, side-by-side with the v1 shape
SCHEMA_SUFFIX=pr123 \
TABLE_SUFFIX=v2 \
python main.py
# → tables land in warehouse_pr123_2614_.customers_v2
```

Both suffixes are independent: the schema suffix governs *where*, the table suffix governs *what shape*. Mixing them is the standard recipe for "I want to test a rebuild in isolation, without touching production."

## See also

- [Runtime overrides](RUNTIME_OVERRIDES.md) — full env-var reference (both `SCHEMA_SUFFIX` and `TABLE_SUFFIX` listed there).
- [TargetSchema](TARGETSCHEMA.md) — the dataclass behind `SCHEMA_SUFFIX`.
- [Target](TARGET.md) — the dataclass behind `TABLE_SUFFIX`.
