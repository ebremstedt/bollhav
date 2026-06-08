[Home](index.md) › [Model](MODEL.md) › **Upstream**

# Upstream

A model's inputs — **all of them** — live in one list: `upstream: list[Source]`. A `Source` is one input, described by two independent dials:

- **`type`** — *what* it is, and its read config: a [`SourceModel`](SOURCETABLE.md) (relational — a managed model, an external table, or a view), a [`SourceFile`](SOURCEFILE.md), or a `SourceApi`.
- **`contract`** — *whether it gates*. A `Source` carrying a contract is a **managed upstream** the state machine waits for (it must be `applied` before this model runs). No contract ⇒ **ungated** — an external input assumed always present, never blocking.

That's the whole model: gated-vs-ungated is just "does this `Source` have a `contract`," not which list it's in.

```python
upstream=[
    # gated managed upstreams (need state — see below):
    Source("warehouse.orders",    type=SourceModel(), contract=IntervalContract()),
    Source("warehouse.customers", type=SourceModel(), contract=ViewContract()),
    Source("warehouse.app_config",type=SourceModel(), contract=MonolithicContract()),
    # ungated external sources:
    Source("raw.landing",  type=SourceModel(dsn_env_var="RAW_DSN")),
    Source("crm.contacts", type=SourceApi(base_url="https://crm/api")),
]
```

A **contract is only valid on a `SourceModel`** — files and APIs aren't state-tracked, so they can't gate. Declaring one on a `SourceFile`/`SourceApi` is a definition-time error.

## Contracts (gating)

A contract is pure gating *policy* — it carries no name (the `Source` owns the identity). Its kind must match the upstream's [kind](KINDS.md):

| Contract | Satisfied when |
|---|---|
| `IntervalContract()` | upstream has an `applied` window covering this unit's `(since, until)`. A daily upstream covers an hourly downstream. |
| `ViewContract()` | the view exists. Window-agnostic. |
| `MonolithicContract()` | the whole table is loaded. Window-agnostic. |

**Gating requires [state](STATE.md).** A contract is checked by the state machine (`@execute_lifecycle`), which only runs for state-tracked models. A gated upstream without `state=State(...)` is an error — the contract would otherwise never be enforced. Ungated sources need no state.

### What happens

Before each unit, every **gated** upstream is checked (no short-circuit):

- all satisfied → runs.
- any unsatisfied → unit is `blocked`; `blocked_reason` names each missing upstream.

On the next run, blocked units re-evaluate — they go `pending` once upstreams catch up. A gated upstream naming an unregistered model is a hard error, not a skip. Ungated sources are never checked.

## Referencing an input in SQL

`model.ref("name")` resolves a declared `SourceModel` input to a quoted identifier for a read query — **suffix-aware when it's gated**, literal when it isn't:

```python
# gated upstream — moves with the env's schema suffix (dev / prod / PR)
query = f"SELECT * FROM {model.ref('warehouse.orders')} WHERE day >= %(since)s"
# -> "warehouse"."orders"   (under SCHEMA_SUFFIX=pr123 -> "warehouse_pr123"."orders")

# ungated source — fixed external location, no suffix
f"... FROM {model.ref('raw.landing')}"   # -> "raw"."landing"
```

A gated source is a managed model, so its schema moves with the suffix just like your own target — the query stays portable. An ungated source is external, at a fixed location in every environment, so it resolves literally. Referencing an undeclared name raises; `ref()` on a `SourceFile`/`SourceApi` raises (there's no `FROM` for a file or an API — read those in your read function).

## Unknown provenance

A model that declares **nothing** (`upstream=[]`) gets a single auto-injected typeless `Source` — see [None](SOURCE_NONE.md). Its provenance is untracked; `model.inputs_known` is `False`.

## See also

- [SourceModel](SOURCETABLE.md) / [SourceFile](SOURCEFILE.md) — the relational and file input types.
- [Sources](SOURCES.md) — the ungated case in depth, and gated-vs-ungated.
- [State](STATE.md) — status values, rerun behaviour, errors, locks.
- [Lineage](LINEAGE.md) — the dependency graph these feed.
- [examples/staging_state_contracts/](https://github.com/ebremstedt/bollhav/tree/main/examples/staging_state_contracts) — a view, a monolith, an interval, and a fourth model gated on all three.
