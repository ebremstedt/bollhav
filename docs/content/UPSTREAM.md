[Home](index.md) › [Model](MODEL.md) › **Upstream**

# Upstream

How one model waits for another. A model's `upstream` is a list of **contracts** — each declares a dependency on **another bollhav-managed model** and how that dependency is checked before a unit of work runs.

Upstream is for **managed, state-tracked dependencies** — models bollhav builds and gates. For *external* tables you only read (a raw landing table, an API, a file), use [sources](SOURCES.md) instead: those are optional and lineage-only, never gated.

**Upstream requires [state](STATE.md).** A contract's satisfaction is checked by the state machine (`@execute_lifecycle`), which only runs for state-tracked models. Declaring `upstream` without `state=State(...)` is an error — the contract would otherwise never be enforced.

## Contract

The contract kind must match the upstream's [kind](KINDS.md):

```python
upstream=[
    IntervalContract("warehouse.orders"),        # an applied window covering mine
    ViewContract("warehouse.customers"),         # the view exists
    MonolithicContract("warehouse.app_config"),  # the table is loaded
]
```

| Contract | Satisfied when |
|---|---|
| `IntervalContract` | upstream has an `applied` window covering this unit's `(since, until)`. A daily upstream covers an hourly downstream. |
| `ViewContract` | the view exists. Window-agnostic. |
| `MonolithicContract` | the whole table is loaded. Window-agnostic. |

## What happens

Before each unit, every contract is checked (no short-circuit):

- all satisfied → runs.
- any unsatisfied → unit is `blocked`; `blocked_reason` names each missing upstream.

On the next run, blocked units re-evaluate — they go `pending` once upstreams catch up. A contract naming an unregistered model is a hard error, not a skip.

## Referencing an upstream in SQL

`model.ref("name")` resolves a declared upstream to a quoted, **schema-suffix-aware** identifier for embedding in a read query — so the SQL and the dependency graph stay in sync, and the query is portable across dev / prod / PR (the upstream moves with the suffix, just like your own target):

```python
upstream=[IntervalContract("warehouse.orders")]
...
query = f"SELECT * FROM {model.ref('warehouse.orders')} WHERE day >= %(since)s"
# -> SELECT * FROM "warehouse"."orders" WHERE ...
#    (under SCHEMA_SUFFIX=pr123 -> "warehouse_pr123"."orders")
```

Referencing a name that isn't a declared upstream raises. Use `ref()` only for managed upstreams; use [`source_ref()`](SOURCES.md) for external sources (resolved literally, no suffix).

## Upstream vs sources

| | [Upstream](UPSTREAM.md) | [Sources](SOURCES.md) |
|---|---|---|
| Refers to | a bollhav-managed model | an external, unmanaged table/input |
| Requires state | yes — gated by the state machine | no — never gated |
| Optional | enforced when declared | always optional (lineage only) |
| Schema suffix | applied (`ref()`) | not applied (`source_ref()`, literal) |
| Purpose | wait-for + lineage | lineage / boundary marker |

## See also

- [State](STATE.md) — status values, rerun behaviour, errors, locks.
- [Staging](STAGING.md) — the chunked atomic write path.
- [examples/staging_state_contracts/](https://github.com/ebremstedt/bollhav/tree/main/examples/staging_state_contracts) — a view, a monolith, an interval, and a fourth model with a contract on all three.
