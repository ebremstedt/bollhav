[Home](index.md) › [Model](MODEL.md) › **Upstream**

# Upstream

How one model waits for another. A model's `upstream` is a list of **contracts** — each declares a dependency on another model and how that dependency is checked before a unit of work runs.

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

## See also

- [State](STATE.md) — status values, rerun behaviour, errors, locks.
- [Staging](STAGING.md) — the chunked atomic write path.
- [examples/staging_state_contracts/](https://github.com/ebremstedt/bollhav/tree/main/examples/staging_state_contracts) — a view, a monolith, an interval, and a fourth model with a contract on all three.
