# Model

A `Model` is a pure data object describing what data looks like and where it goes. It does not contain execution logic — that lives in a separate `execute` function that receives the model as a parameter.

The `Model` itself is the top-level container. Its sub-objects each have their own page:

- [Temporality](temporality.md) — the model's time axis (`TEMPORAL` / `TIMELESS`), plus the `materialization` (table / view) choice
- [Target](target.md) — where data lands (table, schema, columns, write mode)
- [Staging](target.md#staging) — optional staging table on Target
- [Contract](contract.md) — historical envelope for backfill mode
- [Batch](batch.md) — chunk size, lookback, retries
- [Curfew](curfew.md) — wall-clock hours/days the model must not run
- [Notifications](notifications.md) — where a run's outcome is reported (Teams), how much it says, and whom it mentions
- [State](state.md) — per-model state tracking
- [Upstream](upstream.md) — the model's inputs (`list[Source]`): gated upstreams + ungated sources
- [Tagging](tagging.md) — controls which auto-derived tags get added to the model
