[Home](index.md) › **Model**

# Model

A `Model` is a pure data object describing what data looks like and where it goes. It does not contain execution logic — that lives in a separate `execute` function that receives the model as a parameter.

The `Model` itself is the top-level container. Its sub-objects each have their own page:

- [Temporality](TEMPORALITY.md) — the model's time axis (`TEMPORAL` / `TIMELESS`), plus the `materialization` (table / view) choice
- [Target](TARGET.md) — where data lands (table, schema, columns, write mode)
- [Staging](TARGET.md#staging) — optional staging table on Target
- [Contract](CONTRACT.md) — historical envelope for backfill mode
- [Batch](BATCH.md) — chunk size, lookback, retries
- [Curfew](CURFEW.md) — wall-clock hours/days the model must not run
- [State](STATE.md) — per-model state tracking
- [Upstream](UPSTREAM.md) — the model's inputs (`list[Source]`): gated upstreams + ungated sources
- [Tagging](TAGGING.md) — controls which auto-derived tags get added to the model
