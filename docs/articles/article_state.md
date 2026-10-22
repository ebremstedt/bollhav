# State

!!! note "TODO — to be written"
    Placeholder — the conceptual companion to the [State reference](../model/state.md). Ideas to cover:

    - What state is and what it buys you: resumability, skip-`applied`, in-band ordering via [contracts](../model/upstream.md), horizontal concurrency (per-interval advisory locks).
    - When to turn it on vs. leave it off (simple one-shot loads, MSSQL with no state support, or you already run an external orchestrator).
    - The cost: a Postgres state DB; Postgres-only for now.
    - How it changes your run model — see [Orchestration](orchestration.md).

## See also

- [State (reference)](../model/state.md) · [Upstream](../model/upstream.md) · [Orchestration](orchestration.md)
