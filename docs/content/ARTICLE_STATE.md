[Home](index.md) › [Articles](ARTICLES.md) › **State**

# State

!!! note "TODO — to be written"
    Placeholder — the conceptual companion to the [State reference](STATE.md). Ideas to cover:

    - What state is and what it buys you: resumability, skip-`applied`, in-band ordering via [contracts](UPSTREAM.md), horizontal concurrency (per-interval advisory locks).
    - When to turn it on vs. leave it off (simple one-shot loads, MSSQL with no state support, or you already run an external orchestrator).
    - The cost: a Postgres state DB; Postgres-only for now.
    - How it changes your run model — see [Orchestration](ORCHESTRATION.md).

## See also

- [State (reference)](STATE.md) · [Upstream](UPSTREAM.md) · [Orchestration](ORCHESTRATION.md)
