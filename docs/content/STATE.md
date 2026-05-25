[← home](index.md)

# State

Per-model interval state. Opt in by setting `state=State(...)` on a model; bollhav records every interval's lifecycle (`pending` → `applied`) in a per-model state table, and re-runs become resumable.

!!! note "Coming soon"
    The full state-tracking feature lands with the next release. Detailed docs (opt-in, env vars, tables, lifecycle, re-run semantics) will appear here once the feature is merged.
