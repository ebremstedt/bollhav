[Home](index.md) › [State](STATE.md) › **Block codes**

# Block codes

When bollhav inserts a state row as `blocked` instead of `pending`, the
`blocked_reason` column always starts with a stable code like
`STATE_001:`. Codes are namespaced by domain (`STATE`, future: `WRITE`,
`LIB`, etc.), permanent once assigned, and never renumbered — so you
can grep logs, build runbooks, and key alerts off them.

This page explains each code: what triggers it, what to do about it.

## STATE_001 — upstream not registered

**Trigger.** A staged, state-enabled model's `upstream` declares a model
that has never been seen by `@load_models`. The library
(`z_bollhav.library`) has no row for it, so bollhav can't reason
about whether the upstream has produced data for the requested
`(since, until)` window.

**Example reason text.**

```
STATE_001: upstream 'warehouse.orders' not registered
```

**Why this is conservative.** "Not registered" means *nobody has ever
run this model with `@load_models` against this state DB.* It's
distinct from "registered but not yet applied" (that's `STATE_002`) —
a missing registration is a louder signal.

**Remediation.**

- Run the upstream model in any pipeline that matches it. Once
  `@load_models` bootstraps it, the library row exists; the
  downstream's next bootstrap re-evaluates and flips the row from
  `blocked` to `pending` (under `STATE_MODE=discover`).
- Double-check the spelling of `upstream` in the downstream's
  `Model(...)` definition — the string must match the upstream's
  `Target.full_name` exactly (`<schema>.<name>`).
- If the upstream lives in a different state DB, see the note on
  cross-DB state in [STATE.md](STATE.md). Today the library only
  resolves models in the same state DB.

## STATE_002 — upstream has no applied row covering this interval

**Trigger.** The upstream IS in the library, but its state table has
no row with `status='applied'` whose `(since, until)` either exactly
matches or fully encapsulates the downstream's interval.

**Example reason text.**

```
STATE_002: upstream 'warehouse.orders' has no applied row covering 2024-01-01T00:00:00+00:00 → 2024-01-02T00:00:00+00:00
```

**Encapsulation, not exact match.** A daily upstream applied row
(`2024-01-01 → 2024-01-02`) satisfies an hourly downstream's interval
(`2024-01-01T03:00 → 2024-01-01T04:00`) because the upstream window
fully covers the downstream window. This lets upstream and downstream
run at different cadences without coordination.

**Remediation.**

- Run the upstream over the missing window. Then re-run the
  downstream pipeline; blocked rows are re-evaluated under
  `STATE_MODE=discover` and flip to pending once their upstream
  satisfies.
- Check the upstream's state — it may have `pending` or `blocked`
  rows of its own for that window. The block propagates: a downstream
  can't unblock until the upstream is `applied`.
- If you intentionally want to run without the upstream, set
  `STATE_MODE=bulldozer` to reset the blocked rows back to pending
  on the downstream's next run. This bypasses the safety check; use
  with eyes open.

## Adding new codes

When a new block condition lands:

1. Add a value to `BlockCode` in [bollhav/model/state.py](../../bollhav/model/state.py).
2. Pick the next free number in the appropriate domain (`STATE_NNN`,
   or a new domain like `WRITE_NNN`). Never reuse a number.
3. Document the code on this page with the same shape: trigger,
   example, remediation.
4. Add a test pinning the `BlockCode.<NAME>.value` to its string so a
   refactor can't silently move it.
