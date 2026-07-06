---
name: env-vars
description: How to assemble the command that runs bollhav models locally — the run-config environment variables (TAGS, BACKFILL/LATEST modes, RUN_SINCE/RUN_UNTIL window, TIMEZONE_OVERRIDE, USE_SCHEMA_SUFFIX, STATE_MODE, DEBUG). Use when a developer wants to run specific models, backfill a window, re-run an applied window, or asks which env vars control a bollhav run. Set the vars INLINE on a single `python src/main.py` invocation (not exported), so no run config leaks between runs.
---

# bollhav — run env vars

Assemble the command to run a bollhav image/pipeline locally. **Set the
run-config vars inline on the `python src/main.py` invocation — do not
`export` them.** Inline assignment scopes each var to that one process, so the
shell keeps no leftover run config between runs. That matters: a stale
`RUN_UNTIL`, `LATEST_ENABLED`, or `STATE_MODE` left exported from a previous
run silently changing the next is exactly the trouble this avoids. Output the
command — nothing else unless asked.

## Formatting rules

- Set each var inline, `VAR=value`, one per line with a trailing `\`, ending
  in `python src/main.py`. **No `export`.** Quote tag expressions:
  `TAGS="[fact]"`.
- **Connection DSNs** (`TARGET_DSN`, `STATE_DSN`, `CHA`, `LAKEHOUSE`, …) **and
  `REQUESTS_CA_BUNDLE`** are the exception: those are stable session config, so
  assume they're already exported in the shell — never emit them.
- Only emit the run-config vars below + the `python src/main.py` line.
- Don't pre-verify tags/models by reading the source unless asked — just set
  what's requested.

## Run config vars

| Var | Meaning |
|---|---|
| `TAGS` | **Required.** Tag expression selecting which models run (see the `tags` skill). |
| `BACKFILL_ENABLED=True` | Bounded backfill mode (default when `LATEST_ENABLED` unset; set explicitly to avoid the "no run mode" warning). |
| `RUN_SINCE` / `RUN_UNTIL` | ISO-8601 window bounds, half-open `[since, until)`. Bounds must end up timezone-aware — prefer plain dates + `TIMEZONE_OVERRIDE` over typing an offset. (Old names `BACKFILL_SINCE`/`BACKFILL_UNTIL` still work but warn as deprecated — always emit `RUN_SINCE`/`RUN_UNTIL`.) |
| `TIMEZONE_OVERRIDE` | IANA tz name (e.g. `UTC`, `Europe/Stockholm`) that localizes naive `SINCE`/`UNTIL`. Set this so you can write plain dates; without it naive bounds raise `NaiveIntervalBoundsError`. |
| `LATEST_ENABLED=True` | Latest-tick mode instead of backfill (mutually exclusive with backfill). |
| `USE_SCHEMA_SUFFIX=False` | Write to the real schema (no per-run suffix). Commonly `False` for a real run; a suffix isolates a virtual environment. |
| `DEBUG=True` | Debug logging on. |
| `STATE_MODE=torch` / `STATE_DISABLED=True` | Use when re-running an already-applied window so it actually re-executes. |

## "Run 1 day of X" → bounded backfill

Temporal models are chunked (e.g. `@hourly` / `@daily`); a one-calendar-day
`[since, until)` window is that day's chunks.

```bash
TAGS="[FactCase]" \
BACKFILL_ENABLED=True \
RUN_SINCE=2024-11-07 \
RUN_UNTIL=2024-11-08 \
TIMEZONE_OVERRIDE=UTC \
USE_SCHEMA_SUFFIX=False \
DEBUG=True \
python src/main.py
```

## Latest-tick run

```bash
TAGS="[fact]" \
LATEST_ENABLED=True \
USE_SCHEMA_SUFFIX=False \
python src/main.py
```

## Re-run an already-applied window

State normally skips applied intervals. To force re-execution of a window
that's already `applied`, torch/disable state — inline, so the next run
doesn't inherit it:

```bash
TAGS="[FactCase]" \
BACKFILL_ENABLED=True \
RUN_SINCE=2024-11-07 \
RUN_UNTIL=2024-11-08 \
TIMEZONE_OVERRIDE=UTC \
STATE_MODE=torch \
python src/main.py
```

For the tag expression itself (`[a & b]`, `not:`, `r:` reload, …), use the
`tags` skill.
