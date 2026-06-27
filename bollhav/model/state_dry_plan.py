"""Rendering of the `DRY_STATE` plan — the read-only preview that prints, per
model, what would run / is applied / is blocked (and, under `DRY_STATE_EXTRA`,
every interval). Pure presentation: ANSI coloring, the model-name gradient, and
the per-run plan layout. The lifecycle decides *when* to call this (under
`DRY_STATE`); this module owns *how* the plan looks.

Kept out of `lifecycle.py` so the run-bracketing logic there isn't tangled with
terminal-formatting concerns. `_print_state_plan` takes `extra` as a parameter
rather than reading the env itself, so this module depends on nothing in
`lifecycle`."""

from __future__ import annotations

import sys


def _sgr(s: str, code: str) -> str:
    """Wrap `s` in an ANSI SGR `code` for terminal output; left plain when
    stdout isn't a TTY (so piped/captured output has no escape codes)."""
    return f"\033[{code}m{s}\033[0m" if sys.stdout.isatty() else s


def _blue(s: str) -> str:
    return _sgr(s, "34")


def _red(s: str) -> str:
    return _sgr(s, "31")


def _lgreen(s: str) -> str:
    """Light green — actionable (`pending`) units in the DRY_STATE plan."""
    return _sgr(s, "38;2;120;215;120")


def _dgreen(s: str) -> str:
    """Deeper green — already-`applied` units in the DRY_STATE plan."""
    return _sgr(s, "38;2;35;150;70")


# Dark → light blue ramp (truecolor RGB) for rendering a dotted model name as a
# gradient: first segment (catalog) darkest, last (table) lightest. Every stop
# keeps green ≥ red with a dominant blue channel, so it stays squarely blue and
# never drifts toward purple/indigo (which is what the low xterm-256 navies do).
_NAME_RAMP = (
    (35, 130, 220),
    (75, 160, 235),
    (110, 185, 245),
    (140, 205, 252),
    (170, 220, 255),
)


def _gradient_name(full_name: str) -> str:
    """`catalog.schema.table` with each dotted segment a shade lighter than the
    one before it (darkest first). The dots stay the terminal default; plain
    text when stdout isn't a TTY."""
    parts = full_name.split(".")
    last = len(parts) - 1

    def _shade(text: str, rgb: tuple[int, int, int]) -> str:
        r, g, b = rgb
        return _sgr(text, f"38;2;{r};{g};{b}")

    if last <= 0:
        return _shade(full_name, _NAME_RAMP[0])
    return ".".join(
        _shade(part, _NAME_RAMP[round(i * (len(_NAME_RAMP) - 1) / last)])
        for i, part in enumerate(parts)
    )


def _fmt_window(interval) -> str:
    """A compact window label for one unit of work; `None` is the whole-table
    / view oneshot."""
    if interval is None:
        return "(whole table)"
    return f"{interval.since:%Y-%m-%d %H:%M} → {interval.until:%Y-%m-%d %H:%M}"


# DRY_STATE cascade accumulator: full_name → windows that would run this pass.
# Models are processed in dependency order (a real `@load_models` run topo-sorts
# them), so a downstream sees its upstreams' would-run windows here and can show
# "will run after <upstream>" instead of "blocked". Populated only under
# DRY_STATE; never consulted by real gating.
_DRY_STATE_RUNS: dict[str, list] = {}


def _print_state_plan(run, state_handler, extra: bool) -> None:
    """One run's state-resolved plan, for `DRY_STATE`. Each actionable unit is
    classified — **would run** (gates already satisfied), **will run after** an
    upstream that would itself run earlier in this pass (the cascade), or
    **blocked** by an upstream that would NOT run — then summarized (counts) or,
    with `extra` (DRY_STATE_EXTRA), listed per interval. Read-only: gates are
    evaluated live, the cascade via the in-pass `_DRY_STATE_RUNS` overlay."""
    name = run.model.target.full_name  # raw — used as the `_DRY_STATE_RUNS` key
    display = _gradient_name(name)  # colorized for output only
    kind = run.model.temporality.value
    if state_handler is None:
        pending = _lgreen(f"pending {len(run.intervals)} unit(s)")
        print(f"  {display} ({kind})  ·  stateless → {pending}")
        if extra:
            for interval in run.intervals:
                print(f"      {_blue(_fmt_window(interval))}   {_lgreen('pending')}")
        _DRY_STATE_RUNS[name] = list(run.intervals)
        return

    # (interval, status, upstreams) — status in {"run", "after", "blocked"}.
    rows = [
        (interval, *state_handler.dry_state_classify(interval, _DRY_STATE_RUNS))
        for interval in run.intervals
    ]

    would_run = sum(1 for _, s, _ in rows if s in ("run", "after"))
    blocked = sum(1 for _, s, _ in rows if s == "blocked")
    applied = state_handler.read_status_summary()["counts"].get("applied", 0)
    # Show only the non-zero buckets — a `pending 0` / `blocked 0` / `applied 0`
    # segment is just noise. A model with nothing in any bucket falls back to a
    # dash so the line still reads.
    segs = []
    if would_run:
        segs.append(_lgreen(f"pending {would_run}"))
    if blocked:
        segs.append(_red(f"blocked {blocked}"))
    if applied:
        segs.append(_dgreen(f"applied {applied}"))
    print(f"  {display} ({kind})  ·  " + ("  ·  ".join(segs) if segs else "—"))

    # Record would-run (immediate + cascade) windows so downstreams resolve.
    _DRY_STATE_RUNS[name] = [iv for iv, s, _ in rows if s in ("run", "after")]

    if extra:
        for interval, status, ups in rows:
            if status == "run":
                tail = _lgreen("pending")
            elif status == "after":
                tail = f"pending after {', '.join(ups)}"
            else:
                tail = _red(f"blocked: {'; '.join(ups)}")
            print(f"      {_blue(_fmt_window(interval))}   {tail}")
    else:
        agg: dict[str, int] = {}
        for _, s, ups in rows:
            if s == "blocked":
                agg["; ".join(ups)] = agg.get("; ".join(ups), 0) + 1
        for reason, n in sorted(agg.items()):
            print(_red(f"      blocked by: {reason}{f'  ×{n}' if n > 1 else ''}"))
