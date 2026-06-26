from __future__ import annotations

from rich.text import Text

# The run-time env a bollhav project reads (see bollhav load_models docstring),
# plus TARGET_DSN. Each entry: (section, env var, label, placeholder, kind).
# kind ∈ {text, bool, date, select} → drives which widget the form renders.
#
# DRY_RUN / DRY_STATE are intentionally absent — they are driven by the three
# escalating buttons on the RUN tab, not typed into the form.
CONFIG_FIELDS: list[tuple[str, str, str, str, str]] = [
    ("TAGS", "TAGS", "", "[model_a,model_b] — empty = all discovered", "text"),
    ("Window", "WINDOW_MODE", "Mode", "", "mode"),
    ("Window", "RUN_SINCE", "Since", "", "date"),
    ("Window", "RUN_UNTIL", "Until", "", "date"),
    (
        "Window",
        "WINDOW_OVERRIDE",
        "Window expression",
        "cron / @alias",
        "text",
    ),
    ("Suffix", "USE_SCHEMA_SUFFIX", "SCHEMA", "", "bool"),
    ("Suffix", "SCHEMA_SUFFIX", "", "value", "text"),
    ("Suffix", "USE_TABLE_SUFFIX", "TABLE", "", "bool"),
    ("Suffix", "TABLE_SUFFIX", "", "value", "text"),
    (
        "Overrides",
        "INTERVAL_OVERRIDE",
        "Interval expr",
        "@daily / cron",
        "text",
    ),
    ("Overrides", "LOOKBACK_OVERRIDE", "Lookback", "non-negative int", "text"),
    ("State", "STATE_MODE", "Mode", "", "state"),
    ("Diagnostics", "DEBUG", "Debug", "", "bool"),
    ("Diagnostics", "DRY_EXTRA", "Verbose dry output", "", "bool"),
]

# Window fields shown only in one mode (the Mode radio itself is always shown).
# backfill mode → since/until; latest mode → the window expression.
MODE_ONLY: dict[str, str] = {
    "RUN_SINCE": "backfill",
    "RUN_UNTIL": "backfill",
    "WINDOW_OVERRIDE": "latest",
}

# How the CONFIG sections are spread across columns on the RUN tab — one inner
# list per column, left to right. Lets the form use horizontal space.
CONFIG_COLUMNS: list[list[str]] = [
    ["TAGS", "Window"],
    ["Suffix", "Overrides", "Diagnostics"],
]

# DSNs are sensitive, so they get no input — this note names the env var needed
# for a dry run against state (set it in your shell environment).
CONNECTION_NOTE = "ⓘ  STATE_DSN env var must be set to dry-run against state"

# Sections whose fields lay out two-up (each ~50% wide) instead of full-width.
GRID_SECTIONS = {"Overrides"}

# Fields that share one horizontal row (toggle beside its value) instead of each
# taking a full line. Keyed by env var → row id; same row id = same line.
FIELD_ROWS: dict[str, str] = {
    "USE_SCHEMA_SUFFIX": "schema",
    "SCHEMA_SUFFIX": "schema",
    "USE_TABLE_SUFFIX": "table",
    "TABLE_SUFFIX": "table",
}

# Each "bool" field's own default — the radio starts on this, matching what
# bollhav does when the var is left unset.
BOOL_DEFAULTS: dict[str, bool] = {
    "USE_SCHEMA_SUFFIX": True,
    "USE_TABLE_SUFFIX": False,
    "DEBUG": False,
}

# The "mode" field is a single radio that expands to the mutually-exclusive
# LATEST_ENABLED / BACKFILL_ENABLED pair — exactly one is true. `latest` is the
# default, matching bollhav (a bare run does the latest complete tick).
MODE_DEFAULT = "latest"
MODE_ENV = {
    "backfill": {"BACKFILL_ENABLED": "true", "LATEST_ENABLED": "false"},
    "latest": {"BACKFILL_ENABLED": "false", "LATEST_ENABLED": "true"},
}

# One-line crib for the three STATE_MODE choices, shown under the State radio.
STATE_MODE_NOTE = (
    "ⓘ  bulldozer: reset & rerun the window · discover: run only what's "
    "outstanding · torch: wipe all, run the window (rest defers to discover)"
)

# The three escalating run actions: mode → (label, env var to set, button id).
# A None env var means "the real run" — set nothing extra.
RUN_MODES: dict[str, tuple[str, str | None, str]] = {
    "dry_run": ("DRY RUN", "DRY_RUN", "btn-dry-run"),
    "dry_state": ("DRY STATE", "DRY_STATE", "btn-dry-state"),
    "run": ("RUN", None, "btn-run"),
}

# The Verbose toggle (synthetic "DRY_EXTRA" setting) swaps a dry action's env var
# for its verbose variant — but only at run time, never persisted as an env var
# (DRY_RUN_EXTRA=true would otherwise force a real RUN into a dry run).
DRY_EXTRA_KEY = "DRY_EXTRA"
EXTRA_ENV: dict[str, str] = {
    "dry_run": "DRY_RUN_EXTRA",
    "dry_state": "DRY_STATE_EXTRA",
}

# JIRA-board status vocabulary → (label, colour)
STATUS = {
    "todo": ("TO DO", "grey70"),
    "run": ("IN PROGRESS", "deep_sky_blue2"),
    "done": ("DONE", "green3"),
    "fail": ("FAILED", "red3"),
}
# Keyed by `Temporality.name` (the value looked up in explore_tab).
TYPE_ICON = {"TEMPORAL": "⏱ temporal", "TIMELESS": "▣ timeless"}


def pill(state: str) -> Text:
    label, colour = STATUS[state]
    return Text(f" {label} ", style=f"bold black on {colour}")
