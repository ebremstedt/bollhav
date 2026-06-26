# bollhav-tui

A terminal UI for running and exploring [bollhav](../../README.md) models. Set
the run environment in a form — run window, suffixes, state mode, overrides
— and fire a **dry run**, a **dry-state** run, or the **real** run, without
remembering env-var names.

## Install & run

The TUI ships with bollhav under the `tui` extra:

```bash
pip install "bollhav[tui]"
```

Then just run `bollhav`, pointed at a folder that contains your models:

```bash
bollhav                  # browse the current directory
bollhav path/to/models   # browse a specific folder
bollhav a/ b/            # multiple folders (switch between them in-app)
```

`bollhav-tui` is a longer alias for the same command, and
`python -m bollhav.tui [FOLDER ...]` works too.

Each `FOLDER` is scanned recursively: every `Model` defined in any `.py` file
below it is imported and listed. A file that fails to import is skipped and
reported rather than aborting the scan. With no arguments the current directory
is browsed. `$BOLLHAV_PROJECT` is honoured as an extra folder, and folders are
remembered in `~/.config/bollhav-tui/workspaces.json`.

Running a model executes the nearest `main.py` at or above the browsed folder,
using the Python given by `$BOLLHAV_RUN_PYTHON` (default: the current
interpreter).

## The two modes

Switch with **`ctrl+t`**.

### Run

A **source folder** bar spans the top — pick a remembered folder from the
*Recent* dropdown, or type a path in *Open path* and press Enter.

Below it, a collapsible **config menu** (left, ~27% — toggle with `ctrl+b`) and a
**RESULTS** pane (right) where run output streams.

The config menu is grouped into sections:

- **TAGS** — which models to run (`[model_a,model_b]`); empty = all discovered.
- **WINDOW** — a `Mode` of **latest** (the default) or **backfill** (mutually exclusive).
  - *latest* shows the **Window expression** override.
  - *backfill* shows **Since / Until** date pickers (`RUN_SINCE` / `RUN_UNTIL`,
    both optional — empty runs the contract range).
- **SUFFIX** — `SCHEMA` / `TABLE` on/off toggles, each with its suffix value.
- **OVERRIDES** — `Interval expr`, `Lookback` (apply to both modes).
- **STATE** — `Mode` (`STATE_MODE`): **bulldozer** (default — reset & rerun the
  window), **discover** (run only what's still outstanding), or **torch** (wipe
  *all* state; the window scopes what runs now, the rest defers to a later
  discover run).
- **DIAGNOSTICS** — `Debug`, and `Verbose dry output` (uses the `*_EXTRA`
  variants when you run a dry action).

The three run actions escalate in danger:

| Key | Action | Effect |
| --- | --- | --- |
| `ctrl+d` | **DRY RUN** | `DRY_RUN=true` — model-level summary, no database |
| `ctrl+e` | **DRY STATE** | `DRY_STATE=true` — resolves state, writes nothing (needs `STATE_DSN`) |
| `ctrl+r` | **RUN** | the real run (asks to confirm first) |

With **Verbose** on, `ctrl+d` / `ctrl+e` run the verbose `DRY_RUN_EXTRA` /
`DRY_STATE_EXTRA` variants. Verbose is applied only to that dry action — it is
never persisted as an env var (so it can't turn a real `ctrl+r` RUN into a dry
run).

### Explore

A JIRA-style board of every discovered model — `KEY`, summary, type, and a
status pill that moves `TO DO → IN PROGRESS → DONE / FAILED`. Run them one at a
time or all at once, using the same config as the Run tab.

| Key | Action |
| --- | --- |
| `j` / `k` | move down / up |
| `enter` / `r` | run the highlighted model |
| `a` | run all |

## All keybindings

| Key | Action |
| --- | --- |
| `ctrl+t` | switch Run / Explore |
| `ctrl+b` | collapse / restore the config menu |
| `ctrl+g` | hacker mode (bright borders) |
| `ctrl+d` | dry run |
| `ctrl+e` | dry state |
| `ctrl+r` | run (with confirm) |
| `c` | clear the log |
| `q` | quit |

## Connections & config persistence

DSNs are **not** entered in the UI — they are read from your shell environment:

- `TARGET_DSN` for real runs,
- `STATE_DSN` to dry-run against state.

Config is saved per project in **`.bollhav-tui.json`** in the project folder and
restored on the next launch.
