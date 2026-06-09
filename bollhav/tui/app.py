"""bollhav-tui — a two-mode TUI for bollhav models.

A SOURCE box at the top chooses the folder models are discovered from. Below it,
two tabs (switch with ctrl+t):

  • RUN     — set the run env (TARGET_DSN, backfill window, suffixes, …) in the
              multi-column CONFIG box, then fire one of three escalating actions:
                  DRY RUN   (ctrl+d, yellow)  → DRY_RUN=true    model-level, no DB
                  DRY STATE (ctrl+e, orange)  → DRY_STATE=true  resolves state, no writes
                  RUN       (ctrl+r, red)     → the real thing (asks to confirm)
              Output streams into the RESULTS box below.

  • EXPLORE — a JIRA-style board of every discovered model; run them one at a
              time (enter/r) or all (a). Uses the same CONFIG as the RUN tab.

Run:
    bollhav-tui [FOLDER ...]
    python -m bollhav.tui [FOLDER ...]

Each FOLDER is browsed for models recursively. With no args it browses the
current directory. Folders are remembered in
~/.config/bollhav-tui/workspaces.json and offered in the SOURCE dropdown.
$BOLLHAV_PROJECT is honoured as an extra folder.

Config is saved per project in `.bollhav-tui.json` in the project folder and
restored on the next launch. Running executes the nearest `main.py` at or above
the browsed folder, with the Python given by $BOLLHAV_RUN_PYTHON.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widgets import (
    Footer,
    Input,
    RichLog,
    Select,
    TabbedContent,
    TabPane,
)

from bollhav.tui.discovery import (
    discover_models,
    resolve_folders,
    save_workspaces,
)
from bollhav.tui.explore_tab import ExploreTab
from bollhav.tui.modals import ConfirmRun
from bollhav.tui.run_tab import RunTab

# Per-project config lives in this dotfile in the project folder.
SETTINGS_FILENAME = ".bollhav-tui.json"


class BollhavApp(App):
    """Top-level app: owns the shared project / settings / models state and
    hosts the SOURCE selector plus the Run and Explore tabs."""

    CSS = """
    Screen { background: $surface; }
    #source {
        height: auto;
        border: round $primary;
        border-title-color: $primary;
        background: $panel;
        padding: 0 1;
    }
    #source Select { width: 45%; margin: 0 1 0 0; }
    #source Input { width: 1fr; }

    /* hacker mode — bright light-blue borders everywhere (ctrl+g) */
    Screen.hacker #source,
    Screen.hacker #run-results,
    Screen.hacker #board,
    Screen.hacker #output,
    Screen.hacker .cfg-group,
    Screen.hacker ChoiceRadio {
        border: round #5fd7ff;
        border-title-color: #5fd7ff;
    }
    """

    BINDINGS = [
        # global — ctrl combos so they fire even while a config field is focused
        Binding("ctrl+t", "toggle_mode", "Run/Explore", priority=True),
        Binding("ctrl+b", "toggle_config", "Toggle menu", priority=True),
        Binding("ctrl+g", "toggle_hacker", "Hacker mode", priority=True),
        Binding("ctrl+d", "run_dry", "Dry run", priority=True),
        Binding("ctrl+e", "run_state", "Dry state", priority=True),
        Binding("ctrl+r", "run_live", "RUN", priority=True),
        ("c", "clear_log", "Clear log"),
        ("q", "quit", "Quit"),
        # explore navigation
        ("j", "cursor_down", "Down"),
        ("k", "cursor_up", "Up"),
        ("enter,r", "run_selected", "Run model"),
        ("a", "run_all", "Run all"),
    ]

    def __init__(
        self, projects: list[Path], run_python: str, start_index: int = 0
    ) -> None:
        super().__init__()
        self.projects = projects
        self.run_python = run_python
        self.active_index = start_index
        self._discovery: dict[str, tuple[list, list[str]]] = {}
        # (re)bound to the active project by _activate()
        self.project = projects[start_index]
        self.models: list = []
        self.settings_path = self.project / SETTINGS_FILENAME
        self.settings: dict[str, str] = {}

    # ── shared services used by both tabs ────────────────────────────────
    def _safe_discover(self, folder: Path) -> tuple[list, list[str]]:
        """Import every Model found recursively below `folder`, cached."""
        key = str(folder)
        if key not in self._discovery:
            self._discovery[key] = discover_models(folder)
        return self._discovery[key]

    def _load_settings(self) -> dict[str, str]:
        try:
            data = json.loads(self.settings_path.read_text())
            return {str(k): str(v) for k, v in data.items()}
        except (OSError, ValueError):
            return {}

    def save_settings(self) -> None:
        try:
            self.settings_path.write_text(json.dumps(self.settings, indent=2))
        except OSError as exc:
            self.query_one("#run-results", RichLog).write(
                f"[red3]could not save settings:[/] {exc}"
            )

    def _output(self) -> RichLog:
        return self.query_one(ExploreTab).query_one("#output", RichLog)

    # ── layout ───────────────────────────────────────────────────────────
    def compose(self) -> ComposeResult:
        source = Horizontal(id="source")
        source.border_title = "SOURCE FOLDER — models discovered recursively below it"
        with source:
            recent = Select([], prompt="recent folders…", id="source-select")
            recent.border_title = "Recent"
            yield recent
            open_path = Input(
                placeholder="/path/to/folder — press Enter to load",
                id="source-input",
            )
            open_path.border_title = "Open path"
            yield open_path
        with TabbedContent(initial="tab-run", id="tabs"):
            with TabPane("Run", id="tab-run"):
                yield RunTab()
            with TabPane("Explore", id="tab-explore"):
                yield ExploreTab()
        yield Footer()

    def refresh_topbar(self) -> None:
        # topbar removed — kept as a no-op so the tabs can call it freely
        pass

    def on_mount(self) -> None:
        self._activate(self.active_index)
        self.query_one("#cfg-TAGS").focus()

    # ── project switching ────────────────────────────────────────────────
    def _activate(self, index: int) -> None:
        """Switch to projects[index] — discover, reload settings, repaint."""
        self.active_index = index
        self.project = self.projects[index]
        self.settings_path = self.project / SETTINGS_FILENAME
        self.settings = self._load_settings()

        models, skipped = self._safe_discover(self.project)
        self.models = models

        self._refresh_source()
        self.query_one(RunTab).populate(self.settings)
        self.query_one(ExploreTab).on_project_changed(skipped)
        self.refresh_topbar()

    def _refresh_source(self) -> None:
        """Sync the SOURCE dropdown + path input to the current project list."""
        select = self.query_one("#source-select", Select)
        options = [(str(p), i) for i, p in enumerate(self.projects)]
        select.set_options(options)
        select.value = self.active_index
        self.query_one("#source-input", Input).value = str(self.project)

    def _load_folder(self, raw: str) -> None:
        folder = Path(raw).expanduser().resolve()
        if not folder.is_dir():
            self._output().write(f"[red3]not a folder:[/] {folder}")
            self.query_one("#source-input", Input).value = str(self.project)
            return
        if folder in self.projects:
            self._activate(self.projects.index(folder))
        else:
            self.projects.append(folder)
            save_workspaces(self.projects)
            self._activate(len(self.projects) - 1)
        self._output().write(f"[green3]browsing[/] {folder}")

    # ── SOURCE box events ──────────────────────────────────────────────────
    def on_select_changed(self, event: Select.Changed) -> None:
        # config selects bubble here too — only act on the source dropdown
        if event.select.id != "source-select":
            return
        index = event.value
        if isinstance(index, int) and index != self.active_index:
            self._activate(index)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "source-input" and event.value.strip():
            self._load_folder(event.value.strip())

    # ── global actions ────────────────────────────────────────────────────
    def action_toggle_mode(self) -> None:
        tabs = self.query_one("#tabs", TabbedContent)
        target = "tab-explore" if tabs.active == "tab-run" else "tab-run"
        tabs.active = target
        # Move focus into the target pane — otherwise a still-focused widget in
        # the old pane snaps `active` straight back (TabPane.Focused handling).
        if target == "tab-explore":
            self.query_one(ExploreTab).query_one("#board").focus()
        else:
            self.query_one("#cfg-TAGS").focus()

    def action_toggle_config(self) -> None:
        self.query_one(RunTab).toggle_menu()

    def action_toggle_hacker(self) -> None:
        self.screen.toggle_class("hacker")

    def action_clear_log(self) -> None:
        tabs = self.query_one("#tabs", TabbedContent)
        if tabs.active == "tab-run":
            self.query_one("#run-results", RichLog).clear()
        else:
            self.query_one(ExploreTab).clear_log()

    # ── run actions (delegated to the Run tab) ────────────────────────────
    def action_run_dry(self) -> None:
        self.query_one(RunTab).run("dry_run")

    def action_run_state(self) -> None:
        self.query_one(RunTab).run("dry_state")

    def action_run_live(self) -> None:
        def go(ok: bool | None) -> None:
            if ok:
                self.query_one(RunTab).run("run")

        self.push_screen(ConfirmRun(), go)

    # ── explore actions (delegated to the Explore tab) ────────────────────
    def action_cursor_down(self) -> None:
        self.query_one(ExploreTab).cursor_down()

    def action_cursor_up(self) -> None:
        self.query_one(ExploreTab).cursor_up()

    def action_run_selected(self) -> None:
        self.query_one(ExploreTab).run_selected()

    def action_run_all(self) -> None:
        self.query_one(ExploreTab).run_all()


def main() -> None:
    folders, start = resolve_folders(sys.argv)
    run_python = os.environ.get("BOLLHAV_RUN_PYTHON", sys.executable)
    BollhavApp(projects=folders, run_python=run_python, start_index=start).run()


if __name__ == "__main__":
    main()
