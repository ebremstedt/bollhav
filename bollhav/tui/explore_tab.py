from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import TYPE_CHECKING

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, RichLog, Static

from bollhav.tui.constants import STATUS, TYPE_ICON, pill
from bollhav.tui.discovery import nearest_runner

if TYPE_CHECKING:
    from bollhav.tui.app import BollhavApp


def present_name(name: str, style: str) -> str:
    """Present a dotted `catalog.schema.table` name per the site-wide style.

    `lengthen` keeps it on one line; `thicken` stacks one dotted segment per
    line (each non-final segment keeps its trailing dot)."""
    if style == "thicken":
        return ".\n".join(name.split("."))
    return name


class ExploreTab(Vertical):
    """Board + detail pane (top) and an output log (bottom)."""

    DEFAULT_CSS = """
    ExploreTab #body { height: 1fr; }
    ExploreTab #board {
        width: 2fr;
        border: round $primary-darken-2;
        border-title-color: $primary;
        background: $panel;
    }
    ExploreTab DataTable > .datatable--cursor { background: $primary; }
    ExploreTab #detail {
        width: 1fr;
        border: round red;
        border-title-color: red;
        padding: 0 1;
        background: black;
        color: red;
    }
    ExploreTab #output {
        height: 12;
        border: round $primary-darken-2;
        border-title-color: $primary;
        background: $panel;
    }
    """

    def __init__(self) -> None:
        super().__init__()
        # run status per model, kept per project so a switch preserves it
        self._status_store: dict[str, dict[str, str]] = {}
        self.row_keys: list[str] = []
        self.status: dict[str, str] = {}

    @property
    def board(self) -> "BollhavApp":
        return self.app  # type: ignore[return-value]

    def compose(self) -> ComposeResult:
        with Horizontal(id="body"):
            table = DataTable(id="board", cursor_type="row", zebra_stripes=True)
            table.border_title = "BOARD"
            yield table
            detail = Static(id="detail")
            detail.border_title = "DETAILS"
            yield detail
        log = RichLog(id="output", highlight=True, markup=True, wrap=True)
        log.border_title = "OUTPUT"
        yield log

    def on_mount(self) -> None:
        table = self.query_one("#board", DataTable)
        for label in ("KEY", "SUMMARY", "TYPE", "STATUS"):
            table.add_column(label, key=label)

    # ── project switching ───────────────────────────────────────────────
    def on_project_changed(self, skipped: list[str]) -> None:
        """Rebuild the board for the app's current project."""
        self.status = self._status_store.setdefault(str(self.board.project), {})
        self._rebuild_board()

        log = self.query_one("#output", RichLog)
        if skipped:
            log.write(
                f"[yellow]skipped {len(skipped)} file(s)[/] while importing "
                f"{self.board.project}:"
            )
            for s in skipped[:10]:
                log.write(Text.assemble(("  • ", "yellow"), s))

        detail = self.query_one("#detail", Static)
        if self.board.models:
            self._show_detail(0)
        else:
            note = f"[red]No models found below[/]\n{self.board.project}"
            if skipped:
                note += (
                    f"\n\n[yellow]{len(skipped)} file(s) failed to import "
                    "(see OUTPUT)[/]"
                )
            detail.update(note)

    def _rebuild_board(self) -> None:
        table = self.query_one("#board", DataTable)
        table.clear()  # rows only; columns kept
        self.row_keys = []
        style = getattr(self.app, "name_style", "lengthen")
        for i, m in enumerate(self.board.models, start=1):
            key = f"BOL-{i}"
            self.row_keys.append(key)
            state = self.status.setdefault(key, "todo")
            name = present_name(m.target.full_name, style)
            table.add_row(
                Text(key, style="bold cyan"),
                Text(name),
                Text(TYPE_ICON.get(m.temporality.name, m.temporality.name)),
                pill(state),
                key=key,
                height=name.count("\n") + 1,
            )

    # ── selection → detail pane ─────────────────────────────────────────
    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if self.board.models:
            self._show_detail(event.cursor_row)

    def _show_detail(self, row: int) -> None:
        m = self.board.models[row]
        t = m.target
        style = getattr(self.app, "name_style", "lengthen")
        up = [s.name for s in m.upstream if not s.name.startswith("unknown-")]
        sep = "\n" if style == "thicken" else ", "
        up_str = sep.join(present_name(n, style) for n in up) if up else "—"
        b = m.contract
        contract = (
            f"{b.begin:%Y-%m-%d} → {b.end:%Y-%m-%d}"
            if getattr(b, "begin", None) and getattr(b, "end", None)
            else "—"
        )
        wm = getattr(t.write_mode, "name", str(getattr(t, "write_mode", "—")))
        lines = [
            f"[b bright_red]{self.row_keys[row]}[/]  [b red]{present_name(t.full_name, style)}[/]",
            "",
            f"[b bright_red]Type[/]       [red]{TYPE_ICON.get(m.temporality.name, m.temporality.name)}[/]",
            f"[b bright_red]Schema[/]     [red]{t.schema}[/]",
            f"[b bright_red]Catalog[/]    [red]{t.catalog}[/]",
            f"[b bright_red]Write mode[/] [red]{wm}[/]",
            f"[b bright_red]Contract[/]   [red]{contract}[/]",
            f"[b bright_red]Upstream[/]   [red]{up_str}[/]",
            f"[b bright_red]Status[/]     [red]{STATUS[self.status[self.row_keys[row]]][0]}[/]",
            "",
            f"[red]{m.description or 'no description'}[/]",
            "",
            f"[red]tags: {', '.join(sorted(m.tags))}[/]",
        ]
        self.query_one("#detail", Static).update("\n".join(lines))

    # ── status helpers ──────────────────────────────────────────────────
    def _mark(self, project: Path, key: str, state: str) -> None:
        """Record a model's run status, project-aware so a run that finishes
        after the user has switched projects updates the right store and only
        touches the board when its project is the visible one."""
        self._status_store.setdefault(str(project), {})[key] = state
        if project == self.board.project and key in self.row_keys:
            table = self.query_one("#board", DataTable)
            table.update_cell(key, "STATUS", pill(state))
            row = self.row_keys.index(key)
            if table.cursor_row == row:
                self._show_detail(row)

    # ── actions (delegated from the app's bindings) ──────────────────────
    def cursor_down(self) -> None:
        self.query_one("#board", DataTable).action_cursor_down()

    def cursor_up(self) -> None:
        self.query_one("#board", DataTable).action_cursor_up()

    def clear_log(self) -> None:
        self.query_one("#output", RichLog).clear()

    def run_selected(self) -> None:
        if self.board.models:
            self.run_model(self.query_one("#board", DataTable).cursor_row)

    def run_all(self) -> None:
        for row in range(len(self.board.models)):
            self.run_model(row)

    @work(exclusive=False, group="run")
    async def run_model(self, row: int) -> None:
        """Run a single model (its own TAGS) using the shared config."""
        app = self.board
        m = app.models[row]
        name = m.target.name
        # capture folder + key now — the user may switch folders mid-run
        folder = app.project
        key = self.row_keys[row]
        log = self.query_one("#output", RichLog)

        app.save_settings()
        runner_dir = nearest_runner(folder)
        if runner_dir is None:
            log.write(
                f"[red3]✗ cannot run[/] {m.target.full_name} — "
                f"no main.py at or above {folder}"
            )
            self._mark(folder, key, "fail")
            return

        self._mark(folder, key, "run")
        log.write(
            f"[b deep_sky_blue2]▶ running[/] {m.target.full_name}  "
            f"(TAGS=[{name}], cwd={runner_dir.name})"
        )

        env = os.environ.copy()
        env.update({k: v for k, v in app.settings.items() if v})
        env["TAGS"] = f"[{name}]"

        try:
            proc = await asyncio.create_subprocess_exec(
                app.run_python,
                "main.py",
                cwd=str(runner_dir),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            assert proc.stdout is not None
            async for raw in proc.stdout:
                # raw subprocess output → plain Text (no markup parsing, can't crash)
                log.write(
                    Text.assemble(
                        (f"  {name} ", "dim"),
                        raw.decode(errors="replace").rstrip(),
                    )
                )
            rc = await proc.wait()
        except Exception as exc:  # noqa: BLE001
            log.write(f"[red3]✗ launch error[/] {exc}")
            self._mark(folder, key, "fail")
            return

        if rc == 0:
            log.write(f"[green3]✓ done[/] {m.target.full_name}")
            self._mark(folder, key, "done")
        else:
            log.write(f"[red3]✗ failed[/] {m.target.full_name} (exit {rc})")
            self._mark(folder, key, "fail")
