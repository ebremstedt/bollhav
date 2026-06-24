from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.css.query import NoMatches
from textual.widgets import Input, RichLog, Select, Static

from bollhav.tui.constants import (
    BOOL_DEFAULTS,
    CONFIG_FIELDS,
    CONNECTION_NOTE,
    DRY_EXTRA_KEY,
    EXTRA_ENV,
    FIELD_ROWS,
    GRID_SECTIONS,
    MODE_DEFAULT,
    MODE_ENV,
    MODE_ONLY,
    RUN_MODES,
)
from bollhav.tui.date_select import DateSelect
from bollhav.tui.discovery import nearest_runner
from bollhav.tui.radios import BoolRadio, ChoiceRadio, ModeRadio, StateRadio

if TYPE_CHECKING:
    from bollhav.tui.app import BollhavApp


class RunTab(Vertical):
    """CONFIG box (green, multi-column, sectioned) + buttons + RESULTS box."""

    DEFAULT_CSS = """
    RunTab #run-body { height: 1fr; layout: horizontal; }
    RunTab #config {
        width: 27%;
        background: $panel;
        padding: 0 1 0 0;
    }
    RunTab #run-results {
        width: 1fr;
        border: round $primary-darken-2;
        border-title-color: $primary;
        background: $panel;
    }
    RunTab .mode-opts { height: auto; }
    RunTab .cfg-group {
        height: auto;
        border: round $primary;
        border-title-color: $primary;
        border-title-style: bold;
        padding: 0 1;
        margin: 1 0 0 0;
    }
    RunTab .cfg-group Input, RunTab .cfg-group Select { margin: 0; }
    RunTab .field-label { color: $text-muted; margin: 0; }
    RunTab .field-note { color: $text-muted; margin: 0; }

    /* toggle beside its value on one line — radio stays narrow, input fills */
    RunTab .cfg-row { layout: horizontal; height: auto; }
    RunTab .cfg-row ChoiceRadio { width: auto; margin: 0 1 0 0; }
    RunTab .cfg-row Input { width: 1fr; }

    /* two-up fields, each ~50% wide */
    RunTab .cfg-grid { layout: horizontal; height: auto; }
    RunTab .cfg-half { width: 1fr; height: auto; margin: 0 1 0 0; }
    """

    @property
    def board(self) -> "BollhavApp":
        return self.app  # type: ignore[return-value]

    def compose(self) -> ComposeResult:
        by_section: dict[str, list[tuple[str, str, str, str, str]]] = {}
        order: list[str] = []
        for field in CONFIG_FIELDS:
            by_section.setdefault(field[0], []).append(field)
            if field[0] not in order:
                order.append(field[0])
        with Horizontal(id="run-body"):
            with VerticalScroll(id="config"):
                for section in order:
                    group = Vertical(classes="cfg-group")
                    group.border_title = section.upper()
                    with group:
                        if section == "Window":
                            yield from self._compose_window(by_section["Window"])
                        elif section in GRID_SECTIONS:
                            yield from self._compose_grid(by_section[section])
                        else:
                            yield from self._compose_fields(by_section[section])
                yield Static(CONNECTION_NOTE, classes="field-note")
            results = RichLog(id="run-results", highlight=True, markup=True, wrap=True)
            results.border_title = "RESULTS"
            yield results

    def toggle_menu(self) -> None:
        """Collapse / restore the left CONFIG menu so RESULTS can use the width."""
        config = self.query_one("#config")
        config.display = not config.display

    def _compose_fields(
        self, fields: list[tuple[str, str, str, str, str]]
    ) -> ComposeResult:
        """Render fields, pairing FIELD_ROWS siblings onto a shared line."""
        for row in self._rows(fields):
            if len(row) == 1:
                _s, env, label, ph, kind = row[0]
                yield from self._make_field(env, label, ph, kind)
            else:
                with Horizontal(classes="cfg-row"):
                    for _s, env, label, ph, kind in row:
                        yield from self._make_field(env, label, ph, kind)

    def _compose_grid(
        self, fields: list[tuple[str, str, str, str, str]]
    ) -> ComposeResult:
        """Lay fields out two-up, each in a ~50%-wide column."""
        for i in range(0, len(fields), 2):
            with Horizontal(classes="cfg-grid"):
                for _s, env, label, ph, kind in fields[i : i + 2]:
                    with Vertical(classes="cfg-half"):
                        yield from self._make_field(env, label, ph, kind)

    def _compose_window(
        self, fields: list[tuple[str, str, str, str, str]]
    ) -> ComposeResult:
        """Mode radio (always shown) + per-mode option groups toggled by it."""
        for _s, env, label, ph, kind in fields:
            if kind == "mode":
                yield from self._make_field(env, label, ph, kind)
        backfill = Vertical(id="backfill-opts", classes="mode-opts")
        with backfill:
            yield from self._compose_fields(
                [f for f in fields if MODE_ONLY.get(f[1]) == "backfill"]
            )
        latest = Vertical(id="latest-opts", classes="mode-opts")
        with latest:
            yield from self._compose_fields(
                [f for f in fields if MODE_ONLY.get(f[1]) == "latest"]
            )

    @staticmethod
    def _rows(
        fields: list[tuple[str, str, str, str, str]],
    ) -> list[list[tuple[str, str, str, str, str]]]:
        """Group consecutive fields that share a FIELD_ROWS id onto one row."""
        rows: list[list[tuple[str, str, str, str, str]]] = []
        for field in fields:
            row_id = FIELD_ROWS.get(field[1])
            if row_id and rows and FIELD_ROWS.get(rows[-1][-1][1]) == row_id:
                rows[-1].append(field)
            else:
                rows.append([field])
        return rows

    def _make_field(
        self, env: str, label: str, placeholder: str, kind: str
    ) -> ComposeResult:
        if kind == "date":
            yield Static(label, classes="field-label")
            yield DateSelect(id=f"cfg-{env}")
        elif kind == "mode":
            radio: ChoiceRadio = ModeRadio(default=MODE_DEFAULT, id=f"cfg-{env}")
            radio.border_title = label
            yield radio
        elif kind == "state":
            radio = StateRadio(id=f"cfg-{env}")
            radio.border_title = label
            yield radio
        elif kind == "bool":
            radio = BoolRadio(default=BOOL_DEFAULTS.get(env, False), id=f"cfg-{env}")
            radio.border_title = label
            yield radio
        else:
            # name on a line above the box (like the date fields) — clearer in
            # the narrow menu than a border title. Empty label → just the input
            # (e.g. the suffix value sitting next to its SCHEMA/TABLE toggle).
            if label:
                yield Static(label, classes="field-label")
            yield Input(placeholder=placeholder, id=f"cfg-{env}")

    # ── config form ↔ app.settings ──────────────────────────────────────
    def populate(self, settings: dict[str, str]) -> None:
        """Push saved settings into the CONFIG widgets (TAGS is per-run, skipped)."""
        for _sec, env, _label, _ph, kind in CONFIG_FIELDS:
            if env == "TAGS":
                continue
            widget = self.query_one(f"#cfg-{env}")
            if kind == "mode":
                mode = (
                    "latest" if settings.get("LATEST_ENABLED") == "true" else "backfill"
                )
                widget.set_value(mode)  # type: ignore[attr-defined]
            elif kind in ("date", "bool", "state"):
                widget.set_value(settings.get(env, ""))  # type: ignore[attr-defined]
            else:
                widget.value = settings.get(env, "")  # type: ignore[attr-defined]
        self._apply_mode()

    def _apply_mode(self) -> None:
        """Show only the option group for the selected window mode."""
        mode = self.query_one("#cfg-WINDOW_MODE", ModeRadio).value
        self.query_one("#backfill-opts").display = mode == "backfill"
        self.query_one("#latest-opts").display = mode == "latest"

    def collect(self) -> tuple[dict[str, str], str]:
        """Read the CONFIG widgets. Returns (env_settings, tags)."""
        env: dict[str, str] = {}
        tags = ""
        mode = self.query_one("#cfg-WINDOW_MODE", ModeRadio).value
        for _sec, key, _label, _ph, kind in CONFIG_FIELDS:
            # skip the inactive mode's options so they don't leak into the run
            if MODE_ONLY.get(key) not in (None, mode):
                continue
            widget = self.query_one(f"#cfg-{key}")
            if kind == "mode":
                env.update(MODE_ENV[widget.value])  # type: ignore[attr-defined]
                continue
            if kind in ("date", "bool", "state"):
                value = widget.value  # type: ignore[attr-defined]
            else:
                value = widget.value.strip()  # type: ignore[attr-defined]
            if key == "TAGS":
                tags = value
            elif value:
                env[key] = value
        return env, tags

    def _sync_to_app(self) -> None:
        """Mirror the form into app.settings so the Explore tab runs with the
        same config. Persistence to disk happens on run / project switch."""
        env, _ = self.collect()
        self.board.settings = env
        self.board.refresh_topbar()

    def on_input_changed(self, event: Input.Changed) -> None:
        self._sync_to_app()

    def on_select_changed(self, event: Select.Changed) -> None:
        self._sync_to_app()

    def on_radio_set_changed(self, event) -> None:
        # radios pre-press during mount and fire before siblings exist
        try:
            self._apply_mode()
            self._sync_to_app()
        except NoMatches:
            pass

    # ── run (triggered by the app's ^D / ^E / ^R bindings) ───────────────
    @work(exclusive=False, group="run")
    async def run(self, mode: str) -> None:
        """Execute main.py once for the configured TAGS in `mode`, streaming
        output into RESULTS."""
        app = self.board
        label, env_var, _btn = RUN_MODES[mode]
        log = self.query_one("#run-results", RichLog)
        folder = app.project

        env_cfg, tags = self.collect()
        app.settings = env_cfg
        app.save_settings()
        app.refresh_topbar()

        if not tags:
            if not app.models:
                log.write("[red3]✗ no models discovered — nothing to run[/]")
                return
            # one [group] per model — separate groups are OR'd, so this matches
            # every discovered model (a comma/& inside one group would mean AND)
            tags = "".join(f"[{m.target.name}]" for m in app.models)

        runner_dir = nearest_runner(folder)
        if runner_dir is None:
            log.write(f"[red3]✗ no main.py at or above[/] {folder}")
            return

        # Verbose is a UI flag, not a real env var — strip it, then apply it as
        # the _EXTRA variant only for this dry action (never for a real RUN).
        verbose = env_cfg.get(DRY_EXTRA_KEY) == "true"
        env = os.environ.copy()
        env.update({k: v for k, v in env_cfg.items() if v and k != DRY_EXTRA_KEY})
        env["TAGS"] = tags
        if env_var:
            env[EXTRA_ENV[mode] if verbose else env_var] = "true"
            if verbose:
                label += " · verbose"

        log.write(f"[b]▶ {label}[/]  TAGS={tags}  (cwd={runner_dir.name})")
        try:
            proc = await asyncio.create_subprocess_exec(
                app.run_python,
                "main.py",
                cwd=str(runner_dir),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            if proc.stdout is None:
                return
            async for raw in proc.stdout:
                log.write(Text(raw.decode(errors="replace").rstrip()))
            rc = await proc.wait()
        except Exception as exc:  # noqa: BLE001
            log.write(f"[red3]✗ launch error[/] {exc}")
            return

        if rc == 0:
            log.write(f"[green3]✓ {label} complete[/]")
        else:
            log.write(f"[red3]✗ {label} failed (exit {rc})[/]")
