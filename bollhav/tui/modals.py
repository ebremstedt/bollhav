"""The app's modal screens: a guard before a real run."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Static


class ConfirmRun(ModalScreen[bool]):
    """Guard the real RUN. Dismisses True to proceed, False to cancel."""

    CSS = """
    ConfirmRun { align: center middle; }
    #confirm-card {
        width: 60;
        height: auto;
        border: thick red;
        border-title-color: red;
        background: $panel;
        padding: 1 2;
    }
    #confirm-msg { height: auto; margin: 0 0 1 0; }
    #confirm-buttons { height: auto; align: right middle; }
    #confirm-buttons Button { margin: 0 1; }
    """

    BINDINGS = [
        ("escape,n", "no", "Cancel"),
        ("y", "yes", "Run"),
    ]

    def compose(self) -> ComposeResult:
        card = Vertical(id="confirm-card")
        card.border_title = "⚠ REAL RUN"
        with card:
            yield Static(
                "This executes models for real against the target — writes and "
                "all.\n\nProceed?  (y / n)",
                id="confirm-msg",
            )
            with Horizontal(id="confirm-buttons"):
                yield Button("Run", variant="error", id="confirm-yes")
                yield Button("Cancel", id="confirm-no")

    def action_yes(self) -> None:
        self.dismiss(True)

    def action_no(self) -> None:
        self.dismiss(False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "confirm-yes")
