from __future__ import annotations

from textual.widgets import RadioButton, RadioSet


class ChoiceRadio(RadioSet):
    """A horizontal RadioSet whose value is one of `_OPTIONS` (a string).

    `_LABELS`, if set, are the button captions shown to the user (parallel to
    `_OPTIONS`); the stored value is always the matching `_OPTIONS` entry.
    """

    _OPTIONS: list[str] = []
    _LABELS: list[str] = []

    DEFAULT_CSS = """
    ChoiceRadio {
        layout: horizontal;
        height: auto;
        width: 1fr;
        border: round $primary-darken-2;
        border-title-color: $primary;
        padding: 0 1;
        margin: 0;
    }
    ChoiceRadio RadioButton {
        width: auto;
        margin: 0 2 0 0;
        border: none;
        background: transparent;
    }
    """

    def __init__(self, default: str, id: str | None = None) -> None:
        self._default = default if default in self._OPTIONS else self._OPTIONS[0]
        labels = self._LABELS or self._OPTIONS
        # pre-press the default at construction — RadioSet picks it up on mount.
        super().__init__(
            *(
                RadioButton(labels[i], value=(option == self._default))
                for i, option in enumerate(self._OPTIONS)
            ),
            id=id,
        )

    @property
    def value(self) -> str:
        idx = self.pressed_index
        return self._OPTIONS[idx] if 0 <= idx < len(self._OPTIONS) else self._default

    def set_value(self, val: str) -> None:
        target = (
            self._OPTIONS.index(val)
            if val in self._OPTIONS
            else self._OPTIONS.index(self._default)
        )
        buttons = list(self.query(RadioButton))
        for i, button in enumerate(buttons):
            if i != target:
                button.value = False
        buttons[target].value = True


class BoolRadio(ChoiceRadio):
    """On / Off (stored as true / false), pre-selected to `default`."""

    _OPTIONS = ["true", "false"]
    _LABELS = ["On", "Off"]

    def __init__(self, default: bool = False, id: str | None = None) -> None:
        super().__init__("true" if default else "false", id=id)


class ModeRadio(ChoiceRadio):
    """backfill / latest — the window mode (mutually exclusive by nature)."""

    _OPTIONS = ["backfill", "latest"]

    def __init__(self, default: str = "backfill", id: str | None = None) -> None:
        super().__init__(default, id=id)


class StateRadio(ChoiceRadio):
    """discover / bulldozer — how prefill treats existing state rows (STATE_MODE)."""

    _OPTIONS = ["discover", "bulldozer"]

    def __init__(self, default: str = "discover", id: str | None = None) -> None:
        super().__init__(default, id=id)


class NameStyleRadio(ChoiceRadio):
    """Site-wide presentation of dotted model / upstream names:

    * `lengthen` — one line: `catalog.schema.table` (the default).
    * `thicken`  — stacked, one dotted segment per line (taller rows):
                       catalog.
                       schema.
                       table
    """

    _OPTIONS = ["lengthen", "thicken"]
    _LABELS = ["Lengthen", "Thicken"]

    def __init__(self, default: str = "lengthen", id: str | None = None) -> None:
        super().__init__(default, id=id)
