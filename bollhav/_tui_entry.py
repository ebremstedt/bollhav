"""Console-script entrypoint for the bollhav TUI.

The `bollhav` / `bollhav-tui` scripts are installed on every install, but the
TUI itself needs `textual` (the optional `bollhav[tui]` extra). The app is
imported lazily here so a base install fails with a helpful hint rather than a
raw `ModuleNotFoundError` traceback."""

from __future__ import annotations


def main() -> None:
    try:
        from bollhav.tui.app import main as app_main
    except ModuleNotFoundError as exc:
        if exc.name == "textual":
            raise SystemExit(
                "The bollhav TUI requires the optional 'textual' dependency.\n"
                "Install it with:  pip install 'bollhav[tui]'"
            ) from None
        raise
    app_main()
