"""Console-script entrypoint for the bollhav GUI.

The `bollhav-gui` script is installed on every install, but the GUI needs
fastapi and uvicorn (the optional `bollhav[gui]` extra). They are imported
lazily here so a base install fails with a helpful hint rather than a raw
`ModuleNotFoundError` traceback."""

from __future__ import annotations


def main() -> None:
    try:
        from bollhav.gui.__main__ import main as gui_main
    except ModuleNotFoundError as exc:
        if exc.name in ("fastapi", "uvicorn", "pydantic"):
            raise SystemExit(
                "The bollhav GUI requires the optional 'fastapi' and 'uvicorn' "
                "dependencies.\nInstall them with:  pip install 'bollhav[gui]'"
            ) from None
        raise
    gui_main()
