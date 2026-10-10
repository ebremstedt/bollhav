"""`bollhav-gui` / `python -m bollhav.gui`: serve the GUI with uvicorn.

BOLLHAV_GUI_HOST (default 0.0.0.0) and BOLLHAV_GUI_PORT (default 8137) say
where. The app's own settings are the BOLLHAV_STATE_DSN[_<CATALOG>] and
LINEAGE_* variables, see `bollhav.gui.app`."""

import logging
import os

import uvicorn  # pyright: ignore[reportMissingImports]  # optional gui extra


def main() -> None:
    # uvicorn configures only its own loggers; the app's precompute log lines
    # are worth having in the pod log too.
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    uvicorn.run(
        "bollhav.gui.serve:create_app",
        factory=True,
        host=os.environ.get("BOLLHAV_GUI_HOST", "0.0.0.0"),
        port=int(os.environ.get("BOLLHAV_GUI_PORT", "8137")),
    )


if __name__ == "__main__":
    main()
