"""The GUI as one app, for `bollhav-gui`.

`bollhav.gui.app` answers every JSON route (/graph, /config, /state/..., ...).
`create_app` wraps it with what a deployment needs: the env-var aliases, a
DB-free health probe and the built Svelte frontend, served from `static/`
inside this package when the bundle was built (absent in dev, where Vite
serves the UI and proxies the API here). The SPA route is added last, so the
API's routes always win."""

import os
from collections.abc import MutableMapping
from pathlib import Path

from fastapi import FastAPI, HTTPException  # pyright: ignore[reportMissingImports]  # optional gui extra
from fastapi.responses import FileResponse  # pyright: ignore[reportMissingImports]  # optional gui extra
from fastapi.staticfiles import StaticFiles  # pyright: ignore[reportMissingImports]  # optional gui extra

STATIC_DIR = Path(__file__).with_name("static")


def apply_dsn_aliases(environ: MutableMapping[str, str]) -> None:
    """A deployment may name a catalog's database EXPLORER_DSN_<CATALOG>, or
    EXPLORER_DSN for a single one; the app reads BOLLHAV_STATE_DSN_<CATALOG>
    and BOLLHAV_STATE_DSN. Set the latter from the former where unset."""
    for name, value in list(environ.items()):
        if not value:
            continue
        if name.startswith("EXPLORER_DSN_"):
            environ.setdefault("BOLLHAV_STATE_DSN_" + name[len("EXPLORER_DSN_") :], value)
        elif name == "EXPLORER_DSN":
            environ.setdefault("BOLLHAV_STATE_DSN", value)


def mount_static(app: FastAPI, static_dir: Path) -> bool:
    """Serve the built frontend from `static_dir`: hashed assets and public/
    files as files, anything else as the SPA's index.html. False when there
    is no bundle (dev)."""
    if not static_dir.is_dir():
        return False
    root = static_dir.resolve()
    if (root / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=root / "assets"), name="assets")

    @app.get("/{path:path}", include_in_schema=False)
    def spa(path: str):
        file = (root / path).resolve()
        if path and file.is_file():
            if not file.is_relative_to(root):
                raise HTTPException(status_code=404, detail="Not Found")
            return FileResponse(file)
        return FileResponse(root / "index.html")

    return True


def create_app() -> FastAPI:
    """The app `bollhav-gui` serves (a uvicorn factory). A fresh app each
    call, wrapping the API's routes, so the API module's own app is never
    mutated and the factory can be called more than once in a process."""
    apply_dsn_aliases(os.environ)
    from bollhav.gui.app import app as api, lifespan  # reads the env when imported

    app = FastAPI(title=api.title, lifespan=lifespan)
    app.include_router(api.router)

    @app.get("/healthz")
    def healthz():
        # liveness / readiness without touching Postgres, so a slow or
        # unreachable state DB does not take the pod down
        return {"status": "ok"}

    mount_static(app, Path(os.environ.get("STATIC_DIR") or STATIC_DIR))
    return app
