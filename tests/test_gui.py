"""The GUI package: what runs without a database. The API routes themselves
need a state DB and are exercised by the compose demo, not here.

The fastapi-dependent tests skip when the `gui` extra is not installed; the
entry-point hint runs in a subprocess so it can fake the extra's absence
whatever this venv has."""

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_bollhav_gui_package_imports_without_the_extra():
    # The package itself is import-safe on a base install; only the app is not.
    script = (
        "import sys\n"
        "sys.modules['fastapi'] = None\n"
        "sys.modules['uvicorn'] = None\n"
        "import bollhav.gui\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=REPO_ROOT, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_bollhav_gui_entry_point_hints_at_the_extra_when_missing():
    script = (
        "import sys\n"
        "sys.modules['fastapi'] = None\n"
        "sys.modules['uvicorn'] = None\n"
        "from bollhav._gui_entry import main\n"
        "main()\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=REPO_ROOT, capture_output=True, text=True
    )
    assert result.returncode == 1
    assert "pip install 'bollhav[gui]'" in result.stderr
    assert "Traceback" not in result.stderr


def test_dsn_aliases_fill_the_bollhav_names_without_overriding_them():
    pytest.importorskip("fastapi")
    from bollhav.gui.serve import apply_dsn_aliases

    env = {
        "EXPLORER_DSN_VARD": "postgresql://vard",
        "EXPLORER_DSN_EKONOMI": "postgresql://ekonomi-alias",
        "BOLLHAV_STATE_DSN_EKONOMI": "postgresql://ekonomi",
        "EXPLORER_DSN": "postgresql://single",
        "EXPLORER_DSN_EMPTY": "",
    }
    apply_dsn_aliases(env)
    assert env["BOLLHAV_STATE_DSN_VARD"] == "postgresql://vard"
    assert env["BOLLHAV_STATE_DSN_EKONOMI"] == "postgresql://ekonomi"  # kept
    assert env["BOLLHAV_STATE_DSN"] == "postgresql://single"
    assert "BOLLHAV_STATE_DSN_EMPTY" not in env


def test_catalogs_come_from_the_named_dsns_or_fall_back_to_default(monkeypatch):
    pytest.importorskip("fastapi")
    monkeypatch.setenv("LINEAGE_REFRESH_SECONDS", "0")  # no refresher thread
    from bollhav.gui import app as gui_app

    for name in list(gui_app.os.environ):
        if name.startswith("BOLLHAV_STATE_DSN"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("BOLLHAV_STATE_DSN_VARD", "postgresql://vard")
    monkeypatch.setenv("BOLLHAV_STATE_DSN_EKONOMI", "postgresql://ekonomi")
    assert gui_app._catalogs() == {
        "ekonomi": "postgresql://ekonomi",
        "vard": "postgresql://vard",
    }

    monkeypatch.delenv("BOLLHAV_STATE_DSN_VARD")
    monkeypatch.delenv("BOLLHAV_STATE_DSN_EKONOMI")
    monkeypatch.setenv("BOLLHAV_STATE_DSN", "postgresql://only")
    assert gui_app._catalogs() == {"default": "postgresql://only"}


def test_create_app_serves_health_and_the_spa(monkeypatch, tmp_path):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")  # fastapi's TestClient
    from fastapi.testclient import TestClient

    static = tmp_path / "static"
    (static / "assets").mkdir(parents=True)
    (static / "index.html").write_text("<html>spa</html>")
    (static / "assets" / "app.js").write_text("console.log(1)")
    (static / "favicon.png").write_bytes(b"png")
    monkeypatch.setenv("STATIC_DIR", str(static))
    monkeypatch.setenv("LINEAGE_REFRESH_SECONDS", "0")

    from bollhav.gui.serve import create_app

    client = TestClient(create_app())  # no `with`: the lifespan is not started
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/assets/app.js").text == "console.log(1)"
    assert client.get("/favicon.png").content == b"png"
    assert client.get("/some/client/route").text == "<html>spa</html>"
    # the API's own routes win over the SPA catch-all
    assert "title" in client.get("/config").json()


def test_create_app_without_a_bundle_is_api_only(monkeypatch, tmp_path):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    monkeypatch.setenv("STATIC_DIR", str(tmp_path / "nowhere"))
    monkeypatch.setenv("LINEAGE_REFRESH_SECONDS", "0")
    from bollhav.gui.serve import create_app

    client = TestClient(create_app())
    assert client.get("/healthz").status_code == 200
    assert client.get("/some/client/route").status_code == 404
