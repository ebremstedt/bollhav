"""Pure, Textual-free logic: finding models, remembering folders, locating the
runner. Everything here is plain functions so it can be tested without a UI.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from bollhav.model.matching import (
    _load_module,
    _models_from_module,
    _with_sys_path,
)

# Remembered project locations.
WORKSPACES_PATH = Path.home() / ".config" / "bollhav-tui" / "workspaces.json"


def discover_models(folder: Path) -> tuple[list, list[str]]:
    """Import every `.py` file recursively below `folder` independently and
    collect all `Model` instances. Resilient: a file that fails to import is
    skipped (and reported) rather than aborting the whole scan.

    Returns (models, skipped) where `skipped` is a list of "file — reason".
    """
    models: list = []
    seen: set = set()
    skipped: list[str] = []
    for file in sorted(folder.rglob("*.py")):
        if "__pycache__" in file.parts:
            continue
        try:
            # add the file's own dir to sys.path so sibling imports resolve
            with _with_sys_path(file):
                module = _load_module(file)
            if module is None:
                continue
            for model in _models_from_module(module):
                fid = model.target.full_name
                if fid not in seen:
                    seen.add(fid)
                    models.append(model)
        except Exception as exc:  # noqa: BLE001 — importing arbitrary user code
            try:
                rel = file.relative_to(folder)
            except ValueError:
                rel = file
            skipped.append(f"{rel} — {type(exc).__name__}: {exc}")
    return models, skipped


def nearest_runner(folder: Path) -> Path | None:
    """The directory of the nearest `main.py` at `folder` or an ancestor."""
    return next(
        (d for d in [folder, *folder.parents] if (d / "main.py").exists()), None
    )


def load_workspaces() -> list[Path]:
    try:
        data = json.loads(WORKSPACES_PATH.read_text())
        return [Path(p).expanduser() for p in data]
    except (OSError, ValueError):
        return []


def save_workspaces(projects: list[Path]) -> None:
    try:
        WORKSPACES_PATH.parent.mkdir(parents=True, exist_ok=True)
        WORKSPACES_PATH.write_text(json.dumps([str(p) for p in projects], indent=2))
    except OSError:
        pass


def resolve_folders(argv: list[str]) -> tuple[list[Path], int]:
    """Merge CLI folders + $BOLLHAV_PROJECT + remembered folders into an
    ordered, de-duplicated list of existing directories. Returns (folders, start).
    CLI folders are remembered; the first CLI folder becomes the active one.
    With no args and nothing remembered, falls back to the current directory."""
    cli = [Path(a).expanduser().resolve() for a in argv[1:]]
    env_raw = os.environ.get("BOLLHAV_PROJECT")
    if env_raw:
        cli.append(Path(env_raw).expanduser().resolve())

    # remembered first, then any newly-passed ones appended (order-preserving dedup)
    merged: list[Path] = []
    for p in [w.resolve() for w in load_workspaces()] + cli:
        if p not in merged:
            merged.append(p)

    valid = [p for p in merged if p.is_dir()]
    for p in (p for p in merged if p not in valid):
        print(f"skipping {p} — not a folder", file=sys.stderr)

    if not valid:
        valid = [Path.cwd()]

    save_workspaces(valid)

    # start on the first CLI-provided folder if it exists, else 0
    start = 0
    for p in cli:
        if p in valid:
            start = valid.index(p)
            break
    return valid, start
