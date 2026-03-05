import importlib
from pathlib import Path
import sys
import types
from typing import Callable


def _module_matches(
    module: types.ModuleType,
    tags: list[list[str]],
) -> bool:
    model = getattr(module, "model", None)
    if model is None:
        return False
    module_tags: list[str] = getattr(model, "tags", [])
    return any(all(tag in module_tags for tag in tag_group) for tag_group in tags)


def match_execute_functions(
    folder: str = "src/models",
    tags: list[list[str]] | None = None,
) -> list[Callable]:
    if tags is None or len(tags) == 0:
        raise ValueError("tags must be a non-empty jagged array.")

    print(f"Filtering execute functions to those with tags: {tags}")

    folder_path = Path(folder)
    parent_dir = str(folder_path.parent)
    added_to_path = False
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
        added_to_path = True

    try:
        execute_functions = []
        for file in folder_path.rglob("*.py"):
            module_spec = importlib.util.spec_from_file_location(
                name=file.stem, location=file
            )
            module = importlib.util.module_from_spec(spec=module_spec)
            module_spec.loader.exec_module(module)
            if _module_matches(module, tags):
                execute_functions.append(module.execute)
        return execute_functions
    finally:
        if added_to_path:
            sys.path.remove(parent_dir)
