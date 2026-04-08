import importlib.util
import inspect
import logging
import sys
from contextlib import contextmanager
from pathlib import Path
from bollhav.model.tagexpr import PotentialTagGroup, parse_expression, group_matches
from bollhav.model.model import Model
from bollhav.model.ordering import topological_sort

logger = logging.getLogger(__name__)


def _model_matches(
    model: Model, potential_tag_groups: list[PotentialTagGroup]
) -> tuple[Model, bool] | None:
    for group in potential_tag_groups:
        if group_matches(model.tags, group):
            return model, any(tag.reload for tag in group.tags)
    return None


def _load_module(file: Path):
    spec = importlib.util.spec_from_file_location(name=file.stem, location=file)
    if spec is None or spec.loader is None:
        logger.debug("Could not load module spec for %s, skipping", file)
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _models_from_module(module) -> list[Model]:
    models = []
    for _, obj in inspect.getmembers(module):
        if isinstance(obj, Model):
            models.append(obj)
        elif isinstance(obj, list) and all(isinstance(m, Model) for m in obj):
            models.extend(obj)
    return models


@contextmanager
def _with_sys_path(folder_path: Path):
    parent_dir = str(folder_path.parent)
    added = parent_dir not in sys.path
    if added:
        sys.path.insert(0, parent_dir)
    try:
        yield
    finally:
        if added:
            sys.path.remove(parent_dir)


def match_models(
    folder: str = "src/models",
    tags: str | None = None,
) -> list[tuple[Model, bool]]:
    """
    Scan a folder recursively for Python modules, discover all Model instances,
    and return those whose tags match the given tag expression.

    Each result is a (model, reload) tuple. reload is True if the matched expression included r:.

    Usage:
        for model, reload in match_models(folder="src/models", tags="[r:sales & finance]"):
            ...

    Tag expression syntax:
        [foo]                   match if model has tag "foo"
        [foo & bar]             match if model has both "foo" and "bar"
        [foo | bar]             match if model has "foo" or "bar"
        [(foo | bar) & baz]     match if model has ("foo" or "bar") and "baz"
        [foo][bar]              match if model has "foo" or "bar" (separate groups)
        [r:foo]                 match "foo", reload=True
        [r:(foo | bar)]         match "foo" or "bar", reload=True
        r:[foo & bar]           match "foo" and "bar", reload=True for all

    Raises:
        ValueError: If tags is not provided or the expression is invalid.
    """
    if not tags:
        raise ValueError("tags must be a non-empty expression.")

    potential_tag_groups = parse_expression(tags)
    folder_path = Path(folder)
    logger.debug("Matching models in %r with tags %r", folder, tags)

    results: list[tuple[Model, bool]] = []
    with _with_sys_path(folder_path):
        for file in folder_path.rglob("*.py"):
            logger.debug("Scanning %s", file)
            module = _load_module(file)
            if module is None:
                continue
            for model in _models_from_module(module):
                result = _model_matches(model, potential_tag_groups)
                if result:
                    results.append(result)
                    logger.debug(
                        "Matched model %r from %s (reload=%s)",
                        model.name,
                        file,
                        result[1],
                    )

    logger.debug("Found %d model(s) matching tags %r", len(results), tags)
    return topological_sort(results)
