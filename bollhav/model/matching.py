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


# ── errors ──


class EmptyTagsError(ValueError):
    """`load_models` was called with no tag expression. Tags select which
    models to run, so a non-empty expression is required (use a catch-all
    like `"*"` to match everything)"""

    def __init__(self) -> None:
        super().__init__("tags must be a non-empty expression.")


class DuplicateModelError(ValueError):
    """Two model files declare the same `full_name` (catalog.schema.table).
    A model's full name must be unique across the scanned folder, since it
    keys the target, the state rows, and the dependency graph"""

    def __init__(self, full_name: str, file, existing) -> None:
        super().__init__(
            f"Duplicate model {full_name!r} found in {file} "
            f"(already defined in {existing})"
        )


def _model_matches(
    model: Model, potential_tag_groups: list[PotentialTagGroup]
) -> tuple[Model, bool] | None:
    """Return `(model, reload)` if the model matches one of the tag groups,
    else `None`. `reload` is true when the matched group carries a reload
    prefix. The model is NOT mutated — the reload decision is surfaced for
    `runtime` to fold into the resolved window."""
    if not model.enabled:
        logger.debug("Skipping model %r because it is disabled", model.target.full_name)
        return None
    for group in potential_tag_groups:
        if group_matches(model.tags, group):
            reload = any(tag.reload for tag in group.tags)
            return model, reload
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


def matched_with_reload(
    folder: str = "src/models",
    tags: str | None = None,
) -> list[tuple[Model, bool]]:
    """Like `match_models`, but pairs each matched model with its tag-driven
    `reload` flag (topologically sorted). `runtime` uses the flag to resolve
    each model's window; `match_models` is the public, reload-stripped view.

    Tag expression syntax:
        [foo]                   match if model has tag "foo"
        [foo & bar]             match if model has both "foo" and "bar"
        [foo | bar]             match if model has "foo" or "bar"
        [(foo | bar) & baz]     match if model has ("foo" or "bar") and "baz"
        [foo][bar]              match if model has "foo" or "bar" (separate groups)
        [r:foo]                 match "foo", reload=True
        [reload:foo]            same as [r:foo] — "reload" is a full-word alias
        [r:(foo | bar)]         match "foo" or "bar", reload=True
        r:[foo & bar]           match "foo" and "bar", reload=True for all

    Interval and chunk size are model config, not tag overrides. To change
    the interval expression at runtime, use INTERVAL_OVERRIDE (flexible models
    only — it's ignored on fixed-interval models).

    Raises:
        ValueError: If tags is not provided or the expression is invalid.
    """
    if not tags:
        raise EmptyTagsError()

    potential_tag_groups = parse_expression(tags)
    folder_path = Path(folder)
    logger.debug("Matching models in %r with tags %r", folder, tags)

    reload_by_name: dict[str, bool] = {}
    results: list[Model] = []
    total_models = 0
    seen: dict[str, Path] = {}
    with _with_sys_path(folder_path):
        for file in folder_path.rglob("*.py"):
            logger.debug("Scanning %s", file)
            module = _load_module(file)
            if module is None:
                continue

            for model in _models_from_module(module):
                full_name = model.target.full_name

                if full_name in seen:
                    raise DuplicateModelError(full_name, file, seen[full_name])

                seen[full_name] = file
                total_models += 1

                result = _model_matches(model, potential_tag_groups)
                if result:
                    matched_model, reload = result
                    results.append(matched_model)
                    reload_by_name[full_name] = reload
                    logger.debug(
                        "Matched model %r from %s (reload=%s)",
                        full_name,
                        file,
                        reload,
                    )

    if total_models == 0:
        logger.error("No models found in folder: %s", folder)
    elif not results:
        logger.error(
            "No models matched tags: %s (out of %d discovered)", tags, total_models
        )
    else:
        logger.debug("Found %d model(s) matching tags %r", len(results), tags)
    ordered = topological_sort(results)
    return [(m, reload_by_name[m.target.full_name]) for m in ordered]


def match_models(
    folder: str = "src/models",
    tags: str | None = None,
) -> list[Model]:
    """Scan a folder for Model instances and return those matching the tag
    expression, topologically sorted. See `matched_with_reload` for the tag
    syntax; this is the reload-stripped public view."""
    return [model for model, _ in matched_with_reload(folder=folder, tags=tags)]
