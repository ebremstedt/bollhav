import importlib.util
import inspect
import logging
import sys
from contextlib import contextmanager
from pathlib import Path
from bollhav.model.tagexpr import PotentialTagGroup, parse_expression, group_matches
from bollhav.model.model import Model
from bollhav.model.ordering import topological_sort, UpstreamMode

logger = logging.getLogger(__name__)


def _model_matches(
    model: Model, potential_tag_groups: list[PotentialTagGroup]
) -> Model | None:
    if not model.enabled:
        logger.debug("Skipping model %r because it is disabled", model.target.full_name)
        return None
    for group in potential_tag_groups:
        if group_matches(model.tags, group):
            model.runtime_override.reload = any(tag.reload for tag in group.tags)
            model.runtime_override.reload_mode = next(
                (tag.reload_mode for tag in group.tags if tag.reload_mode is not None),
                None,
            )
            model.runtime_override.reload_batch_size = next(
                (
                    tag.reload_batch_size
                    for tag in group.tags
                    if tag.reload_batch_size is not None
                ),
                None,
            )
            model.runtime_override.reload_interval_expression = next(
                (
                    tag.reload_interval_expression
                    for tag in group.tags
                    if tag.reload_interval_expression is not None
                ),
                None,
            )
            model._validate_runtime_reload()
            return model
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
    upstream_mode: UpstreamMode = UpstreamMode.ENFORCE,
) -> list[Model]:
    """
    Scan a folder recursively for Python modules, discover all Model instances,
    and return those whose tags match the given tag expression.

    Runtime state (e.g. reload) is set on model.runtime_override during matching.

    Usage:
        for model in match_models(folder="src/models", tags="[r:sales & finance]"):
            ...

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
        [r_row_100:foo]         match "foo", reload=True, ROW mode, batch_size=100
        r_row_500:[foo & bar]   match "foo" and "bar", reload with ROW/500 for all
        [r_interval_@daily:foo] reload "foo" in INTERVAL mode, interval_expression="@daily"
        reload_interval_@hourly:[foo]   group-level, same but hourly for all

    Allowed cron aliases for r_interval_ are sourced from roskarl's
    INTERVAL_EXPRESSION_SHORTCUTS: @minutely/@minute, @hourly/@hour,
    @daily/@day, @weekly/@week, @monthly/@month. For custom cron
    expressions, configure the model statically or use
    BATCH_EXPRESSION_OVERRIDE — arbitrary cron strings are not accepted
    inside tags.

    Raises:
        ValueError: If tags is not provided or the expression is invalid.
    """
    if not tags:
        raise ValueError("tags must be a non-empty expression.")

    potential_tag_groups = parse_expression(tags)
    folder_path = Path(folder)
    logger.debug("Matching models in %r with tags %r", folder, tags)

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
                    raise ValueError(
                        f"Duplicate model {full_name!r} found in {file} "
                        f"(already defined in {seen[full_name]})"
                    )
                seen[full_name] = file
                total_models += 1
                result = _model_matches(model, potential_tag_groups)
                if result:
                    results.append(result)
                    logger.debug(
                        "Matched model %r from %s (reload=%s)",
                        full_name,
                        file,
                        model.runtime_override.reload,
                    )

    if total_models == 0:
        logger.error("No models found in folder: %s", folder)
    elif not results:
        logger.error(
            "No models matched tags: %s (out of %d discovered)", tags, total_models
        )
    else:
        logger.debug("Found %d model(s) matching tags %r", len(results), tags)
    return topological_sort(results, upstream_mode=upstream_mode)
