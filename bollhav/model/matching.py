import importlib.util
import inspect
import re
import sys
from pathlib import Path
from bollhav.model.model import Model


def _model_matches(model: Model, parsed: list[list[str | list[str]]]) -> bool:
    return _tags_match(model.tags, parsed)


def _tags_match(model_tags: set[str], parsed: list[list[str | list[str]]]) -> bool:
    return any(_group_matches(model_tags, group) for group in parsed)


def _group_matches(model_tags: set[str], group: list[str | list[str]]) -> bool:
    for term in group:
        if isinstance(term, list):
            if not any(t in model_tags for t in term):
                return False
        else:
            if term not in model_tags:
                return False
    return True


def _parse_tag_expression(expr: str) -> list[list[str | list[str]]]:
    groups = re.findall(r"\[([^\]]+)\]", expr)
    if not groups:
        raise ValueError(f"Invalid tag expression: {expr!r}. Must use [group] syntax.")

    parsed = []
    for group in groups:
        terms = group.split("&")
        parsed_terms = []
        for term in terms:
            or_match = re.fullmatch(r"\(([^)]+)\)", term.strip())
            if or_match:
                parsed_terms.append(or_match.group(1).split("|"))
            elif "|" in term:
                parsed_terms.append(term.split("|"))
            else:
                parsed_terms.append(term.strip())
        parsed.append(parsed_terms)

    return parsed


def match_models(
    folder: str = "src/models",
    tags: str | None = None,
) -> list[Model]:
    """
    Discover and return execute functions from Python modules in a folder recursively,
    filtered by a tag expression.

    This can match either a declared model or a declared list of models, or both!

    TAG EXPRESSION SYNTAX
    =====================

    Tags are matched against the `tags` attribute on each module's `model` object.
    The expression must be passed as a string using the syntax described below.

    GROUPS
    ------
    A group is wrapped in square brackets: [expression]
    Multiple groups are separated by commas: [group1],[group2]

    A model is included if it matches ANY group (outer OR).

        [wee],[xyz]         # match if model has "wee" OR "xyz"

    OR WITHIN A GROUP
    -----------------
    Use | to express OR within a group.
    A model matches the group if it has ANY of the tags.

        [wee|x]             # match if model has "wee" OR "x"

    AND WITHIN A GROUP
    ------------------
    Use & to express AND within a group.
    A model matches the group if it has ALL of the terms.

        [xyz&abc]           # match if model has "xyz" AND "abc"

    AND WITH OR SUB-EXPRESSION
    --------------------------
    Use parentheses to group OR terms within an AND expression.
    Only one level of parentheses is supported.

        [xyz&(c|e)]         # match if model has "xyz" AND ("c" OR "e")

    COMBINING
    ---------
    Groups can be combined freely with commas.

        [wee|x],[xyz&(c|e)] # match if:
                            #   model has "wee" OR "x"
                            #   OR model has "xyz" AND ("c" OR "e")

    ENVIRONMENT VARIABLE
    --------------------
    The expression is typically sourced from an environment variable:

        export TAGS="[wee|x],[xyz&(c|e)]"

    LIMITATIONS
    -----------
    - Square brackets are required around every group
    - Only one level of parentheses is supported
    - & and | cannot be mixed at the top level without parentheses

    Args:
        folder: Path to the folder containing model modules. Defaults to "src/models".
        tags: Tag filter expression string. Must be provided.

    Returns:
        List of callable execute functions from matched modules.

    Raises:
        ValueError: If tags is not provided or the expression is invalid.
    """
    if not tags:
        raise ValueError("tags must be a non-empty expression.")
    parsed = _parse_tag_expression(tags)
    folder_path = Path(folder)
    parent_dir = str(folder_path.parent)
    added_to_path = False
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
        added_to_path = True
    try:
        models = []
        for file in folder_path.rglob("*.py"):
            module_spec = importlib.util.spec_from_file_location(
                name=file.stem, location=file
            )
            if module_spec is None or module_spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec=module_spec)
            module_spec.loader.exec_module(module)
            for _, obj in inspect.getmembers(module):
                if isinstance(obj, Model) and _model_matches(obj, parsed):
                    models.append(obj)
                elif isinstance(obj, list) and all(isinstance(m, Model) for m in obj):
                    models.extend(m for m in obj if _model_matches(m, parsed))
        return models
    finally:
        if added_to_path:
            sys.path.remove(parent_dir)
