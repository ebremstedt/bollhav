import importlib
import re
from pathlib import Path
import sys
import types
from typing import Callable


def _parse_tag_expression(expr: str) -> list[list[str]]:
    groups = re.findall(r"\[([^\]]+)\]", expr)
    if not groups:
        raise ValueError(
            f"Invalid tag expression: {expr}. Groups must be wrapped in []."
        )

    result: list[list[str]] = []
    for group in groups:
        if "&" in group:
            and_terms = group.split("&")
            or_sets: list[list[str]] = []
            for term in and_terms:
                term = term.strip()
                paren_match = re.match(r"^\(([^)]+)\)$", term)
                if paren_match:
                    or_sets.append([t.strip() for t in paren_match.group(1).split("|")])
                else:
                    or_sets.append([term])
            result.append(or_sets)
        else:
            result.append([[t.strip() for t in group.split("|")]])

    return result


def _module_matches(
    module: types.ModuleType,
    parsed: list,
) -> bool:
    model = getattr(module, "model", None)
    if model is None:
        return False
    module_tags: set[str] = getattr(model, "tags", set())
    for group in parsed:
        if all(any(tag in module_tags for tag in or_set) for or_set in group):
            return True
    return False


def match_execute_functions(
    folder: str = "src/models",
    tags: str | None = None,
) -> list[Callable]:
    """
    Discover and return execute functions from Python modules in a folder recursively,
    filtered by a tag expression.

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
            if _module_matches(module, parsed):
                execute_functions.append(module.execute)
        return execute_functions
    finally:
        if added_to_path:
            sys.path.remove(parent_dir)
