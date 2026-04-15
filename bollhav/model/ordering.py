from __future__ import annotations

from collections import deque
from enum import Enum
from typing import TYPE_CHECKING

from bollhav.model.write_modes import WriteMode

if TYPE_CHECKING:
    from bollhav.model.model import Model


class UpstreamMode(Enum):
    ENFORCE = "enforce"
    IGNORE_VIEWS = "ignore_views"
    IGNORE_COMPLETELY = "ignore_completely"


def _is_view(model: Model) -> bool:
    return getattr(model.target, "write_mode", None) == WriteMode.VIEW


def topological_sort(
    results: list[Model],
    upstream_mode: UpstreamMode = UpstreamMode.ENFORCE,
) -> list[Model]:
    if upstream_mode == UpstreamMode.IGNORE_COMPLETELY:
        return results

    by_name = {model.target.full_name: model for model in results}
    matched_names = set(by_name)

    def _upstream(model: Model) -> list[str]:
        if upstream_mode == UpstreamMode.IGNORE_VIEWS and _is_view(model):
            return []
        return getattr(model, "upstream", []) or []

    for model in results:
        missing = [dep for dep in _upstream(model) if dep not in matched_names]
        if missing:
            raise ValueError(
                f"Model {model.target.full_name!r} depends on unmatched upstream model(s): {missing}"
            )

    in_degree: dict[str, int] = {name: 0 for name in by_name}
    dependents: dict[str, list[str]] = {name: [] for name in by_name}

    for model in results:
        for dep in _upstream(model):
            in_degree[model.target.full_name] += 1
            dependents[dep].append(model.target.full_name)

    queue = deque(sorted(name for name, deg in in_degree.items() if deg == 0))
    ordered: list[Model] = []

    while queue:
        name = queue.popleft()
        ordered.append(by_name[name])
        for dependent in sorted(dependents[name]):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(ordered) != len(results):
        remaining = matched_names - {m.target.full_name for m in ordered}
        raise ValueError(f"Circular dependency detected among: {remaining}")

    return ordered
