from collections import deque
from enum import Enum
from bollhav.model.model import Model
from bollhav.model.write_modes import WriteMode


class UpstreamMode(Enum):
    ENFORCE = "enforce"
    IGNORE_VIEWS = "ignore_views"
    IGNORE_COMPLETELY = "ignore_completely"


def _is_view(model: Model) -> bool:
    return getattr(model.target, "write_mode", None) == WriteMode.VIEW


def topological_sort(
    results: list[tuple[Model, bool]],
    upstream_mode: UpstreamMode = UpstreamMode.ENFORCE,
) -> list[tuple[Model, bool]]:
    if upstream_mode == UpstreamMode.IGNORE_COMPLETELY:
        return results

    by_name = {model.name: (model, reload) for model, reload in results}
    matched_names = set(by_name)

    def _upstream(model: Model) -> list[str]:
        if upstream_mode == UpstreamMode.IGNORE_VIEWS and _is_view(model):
            return []
        return getattr(model, "upstream", []) or []

    for model, _ in results:
        missing = [dep for dep in _upstream(model) if dep not in matched_names]
        if missing:
            raise ValueError(
                f"Model {model.name!r} depends on unmatched upstream model(s): {missing}"
            )

    in_degree: dict[str, int] = {name: 0 for name in by_name}
    dependents: dict[str, list[str]] = {name: [] for name in by_name}

    for model, _ in results:
        for dep in _upstream(model):
            in_degree[model.name] += 1
            dependents[dep].append(model.name)

    queue = deque(sorted(name for name, deg in in_degree.items() if deg == 0))
    ordered: list[tuple[Model, bool]] = []

    while queue:
        name = queue.popleft()
        ordered.append(by_name[name])
        for dependent in sorted(dependents[name]):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(ordered) != len(results):
        remaining = matched_names - {m.name for m, _ in ordered}
        raise ValueError(f"Circular dependency detected among: {remaining}")

    return ordered
