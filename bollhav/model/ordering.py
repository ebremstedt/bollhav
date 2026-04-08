from collections import deque
from bollhav.model.model import Model


def topological_sort(
    results: list[tuple[Model, bool]],
) -> list[tuple[Model, bool]]:
    by_name = {model.name: (model, reload) for model, reload in results}
    matched_names = set(by_name)

    for model, _ in results:
        missing = [dep for dep in model.upstream if dep not in matched_names]
        if missing:
            raise ValueError(
                f"Model {model.name!r} depends on unmatched upstream model(s): {missing}"
            )

    in_degree: dict[str, int] = {name: 0 for name in by_name}
    dependents: dict[str, list[str]] = {name: [] for name in by_name}

    for model, _ in results:
        for dep in model.upstream:
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
