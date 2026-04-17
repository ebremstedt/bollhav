"""A deliberately flaky mock read.

The first N calls for a given chunk raise. After that the read succeeds.
This demonstrates a retry loop driven by `model.batching.retries` — see
execute.py.

We index failures by the chunk's `since` timestamp so different chunks
fail independently, and the counter survives across retries within a
single chunk.
"""

from datetime import datetime
from bollhav.model import Model


_failures_served: dict[datetime, int] = {}
_FAILURES_PER_CHUNK = 2


def read(model: Model, since: datetime, until: datetime) -> str:  # noqa: ARG001
    served = _failures_served.get(since, 0)
    if served < _FAILURES_PER_CHUNK:
        _failures_served[since] = served + 1
        raise ConnectionError(
            f"upstream hiccup (mock) — failure {served + 1}/{_FAILURES_PER_CHUNK} "
            f"for chunk {since:%H:%M} → {until:%H:%M}"
        )
    return f"[{len(_failures_served)} rows for {since:%H:%M}→{until:%H:%M}]"
