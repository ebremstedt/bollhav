"""Mock source — returns 1000 fake events as a polars DataFrame.

In a real pipeline this would be a SELECT against the upstream system
(Kafka offset replay, log table, CDC stream, etc.). For the demo we
generate them inline so the example needs no external services."""

import polars as pl


def read_all(model) -> pl.DataFrame:
    n = 1000
    return pl.DataFrame(
        {
            "event_id": list(range(1, n + 1)),
            "event_type": [["click", "view", "purchase"][i % 3] for i in range(n)],
            "payload": [f"payload-{i}" for i in range(1, n + 1)],
        }
    )
