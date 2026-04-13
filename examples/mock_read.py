import random
import time
from datetime import datetime
import polars as pl
from bollhav.model import Model

_SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    "products": {"id": pl.Int64, "name": pl.String, "price": pl.Float64},
    "customers": {"id": pl.Int64, "name": pl.String, "email": pl.String},
    "orders": {"id": pl.Int64, "customer_id": pl.Int64, "total": pl.Float64},
}

_DATA: dict[str, list[dict]] = {
    "products": [
        {"id": 1, "name": "Widget A", "price": 9.99},
        {"id": 2, "name": "Widget B", "price": 14.99},
        {"id": 3, "name": "Widget C", "price": 4.49},
    ],
    "customers": [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ],
    "orders": [
        {"id": 101, "customer_id": 1, "total": 24.98},
        {"id": 102, "customer_id": 2, "total": 4.49},
        {"id": 103, "customer_id": 1, "total": 14.99},
    ],
}


def read(model: Model, since: datetime, until: datetime) -> pl.DataFrame:  # noqa: ARG001
    time.sleep(model.extra.get("batch_sleep") or random.uniform(0.2, 0.35))
    source_name = model.source.name if model.source else model.target.name
    rows = _DATA.get(source_name, [])
    schema = _SCHEMAS.get(source_name, {})
    return pl.DataFrame(rows, schema=schema)
