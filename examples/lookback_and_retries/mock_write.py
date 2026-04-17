from datetime import datetime
from bollhav.model import Model


def write(model: Model, payload: str, since: datetime, until: datetime) -> None:
    print(f"    wrote {payload} to {model.target.full_name}")
