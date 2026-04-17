"""The execute function reads `model.batching.retries` and implements the
retry loop itself.

bollhav stores retry *intent* on the model but doesn't perform retries —
that is deliberately left to the user's execute function, since the
right retry strategy (immediate vs backoff, which exceptions to catch,
how to log) is pipeline-specific.
"""

from datetime import datetime
from bollhav.model import Model
from mock_read import read
from mock_write import write


def execute(model: Model, since: datetime, until: datetime) -> None:
    max_attempts = (model.batching.retries or 0) + 1

    for attempt in range(1, max_attempts + 1):
        try:
            payload = read(model=model, since=since, until=until)
            write(model=model, payload=payload, since=since, until=until)
            return
        except Exception as e:
            if attempt == max_attempts:
                print(f"    giving up after {attempt} attempts: {e}")
                raise
            print(f"    attempt {attempt}/{max_attempts} failed: {e} — retrying")
