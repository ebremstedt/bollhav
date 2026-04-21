"""Per-chunk handler. Sleeps a fixed amount per chunk so the BATCH-level
spinner has time to spin (otherwise everything finishes too fast to see).
The @progress_bar decorator counts each call and renders the spinner
accordingly."""

import time
from datetime import datetime
from bollhav.model import Model, progress_bar

_PER_CHUNK_DELAY_SECONDS = 0.15


@progress_bar
def execute(model: Model, since: datetime, until: datetime) -> None:
    time.sleep(_PER_CHUNK_DELAY_SECONDS)
