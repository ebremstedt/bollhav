"""Entry point.

`@load_models` discovers models in `src/models/` and runs the state
bootstrap. The view-model auto-registers in the library so the
downstream `high_value_sums` sees it as a satisfied upstream — no
`STATE_001: not registered` block, no `STATE_002: no applied row`
block, just `pending` intervals from the start.

The loop iterates models in topological order (no upstream → first):
  1. `warehouse.orders`            — 3 daily intervals, state+staging
  2. `warehouse.v_high_value_orders` — single CREATE OR REPLACE VIEW
  3. `warehouse.high_value_sums`   — 3 daily intervals, state+staging

For the view, `model.intervals` is `[None]` — the loop runs it once
with `since=until=None`.
"""

import logging
import os

from bollhav.model import Model, load_models
from execute import execute


def setup_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@load_models
def main(models: list[Model], debug: bool) -> None:
    setup_logging(debug=debug)

    for model in models:
        intervals = model.intervals
        print(f"\n{model.target.full_name}  {len(intervals)} step(s) to process")
        for interval in intervals:
            since, until = (
                (interval.since, interval.until) if interval else (None, None)
            )
            label = (
                f"{since.date()} → {until.date()}"
                if interval
                else "(single shot — view)"
            )
            print(f"  {label}", flush=True)
            execute(model=model, since=since, until=until)
        print()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
