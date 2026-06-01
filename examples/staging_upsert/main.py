"""Entry point — drives the orders model through its pending intervals.

`@load_models` discovers `src/models/`, runs the state bootstrap, and
hands back models whose `intervals` only contain rows that still need
work. The body of this main just iterates them.

Verification queries are in README.md.
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
        print(f"\n{model.target.full_name}  {len(intervals)} interval(s) to process")
        for interval in intervals:
            print(
                f"  {interval.since.date()} → {interval.until.date()}",
                flush=True,
            )
            execute(model=model, since=interval.since, until=interval.until)
        print()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
