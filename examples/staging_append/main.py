"""Entry point — the loop only sees intervals that still need to run.

For models with `target.staging` set, `@load_models` does the state
bootstrap automatically:
  1. Compute the contract (intervals the model says should exist).
  2. Ensure state tables + pre-fill (`STATE_MODE` controls
     respect/disrespect on conflict).
  3. Read `status='pending'` rows back from state.
  4. Stash them on `model.intervals`.

Result: this main only iterates the pending intervals. On a second
run with everything already applied, `model.intervals` is empty and
the loop exits without doing any work.

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
            # @state takes a per-interval advisory lock around
            # each call — concurrent workers on the same model see the
            # lock and skip the held interval, picking up the next one.
            execute(model=model, since=interval.since, until=interval.until)
        print()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
