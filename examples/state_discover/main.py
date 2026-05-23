"""Entry point for the state_discover example.

Demonstrates the @state_tracker decorator with @load_models, including
the DISCOVER + STATE_MODE matrix.

Example-only env vars:
  FAIL_ON_DAY=N    raise on the Nth interval (1-indexed) to simulate a
                   partial pipeline
"""

import logging
import os
import sys

from bollhav.model import Model, load_models, state_tracker


def setup_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _fail_on_day() -> int | None:
    raw = os.environ.get("FAIL_ON_DAY")
    if raw is None or raw == "":
        return None
    return int(raw)


@state_tracker
def execute(model, since, until):
    """Mock execute. Prints what it would do and (optionally) fails on
    a chosen day to simulate a partial pipeline."""
    fail_day = _fail_on_day()
    day = since.day
    print(f"  exec  {model.target.full_name}  {since.date()} -> {until.date()}")
    if fail_day is not None and day == fail_day:
        raise RuntimeError(f"simulated failure on 2024-01-{day:02d}")


@load_models
def main(models: list[Model], debug: bool) -> None:
    setup_logging(debug=debug)

    for model in models:
        print(f"\n{model.target.full_name}  ({len(model.intervals)} intervals)")
        for interval in model.intervals:
            execute(model=model, since=interval.since, until=interval.until)
    print()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    try:
        main()
    except RuntimeError as exc:
        # Surface the simulated failure but don't print a stack trace —
        # the point of the demo is the state-table side effects, which
        # have already been recorded by @state_tracker before we got here.
        print(f"\npipeline crashed: {exc}\n", file=sys.stderr)
        sys.exit(1)
