"""Entry point for the latest_vs_backfill example.

This example does not read or write data. Its purpose is to show how the
runtime mode (latest vs backfill) changes which intervals get resolved for
the same model.

For each matched model we:
  1. `@load_models` reads env vars and bakes runtime overrides into each
     model's batching/bounds/target.schema and directives.
  2. Read `model.intervals` — this is where the mode matters.
  3. Print the first/last chunk and total count.

Run the same folder twice — once with LATEST_ENABLED=true, once with
BACKFILL_ENABLED=true — and compare the output. See README.md.
"""

import os

from bollhav.model import Model, load_models


@load_models
def main(models: list[Model], debug: bool) -> None:
    for model in models:
        intervals = model.intervals

        print(f"\n{model.target.full_name}")
        print(f"  interval_expression : {model.batching.interval.expression}")
        print(f"  bounds              : {model.bounds.begin} → {model.bounds.end}")
        print(f"  chunks returned     : {len(intervals)}")
        if intervals and intervals[0] is not None:
            first, last = intervals[0], intervals[-1]
            print(f"  first chunk         : {first.since} → {first.until}")
            print(f"  last chunk          : {last.since} → {last.until}")
    print()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
