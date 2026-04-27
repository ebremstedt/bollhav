"""Entry point for the latest_window_vs_batch example.

Shows the interval plan for a model that uses a `window_expression` (outer
scope) together with an `interval_expression` (inner chunk size) — a common
setup for large tables that need a daily catch-up but cannot load a whole
day in a single write.

Prints the resolved chunk count plus the first and last three chunks so
you can see the 15-minute slicing of a full day.
"""

import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from bollhav.model import Model, load_models


@load_models
def main(models: list[Model], debug: bool) -> None:
    for model in models:
        intervals = model.infer_intervals()

        print(f"\n{model.target.full_name}")
        print(f"  window_expression   : {model.batching.interval.window_expression}")
        print(f"  interval_expression : {model.batching.interval.expression}")
        print(f"  chunks returned     : {len(intervals)}")

        if not intervals or intervals[0] is None:
            print()
            continue

        print("  first three chunks:")
        for iv in intervals[:3]:
            print(f"    {iv.since} → {iv.until}")
        print("  last three chunks:")
        for iv in intervals[-3:]:
            print(f"    {iv.since} → {iv.until}")
    print()


if __name__ == "__main__":
    main()
