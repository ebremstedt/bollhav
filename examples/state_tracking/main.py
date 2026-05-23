"""Entry point for the state_tracking example.

Demonstrates the @state_tracker decorator alongside @load_models.

@load_models pre-fills `z_warehouse_clean.orders_state` with one
pending row per daily interval, using the current STATE_MODE (defaults
to respect — preserve any already-applied rows).

@state_tracker on execute:
  * gates: skips intervals whose state row is already 'applied'
  * runs: calls the wrapped execute
  * marks 'applied' on success
  * logs to z_warehouse_clean.orders_errors on exception, then re-raises

Run twice in a row under STATE_MODE=respect: the second invocation
should skip every interval because they're all applied. Switch to
STATE_MODE=disrespect to force a full rerun.
"""

import os

from bollhav.model import Model, load_models, state_tracker


@state_tracker
def execute(model, since, until):
    print(f"  ran {model.target.full_name}  {since} → {until}")


@load_models
def main(models: list[Model], debug: bool) -> None:
    for model in models:
        print(f"\n{model.target.full_name}")
        for interval in model.intervals:
            execute(model=model, since=interval.since, until=interval.until)
    print()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
