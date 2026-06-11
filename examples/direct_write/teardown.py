"""Teardown — drop the *suffixed* environment this project created.

Mirror of `main.py`'s discovery: `@load_models` matches the same models (same
TAGS) and resolves the same `SCHEMA_SUFFIX`, so `drop_environment` knows exactly
which schemas to remove — each model's suffixed target schema plus the shared
`z_bollhav_<suffix>` state schema (state + library + errors). These models write
directly (no staging), so there's no `z_<target>` staging schema to clean.

Run it with the SAME env as `main.py` — same TAGS, and a schema suffix set:

    export USE_SCHEMA_SUFFIX=true SCHEMA_SUFFIX=pr123
    python main.py        # run the pipeline into the pr123 environment
    python teardown.py     # …then wipe that environment, no trace

`drop_environment` REFUSES unless a schema suffix is set — it must be impossible
to wipe prod (`z_bollhav` + unsuffixed schemas). Running this without
USE_SCHEMA_SUFFIX raises rather than touching anything.

For a single model instead of the whole environment, wipe just its state with
`PostgresState(model, conn).clear_state()` (drops only that model's state table
+ registration; data and the env schema stay). Neither is a re-run knob — to
re-process applied intervals without dropping anything, use `STATE_MODE=bulldozer`.
"""

import logging
import os

import psycopg
from roskarl import env_var_dsn

from bollhav.model import ModelRun, load_models
from bollhav.postgres import drop_environment


@load_models
def main(runs: list[ModelRun], debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    dsn = env_var_dsn("TARGET_DSN").connection_string
    with psycopg.connect(dsn, autocommit=True) as conn:
        drop_environment(conn, [run.model for run in runs])
    print("\n✓ environment dropped\n")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
