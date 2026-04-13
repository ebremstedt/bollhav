import logging
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from bollhav.pipe import with_pipe_config, PipeConfig
from bollhav.model import match_models
from execute import execute

MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")


def setup_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@with_pipe_config
def main(pipe: PipeConfig) -> None:
    setup_logging(debug=pipe.debug)
    matches = match_models(folder=MODELS_DIR, tags=pipe.tags)

    execute.set_name_width(max(len(m.target.full_name) for m, _ in matches))

    for model, reload in matches:
        model.target.schema.suffix = pipe.schema_suffix

        intervals = model.batching.infer_intervals(
            since=pipe.backfill.since if not reload else model.bounds.begin,
            until=pipe.backfill.until if not reload else model.bounds.end,
            batch_expression=pipe.backfill.batch_expression
            or pipe.latest.batch_expression
            or model.batching.batch_expression,
            latest=pipe.latest.enabled and not reload,
            tz_override=pipe.tz_override,
        )

        execute.set_total(len(intervals))
        for interval in intervals:
            execute(
                model=model,
                since=interval.since,
                until=interval.until,
            )

    execute.finish()


if __name__ == "__main__":
    main()
