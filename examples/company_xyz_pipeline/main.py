import logging
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from bollhav.pipe import with_pipe_config, PipeConfig
from bollhav.model import apply_pipe_to_models, name_width_for
from execute import execute


def setup_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@with_pipe_config
def main(pipe: PipeConfig) -> None:
    setup_logging(debug=pipe.debug)
    models = apply_pipe_to_models(pipe)

    execute.set_name_width(name_width_for(models))

    for model in models:
        intervals = model.infer_intervals()

        execute.set_total(len(intervals))
        for interval in intervals:
            since, until = (
                (interval.since, interval.until) if interval else (None, None)
            )
            execute(model=model, since=since, until=until)

    execute.finish()


if __name__ == "__main__":
    main()
