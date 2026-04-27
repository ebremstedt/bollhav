import logging
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))
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
        intervals = model.infer_intervals()

        execute.set_total(len(intervals))
        for interval in intervals:
            since, until = (
                (interval.since, interval.until) if interval else (None, None)
            )
            execute(model=model, since=since, until=until)


if __name__ == "__main__":
    main()
