from datetime import datetime
from bollhav.model import Model, progress_bar
from mock_read import read
from mock_write import write, write_view


@progress_bar
def execute(model: Model, since: datetime | None, until: datetime | None) -> None:
    if model.target.is_view:
        write_view(model=model)
        return
    df = read(model=model, since=since, until=until)
    write(model=model, df=df, since=since, until=until)
