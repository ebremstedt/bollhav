from datetime import datetime
from bollhav.model import Model, progress_bar
from mock_read import read
from mock_write import write


@progress_bar
def execute(model: Model, since: datetime, until: datetime) -> None:
    df = read(model=model, since=since, until=until)
    write(model=model, df=df, since=since, until=until)
