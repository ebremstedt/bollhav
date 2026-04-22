from bollhav.model import (
    Batch,
    IntervalChunks,
    Model,
    TargetSchema,
    SourceTable,
    Tags,
    Target,
    WriteMode,
)


# A normal time-sliced model — INTERVAL mode (the default). Catches up
# in latest mode picking the most recent complete hour. Lives alongside
# the ROW-mode event_stream model in this same example so the run output
# shows both modes side by side.
customer_summary = Model(
    source=SourceTable(name="customer_summary_source"),
    target=Target(
        name="customer_summary",
        schema=TargetSchema(name="warehouse_clean"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"customers"}),
    batching=Batch(interval=IntervalChunks(expression="@hourly")),
)
