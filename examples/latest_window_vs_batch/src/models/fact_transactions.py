from bollhav.model import (
    Model,
    SourceTable,
    Target,
    TargetSchema,
    WriteMode,
    Tags,
    Batch,
    IntervalChunks,
)


# Scenario: a warehouse fact table of payment transactions.
#
# We want to catch up on one FULL DAY of upstream data at a time — that's
# the business rhythm (source extracts publish a complete day overnight,
# dashboards are read by analysts the next morning). But the table is so
# large that a single INSERT covering 24 hours would blow up memory and
# hold locks for too long.
#
# The fix: use two cron expressions.
#
#   window_expression   = "@daily"          → OUTER scope: one full day
#   interval_expression = "*/15 * * * *"    → INNER chunk: 15 minutes each
#
# In latest mode, model.intervals resolves the outer window (yesterday,
# 00:00 → 00:00) and then splits it into 96 fifteen-minute chunks. Each
# chunk is a small, fast, retryable write. If one chunk fails, only that
# 15-minute slice needs to be reprocessed — not the entire day.
#
# Without the window expression, "latest" would only grab the most recent
# complete 15-minute slice. You'd need 96 scheduled invocations per day to
# cover the full window. The window expression is what lets *one* run
# cover a whole day's worth of chunks.
fact_transactions = Model(
    source=SourceTable(name="fact_transactions"),
    target=Target(
        name="fact_transactions",
        schema=TargetSchema(name="warehouse_finance"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"finance", "large"}),
    batching=Batch(
        interval=IntervalChunks(expression="*/15 * * * *", window_expression="@daily")
    ),
)
