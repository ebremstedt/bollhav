from datetime import datetime, timezone
from bollhav.model import Model, Source, Target, Schema, WriteMode, Tags, Bounds, Batch


# finance_ledger: NO explicit tags.
#
# "finance" is inherited from the schema "warehouse_finance" (snake-split)
# and from the name "finance_ledger" (snake-split). No need to type it
# out in a Tags(...) call — the auto-assembler already has it covered.
#
# Final auto-tag set:
#   {all, finance, finance_ledger, ledger,
#    warehouse, warehouse_finance, warehouse_finance.finance_ledger}
finance_ledger = Model(
    source=Source(name="finance_ledger"),
    target=Target(
        name="finance_ledger",
        schema=Schema(name="warehouse_finance"),
        write_mode=WriteMode.APPEND,
    ),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 3, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)


# finance_budgets: one explicit tag — "forecast".
#
# "finance" is inherited from name + schema, so we don't repeat it. The
# cross-cutting "forecast" tag isn't derivable though — nothing about
# "finance_budgets" or "warehouse_finance" contains the word — so we add
# it by hand. Pairs with sales_forecasts so `[forecast]` matches both
# budget planning domains at once.
finance_budgets = Model(
    source=Source(name="finance_budgets"),
    target=Target(
        name="finance_budgets",
        schema=Schema(name="warehouse_finance"),
        write_mode=WriteMode.TRUNCATE_TABLE_INSERT,
    ),
    tagging=Tags(tags={"forecast"}),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 3, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)
