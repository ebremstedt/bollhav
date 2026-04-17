from datetime import datetime, timezone
from bollhav.model import Model, Source, Target, Schema, WriteMode, Tags, Bounds, Batch


# Some teams name their warehouse objects in PascalCase rather than
# snake_case. bollhav supports splitting PascalCase names into individual
# word tags too — but you have to opt in explicitly, because unlike
# snake_case the split is heuristic and not always desired.
#
# Opt in via the Tags flags:
#   - unpascal_name_for_tags   → splits the table name on PascalCase boundaries
#   - unpascal_schema_for_tags → splits the schema name the same way
#
# With both flags on, a table named "CustomerJourney" in schema "DataMart"
# gets the auto-tags: "customer", "journey", "data", "mart" (plus the usual
# full-name tags, "all", etc.).
CustomerJourney = Model(
    source=Source(name="CustomerJourney"),
    target=Target(
        name="CustomerJourney",
        schema=Schema(name="DataMart"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(
        unpascal_name_for_tags=True,
        unpascal_schema_for_tags=True,
    ),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 3, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)
