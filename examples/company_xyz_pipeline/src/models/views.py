from bollhav.model import (
    Model,
    Source,
    Target,
    Schema,
    WriteMode,
    ModelType,
    Tags,
)

# VIEW — CREATE OR ALTER VIEW, no dataframe consumed, runs once per execution.
high_value_orders = Model(
    source=Source(
        name="high_value_orders",
        query="SELECT * FROM warehouse_clean.orders WHERE total >= 20",
    ),
    target=Target(
        name="high_value_orders",
        schema=Schema(name="warehouse_views"),
        model_type=ModelType.VIEW,
        write_mode=WriteMode.VIEW,
    ),
    tagging=Tags(tags={"views"}),
    upstream=["warehouse_clean.orders"],
)

active_customers = Model(
    source=Source(
        name="active_customers",
        query=(
            "SELECT c.* FROM warehouse_clean.customer_master_data c "
            "JOIN warehouse_clean.orders o ON o.customer_id = c.id"
        ),
    ),
    target=Target(
        name="active_customers",
        schema=Schema(name="warehouse_views"),
        model_type=ModelType.VIEW,
        write_mode=WriteMode.VIEW,
    ),
    tagging=Tags(tags={"views"}),
    upstream=["warehouse_clean.customer_master_data", "warehouse_clean.orders"],
)
