from pyspark import pipelines as dp

ORDERS_SOURCE_PATH = spark.conf.get("volume_path")+"/orders_source"

ORDERS_SCHEMA = """
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    order_complete BOOLEAN,
    updated_at TIMESTAMP
"""

# Bronze: append-only raw log of every order event file ingested from the volume.
@dp.table(
    comment=(
        "Raw order events from the orders_source volume (Auto Loader). "
        "Append-only: one row per event, may contain several rows per order_id."
    )
)
def orders_updates():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(ORDERS_SCHEMA)
        .load(ORDERS_SOURCE_PATH)
    )

# Silver: current state of each order. Auto CDC keeps the latest event per
# order_id (ordered by updated_at), so updates overwrite earlier states.
dp.create_streaming_table(
    name="orders",
    comment="Current state of each order: latest event per order_id wins.",
    schema=f"""
        {ORDERS_SCHEMA},
        CONSTRAINT orders_current_pk PRIMARY KEY (order_id),
        CONSTRAINT orders_current_customer_fk FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
    """
)

dp.create_auto_cdc_flow(
    target="orders",
    source="orders_updates",
    keys=["order_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1,
)