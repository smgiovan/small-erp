from pyspark import pipelines as dp

ORDER_LINE_ITEMS_SOURCE_PATH = spark.conf.get("volume_path")+"/order_line_items_source"

ORDER_LINE_ITEMS_SCHEMA = """
    order_id STRING,
    line_item BIGINT,
    product_id STRING,
    quantity BIGINT,
    delivery_complete_dtts TIMESTAMP,
    updated_at TIMESTAMP
"""


# Bronze: append-only raw log of every line-item event file ingested from the volume.
@dp.table(
    comment=(
        "Raw order line-item events from the order_line_items_source volume "
        "(Auto Loader). Append-only: one row per event, may contain several rows "
        "per (order_id, line_item)."
    )
)
def order_line_items_updates():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(ORDER_LINE_ITEMS_SCHEMA)
        .load(ORDER_LINE_ITEMS_SOURCE_PATH)
    )


# Silver: current state of each order line item. Auto CDC keeps the latest event
# per (order_id, line_item) (ordered by updated_at), so updates overwrite earlier
# states.
dp.create_streaming_table(
    name="order_line_items",
    comment=(
        "Current state of each order line item: latest event per "
        "(order_id, line_item) wins."
    ),
    schema=f"""
        {ORDER_LINE_ITEMS_SCHEMA},
        CONSTRAINT order_line_items_pk PRIMARY KEY (order_id, line_item),
        CONSTRAINT order_line_items_order_fk FOREIGN KEY (order_id) REFERENCES orders (order_id),
        CONSTRAINT order_line_items_product_fk FOREIGN KEY (product_id) REFERENCES products (product_id)
    """
)

dp.create_auto_cdc_flow(
    target="order_line_items",
    source="order_line_items_updates",
    keys=["order_id", "line_item"],
    sequence_by="updated_at",
    stored_as_scd_type=1,
)
