from pyspark import pipelines as dp
import pyspark.sql.functions as sf

@dp.table(
    schema="""
        customer_id STRING,
        customer_name STRING,
        CONSTRAINT customers_pk PRIMARY KEY (customer_id)
    """,
)
def customers():
    return spark.createDataFrame([
        ("c1", 'ACME'),
        ("c2", 'TelCO'),
        ("c3", 'InGen'),
        ("c4", 'Weyland-Yutani'),
    ], ["customer_id", "customer_name"])

@dp.table(
    schema="""
        product_id STRING,
        product_name STRING,
        CONSTRAINT products_pk PRIMARY KEY (product_id)
    """,
)
def products():
    return spark.createDataFrame([
        ("p1", 'Fidget Spinner'),
        ("p2", 'Flux Capacitor'),
        ("p3", 'Discombobulator'),
        ("p4", 'Re-combobulator'),
    ], ["product_id", "product_name"])

@dp.table(
    schema="""
        order_id STRING,
        customer_id STRING,
        order_date DATE,
        order_complete BOOLEAN,
        CONSTRAINT orders_pk PRIMARY KEY (order_id),
        CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
    """,
)
def orders():
    return (
        spark.createDataFrame([
            ("o1", 'c1', '2026-10-01', True),
            ("o2", 'c1', '2026-10-15', False),
            ("o3", 'c1', '2026-11-01', True),
            ("o4", 'c2', '2026-11-05', True),
            ("o5", 'c3', '2026-11-06', True),
            ("o6", 'c3', '2026-11-07', True),
            ("o7", 'c4', '2026-11-10', False),
        ], ["order_id", "customer_id", "order_date", "order_complete"])
        .withColumn("order_date", sf.col("order_date").cast("date"))
    )

@dp.table(
    schema="""
        order_id STRING,
        line_item BIGINT,
        product_id STRING,
        quantity BIGINT,
        delivery_complete_dtts TIMESTAMP,
        CONSTRAINT order_line_items_pk PRIMARY KEY (order_id, line_item),
        CONSTRAINT order_line_items_order_fk FOREIGN KEY (order_id) REFERENCES orders (order_id),
        CONSTRAINT order_line_items_product_fk FOREIGN KEY (product_id) REFERENCES products (product_id)
    """,
)
def order_line_items():
    return (
        spark.createDataFrame([
            ("o1", 1, 'p1', 100, '2026-10-05'),
            ("o1", 2, 'p1', 100, '2026-10-06'),
            ("o1", 3, 'p2', 100, '2026-10-07'),
            ("o2", 1, 'p2', 100, '2026-10-17'),
            ("o2", 2, 'p3', 100, '2026-10-18'),
            ("o2", 3, 'p4', 100, None),
            ("o3", 1, 'p4', 100, '2026-11-04'),
            ("o4", 1, 'p4', 100, '2026-11-07'),
            ("o5", 1, 'p4', 100, '2026-11-08'),
            ("o6", 1, 'p1', 100, '2026-11-09'),
            ("o7", 1, 'p4', 100, None)
        ], ["order_id", "line_item", "product_id", 'quantity', 'delivery_complete_dtts'])
        .withColumn("delivery_complete_dtts", sf.col("delivery_complete_dtts").cast("timestamp"))
    )