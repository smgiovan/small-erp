from pyspark import pipelines as dp
import pyspark.sql.functions as sf

### customers
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

### products
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

