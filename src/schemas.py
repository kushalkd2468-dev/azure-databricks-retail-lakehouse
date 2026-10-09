"""Explicit Silver schemas (schema enforcement).

Every Silver table is cast to exactly these columns/types. Values that cannot be
cast become NULL (via try_cast) and are then rejected by the data-quality rules,
so type violations end up in the quarantine table instead of crashing the job.
"""
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CUSTOMERS = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("updated_at", TimestampType(), False),
    StructField("_ingested_at", TimestampType(), True),
])

PRODUCTS = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("list_price", DoubleType(), False),
    StructField("cost_price", DoubleType(), False),
    StructField("_ingested_at", TimestampType(), True),
])

ORDERS = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_ts", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("payment_method", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
])

ORDER_ITEMS = StructType([
    StructField("order_id", StringType(), False),
    StructField("line_no", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("discount", DoubleType(), False),
    StructField("_ingested_at", TimestampType(), True),
])

CUSTOMER_EVENTS = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), True),
    StructField("event_type", StringType(), False),
    StructField("event_ts", TimestampType(), False),
    StructField("device", StringType(), True),
    StructField("app_version", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
])
