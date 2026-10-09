# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver – clean, validate, deduplicate, upsert
# MAGIC * **Explicit schema enforcement** (`src/schemas.py`) + standardisation
# MAGIC * Data-quality rules; failures go to `silver._quarantine` with the rule names
# MAGIC * Orders / items / products: idempotent `MERGE` (SCD1)
# MAGIC * Customers: **SCD Type 2** history

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath(".."))  # repo root, so `src` is importable

dbutils.widgets.text("catalog", "retail_lakehouse")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("secret_scope", "adls")

from src.config import init_environment

cfg = init_environment(
    spark, dbutils,
    dbutils.widgets.get("catalog"),
    dbutils.widgets.get("storage_account"),
    dbutils.widgets.get("secret_scope"),
)
print(cfg)

# COMMAND ----------
from pyspark.sql import functions as F

from src import transforms as T
from src.delta_utils import upsert, write_quarantine
from src.scd2 import apply_scd2

QUARANTINE = cfg.table("silver", "_quarantine")


def bronze(name):
    return spark.table(cfg.table("bronze", name))


stats = {}

# COMMAND ----------
# MAGIC %md ### Customers → SCD Type 2 dimension

# COMMAND ----------
ok, bad = T.split_valid_invalid(T.clean_customers(bronze("customers")), T.customer_rules())
stats["customers_quarantined"] = write_quarantine(spark, bad, "customers", QUARANTINE)

latest = T.dedupe_latest(ok, ["customer_id"], "updated_at")
incoming = T.add_row_hash(latest, T.CUSTOMER_TRACKED).withColumn(
    "effective_from", F.col("updated_at"))
apply_scd2(spark, incoming, cfg.table("silver", "dim_customers"), "customer_id",
           T.CUSTOMER_TRACKED)

# COMMAND ----------
# MAGIC %md ### Products

# COMMAND ----------
ok, bad = T.split_valid_invalid(T.clean_products(bronze("products")), T.product_rules())
stats["products_quarantined"] = write_quarantine(spark, bad, "products", QUARANTINE)
upsert(spark, T.dedupe_latest(ok, ["product_id"], "_ingested_at"),
       cfg.table("silver", "products"), ["product_id"])

# COMMAND ----------
# MAGIC %md ### Orders

# COMMAND ----------
ok, bad = T.split_valid_invalid(T.clean_orders(bronze("orders")), T.order_rules())
stats["orders_quarantined"] = write_quarantine(spark, bad, "orders", QUARANTINE)
upsert(spark, T.dedupe_latest(ok, ["order_id"], "_ingested_at"),
       cfg.table("silver", "orders"), ["order_id"])

# COMMAND ----------
# MAGIC %md ### Order items

# COMMAND ----------
ok, bad = T.split_valid_invalid(T.clean_order_items(bronze("order_items")), T.order_item_rules())
stats["order_items_quarantined"] = write_quarantine(spark, bad, "order_items", QUARANTINE)
upsert(spark, T.dedupe_latest(ok, ["order_id", "line_no"], "_ingested_at"),
       cfg.table("silver", "order_items"), ["order_id", "line_no"])

# COMMAND ----------
# MAGIC %md ### Customer events (nested JSON flattened)

# COMMAND ----------
ok, bad = T.split_valid_invalid(T.clean_events(bronze("customer_events")), T.event_rules())
stats["events_quarantined"] = write_quarantine(spark, bad, "customer_events", QUARANTINE)
upsert(spark, T.dedupe_latest(ok, ["event_id"], "_ingested_at"),
       cfg.table("silver", "customer_events"), ["event_id"])

# COMMAND ----------
print(stats)
display(spark.table(QUARANTINE).groupBy("entity", "failed_rules").count().orderBy("entity"))
