# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold – analytics-ready star schema & aggregates
# MAGIC `fact_sales` (order-line grain) plus business aggregates that feed the dashboard.

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
from src import transforms as T


def silver(name):
    return spark.table(cfg.table("silver", name))


customers_current = silver("dim_customers").filter("is_current")

fact = T.build_fact_sales(silver("order_items"), silver("orders"), silver("products"),
                          customers_current)


def save(df, name):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(cfg.table("gold", name)))
    print(f"gold.{name}: {spark.table(cfg.table('gold', name)).count()} rows")


save(fact, "fact_sales")
save(T.daily_sales(fact), "agg_daily_sales")
save(T.top_products(fact), "agg_top_products")
save(T.customer_value(fact), "agg_customer_value")
save(T.conversion_funnel(silver("customer_events"), silver("products")), "agg_conversion_funnel")

# COMMAND ----------
# Optimise the biggest table for typical filters
spark.sql(f"OPTIMIZE {cfg.table('gold', 'fact_sales')} ZORDER BY (order_date, category)")

# COMMAND ----------
display(spark.table(cfg.table("gold", "agg_daily_sales")).orderBy("order_date"))
