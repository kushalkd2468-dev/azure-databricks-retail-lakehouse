# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the Unity Catalog catalog + schemas (and Volumes when ADLS is not configured).

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
spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")

# Volumes are only needed when running without an ADLS storage account
if not dbutils.widgets.get("storage_account"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.landing")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.landing.raw")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.landing.checkpoints")
    print("Upload raw files into", cfg.landing_path)
