# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze – incremental ingestion with Auto Loader
# MAGIC * Reads new CSV **and JSON** files only (exactly-once via checkpoints)
# MAGIC * Keeps raw values as strings, adds lineage columns, rescues unexpected columns
# MAGIC * `availableNow` trigger = incremental batch, cheap to schedule as a job

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

# entity -> file format (CSV from the shop database export, JSON from the clickstream)
FORMATS = {"customers": "csv", "products": "csv", "orders": "csv", "order_items": "csv",
           "customer_events": "json"}
ENTITIES = list(FORMATS)

queries = []
for entity in ENTITIES:
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", FORMATS[entity])
        .option("cloudFiles.schemaLocation", f"{cfg.checkpoint_path}/schemas/{entity}")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )
    if FORMATS[entity] == "csv":
        reader = reader.option("header", "true")
    stream = (
        reader.load(f"{cfg.landing_path}/{entity}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    q = (
        stream.writeStream
        .option("checkpointLocation", f"{cfg.checkpoint_path}/bronze/{entity}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(cfg.table("bronze", entity))
    )
    queries.append(q)

for q in queries:
    q.awaitTermination()

# COMMAND ----------
for entity in ENTITIES:
    n = spark.table(cfg.table("bronze", entity)).count()
    print(f"bronze.{entity:<12} {n:>8} rows")
