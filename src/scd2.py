"""Slowly Changing Dimension Type 2 using Delta MERGE."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def apply_scd2(spark: SparkSession, incoming: DataFrame, target: str, key: str,
               attrs: List[str]) -> None:
    """Keep full history of `attrs` per `key`.

    incoming must contain: key, *attrs, row_hash, effective_from
    target gets:           key, *attrs, row_hash, effective_from, effective_to, is_current
    """
    cols = [key, *attrs, "row_hash", "effective_from"]
    incoming = incoming.select(*cols)

    if not spark.catalog.tableExists(target):
        (incoming.withColumn("effective_to", F.lit(None).cast("timestamp"))
         .withColumn("is_current", F.lit(True))
         .write.format("delta").saveAsTable(target))
        return

    current = spark.table(target).filter("is_current").select(
        F.col(key).alias("_t_key"), F.col("row_hash").alias("_t_hash"))

    # new keys, or existing keys whose tracked attributes changed
    changed = (incoming.join(current, incoming[key] == current["_t_key"], "left")
               .filter(F.col("_t_hash").isNull() | (F.col("_t_hash") != F.col("row_hash")))
               .drop("_t_key"))

    key_type = incoming.schema[key].dataType
    inserts = changed.drop("_t_hash").withColumn("merge_key", F.lit(None).cast(key_type))
    closers = (changed.filter(F.col("_t_hash").isNotNull()).drop("_t_hash")
               .withColumn("merge_key", F.col(key)))
    inserts.unionByName(closers).createOrReplaceTempView("_scd2_staged")

    insert_cols = ", ".join([*cols, "effective_to", "is_current"])
    insert_vals = ", ".join([*(f"s.{c}" for c in cols), "NULL", "true"])
    spark.sql(f"""
        MERGE INTO {target} AS t
        USING _scd2_staged AS s
          ON t.{key} = s.merge_key AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET is_current = false, effective_to = s.effective_from
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)
