"""Delta Lake helpers (require a Databricks / delta-enabled Spark session)."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def upsert(spark: SparkSession, df: DataFrame, table: str, keys: List[str]) -> None:
    """Idempotent MERGE (SCD Type 1). Creates the table on first run."""
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return
    from delta.tables import DeltaTable

    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (DeltaTable.forName(spark, table).alias("t")
     .merge(df.alias("s"), cond)
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())


def write_quarantine(spark: SparkSession, invalid: DataFrame, entity: str, table: str) -> int:
    """Store rejected rows (as JSON) with the names of the rules they violated.

    Re-processing is idempotent: the partition for `entity` is replaced each run.
    """
    data_cols = [c for c in invalid.columns if c != "_failed_rules"]
    out = invalid.select(
        F.lit(entity).alias("entity"),
        F.to_json(F.struct(*data_cols)).alias("record"),
        F.col("_failed_rules").alias("failed_rules"),
        F.current_timestamp().alias("quarantined_at"),
    )
    if not spark.catalog.tableExists(table):
        out.write.format("delta").saveAsTable(table)
    else:
        (out.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"entity = '{entity}'").saveAsTable(table))
    return out.count()
