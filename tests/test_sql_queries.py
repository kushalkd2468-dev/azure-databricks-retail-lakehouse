"""Executes every query in sql/analytics_queries.sql on a small local dataset."""
import re
import subprocess
import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from src import transforms as T


def _bronze(spark, path, fmt):
    r = spark.read
    r = r.option("header", "true") if fmt == "csv" else r.option("primitivesAsString", "true")
    return getattr(r, fmt)(str(path)).withColumn("_ingested_at", F.current_timestamp())


def _valid(df, rules, keys, order):
    ok, _ = T.split_valid_invalid(df, rules())
    return T.dedupe_latest(ok, keys, order)


def test_all_analytics_queries_execute(spark, tmp_path):
    for batch in (1, 2):
        subprocess.run([sys.executable, "data_generator/generate_data.py", "--out", str(tmp_path),
                        "--batch", str(batch), "--orders", "300", "--events", "400"], check=True)
    b = lambda e, f: _bronze(spark, tmp_path / e / "*.*", f)  # noqa: E731

    orders = _valid(T.clean_orders(b("orders", "csv")), T.order_rules, ["order_id"], "_ingested_at")
    items = _valid(T.clean_order_items(b("order_items", "csv")), T.order_item_rules,
                   ["order_id", "line_no"], "_ingested_at")
    prods = _valid(T.clean_products(b("products", "csv")), T.product_rules, ["product_id"],
                   "_ingested_at")
    events = _valid(T.clean_events(b("customer_events", "json")), T.event_rules, ["event_id"],
                    "_ingested_at")
    cust = T.add_row_hash(T.clean_customers(b("customers", "csv")), T.CUSTOMER_TRACKED)

    # SCD2 dimension built the simple way: one row per distinct version
    versions = cust.dropDuplicates(["customer_id", "row_hash"]).withColumn(
        "effective_from", F.col("updated_at"))
    w = Window.partitionBy("customer_id").orderBy("effective_from")
    dim = (versions.withColumn("effective_to", F.lead("effective_from").over(w))
           .withColumn("is_current", F.col("effective_to").isNull()))
    current = dim.filter("is_current")

    fact = T.build_fact_sales(items, orders, prods, current)
    fact.createOrReplaceTempView("fact_sales")
    T.conversion_funnel(events, prods).createOrReplaceTempView("agg_conversion_funnel")
    dim.createOrReplaceTempView("dim_customers")

    text = Path("sql/analytics_queries.sql").read_text()
    text = re.sub(r"retail_lakehouse\.(gold|silver)\.", "", text)
    text = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    queries = [q.strip() for q in text.split(";") if q.strip()]
    assert len(queries) == 9
    for i, q in enumerate(queries, 1):
        rows = spark.sql(q).collect()
        assert len(rows) > 0, f"Q{i} returned no rows"
