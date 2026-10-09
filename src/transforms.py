"""Pure PySpark transformations (unit-tested locally with pytest)."""
from __future__ import annotations

from typing import Dict, List

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src import schemas as S

VALID_STATUSES = ["COMPLETED", "SHIPPED", "CANCELLED", "RETURNED"]
EVENT_TYPES = ["VIEW", "ADD_TO_CART", "WISHLIST", "PURCHASE"]
CUSTOMER_TRACKED = ["name", "email", "city", "state", "segment"]


def _s(col: str) -> Column:
    """Trim a string column and turn blanks into NULL."""
    t = F.trim(F.col(col))
    return F.when(t != "", t)


def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Select exactly the columns in `schema`, cast with try_cast.

    * unparsable values become NULL (never an exception, even with ANSI mode on)
    * missing columns become NULL, extra columns are dropped
    """
    cols = []
    for f in schema.fields:
        if f.name in df.columns:
            c = F.expr(f"try_cast(`{f.name}` AS {f.dataType.simpleString()})")
        else:
            c = F.lit(None).cast(f.dataType)
        cols.append(c.alias(f.name))
    return df.select(*cols)


# ---------------------------------------------------------------- generic helpers
def dedupe_latest(df: DataFrame, keys: List[str], order_col: str) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def add_row_hash(df: DataFrame, cols: List[str], name: str = "row_hash") -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    return df.withColumn(name, F.sha2(F.concat_ws("||", *parts), 256))


def split_valid_invalid(df: DataFrame, rules: Dict[str, Column]):
    """Evaluate named boolean rules. Returns (valid_df, invalid_df).

    invalid_df carries a `_failed_rules` array so you can see *why* a row was rejected.
    A rule that evaluates to NULL counts as failed.
    """
    checks = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules.items()]
    failed = F.filter(F.array(*checks), lambda x: x.isNotNull())
    flagged = df.withColumn("_failed_rules", failed)
    valid = flagged.filter(F.size("_failed_rules") == 0).drop("_failed_rules")
    invalid = flagged.filter(F.size("_failed_rules") > 0)
    return valid, invalid


# ---------------------------------------------------------------- silver: cleaning
def clean_customers(df: DataFrame) -> DataFrame:
    std = df.select(
        _s("customer_id").alias("customer_id"), _s("name").alias("name"),
        F.lower(_s("email")).alias("email"), _s("city").alias("city"),
        F.upper(_s("state")).alias("state"), _s("segment").alias("segment"),
        _s("updated_at").alias("updated_at"), "_ingested_at")
    return enforce_schema(std, S.CUSTOMERS)


def customer_rules() -> Dict[str, Column]:
    return {
        "customer_id_not_null": F.col("customer_id").isNotNull(),
        "email_valid": F.col("email").contains("@"),
        "updated_at_valid": F.col("updated_at").isNotNull(),
    }


def clean_products(df: DataFrame) -> DataFrame:
    std = df.select(
        _s("product_id").alias("product_id"), _s("product_name").alias("product_name"),
        _s("category").alias("category"), _s("list_price").alias("list_price"),
        _s("cost_price").alias("cost_price"), "_ingested_at")
    return enforce_schema(std, S.PRODUCTS)


def product_rules() -> Dict[str, Column]:
    return {
        "product_id_not_null": F.col("product_id").isNotNull(),
        "prices_positive": (F.col("list_price") > 0) & (F.col("cost_price") > 0),
    }


def clean_orders(df: DataFrame) -> DataFrame:
    std = df.select(
        _s("order_id").alias("order_id"), _s("customer_id").alias("customer_id"),
        _s("order_ts").alias("order_ts"), F.upper(_s("status")).alias("status"),
        F.upper(_s("payment_method")).alias("payment_method"), "_ingested_at")
    typed = enforce_schema(std, S.ORDERS)
    return typed.withColumn("order_date", F.col("order_ts").cast("date"))


def order_rules() -> Dict[str, Column]:
    return {
        "order_id_not_null": F.col("order_id").isNotNull(),
        "customer_id_not_null": F.col("customer_id").isNotNull(),
        "order_ts_valid": F.col("order_ts").isNotNull(),
        "status_allowed": F.col("status").isin(*VALID_STATUSES),
    }


def clean_order_items(df: DataFrame) -> DataFrame:
    std = df.select(
        _s("order_id").alias("order_id"), _s("line_no").alias("line_no"),
        _s("product_id").alias("product_id"), _s("quantity").alias("quantity"),
        _s("unit_price").alias("unit_price"),
        F.coalesce(_s("discount"), F.lit("0")).alias("discount"), "_ingested_at")
    return enforce_schema(std, S.ORDER_ITEMS)


def order_item_rules() -> Dict[str, Column]:
    return {
        "order_id_not_null": F.col("order_id").isNotNull(),
        "line_no_not_null": F.col("line_no").isNotNull(),
        "product_id_not_null": F.col("product_id").isNotNull(),
        "quantity_positive": F.col("quantity") > 0,
        "unit_price_non_negative": F.col("unit_price") >= 0,
        "discount_in_range": F.col("discount").between(0, 1),
    }


def clean_events(df: DataFrame) -> DataFrame:
    """Flatten the nested JSON `context` object and standardise event fields."""
    has_ctx = "context" in df.columns
    device = F.lower(F.trim(F.col("context.device"))) if has_ctx else F.lit(None)
    app_version = F.trim(F.col("context.app_version")) if has_ctx else F.lit(None)
    std = df.select(
        _s("event_id").alias("event_id"), _s("customer_id").alias("customer_id"),
        _s("product_id").alias("product_id"), F.upper(_s("event_type")).alias("event_type"),
        _s("event_ts").alias("event_ts"), device.alias("device"),
        app_version.alias("app_version"), "_ingested_at")
    return enforce_schema(std, S.CUSTOMER_EVENTS)


def event_rules() -> dict[str, Column]:
    return {
        "event_id_not_null": F.col("event_id").isNotNull(),
        "customer_id_valid": F.col("customer_id").isNotNull(),
        "event_ts_valid": F.col("event_ts").isNotNull(),
        "event_type_allowed": F.col("event_type").isin(*EVENT_TYPES),
    }


# ---------------------------------------------------------------- gold: modelling
def build_fact_sales(items: DataFrame, orders: DataFrame, products: DataFrame,
                     customers_current: DataFrame) -> DataFrame:
    """Order-line grain fact table. Cancelled / returned orders are excluded."""
    good_orders = orders.filter(~F.col("status").isin("CANCELLED", "RETURNED")).select(
        "order_id", "customer_id", "order_ts", "order_date", "status", "payment_method")
    prod = products.select("product_id", "product_name", "category", "cost_price")
    cust = customers_current.select(
        "customer_id", F.col("name").alias("customer_name"), "city", "state", "segment")

    revenue = F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2)
    cost = F.col("quantity") * F.col("cost_price")
    return (
        items.select("order_id", "line_no", "product_id", "quantity", "unit_price", "discount")
        .join(good_orders, "order_id", "inner")
        .join(prod, "product_id", "inner")
        .join(cust, "customer_id", "left")
        .withColumn("revenue", revenue)
        .withColumn("profit", F.round(revenue - cost, 2))
    )


def daily_sales(fact: DataFrame) -> DataFrame:
    return fact.groupBy("order_date", "category").agg(
        F.round(F.sum("revenue"), 2).alias("revenue"),
        F.round(F.sum("profit"), 2).alias("profit"),
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units"),
    )


def top_products(fact: DataFrame) -> DataFrame:
    return (fact.groupBy("product_id", "product_name", "category").agg(
        F.round(F.sum("revenue"), 2).alias("revenue"),
        F.round(F.sum("profit"), 2).alias("profit"),
        F.sum("quantity").alias("units"))
        .withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.desc("revenue")))))


def customer_value(fact: DataFrame) -> DataFrame:
    return fact.groupBy("customer_id", "customer_name", "segment", "city").agg(
        F.round(F.sum("revenue"), 2).alias("lifetime_revenue"),
        F.countDistinct("order_id").alias("orders"),
        F.max("order_date").alias("last_order_date"),
    ).withColumn("avg_order_value", F.round(F.col("lifetime_revenue") / F.col("orders"), 2)) \
     .withColumn("recency_days", F.datediff(F.current_date(), F.col("last_order_date")))


def conversion_funnel(events: DataFrame, products: DataFrame) -> DataFrame:
    """View -> add-to-cart -> purchase funnel per category and device."""
    e = events.join(products.select("product_id", "category"), "product_id", "left")

    def count(kind: str) -> Column:
        return F.sum((F.col("event_type") == kind).cast("int"))

    return (
        e.groupBy("category", "device")
        .agg(count("VIEW").alias("views"), count("ADD_TO_CART").alias("add_to_carts"),
             count("WISHLIST").alias("wishlists"), count("PURCHASE").alias("purchases"))
        .withColumn("view_to_cart_rate", F.when(
            F.col("views") > 0, F.round(F.col("add_to_carts") / F.col("views"), 4)))
        .withColumn("cart_to_purchase_rate", F.when(
            F.col("add_to_carts") > 0, F.round(F.col("purchases") / F.col("add_to_carts"), 4)))
    )
