from pyspark.sql import functions as F

from src import transforms as T

ORDER_SCHEMA = (
    "order_id string, customer_id string, order_ts string, status string, "
    "payment_method string"
)


def _with_ts(df):
    return df.withColumn("_ingested_at", F.current_timestamp())


def test_clean_orders_standardises_and_nulls_blanks(spark):
    raw = _with_ts(spark.createDataFrame(
        [(" O1 ", "5", "2026-09-01 10:00:00", " completed ", "upi"),
         ("O2", "", "2026-09-01 11:00:00", "SHIPPED", "CARD")],
        ORDER_SCHEMA))
    rows = {r.order_id: r for r in T.clean_orders(raw).collect()}
    assert rows["O1"].status == "COMPLETED" and rows["O1"].payment_method == "UPI"
    assert rows["O1"].customer_id == 5
    assert rows["O2"].customer_id is None


def test_split_valid_invalid_reports_failed_rules(spark):
    raw = _with_ts(spark.createDataFrame(
        [("O1", "5", "2026-09-01 10:00:00", "COMPLETED", "UPI"),
         ("O2", "", "2026-09-01 10:00:00", "COMPLETED", "UPI"),
         ("O3", "7", "2026-09-01 10:00:00", "WEIRD", "UPI")],
        ORDER_SCHEMA))
    valid, invalid = T.split_valid_invalid(T.clean_orders(raw), T.order_rules())
    assert [r.order_id for r in valid.collect()] == ["O1"]
    failed = {r.order_id: r._failed_rules for r in invalid.collect()}
    assert failed["O2"] == ["customer_id_not_null"]
    assert failed["O3"] == ["status_allowed"]


def test_order_item_rules_reject_negative_quantity(spark):
    raw = _with_ts(spark.createDataFrame(
        [("O1", "1", "10", "2", "100.0", "0.1"), ("O1", "2", "11", "-1", "50.0", "0")],
        "order_id string, line_no string, product_id string, quantity string, "
        "unit_price string, discount string"))
    valid, invalid = T.split_valid_invalid(T.clean_order_items(raw), T.order_item_rules())
    assert valid.count() == 1
    assert invalid.collect()[0]._failed_rules == ["quantity_positive"]


def test_dedupe_latest_keeps_most_recent(spark):
    df = spark.createDataFrame([("A", 1), ("A", 3), ("B", 2)], "k string, v int")
    out = {r.k: r.v for r in T.dedupe_latest(df, ["k"], "v").collect()}
    assert out == {"A": 3, "B": 2}


def test_row_hash_changes_only_when_tracked_columns_change(spark):
    df = spark.createDataFrame(
        [(1, "Bengaluru", "x"), (1, "Mysuru", "x"), (1, "Bengaluru", "y")],
        "id int, city string, ignored string")
    h = [r.row_hash for r in T.add_row_hash(df, ["city"]).collect()]
    assert h[0] != h[1] and h[0] == h[2]


def test_fact_sales_revenue_profit_and_exclusions(spark):
    items = spark.createDataFrame(
        [("O1", 1, 10, 2, 100.0, 0.1), ("O2", 1, 10, 1, 100.0, 0.0)],
        "order_id string, line_no int, product_id int, quantity int, unit_price double, "
        "discount double")
    orders = spark.createDataFrame(
        [("O1", 5, "2026-09-01 10:00:00", "2026-09-01", "COMPLETED", "UPI"),
         ("O2", 5, "2026-09-01 10:00:00", "2026-09-01", "CANCELLED", "UPI")],
        "order_id string, customer_id int, order_ts string, order_date string, status string, "
        "payment_method string").withColumn("order_ts", F.to_timestamp("order_ts")) \
        .withColumn("order_date", F.to_date("order_date"))
    products = spark.createDataFrame([(10, "Phone", "Electronics", 60.0)],
                                     "product_id int, product_name string, category string, "
                                     "cost_price double")
    customers = spark.createDataFrame([(5, "Asha", "Mysuru", "KA", "Gold")],
                                      "customer_id int, name string, city string, state string, "
                                      "segment string")
    fact = T.build_fact_sales(items, orders, products, customers).collect()
    assert len(fact) == 1  # cancelled order excluded
    assert fact[0].revenue == 180.0          # 2 * 100 * 0.9
    assert fact[0].profit == 60.0            # 180 - 2 * 60
    assert fact[0].customer_name == "Asha"


def test_enforce_schema_turns_bad_types_into_null_and_drops_extras(spark):
    from src import schemas as S
    raw = spark.createDataFrame(
        [("1", "abc", "x")], "customer_id string, updated_at string, extra string")
    out = T.enforce_schema(raw, S.CUSTOMERS).collect()[0]
    assert out.customer_id == 1 and out.updated_at is None and out.name is None
    assert "extra" not in T.enforce_schema(raw, S.CUSTOMERS).columns


def test_orders_with_unparsable_values_are_rejected_not_crashing(spark):
    raw = _with_ts(spark.createDataFrame(
        [("O1", "not-a-number", "2026-09-01 10:00:00", "COMPLETED", "UPI"),
         ("O2", "3", "garbage-date", "COMPLETED", "UPI")], ORDER_SCHEMA))
    valid, invalid = T.split_valid_invalid(T.clean_orders(raw), T.order_rules())
    assert valid.count() == 0
    failed = {r.order_id: r._failed_rules for r in invalid.collect()}
    assert failed["O1"] == ["customer_id_not_null"] and failed["O2"] == ["order_ts_valid"]


def test_clean_events_flattens_nested_json_and_validates(spark):
    raw = _with_ts(spark.createDataFrame(
        [("E1", "5", "10", "view", "2026-09-01T10:00:00", ("Mobile", "3.2.0")),
         ("E2", "abc", "10", "PURCHASE", "2026-09-01T10:00:00", ("desktop", "3.1.0")),
         ("E3", "6", "10", "TELEPORT", "2026-09-01T10:00:00", ("tablet", "3.1.0"))],
        "event_id string, customer_id string, product_id string, event_type string, "
        "event_ts string, context struct<device:string,app_version:string>"))
    clean = T.clean_events(raw)
    valid, invalid = T.split_valid_invalid(clean, T.event_rules())
    row = valid.collect()[0]
    got = (row.event_id, row.device, row.event_type, row.customer_id)
    assert got == ("E1", "mobile", "VIEW", 5)
    failed = {r.event_id: r._failed_rules for r in invalid.collect()}
    assert failed["E2"] == ["customer_id_valid"] and failed["E3"] == ["event_type_allowed"]


def test_conversion_funnel_rates(spark):
    ev = spark.createDataFrame(
        [("E1", 1, 10, "VIEW", "mobile"), ("E2", 1, 10, "VIEW", "mobile"),
         ("E3", 1, 10, "VIEW", "mobile"), ("E4", 1, 10, "VIEW", "mobile"),
         ("E5", 1, 10, "ADD_TO_CART", "mobile"), ("E6", 1, 10, "PURCHASE", "mobile")],
        "event_id string, customer_id int, product_id int, event_type string, device string")
    prods = spark.createDataFrame([(10, "Phone", "Electronics", 60.0)],
                                  "product_id int, product_name string, category string, "
                                  "cost_price double")
    r = T.conversion_funnel(ev, prods).collect()[0]
    assert (r.views, r.add_to_carts, r.purchases) == (4, 1, 1)
    assert r.view_to_cart_rate == 0.25 and r.cart_to_purchase_rate == 1.0
