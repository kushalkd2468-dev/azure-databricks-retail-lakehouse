#!/usr/bin/env python3
"""Synthetic Indian e-commerce data generator.

Produces CSV files for four entities (customers, products, orders, order_items)
plus a newline-delimited JSON clickstream feed (customer_events)
in a layout that Databricks Auto Loader can ingest incrementally:

    <out>/customers/customers_batch1.csv
    <out>/orders/orders_batch1.csv ...

Batch 1 = initial load.
Batch 2 = incremental load that deliberately contains:
  * changed customer attributes (to demonstrate SCD Type 2)
  * brand-new customers
  * dirty records: duplicates, null keys, negative quantities, bad statuses
    (to demonstrate data-quality rules + quarantine)

Usage:
    python data_generator/generate_data.py --out data/raw --batch 1
    python data_generator/generate_data.py --out data/raw --batch 2
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

CITIES = [
    ("Bengaluru", "KA"), ("Mumbai", "MH"), ("Delhi", "DL"), ("Chennai", "TN"),
    ("Hyderabad", "TS"), ("Pune", "MH"), ("Kolkata", "WB"), ("Mysuru", "KA"),
    ("Ahmedabad", "GJ"), ("Jaipur", "RJ"),
]
SEGMENTS = ["Regular", "Premium", "Gold"]
CATEGORIES = {
    "Electronics": (1500, 60000), "Fashion": (300, 5000), "Home": (400, 15000),
    "Grocery": (50, 1500), "Books": (150, 1200), "Beauty": (200, 3000),
}
STATUSES = ["COMPLETED"] * 70 + ["SHIPPED"] * 15 + ["CANCELLED"] * 10 + ["RETURNED"] * 5
PAYMENTS = ["UPI", "CARD", "COD", "NETBANKING"]
FIRST = ["Aarav", "Vivaan", "Aditya", "Ananya", "Diya", "Ishaan", "Kavya", "Meera", "Rohan",
         "Saanvi", "Arjun", "Priya", "Karthik", "Neha", "Rahul", "Sneha", "Vikram", "Pooja"]
LAST = ["Sharma", "Reddy", "Iyer", "Patel", "Gowda", "Nair", "Singh", "Das", "Mehta", "Rao"]

N_CUSTOMERS = 500
N_NEW_CUSTOMERS = 25
N_PRODUCTS = 120


def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {len(rows):>6} rows -> {path}")


def base_customers(start: datetime) -> list[dict]:
    rng = random.Random(42)  # deterministic so batch 2 can modify the same people
    out = []
    for cid in range(1, N_CUSTOMERS + 1):
        city, state = rng.choice(CITIES)
        first, last = rng.choice(FIRST), rng.choice(LAST)
        out.append({
            "customer_id": cid, "name": f"{first} {last}",
            "email": f"{first}.{last}{cid}@example.com".lower(),
            "city": city, "state": state, "segment": rng.choices(SEGMENTS, [70, 20, 10])[0],
            "updated_at": start.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return out


def make_products() -> list[dict]:
    rng = random.Random(7)
    out = []
    for pid in range(1, N_PRODUCTS + 1):
        cat = rng.choice(list(CATEGORIES))
        lo, hi = CATEGORIES[cat]
        price = round(rng.uniform(lo, hi), 2)
        out.append({"product_id": pid, "product_name": f"{cat} Item {pid}", "category": cat,
                    "list_price": price, "cost_price": round(price * rng.uniform(0.55, 0.85), 2)})
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--batch", type=int, default=1)
    ap.add_argument("--orders", type=int, default=2000)
    ap.add_argument("--events", type=int, default=3000)
    ap.add_argument("--start-date", default="2026-09-01")
    args = ap.parse_args()

    out = Path(args.out)
    b = args.batch
    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    batch_start = start + timedelta(days=7 * (b - 1))
    rng = random.Random(1000 + b)
    print(f"Generating batch {b} ...")

    # ---- customers
    customers = base_customers(start)
    if b == 1:
        cust_rows = customers
        max_cust = N_CUSTOMERS
    else:
        crng = random.Random(99 + b)
        changed = crng.sample(customers, 50)  # 10% moved city / upgraded segment
        for c in changed:
            c["city"], c["state"] = crng.choice(CITIES)
            c["segment"] = crng.choice(SEGMENTS)
            c["updated_at"] = (batch_start + timedelta(hours=crng.randint(1, 100))).strftime(
                "%Y-%m-%d %H:%M:%S")
        new = []
        for i in range(1, N_NEW_CUSTOMERS + 1):
            cid = N_CUSTOMERS + i
            city, state = crng.choice(CITIES)
            first, last = crng.choice(FIRST), crng.choice(LAST)
            new.append({"customer_id": cid, "name": f"{first} {last}",
                        "email": f"{first}.{last}{cid}@example.com".lower(), "city": city,
                        "state": state, "segment": "Regular",
                        "updated_at": batch_start.strftime("%Y-%m-%d %H:%M:%S")})
        cust_rows = changed + new
        max_cust = N_CUSTOMERS + N_NEW_CUSTOMERS
    cust_hdr = ["customer_id", "name", "email", "city", "state", "segment", "updated_at"]
    write_csv(out / "customers" / f"customers_batch{b}.csv", cust_hdr,
              [[c[k] for k in cust_hdr] for c in cust_rows])

    # ---- products (initial load only)
    products = make_products()
    if b == 1:
        p_hdr = ["product_id", "product_name", "category", "list_price", "cost_price"]
        write_csv(out / "products" / "products_batch1.csv", p_hdr,
                  [[p[k] for k in p_hdr] for p in products])

    # ---- orders + order_items (with injected dirty data)
    orders, items = [], []
    for i in range(1, args.orders + 1):
        oid = f"O{b}{i:06d}"
        ts = batch_start + timedelta(days=rng.randint(0, 6), seconds=rng.randint(0, 86399))
        orders.append([oid, rng.randint(1, max_cust), ts.strftime("%Y-%m-%d %H:%M:%S"),
                       rng.choice(STATUSES), rng.choice(PAYMENTS)])
        for ln in range(1, rng.randint(1, 4) + 1):
            p = rng.choice(products)
            items.append([oid, ln, p["product_id"], rng.randint(1, 5), p["list_price"],
                          rng.choice([0, 0, 0.05, 0.1, 0.15, 0.2])])

    n_dirty = int(args.orders * 0.02)
    for _ in range(n_dirty):
        kind = rng.choice(["dup", "null_cust", "bad_status", "neg_qty"])
        if kind == "dup":
            orders.append(list(rng.choice(orders)))
        elif kind == "null_cust":
            o = list(rng.choice(orders))
            o[0], o[1] = f"X{b}{rng.randint(1, 10**6)}", ""
            orders.append(o)
        elif kind == "bad_status":
            o = list(rng.choice(orders))
            o[0], o[3] = f"Y{b}{rng.randint(1, 10**6)}", "UNKNOWN"
            orders.append(o)
        else:
            it = list(rng.choice(items))
            it[3] = -1
            items.append(it)

    write_csv(out / "orders" / f"orders_batch{b}.csv",
              ["order_id", "customer_id", "order_ts", "status", "payment_method"], orders)
    write_csv(out / "order_items" / f"order_items_batch{b}.csv",
              ["order_id", "line_no", "product_id", "quantity", "unit_price", "discount"], items)

    # ---- clickstream events (newline-delimited JSON with a nested `context` object)
    events = []
    for i in range(1, args.events + 1):
        ts = batch_start + timedelta(days=rng.randint(0, 6), seconds=rng.randint(0, 86399))
        events.append({
            "event_id": f"E{b}{i:06d}", "customer_id": rng.randint(1, max_cust),
            "product_id": rng.choice(products)["product_id"],
            "event_type": rng.choices(["VIEW", "ADD_TO_CART", "WISHLIST", "PURCHASE"],
                                      [70, 18, 5, 7])[0],
            "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "context": {"device": rng.choice(["mobile", "desktop", "tablet"]),
                        "app_version": rng.choice(["3.1.0", "3.2.0", "3.3.1"])},
        })
    for _ in range(int(args.events * 0.02)):
        e = dict(rng.choice(events))
        kind = rng.choice(["dup", "null_id", "bad_type", "bad_customer"])
        if kind == "null_id":
            e["event_id"] = None
        elif kind == "bad_type":
            e["event_id"], e["event_type"] = f"Z{b}{rng.randint(1, 10**6)}", "TELEPORT"
        elif kind == "bad_customer":
            e["event_id"], e["customer_id"] = f"W{b}{rng.randint(1, 10**6)}", "abc"
        events.append(e)
    path = out / "customer_events" / f"customer_events_batch{b}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    print(f"  wrote {len(events):>6} rows -> {path}")
    print("Done.")


if __name__ == "__main__":
    main()
