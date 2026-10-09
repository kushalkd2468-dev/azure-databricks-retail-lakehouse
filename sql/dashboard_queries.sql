-- Databricks SQL dashboard queries (replace retail_lakehouse with your catalog)

-- 1. Revenue & profit trend
SELECT order_date, SUM(revenue) AS revenue, SUM(profit) AS profit
FROM retail_lakehouse.gold.agg_daily_sales
GROUP BY order_date ORDER BY order_date;

-- 2. Revenue by category
SELECT category, SUM(revenue) AS revenue, ROUND(SUM(profit) / SUM(revenue) * 100, 1) AS margin_pct
FROM retail_lakehouse.gold.agg_daily_sales
GROUP BY category ORDER BY revenue DESC;

-- 3. Top 10 products
SELECT product_name, category, revenue, units
FROM retail_lakehouse.gold.agg_top_products
WHERE revenue_rank <= 10 ORDER BY revenue_rank;

-- 4. Top customers by lifetime value
SELECT customer_name, segment, city, lifetime_revenue, orders, recency_days
FROM retail_lakehouse.gold.agg_customer_value
ORDER BY lifetime_revenue DESC LIMIT 20;

-- 5. Data quality: rejected rows by entity and rule
SELECT entity, failed_rules, COUNT(*) AS rejected_rows
FROM retail_lakehouse.silver._quarantine
GROUP BY entity, failed_rules ORDER BY rejected_rows DESC;

-- 6. SCD2 proof: customers who changed city (history preserved)
SELECT customer_id, name, city, effective_from, effective_to, is_current
FROM retail_lakehouse.silver.dim_customers
WHERE customer_id IN (
  SELECT customer_id FROM retail_lakehouse.silver.dim_customers GROUP BY customer_id HAVING COUNT(*) > 1
)
ORDER BY customer_id, effective_from LIMIT 40;
