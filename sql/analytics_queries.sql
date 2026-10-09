-- =====================================================================
-- Business analytics on the Gold / Silver layers
-- Techniques: CTEs, multi-table joins, window functions
--   (SUM/AVG OVER, ROWS frames, LAG, ROW_NUMBER, DENSE_RANK, NTILE),
--   conditional aggregation, point-in-time (SCD2) join.
-- Replace `retail_lakehouse` with your catalog name.
-- =====================================================================

-- Q1. Running revenue and 7-day moving average (window frame)
WITH daily AS (
  SELECT order_date, SUM(revenue) AS revenue
  FROM retail_lakehouse.gold.fact_sales
  GROUP BY order_date
)
SELECT order_date,
       ROUND(revenue, 2) AS revenue,
       ROUND(SUM(revenue) OVER (ORDER BY order_date), 2) AS running_revenue,
       ROUND(AVG(revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)
         AS revenue_7d_avg
FROM daily
ORDER BY order_date;

-- Q2. Day-over-day growth with LAG
WITH daily AS (
  SELECT order_date, SUM(revenue) AS revenue
  FROM retail_lakehouse.gold.fact_sales
  GROUP BY order_date
)
SELECT order_date,
       ROUND(revenue, 2) AS revenue,
       ROUND(LAG(revenue) OVER (ORDER BY order_date), 2) AS prev_day_revenue,
       ROUND((revenue - LAG(revenue) OVER (ORDER BY order_date))
             / LAG(revenue) OVER (ORDER BY order_date) * 100, 1) AS dod_growth_pct
FROM daily
ORDER BY order_date;

-- Q3. Top 3 products inside every category (DENSE_RANK per partition)
WITH product_rev AS (
  SELECT category, product_name, SUM(revenue) AS revenue
  FROM retail_lakehouse.gold.fact_sales
  GROUP BY category, product_name
),
ranked AS (
  SELECT *, DENSE_RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS rnk
  FROM product_rev
)
SELECT category, product_name, ROUND(revenue, 2) AS revenue, rnk
FROM ranked
WHERE rnk <= 3
ORDER BY category, rnk;

-- Q4. Customer value quartiles (NTILE) and each quartile's share of revenue
WITH cust AS (
  SELECT customer_id, SUM(revenue) AS revenue
  FROM retail_lakehouse.gold.fact_sales
  GROUP BY customer_id
),
q AS (
  SELECT *, NTILE(4) OVER (ORDER BY revenue DESC) AS quartile
  FROM cust
)
SELECT quartile,
       COUNT(*) AS customers,
       ROUND(SUM(revenue), 2) AS revenue,
       ROUND(100 * SUM(revenue) / SUM(SUM(revenue)) OVER (), 1) AS pct_of_total_revenue
FROM q
GROUP BY quartile
ORDER BY quartile;

-- Q5. Repeat-purchase behaviour: days between consecutive orders per customer
WITH orders AS (
  SELECT DISTINCT customer_id, order_id, order_ts
  FROM retail_lakehouse.gold.fact_sales
),
gaps AS (
  SELECT customer_id, order_id,
         DATEDIFF(order_ts, LAG(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts))
           AS days_since_prev_order
  FROM orders
)
SELECT ROUND(AVG(days_since_prev_order), 2) AS avg_days_between_orders,
       COUNT(DISTINCT CASE WHEN days_since_prev_order IS NOT NULL THEN customer_id END)
         AS repeat_customers,
       COUNT(DISTINCT customer_id) AS total_customers
FROM gaps;

-- Q6. Category share and cumulative (Pareto) contribution
WITH cat AS (
  SELECT category, SUM(revenue) AS revenue
  FROM retail_lakehouse.gold.fact_sales
  GROUP BY category
)
SELECT category,
       ROUND(revenue, 2) AS revenue,
       ROUND(100 * revenue / SUM(revenue) OVER (), 1) AS share_pct,
       ROUND(100 * SUM(revenue) OVER (ORDER BY revenue DESC) / SUM(revenue) OVER (), 1)
         AS cumulative_pct
FROM cat
ORDER BY revenue DESC;

-- Q7. Each customer's favourite category (ROW_NUMBER) joined back to customer info
WITH cust_cat AS (
  SELECT customer_id, customer_name, category, SUM(revenue) AS revenue
  FROM retail_lakehouse.gold.fact_sales
  GROUP BY customer_id, customer_name, category
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY revenue DESC) AS rn
  FROM cust_cat
)
SELECT category AS favourite_category, COUNT(*) AS customers, ROUND(AVG(revenue), 2) AS avg_spend
FROM ranked
WHERE rn = 1
GROUP BY category
ORDER BY customers DESC;

-- Q8. Clickstream conversion funnel by category (JSON events)
SELECT category,
       SUM(views) AS views,
       SUM(add_to_carts) AS add_to_carts,
       SUM(purchases) AS purchases,
       ROUND(100 * SUM(add_to_carts) / NULLIF(SUM(views), 0), 1) AS view_to_cart_pct,
       ROUND(100 * SUM(purchases) / NULLIF(SUM(add_to_carts), 0), 1) AS cart_to_purchase_pct
FROM retail_lakehouse.gold.agg_conversion_funnel
WHERE category IS NOT NULL
GROUP BY category
ORDER BY views DESC;

-- Q9. Point-in-time join to the SCD2 dimension: revenue by the city the customer
--     lived in AT THE TIME OF THE ORDER (not today's city)
WITH sales AS (
  SELECT customer_id, order_id, order_ts, SUM(revenue) AS revenue
  FROM retail_lakehouse.gold.fact_sales
  GROUP BY customer_id, order_id, order_ts
)
SELECT d.city AS city_at_order_time,
       COUNT(DISTINCT s.order_id) AS orders,
       ROUND(SUM(s.revenue), 2) AS revenue
FROM sales s
JOIN retail_lakehouse.silver.dim_customers d
  ON s.customer_id = d.customer_id
 AND s.order_ts >= d.effective_from
 AND s.order_ts < COALESCE(d.effective_to, TIMESTAMP '9999-12-31 00:00:00')
GROUP BY d.city
ORDER BY revenue DESC;
