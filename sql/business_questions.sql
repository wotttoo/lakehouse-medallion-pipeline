-- 5 cau hoi kinh doanh cua MiniMart, chay tren cac bang Gold.
-- Dung trong Databricks SQL Editor hoac lam nguon cho dashboard.
-- Ten bang day du 3 cap: workspace.gold.<bang>. Notebook 05_serving chay cung cac truy van nay qua spark.sql.

-- Q1. Doanh thu theo thang cua tung cua hang
SELECT
    store_id,
    store_name,
    city,
    month,
    revenue,
    order_count
FROM workspace.gold.agg_monthly_store_revenue
ORDER BY store_id, month;

-- Q2. Top 10 san pham theo doanh thu
SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(f.quantity) AS quantity,
    SUM(f.revenue)  AS revenue
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_id, p.product_name, p.category
ORDER BY revenue DESC, p.product_id
LIMIT 10;

-- Q3. Doanh thu theo danh muc va thanh pho
SELECT
    category,
    city,
    revenue
FROM workspace.gold.agg_category_city_revenue
ORDER BY category, city;

-- Q4. Gia tri trung binh moi don hang (AOV) theo cua hang
SELECT
    s.store_id,
    s.store_name,
    s.city,
    COUNT(DISTINCT f.order_id)                            AS order_count,
    SUM(f.revenue)                                        AS revenue,
    ROUND(SUM(f.revenue) / COUNT(DISTINCT f.order_id), 2) AS aov
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_store s ON f.store_key = s.store_key
GROUP BY s.store_id, s.store_name, s.city
ORDER BY aov DESC;

-- Q5. Ty le khach hang quay lai (mua >= 2 don), khong tinh khach vang lai
WITH orders_per_customer AS (
    SELECT customer_key, COUNT(DISTINCT order_id) AS n_orders
    FROM workspace.gold.fact_sales
    WHERE customer_key <> -1
    GROUP BY customer_key
)
SELECT
    COUNT(*)                                               AS customers_with_orders,
    SUM(CASE WHEN n_orders >= 2 THEN 1 ELSE 0 END)         AS returning_customers,
    ROUND(SUM(CASE WHEN n_orders >= 2 THEN 1 ELSE 0 END) / COUNT(*), 4) AS returning_rate
FROM orders_per_customer;
