# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Lớp Gold: star schema và bảng tổng hợp
# MAGIC
# MAGIC Gold phục vụ phân tích. Ta dựng một **star schema**:
# MAGIC - Bảng fact `gold.fact_sales`, grain: **một dòng sản phẩm trong một đơn hàng**.
# MAGIC - Các bảng dimension: `dim_date`, `dim_customer`, `dim_product`, `dim_store`.
# MAGIC - Hai bảng tổng hợp sẵn cho dashboard: `agg_monthly_store_revenue`, `agg_category_city_revenue`.
# MAGIC
# MAGIC Gold chỉ đọc từ Silver, không bao giờ đọc thẳng từ Bronze.
# MAGIC
# MAGIC **Vì sao rebuild toàn bộ?** Mỗi lần chạy, mọi bảng Gold được tạo lại bằng `CREATE OR REPLACE TABLE ... AS SELECT`.
# MAGIC Dữ liệu nhỏ nên rebuild nhanh, code đơn giản và tự nhiên idempotent (chạy lại cho cùng kết quả).
# MAGIC Ở quy mô lớn, rebuild sẽ tốn kém; khi đó ta chuyển sang cập nhật tăng dần bằng `MERGE`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

def gold(name):
    return tbl("gold", name)


def silver(name):
    return tbl("silver", name)


def create_gold(name, select_sql):
    spark.sql(f"CREATE OR REPLACE TABLE {gold(name)} USING DELTA AS {select_sql}")
    print(f"{gold(name)}: {spark.table(gold(name)).count()} dong")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dim_date`
# MAGIC
# MAGIC Mỗi ngày của năm 2025 một dòng. `date_key` dạng số `yyyyMMdd` (ví dụ `20250301`).
# MAGIC `day_of_week`: 1 = thứ Hai, ..., 7 = Chủ nhật.

# COMMAND ----------

create_gold("dim_date", """
    SELECT
        CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
        d                                       AS date,
        day(d)                                  AS day,
        month(d)                                AS month,
        quarter(d)                              AS quarter,
        year(d)                                 AS year,
        weekday(d) + 1                          AS day_of_week,
        weekday(d) >= 5                         AS is_weekend
    FROM (SELECT explode(sequence(DATE'2025-01-01', DATE'2025-12-31', INTERVAL 1 DAY)) AS d)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dim_store`
# MAGIC
# MAGIC Surrogate key `store_key` sinh bằng `row_number()` sắp theo natural key `store_id`, nên luôn tất định:
# MAGIC chạy lại bao nhiêu lần, một cửa hàng vẫn nhận cùng một key.

# COMMAND ----------

# TODO: sinh store_key bang row_number() OVER (ORDER BY store_id), kem store_id, store_name, city, opened_date
create_gold("dim_store", f"""
    SELECT ...  -- viet truy van o day
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dim_customer`
# MAGIC
# MAGIC Thêm một dòng đặc biệt `customer_key = -1`, "Khách vãng lai", cho các đơn không có `customer_id` (E10).
# MAGIC Nhờ vậy fact luôn trỏ được tới một dòng dimension, không có key NULL.

# COMMAND ----------

# TODO: them dong customer_key = -1, full_name = Khach vang lai (viet co dau), cac cot con lai NULL, bang UNION ALL
create_gold("dim_customer", f"""
    SELECT
        row_number() OVER (ORDER BY customer_id) AS customer_key,
        customer_id, full_name, email, phone, city, signup_date
    FROM {silver('customers')}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dim_product`

# COMMAND ----------

create_gold("dim_product", f"""
    SELECT
        row_number() OVER (ORDER BY product_id) AS product_key,
        product_id, product_name, category, unit_list_price, currency, is_active
    FROM {silver('products')}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `fact_sales`
# MAGIC
# MAGIC Mỗi dòng của `silver.order_items` là một dòng fact. Ta ghép thêm thông tin đơn hàng (`silver.orders`)
# MAGIC và thay natural key bằng surrogate key của các dimension.
# MAGIC - `customer_id` NULL (khách vãng lai) → `customer_key = -1`.
# MAGIC - `revenue = quantity * unit_price - discount`.

# COMMAND ----------

# TODO: dung fact_sales tu silver.order_items JOIN silver.orders, roi JOIN dim_product, dim_store va
# LEFT JOIN dim_customer. Cot: date_key, customer_key (COALESCE ve -1), product_key, store_key,
# order_id, line_no, quantity, unit_price, discount, revenue = quantity * unit_price - discount
create_gold("fact_sales", f"""
    SELECT ...  -- viet truy van o day
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Kiểm tra grain: số dòng fact phải **bằng** số dòng `silver.order_items`.
# MAGIC Nhiều hơn nghĩa là một phép join đang nhân dòng (khóa dimension bị trùng); ít hơn nghĩa là mất dòng do join.

# COMMAND ----------

n_fact = spark.table(gold("fact_sales")).count()
n_items = spark.table(silver("order_items")).count()
assert n_fact == n_items, f"fact_sales co {n_fact} dong, silver.order_items co {n_items} dong"

n_null_keys = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {gold('fact_sales')}
    WHERE date_key IS NULL OR customer_key IS NULL OR product_key IS NULL OR store_key IS NULL
""").first()["n"]
assert n_null_keys == 0, f"{n_null_keys} dong fact co key NULL"
print(f"Grain dung: {n_fact} dong fact = {n_items} dong silver.order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bảng tổng hợp
# MAGIC
# MAGIC `agg_monthly_store_revenue`: mỗi tháng × cửa hàng: doanh thu, số đơn, giá trị trung bình mỗi đơn (AOV).

# COMMAND ----------

# TODO: hoan thien cac cot tong hop: revenue = SUM(revenue), order_count = COUNT(DISTINCT order_id),
# aov = ROUND(revenue / order_count, 2)
create_gold("agg_monthly_store_revenue", f"""
    SELECT
        d.year,
        d.month,
        s.store_key,
        s.store_id,
        s.store_name,
        s.city,
        -- viet cac cot tong hop o day
        NULL AS revenue
    FROM {gold('fact_sales')} f
    JOIN {gold('dim_date')} d  ON f.date_key = d.date_key
    JOIN {gold('dim_store')} s ON f.store_key = s.store_key
    GROUP BY d.year, d.month, s.store_key, s.store_id, s.store_name, s.city
""")

# COMMAND ----------

# MAGIC %md
# MAGIC `agg_category_city_revenue`: mỗi danh mục × thành phố (của cửa hàng): doanh thu, số lượng bán.

# COMMAND ----------

create_gold("agg_category_city_revenue", f"""
    SELECT
        p.category,
        s.city,
        SUM(f.revenue)   AS revenue,
        SUM(f.quantity)  AS quantity
    FROM {gold('fact_sales')} f
    JOIN {gold('dim_product')} p ON f.product_key = p.product_key
    JOIN {gold('dim_store')} s   ON f.store_key = s.store_key
    GROUP BY p.category, s.city
""")
