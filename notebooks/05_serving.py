# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Serving: trả lời 5 câu hỏi kinh doanh
# MAGIC
# MAGIC Các truy vấn chỉ dùng bảng Gold. Nội dung giống file `sql/business_questions.sql`;
# MAGIC ở đây tên bảng đi qua `tbl()` để chạy được cả trên Databricks lẫn local.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

def gold(name):
    return tbl("gold", name)


def show(title, sql, n=20):
    print(f"\n{title}")
    df = spark.sql(sql)
    if IS_DATABRICKS:
        display(df)
    else:
        df.show(n, truncate=False)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Doanh thu theo tháng của từng cửa hàng

# COMMAND ----------

q1 = show("Q1. Doanh thu theo thang cua tung cua hang", f"""
    SELECT store_id, store_name, city, month, revenue, order_count
    FROM {gold('agg_monthly_store_revenue')}
    ORDER BY store_id, month
""", n=12)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Top 10 sản phẩm theo doanh thu

# COMMAND ----------

q2 = show("Q2. Top 10 san pham theo doanh thu", f"""
    SELECT p.product_id, p.product_name, p.category,
           SUM(f.quantity) AS quantity, SUM(f.revenue) AS revenue
    FROM {gold('fact_sales')} f
    JOIN {gold('dim_product')} p ON f.product_key = p.product_key
    GROUP BY p.product_id, p.product_name, p.category
    ORDER BY revenue DESC, p.product_id
    LIMIT 10
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Doanh thu theo danh mục và thành phố

# COMMAND ----------

q3 = show("Q3. Doanh thu theo danh muc va thanh pho", f"""
    SELECT category, city, revenue
    FROM {gold('agg_category_city_revenue')}
    ORDER BY category, city
""", n=30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4. Giá trị trung bình mỗi đơn hàng (AOV) theo cửa hàng
# MAGIC
# MAGIC AOV = tổng doanh thu / số đơn. Một đơn có nhiều dòng fact nên phải đếm `DISTINCT order_id`.

# COMMAND ----------

q4 = show("Q4. AOV theo cua hang", f"""
    SELECT s.store_id, s.store_name, s.city,
           COUNT(DISTINCT f.order_id) AS order_count,
           SUM(f.revenue) AS revenue,
           ROUND(SUM(f.revenue) / COUNT(DISTINCT f.order_id), 2) AS aov
    FROM {gold('fact_sales')} f
    JOIN {gold('dim_store')} s ON f.store_key = s.store_key
    GROUP BY s.store_id, s.store_name, s.city
    ORDER BY aov DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q5. Tỷ lệ khách hàng quay lại
# MAGIC
# MAGIC Khách quay lại = khách mua từ 2 đơn trở lên. **Không tính khách vãng lai** (`customer_key = -1`),
# MAGIC vì mọi đơn vãng lai dồn vào một key, nếu tính sẽ thành một "khách" mua cả nghìn đơn.

# COMMAND ----------

q5 = show("Q5. Ty le khach hang quay lai", f"""
    WITH orders_per_customer AS (
        SELECT customer_key, COUNT(DISTINCT order_id) AS n_orders
        FROM {gold('fact_sales')}
        WHERE customer_key <> -1
        GROUP BY customer_key
    )
    SELECT COUNT(*) AS customers_with_orders,
           SUM(CASE WHEN n_orders >= 2 THEN 1 ELSE 0 END) AS returning_customers,
           ROUND(SUM(CASE WHEN n_orders >= 2 THEN 1 ELSE 0 END) / COUNT(*), 4) AS returning_rate
    FROM orders_per_customer
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tạo dashboard trên Databricks
# MAGIC
# MAGIC 1. Mở **SQL Editor**, dán từng truy vấn trong `sql/business_questions.sql`, chạy thử và **Save** với tên Q1…Q5.
# MAGIC 2. Mở **Dashboards** → **Create dashboard**. Trong tab **Data**, chọn **Add data source** → các truy vấn vừa lưu
# MAGIC    (hoặc dán trực tiếp SQL).
# MAGIC 3. Trong tab **Canvas**, thêm visualization cho từng câu hỏi, gợi ý:
# MAGIC    - Q1: biểu đồ đường, trục X `month`, trục Y `revenue`, màu theo `store_name`.
# MAGIC    - Q2: biểu đồ cột ngang, `product_name` × `revenue`.
# MAGIC    - Q3: biểu đồ cột nhóm hoặc heatmap, `category` × `city`.
# MAGIC    - Q4: biểu đồ cột, `store_name` × `aov`.
# MAGIC    - Q5: ô chỉ số (counter) hiển thị `returning_rate`.
# MAGIC 4. Bấm **Publish** để chia sẻ dashboard.
