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

# TODO: JOIN fact_sales voi dim_product, GROUP BY san pham, sap theo doanh thu giam dan, LIMIT 10
q2 = show("Q2. Top 10 san pham theo doanh thu", f"""
    SELECT ...  -- viet truy van o day
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Doanh thu theo danh mục và thành phố

# COMMAND ----------

# TODO: doc bang agg_category_city_revenue, sap theo category, city
q3 = show("Q3. Doanh thu theo danh muc va thanh pho", f"""
    SELECT ...  -- viet truy van o day
""", n=30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4. Giá trị trung bình mỗi đơn hàng (AOV) theo cửa hàng
# MAGIC
# MAGIC AOV = tổng doanh thu / số đơn. Một đơn có nhiều dòng fact nên phải đếm `DISTINCT order_id`.

# COMMAND ----------

# TODO: JOIN fact_sales voi dim_store; aov = SUM(revenue) / COUNT(DISTINCT order_id)
q4 = show("Q4. AOV theo cua hang", f"""
    SELECT ...  -- viet truy van o day
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q5. Tỷ lệ khách hàng quay lại
# MAGIC
# MAGIC Khách quay lại = khách mua từ 2 đơn trở lên. **Không tính khách vãng lai** (`customer_key = -1`),
# MAGIC vì mọi đơn vãng lai dồn vào một key, nếu tính sẽ thành một "khách" mua cả nghìn đơn.

# COMMAND ----------

# TODO: dem so don moi khach (bo customer_key = -1), roi tinh customers_with_orders,
# returning_customers (>= 2 don) va returning_rate
q5 = show("Q5. Ty le khach hang quay lai", f"""
    SELECT ...  -- viet truy van o day
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
