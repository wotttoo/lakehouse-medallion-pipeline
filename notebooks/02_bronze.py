# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Lớp Bronze: nạp dữ liệu thô
# MAGIC
# MAGIC Nguyên tắc của Bronze: **ghi trung thực những gì nhận được**.
# MAGIC - Không làm sạch, không ép kiểu: mọi cột CSV đọc dưới dạng chuỗi (`string`).
# MAGIC - Thêm 2 cột kỹ thuật: `_ingested_at` (thời điểm nạp) và `_source_file` (file gốc), để truy vết.
# MAGIC - Ghi chế độ `append`: dữ liệu mới được nối thêm, không ghi đè dữ liệu cũ.
# MAGIC
# MAGIC | File nguồn | Bảng Bronze |
# MAGIC |---|---|
# MAGIC | `stores.csv` | `bronze.stores` |
# MAGIC | `customers.csv`, `customers_update_2025_12.csv` | `bronze.customers` |
# MAGIC | `products.jsonl` | `bronze.products` |
# MAGIC | `orders_2025_MM.csv` (12 file) + `orders_2025_06_resend.csv` | `bronze.orders` |
# MAGIC | `order_items_2025_MM.csv` (12 file) + `order_items_2025_06_resend.csv` | `bronze.order_items` |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hàm đọc CSV
# MAGIC
# MAGIC `header=True` lấy dòng đầu làm tên cột. Không bật `inferSchema` nên mọi cột là `string`.
# MAGIC Cột ẩn `_metadata.file_path` cho biết mỗi dòng đến từ file nào.

# COMMAND ----------

def read_csv(pattern):
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .load(f"{RAW_PATH}/{pattern}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hàm ghi vào bảng Bronze
# MAGIC
# MAGIC Ghi bằng Delta Lake, chế độ `append`.

# COMMAND ----------

def write_bronze(df, table):
    target = tbl("bronze", table)
    df.write.format("delta").mode("append").saveAsTable(target)
    print(f"{target}: tong {spark.table(target).count()} dong")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nạp các nguồn CSV
# MAGIC
# MAGIC Mẫu `orders_*.csv` khớp cả 12 file tháng và file giao lại `orders_2025_06_resend.csv`.
# MAGIC File giao lại vẫn được nạp, đó là chủ đích: Bronze ghi lại đúng những gì nguồn đã gửi,
# MAGIC việc khử trùng để cho lớp Silver.

# COMMAND ----------

write_bronze(read_csv("stores.csv"), "stores")

# COMMAND ----------

write_bronze(read_csv("customers*.csv"), "customers")

# COMMAND ----------

write_bronze(read_csv("orders_*.csv"), "orders")

# COMMAND ----------

write_bronze(read_csv("order_items_*.csv"), "order_items")
