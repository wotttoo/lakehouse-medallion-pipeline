# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Khởi tạo schema và volume
# MAGIC
# MAGIC Mục tiêu: tạo 4 schema của Lakehouse trong catalog `workspace`:
# MAGIC
# MAGIC | Schema | Vai trò |
# MAGIC |---|---|
# MAGIC | `landing` | chứa volume `raw`, nơi đặt file nguồn tải lên |
# MAGIC | `bronze` | dữ liệu thô, giữ nguyên như nguồn gửi |
# MAGIC | `silver` | dữ liệu đã làm sạch, chuẩn hóa, khử trùng |
# MAGIC | `gold` | star schema và bảng tổng hợp phục vụ phân tích |
# MAGIC
# MAGIC Mọi lệnh đều dùng `IF NOT EXISTS` nên chạy lại nhiều lần không lỗi.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

for schema in ["landing", "bronze", "silver", "gold"]:
    if CATALOG:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    else:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"Schema san sang: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC Volume là thư mục được Unity Catalog quản lý, dùng để chứa file (CSV, JSON...). Khi chạy local không có
# MAGIC Unity Catalog nên bỏ qua bước này: file nguồn được đọc thẳng từ thư mục `data/raw`.

# COMMAND ----------

if IS_DATABRICKS:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.raw")
    print(f"Volume san sang: {RAW_PATH}")
else:
    print("Local: bo qua volume, doc file tu", RAW_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tải dữ liệu nguồn lên Volume
# MAGIC
# MAGIC Làm một lần, bằng giao diện Databricks:
# MAGIC
# MAGIC 1. Mở **Catalog** ở thanh bên trái, chọn `workspace` → `landing` → volume `raw`.
# MAGIC 2. Bấm **Upload to this volume** và chọn **toàn bộ file** trong thư mục `data/raw/` của project
# MAGIC    (`stores.csv`, `customers*.csv`, `products.jsonl`, 13 file `orders_*.csv`, 13 file `order_items_*.csv`).
# MAGIC 3. Trong volume `raw`, tạo thư mục con `_expected` (**Create directory**), rồi tải file
# MAGIC    `data/_expected/expected_counts.json` vào đó. File này là đáp án để notebook `06_validation` đối chiếu.
# MAGIC 4. Kiểm tra: đường dẫn cuối cùng phải là `/Volumes/workspace/landing/raw/orders_2025_01.csv` và
# MAGIC    `/Volumes/workspace/landing/raw/_expected/expected_counts.json`.
# MAGIC
# MAGIC Cell dưới đây liệt kê file đang có trong volume để bạn kiểm tra lại.

# COMMAND ----------

if IS_DATABRICKS:
    files = dbutils.fs.ls(RAW_PATH)
    print(f"Co {len(files)} muc trong {RAW_PATH}")
    display(files)
