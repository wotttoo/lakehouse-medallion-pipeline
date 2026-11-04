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
# MAGIC ## Hàm ghi vào bảng Bronze, idempotent theo file
# MAGIC
# MAGIC Nếu chạy notebook hai lần mà cứ `append`, dữ liệu sẽ bị nhân đôi. Cách xử lý:
# MAGIC 1. Lấy danh sách `_source_file` đã có trong bảng đích.
# MAGIC 2. Bỏ các dòng đến từ những file đó (`left_anti` join), chỉ giữ dòng của **file chưa nạp**.
# MAGIC 3. Ghi Delta Lake chế độ `append`.
# MAGIC
# MAGIC Chạy lại lần 2 sẽ không thêm dòng nào. File giao lại có tên khác (`_resend`) nên vẫn được nạp.

# COMMAND ----------

def write_bronze(df, table):
    target = tbl("bronze", table)
    if table_exists(target):
        loaded_files = spark.table(target).select("_source_file").distinct()
        df = df.join(loaded_files, on="_source_file", how="left_anti")

    new_files = [r["_source_file"] for r in df.select("_source_file").distinct().collect()]
    if not new_files:
        print(f"{target}: khong co file moi, bo qua")
        return

    # dua 2 cot ky thuat ve cuoi cho de doc
    cols = [c for c in df.columns if not c.startswith("_")] + ["_ingested_at", "_source_file"]
    df.select(cols).write.format("delta").mode("append").saveAsTable(target)
    print(f"{target}: nap {len(new_files)} file moi, tong {spark.table(target).count()} dong")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nạp `products.jsonl`
# MAGIC
# MAGIC Với JSON ta tự khai báo schema thay vì để Spark đoán. Cột `price` giữ nguyên dạng lồng
# MAGIC `{amount, currency}`; việc làm phẳng thành cột `unit_list_price` để cho Silver.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

product_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", StructType([
        StructField("amount", LongType()),
        StructField("currency", StringType()),
    ])),
    StructField("is_active", BooleanType()),
])

# COMMAND ----------

def read_json(pattern, schema):
    return (
        spark.read.format("json")
        .schema(schema)
        .load(f"{RAW_PATH}/{pattern}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

# COMMAND ----------

write_bronze(read_json("products.jsonl", product_schema), "products")
