# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Lớp Silver: làm sạch, chuẩn hóa, khử trùng
# MAGIC
# MAGIC Silver đọc từ Bronze và tạo ra dữ liệu **đáng tin cậy**: đúng kiểu, giá trị thống nhất, không trùng lặp.
# MAGIC Bản ghi không hợp lệ **không bị xóa im lặng** mà được chuyển vào bảng quarantine kèm lý do (`reject_reason`).
# MAGIC
# MAGIC | Lỗi | Mô tả | Xử lý ở Silver |
# MAGIC |---|---|---|
# MAGIC | E01 | File tháng 6 bị giao lại | khử trùng bằng window |
# MAGIC | E02 | Trùng `order_id` trong cùng file | khử trùng bằng window |
# MAGIC | E03 | `order_date` lẫn 3 định dạng | parse về kiểu `date` |
# MAGIC | E04 | `unit_price` dạng `"25.000"` | bỏ dấu chấm, ép về số |
# MAGIC | E05 | Tên thành phố nhiều cách viết | chuẩn hóa về `Hà Nội`, `TP.HCM`, `Đà Nẵng` |
# MAGIC | E06 | `category` thừa khoảng trắng, lẫn hoa thường | `trim` + chuẩn hóa chữ hoa đầu từ |
# MAGIC | E07 | `quantity <= 0` | quarantine |
# MAGIC | E08 | `product_id` không tồn tại | quarantine |
# MAGIC | E09 | `order_date` ở tương lai | quarantine cả đơn và mọi dòng hàng của đơn |
# MAGIC | E10 | `customer_id` rỗng | **không phải lỗi**: khách vãng lai, giữ lại |
# MAGIC
# MAGIC Các bảng chính ghi bằng **MERGE** theo khóa chính nên chạy lại không làm nhân dòng.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Các hàm dùng chung
# MAGIC
# MAGIC - `to_snake_case`: đưa tên cột về dạng `snake_case` (phòng khi nguồn đổi cách đặt tên cột).
# MAGIC - `trim_strings`: bỏ khoảng trắng thừa ở mọi cột chuỗi.
# MAGIC - `normalize_city`: chuẩn hóa tên thành phố (E05).
# MAGIC - `latest_per_key`: khử trùng bằng `row_number()`, giữ bản mới nhất theo khóa.
# MAGIC - `merge_into`: ghi bằng MERGE theo khóa chính, tạo bảng nếu chưa có.

# COMMAND ----------

import re


def to_snake_case(df):
    for c in df.columns:
        new = re.sub(r"[^0-9a-zA-Z_]+", "_", re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", c.strip())).lower()
        if new != c:
            df = df.withColumnRenamed(c, new)
    return df


def trim_strings(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))
    return df

# COMMAND ----------

HN, HCM, DN = "Hà Nội", "TP.HCM", "Đà Nẵng"

# cach viet (da lower + trim) -> ten chuan
CITY_MAP = {
    "hà nội": HN, "ha noi": HN, "hn": HN,
    "tp.hcm": HCM, "tp hcm": HCM, "hcm": HCM, "sài gòn": HCM, "hồ chí minh": HCM,
    "đà nẵng": DN, "da nang": DN, "dn": DN,
}


def normalize_city(col):
    key = F.lower(F.trim(col))
    expr = None
    for variant, canonical in CITY_MAP.items():
        expr = F.when(key == variant, canonical) if expr is None else expr.when(key == variant, canonical)
    return expr  # cach viet la -> NULL, se bi assert phat hien

# COMMAND ----------

def latest_per_key(df, keys):
    # giu ban nap moi nhat; trung thoi diem nap thi lay file co ten lon hon
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def drop_technical(df):
    return df.drop("_ingested_at", "_source_file")

# COMMAND ----------

def merge_into(df, table, keys):
    target = tbl("silver", table)
    if not table_exists(target):
        df.write.format("delta").saveAsTable(target)
        print(f"{target}: tao bang moi")
        return
    view = f"src_{table}"
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"""
        MERGE INTO {target} t
        USING {view} s
        ON {on}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"{target}: MERGE xong")


def overwrite(df, table):
    target = tbl("silver", table)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(target)
    print(f"{target}: ghi de xong")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.stores`
# MAGIC
# MAGIC Đọc Bronze, chuẩn hóa tên cột, bỏ khoảng trắng, ép `opened_date` về `date`, chuẩn hóa thành phố (E05).

# COMMAND ----------

stores = trim_strings(to_snake_case(spark.table(tbl("bronze", "stores"))))

# COMMAND ----------

stores = (
    stores
    .withColumn("opened_date", F.to_date("opened_date", "yyyy-MM-dd"))
    .withColumn("city", normalize_city(F.col("city")))
)
assert stores.filter("city IS NULL").count() == 0, "Co ten thanh pho chua co trong CITY_MAP"

# COMMAND ----------

stores = drop_technical(latest_per_key(stores, ["store_id"]))
merge_into(stores, "stores", ["store_id"])
