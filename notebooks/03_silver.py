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

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.customers` (SCD Type 1)
# MAGIC
# MAGIC `bronze.customers` chứa cả file gốc và file cập nhật tháng 12. Một khách có thể xuất hiện 2 lần.
# MAGIC SCD Type 1 nghĩa là **ghi đè** thông tin cũ bằng thông tin mới nhất, không giữ lịch sử.
# MAGIC Ta lấy bản mới nhất theo `customer_id` (theo `_ingested_at`, rồi `_source_file`), sau đó MERGE.

# COMMAND ----------

customers = trim_strings(to_snake_case(spark.table(tbl("bronze", "customers"))))

# COMMAND ----------

customers = (
    customers
    .withColumn("email", F.lower("email"))
    .withColumn("city", normalize_city(F.col("city")))
    .withColumn("signup_date", F.to_date("signup_date", "yyyy-MM-dd"))
)
assert customers.filter("city IS NULL").count() == 0, "Co ten thanh pho chua co trong CITY_MAP"

# COMMAND ----------

customers = drop_technical(latest_per_key(customers, ["customer_id"]))
merge_into(customers, "customers", ["customer_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.products`
# MAGIC
# MAGIC - Chuẩn hóa `category` (E06): `trim`, rồi viết hoa chữ cái đầu mỗi từ (`initcap`), ví dụ `"  sữa "` → `"Sữa"`.
# MAGIC - Làm phẳng cột lồng `price`: `price.amount` → `unit_list_price`, `price.currency` → `currency`.

# COMMAND ----------

products = trim_strings(to_snake_case(spark.table(tbl("bronze", "products"))))

# COMMAND ----------

products = products.withColumn("category", F.initcap(F.lower(F.trim("category"))))

# COMMAND ----------

products = (
    products
    .withColumn("unit_list_price", F.col("price.amount").cast("bigint"))
    .withColumn("currency", F.col("price.currency"))
    .drop("price")
)

# COMMAND ----------

products = drop_technical(latest_per_key(products, ["product_id"]))
merge_into(products, "products", ["product_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.orders`
# MAGIC
# MAGIC ### Bước 1 · Ép kiểu `order_date` (E03)
# MAGIC
# MAGIC Nguồn trộn 3 định dạng: `2025-03-01`, `01/03/2025` (ngày/tháng/năm), `2025/03/01`.
# MAGIC Ta thử lần lượt từng định dạng bằng `try_to_timestamp` (trả về NULL nếu không khớp, không báo lỗi),
# MAGIC rồi lấy kết quả đầu tiên khác NULL bằng `coalesce`.
# MAGIC
# MAGIC `customer_id` rỗng (E10) được Spark đọc thành NULL. Đó là **khách vãng lai**, giữ nguyên.

# COMMAND ----------

DATE_FORMATS = ["yyyy-MM-dd", "dd/MM/yyyy", "yyyy/MM/dd"]


def parse_date(col_name):
    return F.coalesce(*[F.try_to_timestamp(F.col(col_name), F.lit(f)).cast("date") for f in DATE_FORMATS])


orders = trim_strings(to_snake_case(spark.table(tbl("bronze", "orders"))))
orders = orders.withColumn("order_date", parse_date("order_date"))

bad_dates = orders.filter("order_date IS NULL").count()
assert bad_dates == 0, f"{bad_dates} dong co order_date khong parse duoc"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bước 2 · Khử trùng (E01, E02)
# MAGIC
# MAGIC Một `order_id` có thể xuất hiện nhiều lần: trùng trong cùng file (E02) hoặc do file tháng 6 được giao lại (E01).
# MAGIC Dùng window function `row_number()` chia theo `order_id`, sắp bản nạp mới nhất lên đầu, giữ dòng số 1.

# COMMAND ----------

orders = drop_technical(latest_per_key(orders, ["order_id"]))

n_orders = orders.count()
n_ids = orders.select("order_id").distinct().count()
assert n_orders == n_ids, f"Van con trung: {n_orders} dong nhung {n_ids} order_id"
print(f"Sau khu trung: {n_orders} don")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bước 3 · Tách quarantine (E09) và MERGE
# MAGIC
# MAGIC Dữ liệu là năm 2025, nên đơn có `order_date` sau ngày cuối kỳ là đơn **ở tương lai** (lỗi nhập liệu).
# MAGIC Đơn lỗi được ghi vào `silver.orders_rejected` kèm `reject_reason`. Bảng quarantine được **ghi đè** mỗi lần chạy.
# MAGIC Phần hợp lệ được MERGE vào `silver.orders` theo `order_id`.

# COMMAND ----------

DATA_END_DATE = "2025-12-31"

orders = orders.withColumn(
    "reject_reason",
    F.when(F.col("order_date") > F.lit(DATA_END_DATE).cast("date"), F.lit("E09: order_date o tuong lai")),
)
orders_rejected = orders.filter("reject_reason IS NOT NULL")
orders_valid = orders.filter("reject_reason IS NULL").drop("reject_reason")

# COMMAND ----------

overwrite(orders_rejected, "orders_rejected")
merge_into(orders_valid, "orders", ["order_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.order_items`
# MAGIC
# MAGIC ### Bước 1 · Ép kiểu (E04)
# MAGIC
# MAGIC Một phần dòng có `unit_price` dạng `"25.000"` (dấu chấm ngăn nghìn). Nếu ép thẳng sang số sẽ ra NULL
# MAGIC (hoặc tệ hơn, ra `25`). Ta bỏ dấu chấm trước rồi mới ép. Tiền VND không có phần lẻ nên dùng kiểu `bigint`.
# MAGIC `try_cast` trả về NULL thay vì báo lỗi, rồi `assert` bảo đảm không dòng nào ép hỏng.

# COMMAND ----------

items = trim_strings(to_snake_case(spark.table(tbl("bronze", "order_items"))))
items = (
    items
    .withColumn("line_no", F.expr("try_cast(line_no AS INT)"))
    .withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
    .withColumn("unit_price", F.expr(r"try_cast(regexp_replace(unit_price, '\\.', '') AS BIGINT)"))
    .withColumn("discount", F.expr("try_cast(discount AS BIGINT)"))
)

bad_casts = items.filter(
    "line_no IS NULL OR quantity IS NULL OR unit_price IS NULL OR discount IS NULL"
).count()
assert bad_casts == 0, f"{bad_casts} dong order_items ep kieu ra NULL"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bước 2 · Khử trùng (E01)
# MAGIC
# MAGIC Khóa của một dòng hàng là `order_id + line_no`.

# COMMAND ----------

items = drop_technical(latest_per_key(items, ["order_id", "line_no"]))

n_items = items.count()
n_keys = items.select("order_id", "line_no").distinct().count()
assert n_items == n_keys, f"Van con trung: {n_items} dong nhung {n_keys} khoa"
print(f"Sau khu trung: {n_items} dong hang")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bước 3 · Tách quarantine (E07, E08, E09) và MERGE
# MAGIC
# MAGIC Mỗi dòng nhận **một** lý do, xét theo thứ tự:
# MAGIC 1. E09: dòng thuộc đơn đã bị loại ở bước trước (đơn lỗi thì toàn bộ dòng hàng của đơn cũng bị loại).
# MAGIC 2. E07: `quantity <= 0`.
# MAGIC 3. E08: `product_id` không có trong `silver.products`.

# COMMAND ----------

rejected_ids = orders_rejected.select("order_id").withColumn("_order_rejected", F.lit(True))
known_products = spark.table(tbl("silver", "products")).select("product_id").withColumn("_product_ok", F.lit(True))

items = (
    items
    .join(rejected_ids, on="order_id", how="left")
    .join(known_products, on="product_id", how="left")
    .withColumn(
        "reject_reason",
        F.when(F.col("_order_rejected"), F.lit("E09: don hang bi loai"))
        .when(F.col("quantity") <= 0, F.lit("E07: quantity <= 0"))
        .when(F.col("_product_ok").isNull(), F.lit("E08: product_id khong ton tai")),
    )
    .drop("_order_rejected", "_product_ok")
    .select("order_id", "line_no", "product_id", "quantity", "unit_price", "discount", "reject_reason")
)
items_rejected = items.filter("reject_reason IS NOT NULL")
items_valid = items.filter("reject_reason IS NULL").drop("reject_reason")

# COMMAND ----------

overwrite(items_rejected, "order_items_rejected")
merge_into(items_valid, "order_items", ["order_id", "line_no"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kết quả
# MAGIC
# MAGIC Số dòng mỗi bảng Silver và số bản ghi bị loại theo từng lý do. Chạy lại notebook, các con số phải giữ nguyên.

# COMMAND ----------

for t in ["stores", "customers", "products", "orders", "orders_rejected", "order_items", "order_items_rejected"]:
    print(f"{tbl('silver', t):<40}{spark.table(tbl('silver', t)).count():>8} dong")

# COMMAND ----------

reasons = spark.sql(f"""
    SELECT reject_reason, COUNT(*) AS n FROM {tbl('silver', 'orders_rejected')} GROUP BY reject_reason
    UNION ALL
    SELECT reject_reason, COUNT(*) AS n FROM {tbl('silver', 'order_items_rejected')} GROUP BY reject_reason
    ORDER BY reject_reason
""")
for r in reasons.collect():
    print(f"{r['reject_reason']:<40}{r['n']:>8}")
