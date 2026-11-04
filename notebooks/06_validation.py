# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Kiểm chứng pipeline
# MAGIC
# MAGIC Notebook này so kết quả pipeline với **đáp án độc lập** trong `expected_counts.json`.
# MAGIC Đáp án do bộ sinh dữ liệu tính trực tiếp từ dữ liệu sạch gốc, không suy ra từ output pipeline,
# MAGIC nên nếu pipeline có lỗi logic (khử trùng sót, join nhân dòng, lọc nhầm...) các con số sẽ lệch.
# MAGIC
# MAGIC Mỗi kiểm tra là một `assert`: lệch là dừng ngay kèm thông báo rõ ràng.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json

with open(EXPECTED_PATH, encoding="utf-8") as f:
    expected = json.load(f)

print(f"Doc dap an tu {EXPECTED_PATH}: {len(expected['tables'])} bang, {len(expected['errors'])} loai loi")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Số dòng của mọi bảng

# COMMAND ----------

def count_rows(name):
    schema, table = name.split(".")
    full = tbl(schema, table)
    return spark.table(full).count() if table_exists(full) else None


mismatches = []
print(f"{'bang':<36}{'thuc te':>10}{'ky vong':>10}  ket qua")
for name, exp in expected["tables"].items():
    actual = count_rows(name)
    ok = actual == exp
    if not ok:
        mismatches.append(f"{name}: thuc te {actual}, ky vong {exp}")
    print(f"{name:<36}{str(actual):>10}{exp:>10}  {'OK' if ok else 'LECH'}")

assert not mismatches, "So dong lech dap an:\n" + "\n".join(mismatches)
print(f"\nCa {len(expected['tables'])} bang khop dap an")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bản ghi lỗi trong quarantine
# MAGIC
# MAGIC Số bản ghi bị loại theo từng lý do phải bằng đúng số lỗi đã cài (E07, E08, E09),
# MAGIC và số đơn khách vãng lai (E10) phải được giữ lại nguyên vẹn trong `silver.orders`.

# COMMAND ----------

errors = expected["errors"]


def count_reason(table, prefix):
    return spark.table(tbl("silver", table)).filter(f"reject_reason LIKE '{prefix}%'").count()


checks = {
    "E09 trong orders_rejected": (count_reason("orders_rejected", "E09"), errors["E09"]),
    "E07 trong order_items_rejected": (count_reason("order_items_rejected", "E07"), errors["E07"]),
    "E08 trong order_items_rejected": (count_reason("order_items_rejected", "E08"), errors["E08"]),
    "E10 khach vang lai trong silver.orders": (
        spark.table(tbl("silver", "orders")).filter("customer_id IS NULL").count(), errors["E10"]),
}
for label, (actual, exp) in checks.items():
    print(f"{label:<45}{actual:>8}{exp:>8}  {'OK' if actual == exp else 'LECH'}")
    assert actual == exp, f"{label}: thuc te {actual}, ky vong {exp}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Giá trị nghiệp vụ

# COMMAND ----------

biz = expected["business"]
fact = tbl("gold", "fact_sales")

totals = spark.sql(f"""
    SELECT SUM(revenue) AS total_revenue,
           COUNT(DISTINCT order_id) AS total_orders,
           COUNT(DISTINCT CASE WHEN customer_key = -1 THEN order_id END) AS guest_orders
    FROM {fact}
""").first()

returning = spark.sql(f"""
    SELECT COUNT(*) AS customers_with_orders,
           SUM(CASE WHEN n >= 2 THEN 1 ELSE 0 END) AS returning_customers
    FROM (SELECT customer_key, COUNT(DISTINCT order_id) AS n
          FROM {fact} WHERE customer_key <> -1 GROUP BY customer_key)
""").first()

top_product = spark.sql(f"""
    SELECT p.product_id FROM {fact} f JOIN {tbl('gold', 'dim_product')} p ON f.product_key = p.product_key
    GROUP BY p.product_id ORDER BY SUM(f.revenue) DESC, p.product_id LIMIT 1
""").first()["product_id"]

revenue_by_city = {r["city"]: r["revenue"] for r in spark.sql(f"""
    SELECT s.city, SUM(f.revenue) AS revenue FROM {fact} f
    JOIN {tbl('gold', 'dim_store')} s ON f.store_key = s.store_key GROUP BY s.city
""").collect()}

customers_by_city = {r["city"]: r["n"] for r in spark.sql(f"""
    SELECT city, COUNT(*) AS n FROM {tbl('silver', 'customers')} GROUP BY city
""").collect()}

# COMMAND ----------

actual_biz = {
    "total_revenue": int(totals["total_revenue"]),
    "total_orders": totals["total_orders"],
    "guest_orders": totals["guest_orders"],
    "customers_with_orders": returning["customers_with_orders"],
    "returning_customers": int(returning["returning_customers"]),
    "returning_rate": round(returning["returning_customers"] / returning["customers_with_orders"], 4),
    "aov": round(int(totals["total_revenue"]) / totals["total_orders"], 2),
    "top_product_id": top_product,
    "revenue_by_city": {k: int(v) for k, v in sorted(revenue_by_city.items())},
    "silver_customers_by_city": dict(sorted(customers_by_city.items())),
}

biz_mismatches = []
for key, exp in biz.items():
    actual = actual_biz[key]
    ok = actual == exp
    if not ok:
        biz_mismatches.append(f"{key}: thuc te {actual}, ky vong {exp}")
    print(f"{key:<26}{'OK' if ok else 'LECH':>6}  {actual}")

assert not biz_mismatches, "Gia tri nghiep vu lech dap an:\n" + "\n".join(biz_mismatches)
print("\nMoi gia tri nghiep vu khop dap an")
