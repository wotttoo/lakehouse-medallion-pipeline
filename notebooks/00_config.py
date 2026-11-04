# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Cấu hình chung
# MAGIC
# MAGIC Notebook này được mọi notebook khác gọi bằng `%run ./00_config`. Nó định nghĩa:
# MAGIC - `IS_DATABRICKS`: đang chạy trên Databricks hay trên máy cá nhân.
# MAGIC - `CATALOG`, `RAW_PATH`, `EXPECTED_PATH`: catalog và đường dẫn dữ liệu nguồn, file đáp án.
# MAGIC - `tbl(schema, name)`: trả về tên bảng đầy đủ, đúng cho cả hai môi trường.
# MAGIC - Khi chạy local: tạo `SparkSession` có Delta Lake.

# COMMAND ----------

import os

# Databricks luon dat bien moi truong nay
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

if IS_DATABRICKS:
    CATALOG = "workspace"
    RAW_PATH = "/Volumes/workspace/landing/raw"
    EXPECTED_PATH = "/Volumes/workspace/landing/raw/_expected/expected_counts.json"
else:
    CATALOG = None  # local khong co Unity Catalog
    RAW_PATH = "data/raw"
    EXPECTED_PATH = "data/_expected/expected_counts.json"

# COMMAND ----------

def tbl(schema, name):
    """Ten bang day du: workspace.schema.name (Databricks) hoac schema.name (local)."""
    if CATALOG:
        return f"{CATALOG}.{schema}.{name}"
    return f"{schema}.{name}"


def table_exists(full_name):
    return spark.catalog.tableExists(full_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Trên Databricks, biến `spark` đã có sẵn. Khi chạy local, ta tự tạo `SparkSession` có cấu hình Delta Lake
# MAGIC (`configure_spark_with_delta_pip` tự thêm JAR của Delta phù hợp với phiên bản `delta-spark`).

# COMMAND ----------

if not IS_DATABRICKS:
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    _root = os.getcwd()  # run_local.py luon chay tu thu muc goc project
    _builder = (
        SparkSession.builder.appName("minimart-lakehouse")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.default", "delta")
        .config("spark.sql.warehouse.dir", os.path.join(_root, "spark-warehouse"))
        .config("javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={os.path.join(_root, 'metastore_db')};create=true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.log.level", "WARN")
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(_builder).getOrCreate()

print(f"IS_DATABRICKS={IS_DATABRICKS}, RAW_PATH={RAW_PATH}, vi du bang: {tbl('silver', 'orders')}")
