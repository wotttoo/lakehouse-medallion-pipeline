# CLAUDE.md — Lakehouse Pipeline Bronze → Silver → Gold

## Tổng quan
Bộ code và data mẫu cho bài học Medallion Architecture: pipeline Lakehouse 3 lớp cho chuỗi cửa hàng hư cấu MiniMart, trả lời 5 câu hỏi kinh doanh.
Chạy trên Databricks Free Edition và cả local; kết quả được đối chiếu với đáp án độc lập do generator sinh ra.
Đặc tả đầy đủ (dữ liệu, lỗi E01–E10, từng lớp, thứ tự làm, definition of done) nằm trong `SPEC.md`: đọc trước khi làm task.

## Tech stack
- Python 3.13, Java 21 (Java 17 cũng được Spark 4.0 hỗ trợ)
- PySpark 4.0.1, delta-spark 4.0.0 (đã thử MERGE, time travel, RESTORE ở local)
- Databricks Free Edition: serverless, Unity Catalog, catalog `workspace`
- Generator chỉ dùng thư viện chuẩn Python

## Cài môi trường (Ubuntu/Debian sạch)
```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv openjdk-21-jdk-headless git
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install pyspark==4.0.1 delta-spark==4.0.0
```
Các phiên bản trên được ghi lại trong `requirements.txt`. Lần chạy Spark đầu tiên cần internet để tải JAR của Delta.

## Chạy project
Chạy từ thư mục gốc, với `.venv` đã kích hoạt. Đường dẫn project có dấu cách, luôn đặt trong ngoặc kép.
```bash
python data_generator/generate_data.py          # sinh data/raw và data/_expected
python local/run_local.py --clean               # chạy 00 -> 06
```
Trên Databricks: làm theo `README.md` (upload dữ liệu lên Volume, import `notebooks/`).

## Chạy test
```bash
python local/run_local.py --clean --twice       # test chính: validation + idempotent
h() { find data -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum; }
python data_generator/generate_data.py; a=$(h)
python data_generator/generate_data.py; [ "$a" = "$(h)" ] && echo DETERMINISTIC
```
Task chỉ đạt khi mọi `assert` trong `06_validation` pass.

## Cấu trúc thư mục
```
CLAUDE.md, SPEC.md, PLAN.md   quy tắc làm việc, đặc tả, kế hoạch + mục "Vướng mắc"
README.md, REPORT.md          hướng dẫn học viên, báo cáo kết quả
requirements.txt              phiên bản thư viện
data_generator/               sinh dữ liệu nguồn và đáp án
data/raw/, data/_expected/    dữ liệu nguồn, expected_counts.json
notebooks/                    bản solution 00_config -> 06_validation
notebooks_student/            bản có TODO, làm sau khi solution pass
sql/business_questions.sql    5 truy vấn nghiệp vụ
local/run_local.py            chạy toàn pipeline ở local bằng exec (--clean, --twice, --until NN, --notebooks DIR)
local/check_layer.py          so số dòng bảng với expected_counts.json theo tiền tố, ví dụ `bronze`, `silver.orders`
```

## Quy ước code
- Notebook định dạng Databricks source `.py`; trừ `00_config`, mỗi notebook mở đầu bằng `# MAGIC %run ./00_config`.
- Notebook 01–04 và 06 viết bằng Python và `spark.sql(...)`, không dùng cell `%sql`; `display`/`dbutils` phải bọc trong `if IS_DATABRICKS:`.
- Tên bảng luôn qua `tbl(schema, name)`; đường dẫn lấy từ `RAW_PATH`, không hardcode.
- Tên cột, biến, hàm: snake_case. Cột kỹ thuật Bronze có tiền tố `_` (`_ingested_at`, `_source_file`).
- Comment code: tiếng Việt không dấu, ngắn. Cell markdown: tiếng Việt có dấu.
- Mỗi bước xử lý một cell, ưu tiên rõ ràng hơn ngắn gọn.
- Không dùng: RDD, `sparkContext`, cache/persist, Scala/R, Auto Loader, `COPY INTO`, Lakeflow/DLT, Jobs, catalog mới.
- Không làm sạch ở Bronze; Gold không đọc thẳng từ Bronze; bản ghi lỗi vào quarantine kèm `reject_reason`.
- Test: viết bằng `assert` trong `06_validation`, có thông báo lỗi rõ. Giá trị kỳ vọng lấy từ `expected_counts.json`, do generator tính độc lập, không suy ra từ output pipeline.

## Quy tắc làm việc
- Mỗi task xong phải chạy test pass mới commit.
- Commit message rõ ràng, mô tả việc đã làm, ví dụ `feat(silver): tach quarantine cho E07-E09`.
- Không xóa dữ liệu. `--clean` chỉ được xóa warehouse/metastore local.
- Không hardcode API key hay secret.
- Gặp vấn đề không tự giải quyết được: ghi vào mục "Vướng mắc" trong `PLAN.md` rồi chuyển sang task khác.
- Quyết định thiết kế ngoài đặc tả: chọn phương án đơn giản nhất và ghi vào `REPORT.md`.
