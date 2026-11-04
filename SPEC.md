# CLAUDE.md — Project: Lakehouse Pipeline Bronze → Silver → Gold

File này mô tả toàn bộ yêu cầu để Claude Code xây dựng phần **code và data** cho bài học "Project: Bronze → Silver → Gold" (Week 4, chương trình Self-Learning Data Engineering của AI VIET NAM). Đọc hết file trước khi bắt đầu. Làm theo đúng thứ tự ở mục "Thứ tự thực hiện".

---

## 1. Bối cảnh bài học

- Học viên xây một pipeline Lakehouse 3 lớp theo **Medallion Architecture** trên **Databricks Free Edition**, dùng **PySpark + Delta Lake + Unity Catalog**.
- Chuỗi xử lý: ingest raw → clean/standardize → enrich → aggregate → serve.
- Đầu ra cuối cùng của học viên: một pipeline Lakehouse 3 lớp hoàn chỉnh, trả lời được 5 câu hỏi kinh doanh.
- Bài học **độc lập (self-contained)**: không dùng lại dữ liệu hay code của bài khác, không tham chiếu tới bài khác.
- Các chủ đề orchestration (Airflow/Jobs), streaming, Auto Loader, Lakeflow Declarative Pipelines **nằm ngoài phạm vi** bài này. Không dùng chúng.

## 2. Ràng buộc môi trường (bắt buộc tuân thủ)

Databricks Free Edition:
- Chỉ có **serverless compute**. Không dùng RDD, `sparkContext`, `df.cache()`/`persist()`, không cấu hình cluster.
- Chỉ dùng **Python và SQL** (không R, không Scala).
- Catalog mặc định là `workspace`. **Không tạo catalog mới.**
- Không giả định có truy cập internet từ notebook. Toàn bộ dữ liệu được tải lên Volume bằng tay.

Phải chạy được cả **local** (phương án dự phòng, và là cách Claude Code tự kiểm tra code):
- PySpark + `delta-spark` với phiên bản tương thích nhau (kiểm tra bảng tương thích trên docs.delta.io, ghi rõ phiên bản đã dùng vào `requirements.txt`).
- Local không có Unity Catalog: tên bảng hai cấp `schema.table`.

## 3. Cấu trúc thư mục cần tạo

```
lakehouse-project/
├── CLAUDE.md
├── README.md                  # hướng dẫn cho học viên (tiếng Việt)
├── requirements.txt           # cho chạy local + sinh dữ liệu
├── data_generator/
│   └── generate_data.py       # sinh dữ liệu nguồn + file đáp án
├── data/
│   ├── raw/                   # output của generator (sẽ upload lên Volume)
│   └── _expected/
│       └── expected_counts.json
├── notebooks/                 # bản SOLUTION, định dạng Databricks source .py
│   ├── 00_config.py
│   ├── 01_setup.py
│   ├── 02_bronze.py
│   ├── 03_silver.py
│   ├── 04_gold.py
│   ├── 05_serving.py
│   └── 06_validation.py
├── notebooks_student/         # bản học viên, có # TODO (làm SAU CÙNG)
├── sql/
│   └── business_questions.sql # 5 truy vấn trả lời 5 câu hỏi
├── local/
│   └── run_local.py           # chạy toàn bộ pipeline trên máy
└── REPORT.md                  # Claude Code tự viết sau khi chạy xong
```

## 4. Quy ước code

- Notebook ở định dạng **Databricks source**: dòng đầu `# Databricks notebook source`, các cell ngăn bằng `# COMMAND ----------`, cell markdown dùng `# MAGIC %md`.
- Mỗi notebook (trừ `00_config`) bắt đầu bằng cell `# MAGIC %run ./00_config`.
- **Logic xử lý viết bằng Python và `spark.sql(...)`**, không dùng cell `%sql` trong các notebook 01–04 và 06. Lý do: file `.py` phải chạy được như script Python thường khi chạy local (các dòng `# MAGIC` là comment nên bị bỏ qua).
- Không gọi `display()` hay `dbutils` trong logic chính. Nếu cần, bọc trong `if IS_DATABRICKS:`.
- `00_config.py` định nghĩa:
  - `IS_DATABRICKS` (tự phát hiện, ví dụ kiểm tra biến môi trường `DATABRICKS_RUNTIME_VERSION`).
  - `CATALOG = "workspace"` trên Databricks, `None` khi local.
  - `RAW_PATH`: `/Volumes/workspace/landing/raw` trên Databricks, `data/raw` khi local.
  - Hàm `tbl(schema, name)` trả về `workspace.schema.name` hoặc `schema.name`.
  - Khi local: tạo SparkSession có Delta (dùng `configure_spark_with_delta_pip`).
- Comment trong code: tiếng Việt **không dấu**, ngắn gọn (để đưa vào reading LaTeX không lỗi font).
- Cell markdown trong notebook: tiếng Việt có dấu, giải thích mục tiêu từng bước cho học viên.
- Code ngắn, dễ đọc, ưu tiên rõ ràng hơn ngắn gọn. Mỗi bước xử lý là một cell riêng để dễ trích vào reading.
- Không hardcode đường dẫn tuyệt đối của máy.

## 5. Bối cảnh dữ liệu

Một **chuỗi cửa hàng tiện lợi** (tên hư cấu, ví dụ "MiniMart") có 10 cửa hàng ở 3 thành phố: Hà Nội, TP.HCM, Đà Nẵng. Dữ liệu bán hàng năm **2025** (01/01/2025 – 31/12/2025).

### 5.1. Các file nguồn (output của generator vào `data/raw/`)

| File | Định dạng | Quy mô | Cột chính |
|---|---|---|---|
| `stores.csv` | CSV | 10 | store_id, store_name, city, opened_date |
| `customers.csv` | CSV | ~2.000 | customer_id, full_name, email, phone, city, signup_date |
| `customers_update_2025_12.csv` | CSV | ~150 | cùng cột; gồm ~100 khách cũ đổi thông tin (city/phone) + ~50 khách mới |
| `products.jsonl` | JSON Lines | ~150 | product_id, product_name, category, `price: {amount, currency}`, is_active |
| `orders_2025_MM.csv` (12 file) | CSV | ~20.000 tổng | order_id, order_date, customer_id, store_id, payment_method |
| `order_items_2025_MM.csv` (12 file) | CSV | ~50.000 tổng | order_id, line_no, product_id, quantity, unit_price, discount |
| `orders_2025_06_resend.csv` + `order_items_2025_06_resend.csv` | CSV | bằng tháng 6 | bản giao lại y hệt nội dung tháng 6 (mô phỏng nguồn gửi trùng) |

- Doanh thu một dòng: `revenue = quantity * unit_price - discount`.
- Phân phối dữ liệu nên có xu hướng thực tế: cuối tuần và tháng Tết (tháng 1–2) bán nhiều hơn, một số sản phẩm bán chạy hơn hẳn (long-tail), cửa hàng ở Hà Nội và TP.HCM doanh thu cao hơn Đà Nẵng. Mục đích là kết quả ở Gold trông có ý nghĩa khi học viên làm dashboard.
- Tên khách hàng, sản phẩm bằng tiếng Việt, hư cấu. Không dùng dữ liệu cá nhân thật.

### 5.2. Lỗi cài sẵn (bắt buộc)

Generator cài **có chủ đích** các lỗi sau, số lượng cố định theo seed, và **ghi số lượng thực tế của từng lỗi** vào `expected_counts.json`.

| Mã | Lỗi | Vị trí | Cách xử lý mong đợi |
|---|---|---|---|
| E01 | File tháng 6 được giao lại (`*_resend.csv`) | orders, order_items | Bronze giữ nguyên (append trung thực); Silver khử trùng |
| E02 | Dòng trùng `order_id` trong cùng file (~1%) | orders | Silver: window function, giữ 1 bản |
| E03 | `order_date` lẫn định dạng: `2025-03-01`, `01/03/2025`, `2025/03/01` | orders | Silver: parse về kiểu date |
| E04 | `unit_price` dạng chuỗi có dấu chấm ngăn nghìn (`"25.000"`) ở một phần dòng | order_items | Silver: ép về số |
| E05 | Tên thành phố nhiều cách viết: "HN", "Ha Noi", "hà nội", "HCM", "Sài Gòn", "TP HCM", "Da Nang" | stores, customers | Silver: chuẩn hóa về "Hà Nội", "TP.HCM", "Đà Nẵng" |
| E06 | `category` thừa khoảng trắng, chữ hoa/thường lẫn lộn | products | Silver: trim + chuẩn hóa |
| E07 | `quantity <= 0` | order_items | Silver: chuyển sang bảng quarantine |
| E08 | `product_id` không tồn tại trong products | order_items | Silver: chuyển sang bảng quarantine |
| E09 | `order_date` ở tương lai (năm 2027) | orders | Silver: chuyển sang bảng quarantine (kèm toàn bộ item của đơn đó) |
| E10 | `customer_id` rỗng (~5% đơn) | orders | **Không phải lỗi**: khách vãng lai. Giữ lại, map sang `customer_key = -1` ở Gold |

Tỷ lệ lỗi nhỏ (mỗi loại 0,5–3%) để dữ liệu vẫn trông thực tế.

### 5.3. Yêu cầu với generator

- Chỉ dùng thư viện chuẩn Python (`csv`, `json`, `random`, `datetime`, `pathlib`). Không cần Spark.
- Seed cố định (mặc định 42), có tham số dòng lệnh `--seed`, `--out`.
- Chạy lại cùng seed phải ra **byte-for-byte giống nhau**.
- Ghi `data/_expected/expected_counts.json`, gồm:
  - Số lượng của từng lỗi E01–E10.
  - Số dòng kỳ vọng của mọi bảng Bronze, Silver (kể cả bảng quarantine) và Gold.
  - Một vài giá trị kiểm tra nghiệp vụ, ví dụ tổng doanh thu cả năm sau làm sạch, số khách mua từ 2 đơn trở lên.
- Các con số này tính trực tiếp trong generator (từ dữ liệu "sạch gốc" trước khi cài lỗi), **không** tính bằng cách chạy pipeline. Đây là "đáp án độc lập" để kiểm chứng pipeline.

## 6. Đặc tả từng lớp

Schema trong catalog: `landing` (chứa volume `raw`), `bronze`, `silver`, `gold`.

### 6.1. `01_setup`
- Tạo các schema và volume `landing.raw` (`CREATE ... IF NOT EXISTS`).
- Cell markdown hướng dẫn học viên upload file từ `data/raw/` lên Volume qua giao diện Catalog.
- Khi local: bỏ qua phần volume, chỉ tạo schema.

### 6.2. `02_bronze`
- Mỗi file nguồn → một bảng Bronze: `bronze.stores`, `bronze.customers`, `bronze.products`, `bronze.orders`, `bronze.order_items`.
  - Các file `customers*.csv` vào chung `bronze.customers`.
  - 12 file tháng và file resend vào chung bảng tương ứng.
- Đọc mọi cột CSV dưới dạng **string** (không inferSchema). JSON đọc với schema tự khai báo, giữ nguyên cấu trúc lồng.
- Thêm `_ingested_at` (timestamp) và `_source_file` (từ `_metadata.file_path`).
- Chế độ ghi **append**.
- **Idempotent theo file**: trước khi nạp, lấy danh sách `_source_file` đã có trong bảng, chỉ nạp file chưa có. Chạy notebook lần 2 không thêm dòng nào. Không dùng `COPY INTO` (không chạy được local).
- File resend có tên khác nên vẫn được nạp. Đây là chủ đích: Bronze ghi trung thực những gì nhận được.

### 6.3. `03_silver`
Bảng đích: `silver.stores`, `silver.customers`, `silver.products`, `silver.orders`, `silver.order_items`, cùng hai bảng quarantine `silver.orders_rejected` và `silver.order_items_rejected` (có thêm cột `reject_reason`).

Các bước, mỗi bước một cell:
1. Chuẩn hóa tên cột về snake_case.
2. Ép kiểu: date, decimal/long cho tiền, int cho số lượng.
3. Chuẩn hóa giá trị: thành phố (E05), category (E06), trim chuỗi.
4. Khử trùng: window `row_number()` theo khóa (`order_id`; `order_id + line_no` cho items), ưu tiên bản `_ingested_at` mới nhất (E01, E02).
5. Kiểm tra hợp lệ và tách quarantine (E07, E08, E09). Đơn bị loại do E09 thì toàn bộ item của đơn cũng bị loại.
6. Làm phẳng `price.amount` của products thành cột `unit_list_price`.
7. Ghi bằng **MERGE** theo khóa chính (tạo bảng nếu chưa có).
   - `silver.customers`: MERGE với bản mới nhất theo `customer_id` (SCD Type 1, áp dụng file update).
   - Bảng quarantine: overwrite mỗi lần chạy.
8. Cell in số dòng mỗi bảng sau khi ghi.

Chạy lại notebook không làm thay đổi số dòng (idempotent).

### 6.4. `04_gold`
Star schema, grain của fact: **một dòng sản phẩm trong một đơn hàng**.
- `gold.dim_date`: mọi ngày năm 2025; cột date_key (yyyyMMdd), date, day, month, quarter, year, day_of_week, is_weekend.
- `gold.dim_customer`: surrogate `customer_key`, natural `customer_id`, các thuộc tính. Thêm dòng `customer_key = -1`, "Khách vãng lai".
- `gold.dim_product`, `gold.dim_store`: surrogate key + natural key + thuộc tính.
- `gold.fact_sales`: date_key, customer_key, product_key, store_key, order_id, line_no, quantity, unit_price, discount, revenue.
- `gold.agg_monthly_store_revenue`: tháng × cửa hàng → doanh thu, số đơn, AOV.
- `gold.agg_category_city_revenue`: danh mục × thành phố → doanh thu.
- Surrogate key sinh bằng `row_number()` sắp theo natural key (tất định).
- Gold **rebuild toàn bộ** mỗi lần chạy (`CREATE OR REPLACE TABLE ... AS SELECT` hoặc overwrite). Ghi chú lý do trong cell markdown: dữ liệu nhỏ, rebuild đơn giản và idempotent; ở quy mô lớn sẽ dùng MERGE.
- Kiểm tra trong notebook: số dòng fact phải bằng số dòng `silver.order_items` hợp lệ. Nếu khác là join đang làm nhân dòng.

### 6.5. `05_serving` và `sql/business_questions.sql`
Năm truy vấn trên các bảng Gold:
1. Doanh thu theo tháng của từng cửa hàng.
2. Top 10 sản phẩm theo doanh thu.
3. Doanh thu theo danh mục và thành phố.
4. Giá trị trung bình mỗi đơn hàng (AOV) theo cửa hàng.
5. Tỷ lệ khách hàng quay lại (mua ≥ 2 đơn), **không tính khách vãng lai**.

Notebook chạy từng truy vấn qua `spark.sql` và in kết quả. Cell markdown cuối hướng dẫn học viên tạo dashboard từ các truy vấn này trong giao diện Databricks (Claude Code không tự tạo dashboard).

### 6.6. `06_validation`
- Đọc `expected_counts.json` và so với số dòng thực tế của mọi bảng. In bảng so sánh, `assert` khi lệch.
- Kiểm tra các giá trị nghiệp vụ (tổng doanh thu, số khách quay lại).
- Demo time travel trên `silver.orders`:
  1. `DESCRIBE HISTORY`.
  2. Cố tình ghi đè sai (ví dụ xóa một tháng).
  3. Đọc `VERSION AS OF` bản trước.
  4. `RESTORE` và kiểm tra số dòng trở lại đúng.

## 7. `local/run_local.py`

- Chạy lần lượt `00_config` → `06_validation` bằng `exec` trong cùng một namespace (vì `%run` không chạy local).
- Tham số `--twice`: chạy pipeline 01–04 hai lần liên tiếp, rồi mới chạy validation. Đây là cách kiểm chứng tính idempotent.
- Tham số `--clean`: xóa thư mục warehouse/metastore local trước khi chạy.

## 8. Bản học viên (`notebooks_student/`)

Chỉ làm **sau khi** bản solution chạy qua validation, kể cả chế độ `--twice`.
- Sao chép từ solution, thay phần lõi của các bước chính bằng `# TODO: ...` kèm gợi ý ngắn.
- Giữ nguyên `00_config`, `01_setup`, `06_validation` để học viên tự kiểm tra bài làm.
- Mỗi notebook khoảng 3–6 chỗ TODO, tập trung vào: lọc file chưa nạp (Bronze), ép kiểu, chuẩn hóa, khử trùng bằng window, MERGE, tách quarantine, dựng fact và surrogate key.

## 9. Thứ tự thực hiện

1. Viết `generate_data.py`, chạy, kiểm tra thủ công vài file và `expected_counts.json`. Chạy lại để xác nhận output giống hệt.
2. Viết `00_config`, `01_setup`, `02_bronze`; chạy local; đối chiếu số dòng Bronze.
3. Viết `03_silver`; chạy; đối chiếu số dòng và quarantine với đáp án.
4. Viết `04_gold`, `05_serving`, `sql/business_questions.sql`.
5. Viết `06_validation`, `run_local.py`; chạy `python local/run_local.py --clean --twice`. Mọi assert phải pass.
6. Viết `README.md`: mục tiêu, cấu trúc thư mục, cách chạy trên Databricks Free Edition (upload file, import notebook), cách chạy local, phiên bản thư viện.
7. Tạo `notebooks_student/`.
8. Viết `REPORT.md`.

Sau mỗi bước, nếu có quyết định thiết kế chưa được đặc tả ở đây, chọn phương án đơn giản nhất, phù hợp mục tiêu giảng dạy, và ghi lại vào `REPORT.md`.

## 10. Definition of done

- `python data_generator/generate_data.py` chạy không lỗi, tất định theo seed.
- `python local/run_local.py --clean --twice` chạy hết, mọi kiểm tra trong `06_validation` pass.
- Không dùng API bị cấm ở mục 2.
- `REPORT.md` gồm:
  - Bảng số dòng thực tế của mọi bảng so với kỳ vọng.
  - Kết quả 5 câu hỏi kinh doanh (top vài dòng).
  - Phiên bản Python, PySpark, delta-spark đã dùng.
  - Danh sách quyết định thiết kế tự đưa ra.
  - Những điểm **chưa kiểm chứng được trên Databricks thật**, ví dụ hành vi `_metadata.file_path` trên Volume hay quyền tạo schema trên Free Edition. Phần này để tác giả bài học tự chạy thử.

## 11. Không được làm

- Không tạo catalog mới, không dùng Scala/R, RDD, cache, Auto Loader, `COPY INTO`, Lakeflow/DLT, Jobs.
- Không làm sạch dữ liệu ở lớp Bronze.
- Không cho bảng Gold đọc thẳng từ Bronze.
- Không xóa im lặng bản ghi lỗi: luôn đưa vào quarantine kèm lý do.
- Không dùng dữ liệu thật hay dữ liệu từ bài học khác.
- Không viết nội dung reading, slide hay quiz. Việc đó tác giả làm riêng.
