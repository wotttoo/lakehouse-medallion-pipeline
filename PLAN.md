# PLAN.md — Lakehouse Pipeline Bronze → Silver → Gold

Đặc tả chi tiết: `SPEC.md`. Quy tắc làm việc: `CLAUDE.md`.
Mọi lệnh chạy từ thư mục gốc, với `.venv` đã kích hoạt (từ sau task 1).
Task xong thì đánh dấu `[x]` và commit theo quy tắc trong `CLAUDE.md`.

## 1. Mục tiêu cuối cùng

Bộ code và data hoàn chỉnh cho bài học: pipeline Lakehouse 3 lớp cho MiniMart, chạy được trên Databricks Free Edition và local, trả lời 5 câu hỏi kinh doanh, có bản solution và bản học viên có TODO.

## 2. Định nghĩa "hoàn thành"

Project hoàn thành khi tất cả điều kiện sau đúng:

- [ ] `python data_generator/generate_data.py` chạy không lỗi và tất định theo seed (lệnh kiểm tra trong `CLAUDE.md`, mục "Chạy test").
- [ ] `python local/run_local.py --clean --twice` chạy hết, exit code 0, mọi `assert` trong `06_validation` pass.
- [ ] Số dòng mọi bảng Bronze, Silver, Gold và các giá trị nghiệp vụ khớp `data/_expected/expected_counts.json`.
- [ ] Lệnh grep API cấm ở task 37 không trả về dòng nào.
- [ ] `notebooks_student/` có đủ 7 notebook; `00`, `01`, `06` giống hệt bản solution.
- [ ] `README.md` và `REPORT.md` đủ nội dung theo SPEC mục 9 và 10, gồm danh sách điểm chưa kiểm chứng trên Databricks thật.

## 3. Nhóm task làm song song

Chỉ các nhóm dưới đây là độc lập, vì chúng sửa các file khác nhau. Mọi task còn lại làm tuần tự theo số.

| Nhóm | Task | Điều kiện bắt đầu | Ghi chú |
|---|---|---|---|
| A | 13, 14, 15 | Task 12 xong | Ba file độc lập |
| B | Luồng 33→34 và luồng 35→36 | Task 32 xong | Hai luồng song song; trong mỗi luồng vẫn làm tuần tự |
| C | 38, 40, 41, 42, 43 | Task 39 xong | Mỗi task sửa một file riêng |

Task 2–11, 16–18, 19–28, 29–32 không song song được vì cùng sửa một file.

## 4. Danh sách task

### Giai đoạn 0: Nền tảng

- [ ] **1. Khởi tạo repo và môi trường**
  - Việc: `git init`; tạo `.gitignore` (`.venv/`, `__pycache__/`, `spark-warehouse/`, `metastore_db/`, `derby.log`); tạo `requirements.txt` với `pyspark==4.0.1`, `delta-spark==4.0.0`; tạo `.venv` và cài đặt.
  - File: `.gitignore`, `requirements.txt`
  - Kiểm chứng: `git rev-parse --is-inside-work-tree` in `true`; `python -c "import pyspark, delta; print(pyspark.__version__)"` in `4.0.1`.

### Giai đoạn 1: Generator

- [ ] **2. Khung CLI và `stores.csv`**
  - Việc: tham số `--seed` (mặc định 42), `--out` (mặc định `data`, ghi vào `<out>/raw` và `<out>/_expected`); sinh 10 cửa hàng sạch ở 3 thành phố.
  - File: `data_generator/generate_data.py`, `data/raw/stores.csv`
  - Kiểm chứng: `python data_generator/generate_data.py && wc -l data/raw/stores.csv` ra 11 dòng.

- [ ] **3. Sinh customers**
  - Việc: `customers.csv` (~2.000 khách), `customers_update_2025_12.csv` (~100 khách cũ đổi city/phone, ~50 khách mới). Tên tiếng Việt hư cấu.
  - File: `data_generator/generate_data.py`, `data/raw/customers.csv`, `data/raw/customers_update_2025_12.csv`
  - Kiểm chứng: `wc -l data/raw/customers*.csv` ra khoảng 2.001 và 151 dòng.

- [ ] **4. Sinh `products.jsonl`**
  - Việc: ~150 sản phẩm có `price: {amount, currency}`, `is_active`, và trọng số bán long-tail.
  - File: `data_generator/generate_data.py`, `data/raw/products.jsonl`
  - Kiểm chứng: `python -c "import json; r=[json.loads(l) for l in open('data/raw/products.jsonl',encoding='utf-8')]; print(len(r), r[0]['price'])"` in khoảng 150 và dict có `amount`, `currency`.

- [ ] **5. Sinh orders và order_items sạch**
  - Việc: 12 cặp file tháng, ~20.000 đơn, ~50.000 dòng; có xu hướng cuối tuần, tháng Tết, thành phố lớn, long-tail. Giữ bộ dữ liệu sạch trong bộ nhớ để tính đáp án ở task 10.
  - File: `data_generator/generate_data.py`, `data/raw/orders_2025_MM.csv`, `data/raw/order_items_2025_MM.csv`
  - Kiểm chứng: `ls data/raw/orders_2025_*.csv | wc -l` ra 12; `cat data/raw/order_items_2025_*.csv | wc -l` ra khoảng 50.000.

- [ ] **6. Cài lỗi E05, E06**
  - Việc: nhiều cách viết tên thành phố trong stores, customers; `category` thừa khoảng trắng và lẫn hoa thường.
  - File: `data_generator/generate_data.py`, `data/raw/stores.csv`, `data/raw/customers*.csv`, `data/raw/products.jsonl`
  - Kiểm chứng: `grep -cE 'HN|Ha Noi|Sài Gòn|HCM|Da Nang' data/raw/customers.csv` lớn hơn 0; `grep -c '"category": " ' data/raw/products.jsonl` lớn hơn 0.

- [ ] **7. Cài lỗi E02, E03, E10 trên orders**
  - Việc: trùng `order_id` trong cùng file (~1%); `order_date` lẫn 3 định dạng; `customer_id` rỗng (~5%).
  - File: `data_generator/generate_data.py`, `data/raw/orders_2025_MM.csv`
  - Kiểm chứng: với `data/raw/orders_2025_03.csv`, `cut -d, -f1 | sort | uniq -d | wc -l` lớn hơn 0; `cut -d, -f2 | grep -cE '^[0-9]{2}/'` lớn hơn 0; `awk -F, 'NR>1 && $3==""' | wc -l` lớn hơn 0.

- [ ] **8. Cài lỗi E04, E07, E08, E09**
  - Việc: `unit_price` dạng `"25.000"`; `quantity <= 0`; `product_id` không tồn tại; `order_date` năm 2027. Mỗi dòng chỉ mang một lỗi để `reject_reason` và đáp án không mơ hồ.
  - File: `data_generator/generate_data.py`, `data/raw/orders_2025_MM.csv`, `data/raw/order_items_2025_MM.csv`
  - Kiểm chứng: `cat data/raw/orders_2025_*.csv | cut -d, -f2 | grep -c 2027` lớn hơn 0; `cat data/raw/order_items_2025_*.csv | awk -F, '$4<=0 && NR>1' | wc -l` lớn hơn 0.

- [ ] **9. Cài lỗi E01**
  - Việc: tạo `orders_2025_06_resend.csv` và `order_items_2025_06_resend.csv`, nội dung y hệt tháng 6.
  - File: `data_generator/generate_data.py`, `data/raw/orders_2025_06_resend.csv`, `data/raw/order_items_2025_06_resend.csv`
  - Kiểm chứng: `cmp data/raw/orders_2025_06.csv data/raw/orders_2025_06_resend.csv` và `cmp data/raw/order_items_2025_06.csv data/raw/order_items_2025_06_resend.csv` không báo khác.

- [ ] **10. Ghi `expected_counts.json`**
  - Việc: tính từ dữ liệu sạch gốc, không chạy pipeline. Cấu trúc gồm 3 khóa: `errors` (E01–E10), `tables` (`"bronze.orders": n`, ... cho mọi bảng), `business` (tổng doanh thu, số khách quay lại).
  - File: `data_generator/generate_data.py`, `data/_expected/expected_counts.json`
  - Kiểm chứng: `python -c "import json; d=json.load(open('data/_expected/expected_counts.json')); assert set(d)=={'errors','tables','business'}; assert len(d['errors'])==10; print(len(d['tables']))"` in 19 (5 Bronze, 7 Silver, 7 Gold).

- [ ] **11. Xác nhận generator tất định**
  - Việc: chạy lại hai lần, sửa mọi nguồn không tất định nếu có; kiểm tra thủ công vài file.
  - File: `data_generator/generate_data.py`
  - Kiểm chứng: lệnh `h()` trong `CLAUDE.md` in `DETERMINISTIC`; chạy với `--seed 7 --out <thư mục scratchpad>` ra dữ liệu khác seed 42.

### Giai đoạn 2: Khung chạy

- [ ] **12. `notebooks/00_config.py`**
  - Việc: `IS_DATABRICKS`, `CATALOG`, `RAW_PATH`, `tbl()`, SparkSession có Delta khi local (dùng `configure_spark_with_delta_pip`, `enableHiveSupport`, không gọi `sparkContext`).
  - File: `notebooks/00_config.py`
  - Kiểm chứng: `python -c "exec(open('notebooks/00_config.py').read()); print(tbl('silver','orders'), spark.version)"` in `silver.orders 4.0.1`.

- [ ] **13. `notebooks/01_setup.py`** (nhóm A)
  - Việc: tạo schema `landing`, `bronze`, `silver`, `gold`; volume `landing.raw` chỉ trên Databricks; cell markdown hướng dẫn upload, gồm cả `expected_counts.json`.
  - File: `notebooks/01_setup.py`
  - Kiểm chứng: `python -c "exec(open('notebooks/00_config.py').read()); exec(open('notebooks/01_setup.py').read()); spark.sql('SHOW SCHEMAS').show()"` liệt kê đủ 4 schema.

- [ ] **14. `local/run_local.py`** (nhóm A)
  - Việc: chạy notebook bằng `exec` trong cùng namespace; tham số `--clean`, `--twice`, và thêm `--until NN` để dừng sau notebook NN khi kiểm chứng từng phần. `--clean` chỉ xóa warehouse/metastore.
  - File: `local/run_local.py`
  - Kiểm chứng: `python local/run_local.py --clean --until 01` exit code 0; `data/raw` còn nguyên.

- [ ] **15. `local/check_layer.py`** (nhóm A)
  - Việc: so số dòng bảng thật với `tables` trong `expected_counts.json`, lọc theo tiền tố tên bảng, exit khác 0 khi lệch. Cập nhật mục "Cấu trúc thư mục" trong `CLAUDE.md`.
  - File: `local/check_layer.py`, `CLAUDE.md`
  - Kiểm chứng: `python local/check_layer.py bronze; echo $?` in khác 0 khi Bronze chưa tồn tại, không crash bằng traceback.

### Giai đoạn 3: Bronze

- [ ] **16. Nạp các nguồn CSV**
  - Việc: đọc `stores`, `customers*`, `orders*`, `order_items*` toàn string; thêm `_ingested_at`, `_source_file`; ghi append.
  - File: `notebooks/02_bronze.py`
  - Kiểm chứng: `python local/run_local.py --clean --until 02 && python local/check_layer.py bronze.orders && python local/check_layer.py bronze.order_items`.

- [ ] **17. Nạp `products.jsonl`**
  - Việc: khai báo schema, giữ cấu trúc lồng `price`.
  - File: `notebooks/02_bronze.py`
  - Kiểm chứng: `python local/run_local.py --clean --until 02 && python local/check_layer.py bronze`.

- [ ] **18. Idempotent theo file**
  - Việc: lấy danh sách `_source_file` đã nạp, chỉ nạp file mới.
  - File: `notebooks/02_bronze.py`
  - Kiểm chứng: `python local/run_local.py --clean --until 02 --twice && python local/check_layer.py bronze`.

### Giai đoạn 4: Silver

Mỗi bước xử lý một cell.

- [ ] **19. `silver.stores`**
  - Việc: snake_case, ép kiểu, chuẩn hóa thành phố (E05), MERGE.
  - File: `notebooks/03_silver.py`
  - Kiểm chứng: `python local/run_local.py --clean --until 03 && python local/check_layer.py silver.stores`.

- [ ] **20. `silver.customers`**
  - Việc: chuẩn hóa E05, lấy bản mới nhất theo `customer_id`, MERGE SCD Type 1. Tiêu chí phụ khi trùng `_ingested_at`: `_source_file` giảm dần.
  - File: `notebooks/03_silver.py`
  - Kiểm chứng: như task 19 với `silver.customers`.

- [ ] **21. `silver.products`**
  - Việc: chuẩn hóa `category` (E06), làm phẳng `price.amount` thành `unit_list_price`, MERGE.
  - File: `notebooks/03_silver.py`
  - Kiểm chứng: như task 19 với `silver.products`.

- [ ] **22. Ép kiểu orders**
  - Việc: parse `order_date` 3 định dạng (E03), `01/03/2025` hiểu là `dd/MM/yyyy`. Thêm `assert` không dòng nào parse ra null.
  - File: `notebooks/03_silver.py`
  - Kiểm chứng: `python local/run_local.py --clean --until 03` exit code 0.

- [ ] **23. Khử trùng orders**
  - Việc: `row_number()` theo `order_id`, ưu tiên `_ingested_at` mới nhất, tiêu chí phụ `_source_file` (E01, E02). Giữ `customer_id` rỗng (E10).
  - File: `notebooks/03_silver.py`
  - Kiểm chứng: thêm `assert` trong cell: số dòng bằng số `order_id` phân biệt; `python local/run_local.py --clean --until 03` exit code 0.

- [ ] **24. Quarantine và MERGE orders**
  - Việc: tách E09 sang `silver.orders_rejected` (overwrite, có `reject_reason`); MERGE phần hợp lệ vào `silver.orders`.
  - File: `notebooks/03_silver.py`
  - Kiểm chứng: `python local/run_local.py --clean --until 03 && python local/check_layer.py silver.orders`.

- [ ] **25. Ép kiểu order_items**
  - Việc: ép `unit_price` (E04), `quantity`, `discount`; thêm `assert` không dòng nào ép ra null.
  - File: `notebooks/03_silver.py`
  - Kiểm chứng: `python local/run_local.py --clean --until 03` exit code 0.

- [ ] **26. Khử trùng order_items**
  - Việc: `row_number()` theo `order_id + line_no` (E01).
  - File: `notebooks/03_silver.py`
  - Kiểm chứng: `assert` trong cell: số dòng bằng số khóa phân biệt; `python local/run_local.py --clean --until 03` exit code 0.

- [ ] **27. Quarantine và MERGE order_items**
  - Việc: tách E07, E08 và toàn bộ item của đơn E09 sang `silver.order_items_rejected`; MERGE phần hợp lệ vào `silver.order_items`.
  - File: `notebooks/03_silver.py`
  - Kiểm chứng: `python local/run_local.py --clean --until 03 && python local/check_layer.py silver.order_items`.

- [ ] **28. In số dòng và xác nhận idempotent Silver**
  - Việc: cell cuối in số dòng mỗi bảng.
  - File: `notebooks/03_silver.py`
  - Kiểm chứng: `python local/run_local.py --clean --until 03 --twice && python local/check_layer.py silver`.

### Giai đoạn 5: Gold

- [ ] **29. `dim_date` và `dim_store`**
  - Việc: `dim_date` đủ 365 ngày; surrogate key bằng `row_number()` theo natural key; cell markdown giải thích lý do rebuild.
  - File: `notebooks/04_gold.py`
  - Kiểm chứng: `python local/run_local.py --clean --until 04 && python local/check_layer.py gold.dim_date && python local/check_layer.py gold.dim_store`.

- [ ] **30. `dim_customer` và `dim_product`**
  - Việc: thêm dòng `customer_key = -1`, "Khách vãng lai".
  - File: `notebooks/04_gold.py`
  - Kiểm chứng: như task 29 với `gold.dim_customer`, `gold.dim_product`.

- [ ] **31. `fact_sales`**
  - Việc: dựng fact grain một dòng sản phẩm trong một đơn; `customer_id` rỗng map sang `-1`; tính `revenue`. Thêm `assert` số dòng fact bằng `silver.order_items`.
  - File: `notebooks/04_gold.py`
  - Kiểm chứng: như task 29 với `gold.fact_sales`.

- [ ] **32. Hai bảng agg**
  - Việc: `agg_monthly_store_revenue` (doanh thu, số đơn, AOV), `agg_category_city_revenue`.
  - File: `notebooks/04_gold.py`
  - Kiểm chứng: `python local/run_local.py --clean --until 04 --twice && python local/check_layer.py gold`.

### Giai đoạn 6: Serving và Validation

- [ ] **33. `sql/business_questions.sql`** (nhóm B, luồng 1)
  - Việc: 5 truy vấn trên Gold; câu 5 loại khách vãng lai.
  - File: `sql/business_questions.sql`
  - Kiểm chứng: `grep -c '^-- Q[1-5]' sql/business_questions.sql` ra 5.

- [ ] **34. `notebooks/05_serving.py`** (nhóm B, luồng 1, sau 33)
  - Việc: chạy từng truy vấn qua `spark.sql` và in kết quả; cell markdown cuối hướng dẫn tạo dashboard.
  - File: `notebooks/05_serving.py`, `sql/business_questions.sql`
  - Kiểm chứng: `python local/run_local.py --clean --until 05` exit code 0 và in đủ 5 kết quả.

- [ ] **35. `06_validation`: đối chiếu đáp án** (nhóm B, luồng 2)
  - Việc: so mọi bảng với `tables`, kiểm tra `business`, in bảng so sánh, `assert` khi lệch. Trên Databricks đọc file đáp án từ Volume.
  - File: `notebooks/06_validation.py`
  - Kiểm chứng: `python local/run_local.py --clean` exit code 0.

- [ ] **36. `06_validation`: demo time travel** (nhóm B, luồng 2, sau 35)
  - Việc: `DESCRIBE HISTORY`, cố tình xóa một tháng, đọc `VERSION AS OF`, `RESTORE`, `assert` số dòng trở lại đúng.
  - File: `notebooks/06_validation.py`
  - Kiểm chứng: `python local/run_local.py --clean` exit code 0.

- [ ] **37. Kiểm tra toàn pipeline**
  - Việc: chạy chế độ `--twice` trọn vẹn; sửa mọi vi phạm API cấm.
  - File: `notebooks/*.py`, `local/*.py`
  - Kiểm chứng: `python local/run_local.py --clean --twice` exit code 0; `grep -nE 'sparkContext|\.rdd\b|\.cache\(|\.persist\(|COPY INTO|cloudFiles|import dlt' notebooks/*.py local/*.py` không ra dòng nào.

### Giai đoạn 7: Tài liệu và bản học viên

- [ ] **38. `README.md`** (nhóm C)
  - Việc: mục tiêu, cấu trúc thư mục, cách chạy trên Databricks Free Edition, cách chạy local, phiên bản thư viện.
  - File: `README.md`
  - Kiểm chứng: `grep -cE '^#+ ' README.md` lớn hơn hoặc bằng 5; `grep -E 'pyspark|delta-spark|Volume' README.md` có kết quả.

- [ ] **39. Sao chép notebook giữ nguyên sang bản học viên**
  - Việc: chép `00_config`, `01_setup`, `06_validation` sang `notebooks_student/`.
  - File: `notebooks_student/00_config.py`, `notebooks_student/01_setup.py`, `notebooks_student/06_validation.py`
  - Kiểm chứng: `for f in 00_config 01_setup 06_validation; do cmp notebooks/$f.py notebooks_student/$f.py; done` không báo khác.

- [ ] **40. Bản học viên `02_bronze`** (nhóm C)
  - Việc: thay phần lọc file chưa nạp và đọc nguồn bằng TODO có gợi ý.
  - File: `notebooks_student/02_bronze.py`
  - Kiểm chứng: `python -m py_compile notebooks_student/02_bronze.py`; `grep -c '# TODO' notebooks_student/02_bronze.py` từ 3 đến 6.

- [ ] **41. Bản học viên `03_silver`** (nhóm C)
  - Việc: TODO cho ép kiểu, chuẩn hóa, khử trùng window, quarantine, MERGE.
  - File: `notebooks_student/03_silver.py`
  - Kiểm chứng: như task 40 với `03_silver`.

- [ ] **42. Bản học viên `04_gold`** (nhóm C)
  - Việc: TODO cho surrogate key, dựng fact, agg.
  - File: `notebooks_student/04_gold.py`
  - Kiểm chứng: như task 40 với `04_gold`.

- [ ] **43. Bản học viên `05_serving`** (nhóm C)
  - Việc: TODO cho các truy vấn nghiệp vụ.
  - File: `notebooks_student/05_serving.py`
  - Kiểm chứng: như task 40 với `05_serving`.

- [ ] **44. `REPORT.md`**
  - Việc: chạy lại lần cuối để lấy số; ghi bảng số dòng so với kỳ vọng, kết quả 5 câu hỏi, phiên bản Python/PySpark/delta-spark, quyết định thiết kế, điểm chưa kiểm chứng trên Databricks.
  - File: `REPORT.md`
  - Kiểm chứng: `python local/run_local.py --clean --twice` exit code 0; `grep -cE '^#+ ' REPORT.md` lớn hơn hoặc bằng 5; `grep -c 'Databricks' REPORT.md` lớn hơn 0.

## 5. Vướng mắc

