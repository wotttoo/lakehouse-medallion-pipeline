# Project: Bronze → Silver → Gold

Xây một pipeline Lakehouse 3 lớp theo **Medallion Architecture** cho chuỗi cửa hàng tiện lợi hư cấu **MiniMart**
(10 cửa hàng ở Hà Nội, TP.HCM, Đà Nẵng; dữ liệu bán hàng năm 2025), dùng **PySpark + Delta Lake + Unity Catalog**
trên **Databricks Free Edition**.

## Mục tiêu

Sau project, bạn có một pipeline hoàn chỉnh:

```
file nguồn (Volume) → Bronze (thô) → Silver (sạch) → Gold (star schema) → 5 câu hỏi kinh doanh + dashboard
```

và trả lời được 5 câu hỏi:

1. Doanh thu theo tháng của từng cửa hàng.
2. Top 10 sản phẩm theo doanh thu.
3. Doanh thu theo danh mục và thành phố.
4. Giá trị trung bình mỗi đơn hàng (AOV) theo cửa hàng.
5. Tỷ lệ khách hàng quay lại (mua từ 2 đơn trở lên), không tính khách vãng lai.

Dữ liệu nguồn được cài sẵn 10 loại "lỗi" thường gặp (E01–E10: file giao trùng, ngày lẫn định dạng, giá dạng `"25.000"`,
tên thành phố viết nhiều kiểu, số lượng âm, mã sản phẩm không tồn tại, ngày ở tương lai...). Pipeline của bạn phải xử lý
đúng từng loại. Notebook `06_validation` so kết quả với đáp án và báo ngay chỗ sai.

## Cấu trúc thư mục

```
README.md                     hướng dẫn này
requirements.txt              phiên bản thư viện cho chạy local
data_generator/
  generate_data.py            sinh dữ liệu nguồn và đáp án (chỉ dùng thư viện chuẩn Python)
data/
  raw/                        30 file nguồn: tải lên Volume
  _expected/
    expected_counts.json      đáp án: số lỗi, số dòng mọi bảng, giá trị nghiệp vụ
notebooks/                    bản lời giải (solution)
  00_config.py                cấu hình chung, hàm tbl()
  01_setup.py                 tạo schema, volume
  02_bronze.py                nạp dữ liệu thô, idempotent theo file
  03_silver.py                làm sạch, khử trùng, quarantine, MERGE
  04_gold.py                  star schema và bảng tổng hợp
  05_serving.py               5 truy vấn nghiệp vụ, hướng dẫn dashboard
  06_validation.py            đối chiếu đáp án, demo time travel
notebooks_student/            bản dành cho học viên, có chỗ # TODO
sql/
  business_questions.sql      5 truy vấn, dùng cho SQL Editor và dashboard
local/
  run_local.py                chạy toàn pipeline trên máy cá nhân
  check_layer.py              so số dòng một lớp/bảng với đáp án
REPORT.md                     báo cáo kết quả chạy bản solution
```

## Dữ liệu nguồn

| File | Bảng Bronze | Ghi chú |
|---|---|---|
| `stores.csv` | `bronze.stores` | 10 cửa hàng |
| `customers.csv`, `customers_update_2025_12.csv` | `bronze.customers` | 2.000 khách + bản cập nhật tháng 12 (100 khách đổi thông tin, 50 khách mới) |
| `products.jsonl` | `bronze.products` | 150 sản phẩm, giá lồng `price: {amount, currency}` |
| `orders_2025_MM.csv` ×12 + `orders_2025_06_resend.csv` | `bronze.orders` | ~20.000 đơn |
| `order_items_2025_MM.csv` ×12 + `order_items_2025_06_resend.csv` | `bronze.order_items` | ~50.000 dòng hàng |

Doanh thu một dòng: `revenue = quantity * unit_price - discount` (VND).

## Chạy trên Databricks Free Edition

1. **Tạo workspace** Databricks Free Edition và đăng nhập. Mọi thứ chạy trên serverless compute, catalog `workspace`.
2. **Import notebook**: vào **Workspace** → thư mục của bạn → **Import** → chọn các file `.py` trong `notebooks_student/`
   (hoặc `notebooks/` để xem lời giải). Databricks nhận ra định dạng *Databricks source* và chuyển thành notebook.
   Các notebook phải nằm **cùng một thư mục** để `%run ./00_config` hoạt động.
   Cách khác: dùng **Git folder** trỏ tới repo này.
3. **Chạy `01_setup`** để tạo schema `landing`, `bronze`, `silver`, `gold` và volume `landing.raw`.
4. **Tải dữ liệu lên Volume** (cell markdown cuối `01_setup` có hướng dẫn chi tiết):
   - Mở **Catalog** → `workspace` → `landing` → `raw` → **Upload to this volume**, chọn toàn bộ file trong `data/raw/`.
   - Tạo thư mục `_expected` trong volume, tải `data/_expected/expected_counts.json` vào đó.
   - Kết quả: `/Volumes/workspace/landing/raw/orders_2025_01.csv`, `/Volumes/workspace/landing/raw/_expected/expected_counts.json`.
5. **Chạy lần lượt** `02_bronze` → `03_silver` → `04_gold` → `05_serving` → `06_validation`.
6. `06_validation` in bảng so sánh và dừng ở `assert` đầu tiên bị lệch. Khi mọi thứ đúng, dòng cuối là `TAT CA KIEM TRA DA PASS`.
7. Thử chạy lại `02_bronze` → `04_gold` lần nữa rồi chạy `06_validation`: số dòng phải giữ nguyên (pipeline **idempotent**).
8. Tạo dashboard từ `sql/business_questions.sql` theo hướng dẫn ở cuối `05_serving`.

Ràng buộc của Free Edition mà code đã tuân theo: chỉ Python và SQL, không RDD, không `sparkContext`, không `cache()`,
không tạo catalog mới, không Auto Loader, `COPY INTO`, Lakeflow/DLT hay Jobs.

## Chạy local (dự phòng)

Cần Python 3.13 và Java 17 hoặc 21. Trên Ubuntu/Debian:

```bash
sudo apt-get install -y python3 python3-venv openjdk-21-jdk-headless
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Chạy từ thư mục gốc project (đường dẫn có dấu cách thì đặt trong ngoặc kép):

```bash
python data_generator/generate_data.py          # sinh lại data/raw và data/_expected (seed 42)
python local/run_local.py --clean               # chạy 00 -> 06
python local/run_local.py --clean --twice       # chạy 01 -> 04 hai lần rồi mới validation
python local/run_local.py --until 03            # dừng sau notebook 03
python local/check_layer.py silver              # so số dòng các bảng silver.* với đáp án
```

- Local không có Unity Catalog: bảng dùng tên 2 cấp `schema.table`, dữ liệu đọc thẳng từ `data/raw`.
- `--clean` chỉ xóa `spark-warehouse/`, `metastore_db/`, `derby.log`, không đụng tới `data/`.
- Lần chạy đầu tiên cần internet để tải JAR của Delta Lake.
- Chạy bản học viên ở local: `python local/run_local.py --clean --notebooks notebooks_student`.

## Phiên bản thư viện

| Thành phần | Phiên bản |
|---|---|
| Python | 3.13 |
| Java | 21 (17 cũng được) |
| pyspark | 4.0.1 |
| delta-spark | 4.0.0 |
| Bộ sinh dữ liệu | chỉ thư viện chuẩn Python |

## Sinh lại dữ liệu

```bash
python data_generator/generate_data.py --seed 42 --out data
```

Cùng seed thì output giống hệt từng byte. Đổi `--seed` để có bộ dữ liệu khác (đáp án `expected_counts.json` cũng được tính lại tương ứng).
