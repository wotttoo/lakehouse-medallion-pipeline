# REPORT: kết quả chạy bản solution

Báo cáo lần chạy cuối của `python local/run_local.py --clean --twice` (pipeline 01–04 chạy hai lần liên tiếp,
sau đó 05_serving và 06_validation). Kết quả: **exit code 0, mọi `assert` trong `06_validation` pass**
(dòng cuối `TAT CA KIEM TRA DA PASS`), tổng thời gian khoảng 4 phút trên máy 4 nhân.

## Môi trường đã dùng

| Thành phần | Phiên bản |
|---|---|
| Python | 3.13.18 |
| Java | OpenJDK 21.0.12 |
| pyspark | 4.0.1 |
| delta-spark | 4.0.0 (JAR `io.delta:delta-spark_2.13:4.0.0`) |
| Bộ sinh dữ liệu | chỉ thư viện chuẩn Python, seed 42 |

Đã chạy được ở local: Delta `MERGE`, `DESCRIBE HISTORY`, `VERSION AS OF`, `RESTORE`, `CREATE OR REPLACE TABLE ... AS SELECT`.
Chạy generator hai lần cho ra output giống hệt từng byte (lệnh kiểm tra `h()` trong `CLAUDE.md` in `DETERMINISTIC`).

## Số lỗi đã cài (seed 42)

| Mã | Lỗi | Số lượng |
|---|---|---|
| E01 | File tháng 6 giao lại | 1.436 dòng orders, 3.578 dòng order_items |
| E02 | Trùng `order_id` trong cùng file | 200 đơn |
| E03 | `order_date` sai định dạng chuẩn (`dd/MM/yyyy` hoặc `yyyy/MM/dd`) | 600 đơn |
| E04 | `unit_price` dạng `"25.000"` | 1.497 dòng hàng |
| E05 | Tên thành phố viết khác chuẩn | 75 dòng (4 cửa hàng + 71 dòng khách hàng) |
| E06 | `category` thừa khoảng trắng, lẫn hoa thường | 12 sản phẩm |
| E07 | `quantity <= 0` | 499 dòng hàng |
| E08 | `product_id` không tồn tại | 399 dòng hàng |
| E09 | `order_date` năm 2027 | 100 đơn (kéo theo 240 dòng hàng) |
| E10 | `customer_id` rỗng, khách vãng lai (không phải lỗi) | 1.010 đơn |

## Số dòng thực tế so với kỳ vọng

| Bảng | Thực tế | Kỳ vọng | Kết quả |
|---|---:|---:|---|
| bronze.stores | 10 | 10 | OK |
| bronze.customers | 2.150 | 2.150 | OK |
| bronze.products | 150 | 150 | OK |
| bronze.orders | 21.636 | 21.636 | OK |
| bronze.order_items | 53.490 | 53.490 | OK |
| silver.stores | 10 | 10 | OK |
| silver.customers | 2.050 | 2.050 | OK |
| silver.products | 150 | 150 | OK |
| silver.orders | 19.900 | 19.900 | OK |
| silver.orders_rejected | 100 | 100 | OK |
| silver.order_items | 48.774 | 48.774 | OK |
| silver.order_items_rejected | 1.138 | 1.138 | OK |
| gold.dim_date | 365 | 365 | OK |
| gold.dim_customer | 2.051 | 2.051 | OK |
| gold.dim_product | 150 | 150 | OK |
| gold.dim_store | 10 | 10 | OK |
| gold.fact_sales | 48.774 | 48.774 | OK |
| gold.agg_monthly_store_revenue | 120 | 120 | OK |
| gold.agg_category_city_revenue | 24 | 24 | OK |

Lần chạy thứ hai của 01–04 không làm thay đổi số dòng nào: Bronze báo `khong co file moi, bo qua` cho cả 5 bảng,
Silver MERGE lại cùng dữ liệu, Gold rebuild ra cùng kết quả.

Kiểm tra quarantine và nghiệp vụ trong `06_validation` (đều khớp):

| Kiểm tra | Giá trị |
|---|---|
| E09 trong `orders_rejected` / E07, E08 trong `order_items_rejected` | 100 / 499 / 399 |
| Đơn khách vãng lai giữ lại trong `silver.orders` | 1.010 |
| Tổng doanh thu cả năm sau làm sạch | 2.718.752.000 VND |
| Số đơn có doanh thu (trong `fact_sales`) | 19.796 |
| Số khách có mua / khách quay lại / tỷ lệ | 1.375 / 1.130 / 0,8218 |
| AOV toàn chuỗi | 137.338,45 VND |
| Doanh thu theo thành phố | Hà Nội 1.194.113.000 · TP.HCM 1.226.144.000 · Đà Nẵng 298.495.000 |
| Khách theo thành phố sau SCD Type 1 | Hà Nội 787 · TP.HCM 937 · Đà Nẵng 326 |
| Time travel | xóa nhầm tháng 6 làm mất 1.414 đơn; `VERSION AS OF` đọc lại đủ 19.900; `RESTORE` đưa về 19.900 |

## Kết quả 5 câu hỏi kinh doanh

**Q1. Doanh thu theo tháng của từng cửa hàng** (trích cửa hàng S01; đủ 120 dòng = 12 tháng × 10 cửa hàng)

| store_id | store_name | month | revenue | order_count |
|---|---|---:|---:|---:|
| S01 | MiniMart Hoàn Kiếm | 1 | 42.017.000 | 305 |
| S01 | MiniMart Hoàn Kiếm | 2 | 37.037.000 | 269 |
| S01 | MiniMart Hoàn Kiếm | 3 | 28.389.500 | 214 |
| S01 | MiniMart Hoàn Kiếm | 6 | 24.058.500 | 189 |
| S01 | MiniMart Hoàn Kiếm | 12 | 32.532.000 | 222 |

Tháng 1–2 (mùa Tết) cao hơn hẳn các tháng khác, đúng xu hướng đã cài trong dữ liệu.

**Q2. Top 10 sản phẩm theo doanh thu** (5 dòng đầu)

| product_id | product_name | category | quantity | revenue |
|---|---|---|---:|---:|
| P102 | Trà sữa đóng chai Việt Xanh nhỏ | Đồ Uống | 17.096 | 292.415.000 |
| P136 | Sữa tươi tiệt trùng Sao Mai gói gia đình | Sữa | 9.439 | 166.076.000 |
| P027 | Chả giò Phú Quý lớn | Đồ Đông Lạnh | 2.303 | 159.754.000 |
| P085 | Há cảo Việt Xanh gói gia đình | Đồ Đông Lạnh | 6.212 | 130.482.000 |
| P030 | Sữa tắm Hương Quê nhỏ | Chăm Sóc Cá Nhân | 946 | 117.981.000 |

**Q3. Doanh thu theo danh mục và thành phố** (trích; đủ 24 dòng = 8 danh mục × 3 thành phố)

| category | Hà Nội | TP.HCM | Đà Nẵng |
|---|---:|---:|---:|
| Đồ Đông Lạnh | 313.945.000 | 312.632.500 | 81.991.500 |
| Đồ Gia Dụng | 188.661.000 | 194.173.000 | 46.025.500 |
| Đồ Uống | 166.141.000 | 173.920.500 | 39.086.000 |
| Chăm Sóc Cá Nhân | 162.450.500 | 171.458.500 | 41.017.000 |
| Sữa | 158.199.500 | 155.151.000 | 40.421.000 |

**Q4. AOV theo cửa hàng** (5 dòng đầu, sắp theo AOV giảm dần)

| store_id | store_name | city | order_count | revenue | aov |
|---|---|---|---:|---:|---:|
| S04 | MiniMart Long Biên | Hà Nội | 1.730 | 244.122.500 | 141.111,27 |
| S09 | MiniMart Hải Châu | Đà Nẵng | 1.241 | 174.792.500 | 140.848,11 |
| S06 | MiniMart Quận 3 | TP.HCM | 2.507 | 350.897.000 | 139.966,89 |
| S10 | MiniMart Sơn Trà | Đà Nẵng | 892 | 123.702.500 | 138.679,93 |
| S03 | MiniMart Hà Đông | Hà Nội | 1.906 | 264.295.500 | 138.665,01 |

**Q5. Tỷ lệ khách hàng quay lại** (không tính khách vãng lai)

| customers_with_orders | returning_customers | returning_rate |
|---:|---:|---:|
| 1.375 | 1.130 | 0,8218 |

## Quyết định thiết kế tự đưa ra

Những điểm SPEC chưa quy định; mỗi điểm chọn phương án đơn giản nhất:

**Dữ liệu và generator**
1. `data/raw/` và `data/_expected/` (khoảng 2,4 MB) được commit vào repo để học viên không phải tự chạy generator.
2. Khách vãng lai (E10) được sinh ngay trong dữ liệu sạch (khoảng 5% đơn), vì đây là thuộc tính thật của nghiệp vụ, không phải lỗi; generator chỉ đếm.
3. Tỷ lệ lỗi giữ trong khoảng 0,5–3%, trừ hai bảng nhỏ: 4/10 cửa hàng có tên thành phố sai (E05) và 12/150 sản phẩm có category sai (E06, 8%). Với bảng nhỏ, 3% sẽ chỉ còn 0–4 dòng, quá ít để thấy được lỗi.
4. Mỗi dòng mang tối đa một lỗi: E02, E03, E09 không trùng nhau trên cùng một đơn; đơn E09 luôn có `customer_id`; E04, E07, E08 chỉ cài vào dòng hàng của đơn không bị E09 và không trùng nhau. Nhờ vậy `reject_reason` và đáp án không mơ hồ.
5. Dòng trùng E02 là hai dòng giống hệt nhau, nên bản nào được giữ lại cũng không ảnh hưởng kết quả.
6. Định dạng `01/03/2025` được hiểu là `dd/MM/yyyy` (cách viết ngày ở Việt Nam).
7. Category chuẩn viết hoa chữ đầu mỗi từ (`Đồ Uống`, `Chăm Sóc Cá Nhân`) để khớp với kết quả của `initcap` ở Silver.
8. File đáp án được đặt ở thư mục con `_expected/` trong volume `raw`, giống cấu trúc `data/` ở local; Bronze đọc theo mẫu tên file nên không đụng tới thư mục này.
9. Ngoài các khóa SPEC yêu cầu, `business` có thêm `aov`, `top_product_id`, `revenue_by_city`, `silver_customers_by_city`. Mục cuối dùng để kiểm chứng cả E05 lẫn SCD Type 1.

**Bronze và Silver**

10. Idempotent theo file: đọc lại toàn bộ file nguồn, bỏ các dòng có `_source_file` đã có trong bảng đích bằng `left_anti` join, rồi chỉ append phần còn lại. Cách này đơn giản và chạy được cả ở local; dữ liệu lớn hơn thì nên liệt kê file trước khi đọc.
11. Khử trùng theo `_ingested_at` giảm dần, rồi đến `_source_file` giảm dần. Các file nạp trong cùng một lần có cùng `_ingested_at`, nên tiêu chí phụ quyết định: file `_resend` thắng bản tháng 6 (hai bản giống hệt nhau), và `customers_update_2025_12.csv` thắng `customers.csv` (đúng tinh thần SCD Type 1).
12. Mỗi lần chạy, Silver tính lại từ toàn bộ Bronze rồi MERGE vào bảng đích. Không làm xử lý tăng dần, vì dữ liệu nhỏ.
13. Silver bỏ hai cột kỹ thuật `_ingested_at` và `_source_file` sau khi khử trùng.
14. Tiền VND không có phần lẻ nên dùng kiểu `bigint` (SPEC cho phép "decimal/long cho tiền").
15. Tên thành phố được chuẩn hóa bằng bảng ánh xạ tường minh `CITY_MAP` (sau `lower` + `trim`). Cách viết lạ sẽ ra NULL và bị `assert` chặn, thay vì lọt qua im lặng.
16. Quy tắc E09 là `order_date > 2025-12-31` (sau ngày cuối kỳ dữ liệu), không phải `> current_date()`. Nếu dùng ngày hiện tại, dữ liệu năm 2027 sẽ không còn bị coi là "tương lai" khi chạy bài từ năm 2027 trở đi.
17. `reject_reason` có dạng `"E0x: mô tả"`. Một dòng hàng chỉ nhận một lý do, xét theo thứ tự E09 → E07 → E08.
18. Ép kiểu dùng `try_cast` của SQL (qua `F.expr`) và `try_to_timestamp`, rồi `assert` không có NULL. Không dùng `Column.try_cast` vì API này chỉ có từ PySpark 4.0.

**Gold và Serving**

19. Dòng khách vãng lai trong `dim_customer` có `customer_key = -1` và `customer_id = NULL`. Trong `dim_date`, `day_of_week` đánh số 1 = thứ Hai … 7 = Chủ nhật.
20. `agg_monthly_store_revenue` có thêm cột `year`; `agg_category_city_revenue` tính theo **thành phố của cửa hàng**, có thêm cột `quantity`.
21. Các chỉ số nghiệp vụ (số đơn, AOV, khách quay lại) tính trên `fact_sales`, tức trên các đơn còn ít nhất một dòng hàng hợp lệ. Vì vậy số đơn là 19.796, ít hơn 19.900 trong `silver.orders`: 104 đơn hợp lệ nhưng mọi dòng hàng đều bị loại do E07/E08.
22. `05_serving` viết lại 5 truy vấn với `tbl()` để chạy được ở cả hai môi trường. `sql/business_questions.sql` dùng tên 3 cấp `workspace.gold.*` để dán thẳng vào SQL Editor. Hai bản cần được sửa đồng thời khi thay đổi.

**Công cụ local và bản học viên**

23. Có thêm `local/check_layer.py` và các tùy chọn `--until NN`, `--notebooks DIR` cho `run_local.py`, để kiểm chứng từng phần và chạy bản học viên.
24. Spark ở local dùng `spark.sql.shuffle.partitions=4`, nguồn mặc định là Delta và tắt UI để chạy nhanh hơn. Các cấu hình này chỉ áp dụng ở local.
25. Bản học viên thay phần lõi bằng `...` kèm `# TODO` (mỗi notebook có 4–6 chỗ). File vẫn compile được, nhưng chỗ chưa làm sẽ báo lỗi khi chạy.
26. Một số task trong `PLAN.md` cùng sửa một khối code liên tục nên được gộp commit: 20–21, 22–24, 25–28, 29–32, 40–43. Mỗi commit đều chạy kiểm chứng của các task trong nhóm trước khi commit.

## Điểm chưa kiểm chứng được trên Databricks thật

Toàn bộ kiểm chứng ở trên chạy ở local. Tác giả bài học cần chạy thử trên Databricks Free Edition để xác nhận:

1. **Quyền tạo schema và volume**: `CREATE SCHEMA IF NOT EXISTS workspace.landing|bronze|silver|gold` và `CREATE VOLUME IF NOT EXISTS workspace.landing.raw` trên tài khoản Free Edition.
2. **`_metadata.file_path` trên Volume**: định dạng thực tế (`/Volumes/...` hay `dbfs:/Volumes/...`). Logic idempotent chỉ so giá trị do chính cột này sinh ra nên không phụ thuộc định dạng, nhưng nên xem lại cho đúng khi trình bày trong bài.
3. **Đọc file đáp án bằng `open()`** của Python tại `/Volumes/workspace/landing/raw/_expected/expected_counts.json` trên serverless.
4. **Phiên bản runtime serverless**: các hàm `try_to_timestamp`, SQL `try_cast`, `MERGE ... UPDATE SET * / INSERT *`, `RESTORE TABLE` có sẵn từ Spark 3.5 và Databricks SQL, nhưng chưa chạy trên runtime thật.
5. **`spark.catalog.tableExists`** với tên 3 cấp `workspace.schema.table` trên serverless.
6. **Import notebook** định dạng Databricks source `.py` và `%run ./00_config` theo đường dẫn tương đối, khi import thủ công và khi dùng Git folder.
7. **Upload file UTF-8 tiếng Việt** qua giao diện Volume và đọc CSV không lỗi font (local đọc đúng với mã hóa mặc định UTF-8).
8. **`display(dbutils.fs.ls(...))`** ở cuối `01_setup` và `display(...)` trong `05_serving`, `06_validation`.
9. **`DESCRIBE HISTORY`, `DELETE`, `VERSION AS OF`, `RESTORE`** trên bảng managed của Unity Catalog: số phiên bản có thể khác local (Databricks có thể ghi thêm thao tác bảo trì), nên notebook lấy phiên bản lớn nhất thay vì hardcode số phiên bản.
10. **Hướng dẫn tạo dashboard** (tên menu **Dashboards**, **Data**, **Canvas**, **Publish**) viết theo giao diện AI/BI Dashboard hiện tại, cần đối chiếu lại với giao diện lúc ra bài.
11. Thời gian chạy trên serverless (ở local cả pipeline mất khoảng 2 phút mỗi lượt).
