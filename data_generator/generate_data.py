"""Sinh du lieu nguon cho chuoi cua hang MiniMart va file dap an.

Chi dung thu vien chuan. Cung seed thi output giong het tung byte.

Cach chay:
    python data_generator/generate_data.py [--seed 42] [--out data]
"""
import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

YEAR = 2025

# Ten thanh pho chuan (dang dung o Silver)
HN, HCM, DN = "Hà Nội", "TP.HCM", "Đà Nẵng"

# 10 cua hang: (store_id, ten, thanh pho chuan, ngay mo cua, trong so doanh so)
STORES = [
    ("S01", "MiniMart Hoàn Kiếm", HN, "2019-03-15", 1.30),
    ("S02", "MiniMart Cầu Giấy", HN, "2020-07-01", 1.15),
    ("S03", "MiniMart Hà Đông", HN, "2021-05-20", 0.95),
    ("S04", "MiniMart Long Biên", HN, "2022-09-10", 0.85),
    ("S05", "MiniMart Quận 1", HCM, "2018-11-11", 1.40),
    ("S06", "MiniMart Quận 3", HCM, "2019-08-08", 1.20),
    ("S07", "MiniMart Bình Thạnh", HCM, "2020-12-12", 1.00),
    ("S08", "MiniMart Thủ Đức", HCM, "2023-02-14", 0.90),
    ("S09", "MiniMart Hải Châu", DN, "2021-04-30", 0.60),
    ("S10", "MiniMart Sơn Trà", DN, "2023-06-01", 0.45),
]


HO = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ",
      "Đặng", "Bùi", "Đỗ", "Hồ", "Ngô", "Dương", "Lý"]
TEN_DEM = ["Văn", "Thị", "Minh", "Thu", "Ngọc", "Đức", "Quang", "Thanh",
           "Hoài", "Gia", "Bảo", "Khánh", "Anh", "Hữu", "Phương"]
TEN = ["An", "Bình", "Châu", "Dũng", "Giang", "Hà", "Hải", "Hạnh", "Hiếu",
       "Hoa", "Hùng", "Hương", "Khoa", "Lan", "Linh", "Long", "Mai", "Nam",
       "Nga", "Nhung", "Phong", "Phúc", "Quân", "Quỳnh", "Sơn", "Tâm",
       "Thảo", "Trang", "Trung", "Tú", "Tuấn", "Uyên", "Vân", "Vy", "Yến"]

# Bo dau tieng Viet de tao email ascii
_VN = "àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ"
_AS = "aaaaaaaaaaaaaaaaaeeeeeeeeeeeiiiiiooooooooooooooooouuuuuuuuuuuyyyyyd"
_ASCII = str.maketrans(_VN + _VN.upper(), _AS + _AS.upper())

CUSTOMER_CITY_WEIGHTS = [(HN, 0.40), (HCM, 0.45), (DN, 0.15)]
N_CUSTOMERS = 2000
N_UPDATE_CHANGED = 100
N_UPDATE_NEW = 50
CUSTOMER_HEADER = ["customer_id", "full_name", "email", "phone", "city", "signup_date"]


def pick_weighted(rng, pairs):
    values = [v for v, _ in pairs]
    weights = [w for _, w in pairs]
    return rng.choices(values, weights=weights, k=1)[0]


def random_phone(rng):
    return rng.choice(["090", "091", "093", "097", "098", "086"]) + f"{rng.randrange(10**7):07d}"


def random_date(rng, start, end):
    return start + timedelta(days=rng.randrange((end - start).days + 1))


def make_customer(rng, idx, signup_start, signup_end):
    name = f"{rng.choice(HO)} {rng.choice(TEN_DEM)} {rng.choice(TEN)}"
    slug = ".".join(name.translate(_ASCII).lower().split()[-2:])
    return {
        "customer_id": f"C{idx:05d}",
        "full_name": name,
        "email": f"{slug}{idx}@example.com",
        "phone": random_phone(rng),
        "city": pick_weighted(rng, CUSTOMER_CITY_WEIGHTS),
        "signup_date": random_date(rng, signup_start, signup_end).isoformat(),
    }


def gen_customers(rng):
    base = [
        make_customer(rng, i, date(2022, 1, 1), date(2024, 12, 31))
        for i in range(1, N_CUSTOMERS + 1)
    ]
    # Ban cap nhat thang 12: khach cu doi city/phone + khach moi
    changed_ids = sorted(rng.sample(range(N_CUSTOMERS), N_UPDATE_CHANGED))
    update = []
    for i in changed_ids:
        c = dict(base[i])
        kind = rng.choice(["city", "phone", "both"])
        if kind in ("city", "both"):
            c["city"] = rng.choice([x for x, _ in CUSTOMER_CITY_WEIGHTS if x != c["city"]])
        if kind in ("phone", "both"):
            c["phone"] = random_phone(rng)
        update.append(c)
    for i in range(N_CUSTOMERS + 1, N_CUSTOMERS + N_UPDATE_NEW + 1):
        update.append(make_customer(rng, i, date(2025, 12, 1), date(2025, 12, 31)))
    return base, update


def customer_rows(customers):
    # ghi gia tri tho (co loi) neu co, nguoc lai ghi gia tri sach
    return [[c.get(k + "_raw", c[k]) for k in CUSTOMER_HEADER] for c in customers]


# ---------------- Cai loi ----------------

# E05: cac cach viet khac cua ten thanh pho
CITY_VARIANTS = {
    HN: ["HN", "Ha Noi", "hà nội", "Hà Nội ", "ha noi"],
    HCM: ["HCM", "Sài Gòn", "TP HCM", "tp.hcm", "Hồ Chí Minh"],
    DN: ["Da Nang", "đà nẵng", "DN", " Đà Nẵng"],
}
STORE_CITY_ERRORS = 4        # so cua hang bi viet sai
CUSTOMER_CITY_ERROR_RATE = 0.03
PRODUCT_CATEGORY_ERRORS = 12  # E06


def inject_e05(rng, stores, customers, customers_update):
    count = 0
    for s in sorted(rng.sample(stores, STORE_CITY_ERRORS), key=lambda x: x["store_id"]):
        s["city_raw"] = rng.choice(CITY_VARIANTS[s["city"]])
        count += 1
    for c in customers + customers_update:
        if rng.random() < CUSTOMER_CITY_ERROR_RATE:
            c["city_raw"] = rng.choice(CITY_VARIANTS[c["city"]])
            count += 1
    return count


def inject_e06(rng, products):
    variants = [
        lambda c: " " + c,
        lambda c: c + "  ",
        lambda c: c.upper(),
        lambda c: c.lower(),
        lambda c: "  " + c.lower() + " ",
    ]
    picked = sorted(rng.sample(range(len(products)), PRODUCT_CATEGORY_ERRORS))
    for n, i in enumerate(picked):
        # bien the dau tien luon co khoang trang dau dong de de kiem tra
        f = variants[0] if n == 0 else rng.choice(variants)
        products[i]["category_raw"] = f(products[i]["category"])
    return len(picked)


E02_RATE = 0.01   # dong trung order_id trong cung file
E03_RATE = 0.03   # order_date sai dinh dang chuan


def free_orders(orders):
    # don chua mang loi nao (moi dong chi mang mot loi)
    return [o for o in orders if "_err" not in o]


def inject_e02(rng, orders):
    picked = rng.sample(free_orders(orders), round(len(orders) * E02_RATE))
    for o in picked:
        o["_err"] = "E02"
        o["_copies"] = 2  # ghi 2 dong y het nhau trong cung file
    return len(picked)


def inject_e03(rng, orders):
    picked = rng.sample(free_orders(orders), round(len(orders) * E03_RATE))
    for o in picked:
        o["_err"] = "E03"
        d = o["order_date"]
        if rng.random() < 0.5:
            o["order_date_raw"] = d.strftime("%d/%m/%Y")
        else:
            o["order_date_raw"] = d.strftime("%Y/%m/%d")
    return len(picked)


E09_RATE = 0.005  # don co order_date nam 2027
E07_RATE = 0.01   # quantity <= 0
E08_RATE = 0.008  # product_id khong ton tai
E04_RATE = 0.03   # unit_price dang "25.000"


def inject_e09(rng, orders):
    pool = [o for o in free_orders(orders) if o["customer_id"] != ""]
    picked = rng.sample(pool, round(len(orders) * E09_RATE))
    for o in picked:
        o["_err"] = "E09"
        o["order_date_raw"] = o["order_date"].replace(year=2027).isoformat()
    return len(picked)


def free_items(orders):
    # chi cai loi dong hang vao don khong bi E09 va dong chua co loi
    return [it for o in orders if o.get("_err") != "E09"
            for it in o["items"] if "_err" not in it]


def inject_e07(rng, orders, n_items):
    picked = rng.sample(free_items(orders), round(n_items * E07_RATE))
    for it in picked:
        it["_err"] = "E07"
        it["quantity_raw"] = rng.choice([0, 0, -1, -2])
    return len(picked)


def inject_e08(rng, orders, n_items):
    picked = rng.sample(free_items(orders), round(n_items * E08_RATE))
    for it in picked:
        it["_err"] = "E08"
        it["product_id"] = f"P9{rng.randrange(1, 100):02d}"  # P901..P999 khong ton tai
    return len(picked)


def inject_e04(rng, orders, n_items):
    picked = rng.sample(free_items(orders), round(n_items * E04_RATE))
    for it in picked:
        it["_err"] = "E04"
        it["unit_price_raw"] = f"{it['unit_price']:,}".replace(",", ".")
    return len(picked)


def compute_expected(errors, stores, customers, customers_update, products, orders):
    """Tinh dap an truc tiep tu du lieu trong bo nho (khong chay pipeline)."""
    store_city = {s["store_id"]: s["city"] for s in stores}
    category = {p["product_id"]: p["category"] for p in products}
    june = [o for o in orders if o["month"] == 6]

    valid_orders = [o for o in orders if o.get("_err") != "E09"]
    valid_items = [(o, it) for o in valid_orders for it in o["items"]
                   if it.get("_err") not in ("E07", "E08")]
    n_items = sum(len(o["items"]) for o in orders)

    # Khach hang sau SCD Type 1: ban cap nhat de len ban goc
    final_customers = {c["customer_id"]: c for c in customers}
    final_customers.update({c["customer_id"]: c for c in customers_update})

    tables = {
        "bronze.stores": len(stores),
        "bronze.customers": len(customers) + len(customers_update),
        "bronze.products": len(products),
        "bronze.orders": sum(o.get("_copies", 1) for o in orders)
                         + sum(o.get("_copies", 1) for o in june),
        "bronze.order_items": n_items + sum(len(o["items"]) for o in june),
        "silver.stores": len(stores),
        "silver.customers": len(final_customers),
        "silver.products": len(products),
        "silver.orders": len(valid_orders),
        "silver.orders_rejected": len(orders) - len(valid_orders),
        "silver.order_items": len(valid_items),
        "silver.order_items_rejected": n_items - len(valid_items),
        "gold.dim_date": (date(YEAR, 12, 31) - date(YEAR, 1, 1)).days + 1,
        "gold.dim_customer": len(final_customers) + 1,  # + khach vang lai
        "gold.dim_product": len(products),
        "gold.dim_store": len(stores),
        "gold.fact_sales": len(valid_items),
        "gold.agg_monthly_store_revenue": len({(o["month"], o["store_id"]) for o, _ in valid_items}),
        "gold.agg_category_city_revenue": len({(category[it["product_id"]], store_city[o["store_id"]])
                                               for o, it in valid_items}),
    }

    # Gia tri nghiep vu
    revenue_by_order, revenue_by_city, revenue_by_product = {}, {}, {}
    for o, it in valid_items:
        r = it["quantity"] * it["unit_price"] - it["discount"]
        revenue_by_order[o["order_id"]] = revenue_by_order.get(o["order_id"], 0) + r
        city = store_city[o["store_id"]]
        revenue_by_city[city] = revenue_by_city.get(city, 0) + r
        revenue_by_product[it["product_id"]] = revenue_by_product.get(it["product_id"], 0) + r
    order_customer = {o["order_id"]: o["customer_id"] for o, _ in valid_items}
    orders_per_customer = {}
    for cid in order_customer.values():
        if cid != "":
            orders_per_customer[cid] = orders_per_customer.get(cid, 0) + 1
    returning = sum(1 for n in orders_per_customer.values() if n >= 2)
    total_revenue = sum(revenue_by_order.values())
    customers_by_city = {}
    for c in final_customers.values():
        customers_by_city[c["city"]] = customers_by_city.get(c["city"], 0) + 1

    business = {
        "total_revenue": total_revenue,
        "total_orders": len(revenue_by_order),
        "guest_orders": sum(1 for cid in order_customer.values() if cid == ""),
        "customers_with_orders": len(orders_per_customer),
        "returning_customers": returning,
        "returning_rate": round(returning / len(orders_per_customer), 4),
        "aov": round(total_revenue / len(revenue_by_order), 2),
        "top_product_id": max(sorted(revenue_by_product), key=lambda p: revenue_by_product[p]),
        "revenue_by_city": dict(sorted(revenue_by_city.items())),
        "silver_customers_by_city": dict(sorted(customers_by_city.items())),
    }
    return {
        "errors": {k: errors[k] for k in sorted(errors)},
        "tables": tables,
        "business": business,
    }


def count_e10(orders):
    # khach vang lai da sinh san trong du lieu sach, o day chi dem
    return sum(1 for o in orders if o["customer_id"] == "")


# Danh muc chuan -> (mat hang mau, khoang gia VND)
CATEGORIES = {
    "Đồ Uống": (["Nước suối", "Trà xanh", "Cà phê sữa lon", "Nước ngọt có ga",
                 "Nước cam ép", "Trà sữa đóng chai", "Nước tăng lực"], (6000, 30000)),
    "Bánh Kẹo": (["Bánh quy bơ", "Kẹo dẻo", "Bánh gạo", "Socola thanh",
                  "Bánh bông lan", "Snack khoai tây"], (5000, 60000)),
    "Mì Gói": (["Mì tôm chua cay", "Phở ăn liền", "Miến trộn", "Hủ tiếu ăn liền",
                "Mì xào khô"], (4000, 15000)),
    "Sữa": (["Sữa tươi tiệt trùng", "Sữa chua uống", "Sữa đặc", "Sữa hạt",
             "Sữa chua ăn"], (7000, 45000)),
    "Gia Vị": (["Nước mắm", "Dầu ăn", "Hạt nêm", "Tương ớt", "Nước tương",
                "Muối tiêu"], (10000, 80000)),
    "Đồ Gia Dụng": (["Khăn giấy", "Nước rửa chén", "Túi rác", "Bột giặt",
                     "Nước lau sàn"], (15000, 120000)),
    "Chăm Sóc Cá Nhân": (["Dầu gội", "Sữa tắm", "Kem đánh răng", "Bàn chải",
                          "Khăn ướt"], (12000, 150000)),
    "Đồ Đông Lạnh": (["Xúc xích", "Há cảo", "Chả giò", "Kem que",
                      "Cá viên"], (20000, 90000)),
}
BRANDS = ["Sao Mai", "Hồng Hà", "An Phát", "Bình Minh", "Thiên Long",
          "Phú Quý", "Việt Xanh", "Hương Quê"]
SIZES = ["nhỏ", "vừa", "lớn", "gói gia đình", "combo 3"]
N_PRODUCTS = 150


def gen_products(rng):
    cat_names = list(CATEGORIES)
    seen = set()
    products = []
    while len(products) < N_PRODUCTS:
        cat = rng.choice(cat_names)
        items, (lo, hi) = CATEGORIES[cat]
        name = f"{rng.choice(items)} {rng.choice(BRANDS)} {rng.choice(SIZES)}"
        if name in seen:
            continue
        seen.add(name)
        products.append({
            "product_id": f"P{len(products) + 1:03d}",
            "product_name": name,
            "category": cat,
            "price": {"amount": rng.randrange(lo, hi + 1, 500), "currency": "VND"},
            "is_active": rng.random() < 0.95,
        })
    # Trong so ban long-tail: thu hang ngau nhien, trong so ~ 1/rank^1.1
    ranks = list(range(1, N_PRODUCTS + 1))
    rng.shuffle(ranks)
    for p, r in zip(products, ranks):
        p["_weight"] = 1.0 / (r ** 1.1)
    return products


N_ORDERS = 20000
GUEST_RATE = 0.05  # E10: khach vang lai, khong phai loi
PAYMENT_METHODS = [("cash", 0.45), ("card", 0.25), ("momo", 0.20), ("bank_transfer", 0.10)]
ORDER_HEADER = ["order_id", "order_date", "customer_id", "store_id", "payment_method"]
ITEM_HEADER = ["order_id", "line_no", "product_id", "quantity", "unit_price", "discount"]


def day_weight(d):
    w = 1.0
    if d.weekday() >= 5:      # thu 7, chu nhat
        w *= 1.4
    if d.month in (1, 2):     # mua Tet
        w *= 1.5
    if d.month == 12:         # cuoi nam
        w *= 1.15
    return w


def weighted_sample_distinct(rng, items, weights, k):
    chosen = []
    while len(chosen) < k:
        x = rng.choices(items, weights=weights, k=1)[0]
        if x not in chosen:
            chosen.append(x)
    return chosen


def gen_orders(rng, customers, products):
    """Sinh don hang va dong hang SACH. Moi don: dict co 'items' (list dict)."""
    days = [date(YEAR, 1, 1) + timedelta(days=i) for i in range(365)]
    order_days = sorted(rng.choices(days, weights=[day_weight(d) for d in days], k=N_ORDERS))

    store_ids = [s[0] for s in STORES]
    store_w = [s[4] for s in STORES]
    cust_ids = [c["customer_id"] for c in customers]
    cust_w = [rng.expovariate(1.0) ** 2 for _ in cust_ids]  # co khach mua nhieu, khach mua it
    prod_ids = [p["product_id"] for p in products]
    prod_w = [p["_weight"] for p in products]
    price = {p["product_id"]: p["price"]["amount"] for p in products}

    orders = []
    for i, d in enumerate(order_days, start=1):
        is_guest = rng.random() < GUEST_RATE
        o = {
            "order_id": f"O{i:06d}",
            "order_date": d,
            "customer_id": "" if is_guest else rng.choices(cust_ids, weights=cust_w, k=1)[0],
            "store_id": rng.choices(store_ids, weights=store_w, k=1)[0],
            "payment_method": pick_weighted(rng, PAYMENT_METHODS),
            "items": [],
        }
        n_lines = rng.choices([1, 2, 3, 4, 5], weights=[25, 30, 22, 14, 9], k=1)[0]
        for line_no, pid in enumerate(weighted_sample_distinct(rng, prod_ids, prod_w, n_lines), 1):
            qty = rng.choices([1, 2, 3, 4, 5, 6], weights=[50, 25, 12, 7, 4, 2], k=1)[0]
            unit_price = price[pid]
            discount = 0
            if rng.random() < 0.25:
                discount = max(1000, round(qty * unit_price * rng.uniform(0.03, 0.15) / 1000) * 1000)
                discount = min(discount, qty * unit_price - 1000)
                discount = max(discount, 0)
            o["items"].append({
                "order_id": o["order_id"], "line_no": line_no, "product_id": pid,
                "quantity": qty, "unit_price": unit_price, "discount": discount,
            })
        orders.append(o)
    return orders


def order_row(o):
    d = o["order_date"]
    return [o["order_id"], o.get("order_date_raw", d.isoformat() if isinstance(d, date) else d),
            o["customer_id"], o["store_id"], o["payment_method"]]


def item_row(it):
    return [it["order_id"], it["line_no"], it["product_id"],
            it.get("quantity_raw", it["quantity"]), it.get("unit_price_raw", it["unit_price"]),
            it["discount"]]


def write_month_files(raw_dir, orders):
    """Ghi 12 cap file thang. Moi don co the co nhieu dong (E02) qua key '_copies'."""
    for m in range(1, 13):
        month_orders = [o for o in orders if o["month"] == m]
        o_rows, i_rows = [], []
        for o in month_orders:
            for _ in range(o.get("_copies", 1)):
                o_rows.append(order_row(o))
            i_rows.extend(item_row(it) for it in o["items"])
        write_csv(raw_dir / f"orders_{YEAR}_{m:02d}.csv", ORDER_HEADER, o_rows)
        write_csv(raw_dir / f"order_items_{YEAR}_{m:02d}.csv", ITEM_HEADER, i_rows)


def write_jsonl(path, records):
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def count_data_lines(path):
    # so dong du lieu cua file CSV (tru header)
    with open(path, encoding="utf-8") as f:
        return sum(1 for _ in f) - 1


def write_csv(path, header, rows):
    # lineterminator co dinh de output tat dinh tren moi he dieu hanh
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def gen_stores(rng):
    return [
        {"store_id": sid, "store_name": name, "city": city, "opened_date": opened}
        for sid, name, city, opened, _ in STORES
    ]


def main():
    ap = argparse.ArgumentParser(description="Sinh du lieu MiniMart")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    raw_dir = Path(args.out) / "raw"
    exp_dir = Path(args.out) / "_expected"
    raw_dir.mkdir(parents=True, exist_ok=True)
    exp_dir.mkdir(parents=True, exist_ok=True)

    # 1. Sinh du lieu sach
    stores = gen_stores(rng)
    customers, customers_update = gen_customers(rng)
    products = gen_products(rng)
    orders = gen_orders(rng, customers, products)
    for o in orders:
        o["month"] = o["order_date"].month  # file thang theo ngay goc

    # 2. Cai loi co chu dich
    errors = {}
    errors["E05"] = inject_e05(rng, stores, customers, customers_update)
    errors["E06"] = inject_e06(rng, products)
    errors["E02"] = inject_e02(rng, orders)
    errors["E03"] = inject_e03(rng, orders)
    errors["E09"] = inject_e09(rng, orders)
    n_items = sum(len(o["items"]) for o in orders)
    errors["E07"] = inject_e07(rng, orders, n_items)
    errors["E08"] = inject_e08(rng, orders, n_items)
    errors["E04"] = inject_e04(rng, orders, n_items)
    errors["E10"] = count_e10(orders)

    # 3. Ghi file nguon
    write_csv(
        raw_dir / "stores.csv",
        ["store_id", "store_name", "city", "opened_date"],
        [[s["store_id"], s["store_name"], s.get("city_raw", s["city"]), s["opened_date"]]
         for s in stores],
    )
    write_csv(raw_dir / "customers.csv", CUSTOMER_HEADER, customer_rows(customers))
    write_csv(raw_dir / "customers_update_2025_12.csv", CUSTOMER_HEADER,
              customer_rows(customers_update))
    write_jsonl(raw_dir / "products.jsonl", [
        {"product_id": p["product_id"], "product_name": p["product_name"],
         "category": p.get("category_raw", p["category"]), "price": p["price"],
         "is_active": p["is_active"]}
        for p in products
    ])
    write_month_files(raw_dir, orders)

    # E01: nguon giao lai file thang 6, noi dung y het
    e01 = {}
    for prefix in ("orders", "order_items"):
        src = raw_dir / f"{prefix}_{YEAR}_06.csv"
        dst = raw_dir / f"{prefix}_{YEAR}_06_resend.csv"
        dst.write_bytes(src.read_bytes())
        e01[prefix] = count_data_lines(dst)
    errors["E01"] = e01

    # 4. Dap an doc lap
    expected = compute_expected(errors, stores, customers, customers_update, products, orders)
    with open(exp_dir / "expected_counts.json", "w", encoding="utf-8", newline="\n") as f:
        json.dump(expected, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"Da ghi du lieu vao {raw_dir} va {exp_dir}")
    print("So loi da cai:", expected["errors"])


if __name__ == "__main__":
    main()
