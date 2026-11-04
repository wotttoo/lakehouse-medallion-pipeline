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
    return [[c[k] for k in CUSTOMER_HEADER] for c in customers]


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

    stores = gen_stores(rng)
    write_csv(
        raw_dir / "stores.csv",
        ["store_id", "store_name", "city", "opened_date"],
        [[s["store_id"], s["store_name"], s["city"], s["opened_date"]] for s in stores],
    )

    customers, customers_update = gen_customers(rng)
    write_csv(raw_dir / "customers.csv", CUSTOMER_HEADER, customer_rows(customers))
    write_csv(raw_dir / "customers_update_2025_12.csv", CUSTOMER_HEADER,
              customer_rows(customers_update))
    print(f"Da ghi du lieu vao {raw_dir}")


if __name__ == "__main__":
    main()
