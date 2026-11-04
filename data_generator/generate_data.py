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
    print(f"Da ghi du lieu vao {raw_dir}")


if __name__ == "__main__":
    main()
