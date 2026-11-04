"""Chay toan bo pipeline tren may ca nhan.

Moi notebook la file Python hop le (dong # MAGIC la comment), nen ta exec lan luot
trong cung mot namespace, thay cho %run tren Databricks.

Vi du:
    python local/run_local.py --clean            # chay 00 -> 06
    python local/run_local.py --clean --twice    # chay 01 -> 04 hai lan roi moi validation
    python local/run_local.py --until 02         # dung sau notebook 02
"""
import argparse
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_DIR = ROOT / "notebooks"
CONFIG = "00_config"
PIPELINE = ["01_setup", "02_bronze", "03_silver", "04_gold"]
AFTER = ["05_serving", "06_validation"]

# Chi xoa trang thai Spark local, khong bao gio xoa du lieu
CLEAN_TARGETS = ["spark-warehouse", "metastore_db", "derby.log"]


def clean():
    for name in CLEAN_TARGETS:
        p = ROOT / name
        if p.is_dir():
            shutil.rmtree(p)
            print(f"[clean] Da xoa thu muc {name}/")
        elif p.exists():
            p.unlink()
            print(f"[clean] Da xoa file {name}")


def run_notebook(name, namespace):
    path = NOTEBOOK_DIR / f"{name}.py"
    print(f"\n{'=' * 70}\n>>> {name}\n{'=' * 70}")
    start = time.time()
    code = compile(path.read_text(encoding="utf-8"), str(path), "exec")
    exec(code, namespace)
    print(f"<<< {name} xong sau {time.time() - start:.1f}s")


def build_plan(twice, until):
    plan = [CONFIG] + PIPELINE + (PIPELINE if twice else []) + AFTER
    if until is None:
        return plan
    names = [n for n in [CONFIG] + PIPELINE + AFTER if n[:2] <= until]
    if not names or names[-1][:2] != until:
        sys.exit(f"Khong co notebook {until}")
    return [n for n in plan if n in names]


def main():
    ap = argparse.ArgumentParser(description="Chay pipeline Lakehouse o local")
    ap.add_argument("--clean", action="store_true", help="xoa warehouse/metastore local truoc khi chay")
    ap.add_argument("--twice", action="store_true", help="chay 01-04 hai lan de kiem tra idempotent")
    ap.add_argument("--until", metavar="NN", help="dung sau notebook NN, vi du 02")
    args = ap.parse_args()

    os.chdir(ROOT)  # duong dan trong notebook la tuong doi so voi goc project
    if args.clean:
        clean()

    plan = build_plan(args.twice, args.until)
    print("Thu tu chay:", " -> ".join(plan))
    namespace = {"__name__": "__main__"}
    start = time.time()
    for name in plan:
        run_notebook(name, namespace)
    print(f"\nPIPELINE OK sau {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
