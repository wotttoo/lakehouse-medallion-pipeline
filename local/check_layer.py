"""So so dong bang thuc te voi expected_counts.json, loc theo tien to ten bang.

Vi du:
    python local/check_layer.py bronze            # moi bang bronze.*
    python local/check_layer.py silver.orders     # dung mot bang
Exit code 0 khi khop het, 1 khi lech hoac bang chua ton tai.
"""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main():
    if len(sys.argv) != 2:
        sys.exit("Cach dung: python local/check_layer.py <tien_to>, vi du bronze hoac silver.orders")
    prefix = sys.argv[1]
    os.chdir(ROOT)

    ns = {"__name__": "__main__"}
    exec(compile((ROOT / "notebooks" / "00_config.py").read_text(encoding="utf-8"), "00_config", "exec"), ns)
    spark, tbl = ns["spark"], ns["tbl"]

    with open(ns["EXPECTED_PATH"], encoding="utf-8") as f:
        expected = json.load(f)["tables"]
    targets = {k: v for k, v in expected.items() if k == prefix or k.startswith(prefix + ".")}
    if not targets:
        print(f"Khong co bang nao trong dap an khop tien to '{prefix}'")
        return 1

    failed = 0
    print(f"\n{'bang':<36}{'thuc te':>10}{'ky vong':>10}  ket qua")
    for name, exp in targets.items():
        schema, table = name.split(".")
        full = tbl(schema, table)
        if not spark.catalog.tableExists(full):
            actual, ok = "-", False
        else:
            actual = spark.table(full).count()
            ok = actual == exp
        failed += not ok
        print(f"{name:<36}{actual:>10}{exp:>10}  {'OK' if ok else 'LECH'}")
    print(f"\n{len(targets) - failed}/{len(targets)} bang khop")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
