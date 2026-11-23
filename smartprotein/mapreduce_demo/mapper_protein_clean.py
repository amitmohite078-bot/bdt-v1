#!/usr/bin/env python3
"""SmartProtein Stage 12: Hadoop Streaming Mapper for Food Composition Cleaning."""

from __future__ import annotations

import csv
import sys


def main() -> None:
    reader = csv.reader(sys.stdin)
    for row in reader:
        if not row or row[0] == "record_id" or len(row) < 14:
            continue
        food_name = row[2].strip()
        state = row[5].strip()
        unit = row[6].strip()
        try:
            prot = float(row[7])
            carbs = float(row[8])
            fat = float(row[9])
            kcal = float(row[12])
        except ValueError:
            continue

        e_unit = row[13].strip()
        mult = 2.0 if unit == "per_50g_serving" else 1.0
        div = 4.184 if e_unit == "kJ" else 1.0

        p_100 = prot * mult
        c_100 = carbs * mult
        f_100 = fat * mult
        k_100 = (kcal * mult) / div

        if not food_name or p_100 < 0 or p_100 > 100.0:
            continue
        atwater = 4.0 * p_100 + 4.0 * c_100 + 9.0 * f_100
        if abs(k_100 - atwater) > max(35.0, atwater * 0.22):
            continue

        sys.stdout.write(f"{food_name.lower()}|{state}\t{p_100:.2f}\n")


if __name__ == "__main__":
    main()

