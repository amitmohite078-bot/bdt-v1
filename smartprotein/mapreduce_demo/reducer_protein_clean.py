#!/usr/bin/env python3
"""SmartProtein Stage 12: Hadoop Streaming Reducer for Mean Protein Calculation."""

from __future__ import annotations

import sys


def main() -> None:
    cur_key = None
    total = 0.0
    cnt = 0

    for line in sys.stdin:
        line = line.strip()
        if "\t" not in line:
            continue
        key, val_str = line.split("\t", 1)
        val = float(val_str)

        if cur_key is None:
            cur_key = key
        if key != cur_key:
            sys.stdout.write(f"{cur_key}\t{total / cnt:.2f}\t{cnt}\n")
            cur_key = key
            total = 0.0
            cnt = 0
        total += val
        cnt += 1

    if cur_key is not None and cnt > 0:
        sys.stdout.write(f"{cur_key}\t{total / cnt:.2f}\t{cnt}\n")


if __name__ == "__main__":
    main()

