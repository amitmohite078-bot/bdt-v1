#!/usr/bin/env python3
"""Hadoop Streaming Reducer for Wearable Telemetry Deduplication & Quarantine Counting.

Receives sorted (key \t value) pairs from Hadoop Shuffle/Sort.
For identical (user_id|normalized_ts) keys, keeps the first clean record and
counts duplicate multi-device syncs as quarantined DQ-UNQ-001.
"""

from __future__ import annotations

import sys


def main() -> None:
    last_key: str | None = None
    clean_count = 0
    quarantine_count = 0
    dup_count = 0

    for line in sys.stdin:
        line = line.rstrip("\n")
        if "\t" not in line:
            continue
        key, _payload = line.split("\t", 1)

        if key.startswith("QUARANTINE|"):
            quarantine_count += 1
            continue

        if key == last_key:
            dup_count += 1
            quarantine_count += 1
        else:
            last_key = key
            clean_count += 1

    sys.stdout.write(f"SUMMARY\tclean={clean_count}\tquarantined={quarantine_count}\tduplicates={dup_count}\n")


if __name__ == "__main__":
    main()

