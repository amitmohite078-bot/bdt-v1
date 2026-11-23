#!/usr/bin/env python3
"""Hadoop Streaming Mapper for Wearable Telemetry Deduplication & DQ Filtering.

Reads raw JSONL telemetry from sys.stdin and emits composite key:
  (user_id + '|' + normalized_ts) \t JSON_payload
Invalid/corrupt lines are tagged with key prefix 'QUARANTINE|' so the reducer
can route them to the dead-letter bucket.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone


def normalize_ts(raw_ts: object) -> str | None:
    if raw_ts is None:
        return None
    if isinstance(raw_ts, (int, float)):
        dt = datetime.fromtimestamp(int(raw_ts) / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    s = str(raw_ts).strip()
    if s.isdigit():
        dt = datetime.fromtimestamp(int(s) / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return s


def main() -> None:
    for line in sys.stdin:
        raw = line.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            sys.stdout.write(f"QUARANTINE|DQ-SCH-001\t{raw[:120]}\n")
            continue

        uid = obj.get("user_id")
        ts = normalize_ts(obj.get("event_ts"))
        hr = obj.get("heart_rate_bpm")
        spo2 = obj.get("spo2_pct")
        steps = obj.get("steps")
        sleep_stage = obj.get("sleep_stage")

        if not uid or not ts:
            sys.stdout.write(f"QUARANTINE|DQ-CMP-001\t{raw}\n")
            continue
        if sleep_stage == "DEEP_SLEEP" and steps is not None and steps > 300:
            sys.stdout.write(f"QUARANTINE|DQ-XSIG-001\t{raw}\n")
            continue
        if hr is not None and (hr < 32 or hr > 220):
            sys.stdout.write(f"QUARANTINE|DQ-PHY-001\t{raw}\n")
            continue
        if spo2 is not None and (spo2 < 70.0 or spo2 > 100.0):
            sys.stdout.write(f"QUARANTINE|DQ-PHY-001\t{raw}\n")
            continue

        obj["normalized_ts"] = ts
        sys.stdout.write(f"{uid}|{ts}\t{json.dumps(obj)}\n")


if __name__ == "__main__":
    main()

