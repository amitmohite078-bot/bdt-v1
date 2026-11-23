"""SmartProtein Stage 5: Kafka Speed-Layer Producers.

Streams real-time events to three Kafka topics:
  1. smartprotein.price_alerts     (flash store discounts & seasonal price drops)
  2. smartprotein.wearable_events  (live fitness tracker telemetry + deliberate DQ anomalies)
  3. smartprotein.crowd_flags      (crowdsourced nutrition correction votes)

Supports --mode dry-run (writes local JSONL sample streams) or --mode kafka (live broker).
"""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
SEED_PATH = ROOT_DIR / "config" / "reference_foods_seed.json"
STREAM_SAMPLE_DIR = ROOT_DIR / "data" / "raw" / "streaming_samples"


def build_price_event(idx: int, foods: list[dict], cities: list[tuple[str, str]]) -> dict:
    f = random.choice(foods)
    region, city = random.choice(cities)
    base = f["base_inr_100g"]
    discount_pct = random.choice([15, 22, 30, 40])
    new_price = round(base * (1.0 - discount_pct / 100.0), 2)

    # Inject 10% bad streaming price to test real-time DQ alert
    if random.random() < 0.10:
        new_price = -45.0

    return {
        "event_id": f"PRC-STRM-{idx:06d}",
        "store_id": f"STR-{random.randint(1, 400):05d}",
        "region": region,
        "city": city,
        "canonical_name": f["canonical_name"],
        "diet_type": f["diet_type"],
        "old_price_100g_inr": base,
        "new_price_100g_inr": new_price,
        "discount_pct": discount_pct,
        "in_stock": True,
        "event_ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def build_wearable_event(idx: int) -> dict:
    hr = random.randint(62, 168)
    steps = random.randint(400, 3200)
    sleep_hours = round(random.uniform(5.5, 8.5), 1)
    # Inject 10% real-time physiological/impossible spike for Speed-Layer DQ alert
    if random.random() < 0.10:
        hr = random.choice([295, 10])
    return {
        "event_id": f"WRB-STRM-{idx:06d}",
        "user_id": f"USR-{random.randint(1, 500):06d}",
        "heart_rate_bpm": hr,
        "steps_increment": steps,
        "workout_minutes": random.choice([0, 30, 45, 60]),
        "sleep_hours": sleep_hours,
        "event_ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def build_crowd_flag_event(idx: int) -> dict:
    return {
        "flag_id": f"FLG-STRM-{idx:06d}",
        "user_id": f"USR-{random.randint(1, 500):06d}",
        "user_trust_score": round(random.uniform(0.45, 0.96), 2),
        "record_id": f"FC-{random.randint(1, 15000):08d}",
        "field_name": random.choice(["protein_g", "diet_type", "price_value"]),
        "suggested_value": random.choice(["20.5", "Veg", "38.0"]),
        "submitted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="SmartProtein Kafka Speed-Layer Producer")
    parser.add_argument("--mode", choices=["dry-run", "kafka"], default="dry-run")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--count", type=int, default=60, help="Events per topic to produce")
    parser.add_argument("--delay-ms", type=int, default=0, help="Optional inter-message delay")
    args = parser.parse_args()

    seed = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    foods = seed["foods"]
    cities = [(reg, c["name"]) for reg, info in seed["regions"].items() for c in info["cities"]]

    producer = None
    if args.mode == "kafka":
        try:
            from kafka import KafkaProducer
        except ImportError as exc:
            raise SystemExit("Install kafka-python first: pip install kafka-python") from exc
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    else:
        STREAM_SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    topics_builders = {
        "smartprotein.price_alerts": lambda i: build_price_event(i, foods, cities),
        "smartprotein.wearable_events": build_wearable_event,
        "smartprotein.crowd_flags": build_crowd_flag_event,
    }

    for topic, fn in topics_builders.items():
        events = [fn(i) for i in range(1, args.count + 1)]
        if args.mode == "kafka" and producer is not None:
            for evt in events:
                producer.send(topic, evt)
                if args.delay_ms > 0:
                    time.sleep(args.delay_ms / 1000.0)
        else:
            out_file = STREAM_SAMPLE_DIR / f"{topic.replace('.', '_')}.jsonl"
            out_file.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
            print(f"[Dry-Run] Wrote {len(events)} sample events -> {out_file}")

    if producer is not None:
        producer.flush()
        producer.close()
        print(f"[Kafka] Published {args.count} events to each of the 3 SmartProtein topics.")


if __name__ == "__main__":
    main()

