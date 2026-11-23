"""Synthetic Dirty Big Data Generator for FitGuard-HDFS.

Generates three heterogeneous raw datasets in Bronze zone:
  1. user_profiles.csv          (dietary choice, daily budget INR, location, weight, fitness goal)
  2. protein_foods_catalog.csv  (regional protein foods, macros, mixed pricing units & currencies, vendor tags)
  3. wearable_telemetry/*.jsonl (multi-file JSONL stream simulating IoT small-files on HDFS)

Intentionally injects realistic Big Data quality faults (nulls, corrupt JSON, duplicates,
unit/currency mismatches, city typos, physiological spikes, cross-signal contradictions,
and dangerous semantic diet-tag lies such as Chicken labeled Vegan).
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config" / "pipeline_config.json"
ALIASES_PATH = ROOT_DIR / "config" / "city_aliases.json"

with CONFIG_PATH.open("r", encoding="utf-8") as fh:
    CFG = json.load(fh)
with ALIASES_PATH.open("r", encoding="utf-8") as fh:
    ALIASES = json.load(fh)

CANONICAL_CITIES: list[str] = ALIASES["canonical_cities"]

# Regional cost index multiplier (reflects real Indian metro food price variation)
CITY_COST_INDEX = {
    "Mumbai": 1.15,
    "Delhi": 1.08,
    "Bengaluru": 1.12,
    "Hyderabad": 0.96,
    "Pune": 1.02,
    "Chennai": 1.00,
    "Kolkata": 0.90,
    "Jaipur": 0.88,
}

# Clean base food catalog: (name, true_diet, protein_per_100g, calories_per_100g, base_inr_per_100g, category)
BASE_PROTEIN_FOODS = [
    ("Soya Chunks (Nutrela)", "Vegan", 52.0, 345.0, 14.0, "Legumes & Soy"),
    ("Moong Dal Split", "Vegan", 24.0, 347.0, 13.5, "Legumes & Soy"),
    ("Roasted Chana (Bengal Gram)", "Vegan", 21.5, 369.0, 12.0, "Legumes & Soy"),
    ("Rajma (Kidney Beans)", "Vegan", 22.5, 333.0, 14.5, "Legumes & Soy"),
    ("Kabuli Chana (Chickpeas)", "Vegan", 19.0, 364.0, 13.0, "Legumes & Soy"),
    ("Tofu (Firm Soy Paneer)", "Vegan", 16.0, 144.0, 42.0, "Plant Protein"),
    ("Tempeh Fermented Soy", "Vegan", 19.5, 193.0, 68.0, "Plant Protein"),
    ("Peanut Butter (Unsweetened)", "Vegan", 28.0, 588.0, 38.0, "Nuts & Seeds"),
    ("Pumpkin Seeds", "Vegan", 30.0, 559.0, 75.0, "Nuts & Seeds"),
    ("Pea Protein Isolate", "Vegan", 78.0, 380.0, 145.0, "Supplement"),
    ("Quinoa Cooked/Dry Mix", "Vegan", 14.1, 368.0, 48.0, "Grains"),
    ("Sprouts Mixed Moong", "Vegan", 8.5, 72.0, 10.0, "Legumes & Soy"),
    ("Low-Fat Paneer", "Veg", 20.5, 220.0, 44.0, "Dairy"),
    ("Malai Paneer", "Veg", 18.0, 296.0, 46.0, "Dairy"),
    ("Greek Yogurt (Plain)", "Veg", 10.0, 95.0, 48.0, "Dairy"),
    ("Homemade Dahi (Curd)", "Veg", 4.2, 65.0, 8.5, "Dairy"),
    ("Skimmed Milk Powder", "Veg", 35.0, 358.0, 45.0, "Dairy"),
    ("Whey Protein Concentrate", "Veg", 76.0, 395.0, 185.0, "Supplement"),
    ("Whey Protein Isolate", "Veg", 86.0, 370.0, 240.0, "Supplement"),
    ("Whole Boiled Eggs", "Eggetarian", 13.0, 155.0, 14.0, "Eggs"),
    ("Egg Whites (Pasteurized)", "Eggetarian", 11.0, 52.0, 22.0, "Eggs"),
    ("Chicken Breast (Skinless)", "Non-Veg", 31.0, 165.0, 34.0, "Poultry"),
    ("Chicken Keema (Lean)", "Non-Veg", 26.0, 178.0, 36.0, "Poultry"),
    ("Rohu Fish Fillet", "Non-Veg", 19.5, 118.0, 32.0, "Seafood"),
    ("Tuna in Water", "Non-Veg", 25.0, 116.0, 58.0, "Seafood"),
    ("Prawns (Cleaned)", "Non-Veg", 23.0, 99.0, 62.0, "Seafood"),
    ("Lean Mutton Cut", "Non-Veg", 25.0, 210.0, 78.0, "Meat"),
]

VENDORS = ["FreshBasket", "NutriMart", "LocalKirana", "ProteinHub", "FarmDirect"]
GOALS = list(CFG["protein_target_g_per_kg"].keys())
DIET_CHOICES = ["Vegan", "Veg", "Eggetarian", "Non-Veg"]

CITY_MESSY_VARIANTS = {
    "Bengaluru": ["Bengaluru", "bangalore", "BLR", "b'lore", "Bengaluru Urban"],
    "Mumbai": ["Mumbai", "Bombay", "BOM", "navi mumbai"],
    "Delhi": ["Delhi", "New Delhi", "NCR", "del"],
    "Hyderabad": ["Hyderabad", "hyd", "Cyberabad"],
    "Pune": ["Pune", "Poona", "pnq"],
    "Chennai": ["Chennai", "Madras", "maa"],
    "Kolkata": ["Kolkata", "Calcutta", "ccu"],
    "Jaipur": ["Jaipur", "jpr", "Pink City"],
}

DIET_MESSY_VARIANTS = {
    "Vegan": ["Vegan", "plant-based", "Plant Based"],
    "Veg": ["Veg", "vegetarian", "Pure Veg"],
    "Eggetarian": ["Eggetarian", "egg", "ovo-veg"],
    "Non-Veg": ["Non-Veg", "non veg", "NonVeg"],
}


def generate_user_profiles(bronze_dir: Path, rng: np.random.Generator, num_users: int) -> pd.DataFrame:
    rows = []
    for i in range(1, num_users + 1):
        uid = f"USR-{i:04d}"
        city = random.choice(CANONICAL_CITIES)
        diet = random.choice(DIET_CHOICES)
        weight = round(float(rng.uniform(48.0, 102.0)), 1)
        budget = round(float(rng.uniform(90.0, 480.0)), 0)
        goal = random.choice(GOALS)

        raw_city = random.choice(CITY_MESSY_VARIANTS[city]) if random.random() < 0.30 else city
        raw_diet = random.choice(DIET_MESSY_VARIANTS[diet]) if random.random() < 0.25 else diet

        rows.append(
            {
                "user_id": uid,
                "full_name": f"Athlete_{i:04d}",
                "age": int(rng.integers(19, 58)),
                "weight_kg": weight,
                "fitness_goal": goal,
                "diet_choice": raw_diet,
                "daily_food_budget_inr": budget,
                "location_city": raw_city,
                "updated_at": (datetime.now(timezone.utc) - timedelta(days=random.randint(0, 5))).strftime("%Y-%m-%d"),
            }
        )

    # Inject duplicate user rows (DQ-UNQ-001)
    for dup in random.sample(rows, k=max(10, num_users // 25)):
        dup_copy = dict(dup)
        dup_copy["updated_at"] = (datetime.now(timezone.utc) - timedelta(days=6)).strftime("%Y-%m-%d")
        rows.append(dup_copy)

    # Inject corrupt/invalid user rows
    bad_rows = [
        {"user_id": None, "full_name": "Missing_ID", "age": 27, "weight_kg": 70.0, "fitness_goal": "Muscle Gain", "diet_choice": "Veg", "daily_food_budget_inr": 200, "location_city": "Pune", "updated_at": "2026-11-08"},
        {"user_id": "USR-9001", "full_name": "Bad_Weight_Low", "age": 25, "weight_kg": 4.5, "fitness_goal": "Fat Loss", "diet_choice": "Vegan", "daily_food_budget_inr": 220, "location_city": "Mumbai", "updated_at": "2026-11-08"},
        {"user_id": "USR-9002", "full_name": "Bad_Budget_Neg", "age": 31, "weight_kg": 74.0, "fitness_goal": "Muscle Gain", "diet_choice": "Non-Veg", "daily_food_budget_inr": -150, "location_city": "Delhi", "updated_at": "2026-11-08"},
        {"user_id": "USR-9003", "full_name": "Bad_City_Mars", "age": 29, "weight_kg": 68.0, "fitness_goal": "General Wellness", "diet_choice": "Eggetarian", "daily_food_budget_inr": 180, "location_city": "Atlantis", "updated_at": "2026-11-08"},
        {"user_id": "USR-9004", "full_name": "Bad_Diet_Null", "age": 34, "weight_kg": 80.0, "fitness_goal": "Muscle Gain", "diet_choice": None, "daily_food_budget_inr": 300, "location_city": "Chennai", "updated_at": "2026-11-08"},
    ]
    rows.extend(bad_rows)

    df = pd.DataFrame(rows)
    out_dir = bronze_dir / "user_profiles"
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_dir / "user_profiles.csv", index=False)
    return df


def generate_protein_foods(bronze_dir: Path, rng: np.random.Generator, num_rows: int) -> pd.DataFrame:
    rows = []
    unit_choices = ["per_100g", "per_kg", "per_500g", "per_250g", "per_serving_50g"]
    unit_multipliers = {
        "per_100g": 1.0,
        "per_kg": 10.0,
        "per_500g": 5.0,
        "per_250g": 2.5,
        "per_serving_50g": 0.5,
    }

    for idx in range(1, num_rows + 1):
        name, true_diet, p100, cal100, base_cost, category = random.choice(BASE_PROTEIN_FOODS)
        city = random.choice(CANONICAL_CITIES)
        vendor = random.choice(VENDORS)

        # Realistic market noise +- 18% + city index
        cost_100g_inr = base_cost * CITY_COST_INDEX[city] * float(rng.uniform(0.84, 1.18))
        unit = random.choices(unit_choices, weights=[0.50, 0.18, 0.14, 0.10, 0.08])[0]
        mult = unit_multipliers[unit]

        reported_cost = round(cost_100g_inr * mult, 2)
        reported_protein = round(p100 * mult * float(rng.uniform(0.96, 1.04)), 2)
        reported_calories = round(cal100 * mult * float(rng.uniform(0.96, 1.04)), 1)

        currency = "INR"
        if random.random() < 0.08:
            # Currency mismatch: vendor reported in USD
            currency = "USD"
            reported_cost = round(reported_cost / CFG["dq_thresholds"]["usd_to_inr"], 2)

        raw_city = random.choice(CITY_MESSY_VARIANTS[city]) if random.random() < 0.32 else city
        raw_diet = random.choice(DIET_MESSY_VARIANTS[true_diet]) if random.random() < 0.22 else true_diet

        record = {
            "listing_id": f"FD-{idx:05d}",
            "food_name": name,
            "category": category,
            "diet_tag": raw_diet,
            "protein_g": reported_protein,
            "calories_kcal": reported_calories,
            "cost": reported_cost,
            "currency": currency,
            "unit_basis": unit,
            "vendor_name": vendor,
            "city": raw_city,
            "in_stock": random.choices([True, False], weights=[0.93, 0.07])[0],
            "scraped_at": (datetime.now(timezone.utc) - timedelta(hours=random.randint(1, 72))).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        # Inject controlled, realistic Big Data dirty anomalies (~18% overall fault rate)
        fault_roll = random.random()
        if fault_roll < 0.045:
            # CRITICAL SEMANTIC LIE (DQ-SEM-001): Wrong diet tag!
            if true_diet == "Non-Veg":
                record["diet_tag"] = random.choice(["Veg", "Vegan"])
            elif true_diet == "Eggetarian":
                record["diet_tag"] = random.choice(["Veg", "Vegan"])
            elif true_diet == "Veg":
                record["diet_tag"] = "Vegan"
        elif fault_roll < 0.085:
            # Physiological / Macro impossibility (DQ-PHY-002): protein > 100g/100g or calories < protein*4
            if random.random() < 0.5:
                record["protein_g"] = round(140.0 * mult, 1)
            else:
                record["calories_kcal"] = round(reported_protein * 1.2, 1)
        elif fault_roll < 0.120:
            # Invalid or extreme cost (DQ-VAL-002)
            record["cost"] = random.choice([-25.0, 0.0, 99999.0])
        elif fault_roll < 0.150:
            # Unresolvable / corrupt location (DQ-LOC-001)
            record["city"] = random.choice(["Unknown", "N/A", "", "Mars_Colony", "12345"])
        elif fault_roll < 0.175:
            # Missing critical key or null macro (DQ-CMP-001)
            if random.random() < 0.5:
                record["food_name"] = None
            else:
                record["protein_g"] = None
        elif fault_roll < 0.195:
            # Stale scraped price > 90 days old (DQ-TIM-001)
            record["scraped_at"] = "2025-01-15T10:00:00Z"

        rows.append(record)

    # Inject ~5% exact/near duplicate vendor listings (DQ-UNQ-001)
    dup_samples = random.sample(rows, k=max(20, num_rows // 20))
    for d in dup_samples:
        dup_row = dict(d)
        dup_row["listing_id"] = d["listing_id"] + "-DUP"
        rows.append(dup_row)

    df = pd.DataFrame(rows)
    out_dir = bronze_dir / "protein_foods_catalog"
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_dir / "protein_foods_catalog.csv", index=False)
    return df


def generate_wearable_telemetry(
    bronze_dir: Path,
    rng: np.random.Generator,
    valid_user_ids: list[str],
    days: int,
) -> int:
    """Write partitioned small JSONL files mimicking IoT/wearable HDFS small-files ingestion."""
    out_dir = bronze_dir / "wearable_telemetry"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Clean old jsonl parts before regenerating
    for old_file in out_dir.glob("telemetry_part_*.jsonl"):
        old_file.unlink()

    base_end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    total_events = 0
    file_idx = 0
    buffer_lines: list[str] = []

    # 8 hourly readings per day per user -> ~56,000 events for 500 users x 14 days
    sample_hours = [0, 3, 6, 9, 12, 15, 18, 21]

    for day_offset in range(days, 0, -1):
        day_dt = base_end - timedelta(days=day_offset)
        for uid in valid_user_ids:
            device = random.choice(["FitBand-X", "SmartWatch-Pro", "HealthRing-V2"])
            for hr_slot in sample_hours:
                ts = day_dt.replace(hour=hr_slot)
                is_sleep_window = hr_slot in (0, 3)

                if is_sleep_window:
                    sleep_stage = random.choice(["DEEP_SLEEP", "REM", "LIGHT_SLEEP"])
                    steps = int(rng.integers(0, 20))
                    heart_rate = int(rng.integers(52, 68))
                    calories_burned = round(float(rng.uniform(45.0, 75.0)), 1)
                else:
                    sleep_stage = "AWAKE"
                    steps = int(rng.integers(180, 2600))
                    heart_rate = int(rng.integers(68, 152))
                    calories_burned = round(float(steps * 0.045 + rng.uniform(60.0, 140.0)), 1)

                spo2 = round(float(rng.uniform(95.5, 99.8)), 1)

                # Timestamp format variation (ISO vs epoch ms)
                ts_val: str | int = (
                    int(ts.timestamp() * 1000)
                    if random.random() < 0.20
                    else ts.strftime("%Y-%m-%dT%H:%M:%SZ")
                )

                event = {
                    "event_id": f"EVT-{total_events + 1:07d}",
                    "user_id": uid,
                    "device_type": device,
                    "event_ts": ts_val,
                    "heart_rate_bpm": heart_rate,
                    "spo2_pct": spo2,
                    "steps": steps,
                    "sleep_stage": sleep_stage,
                    "calories_burned": calories_burned,
                }

                anomaly_roll = random.random()
                if anomaly_roll < 0.008:
                    # Corrupt unparseable JSON line (DQ-SCH-001)
                    buffer_lines.append('{"event_id": "CORRUPT", "user_id": ' + f'"{uid}", broken_json')
                    total_events += 1
                    continue
                elif anomaly_roll < 0.025:
                    # Physiological spike (DQ-PHY-001)
                    event["heart_rate_bpm"] = random.choice([12, 265, 310, -40])
                elif anomaly_roll < 0.040:
                    # SpO2 sensor glitch (DQ-PHY-001)
                    event["spo2_pct"] = random.choice([42.0, 108.5, 130.0])
                elif anomaly_roll < 0.055:
                    # Cross-signal contradiction (DQ-XSIG-001): Running 3000 steps during DEEP_SLEEP!
                    event["sleep_stage"] = "DEEP_SLEEP"
                    event["steps"] = int(rng.integers(1200, 4800))
                elif anomaly_roll < 0.070:
                    # Missing HR dropout -> eligible for smart rolling imputation in Silver!
                    event["heart_rate_bpm"] = None
                elif anomaly_roll < 0.080:
                    # Future / stale timestamp (DQ-TIM-001)
                    event["event_ts"] = random.choice(["2032-05-01T12:00:00Z", "2022-01-01T08:00:00Z"])
                elif anomaly_roll < 0.090:
                    # Missing user_id (DQ-CMP-001)
                    event["user_id"] = None

                buffer_lines.append(json.dumps(event))
                total_events += 1

                # Inject ~3% multi-device duplicate syncs
                if random.random() < 0.03 and event.get("user_id"):
                    dup_evt = dict(event)
                    dup_evt["device_type"] = "MobileApp-Sync"
                    buffer_lines.append(json.dumps(dup_evt))
                    total_events += 1

                # Write chunks of ~2,500 lines to simulate multiple HDFS small files
                if len(buffer_lines) >= 2500:
                    file_idx += 1
                    part_path = out_dir / f"telemetry_part_{file_idx:03d}.jsonl"
                    part_path.write_text("\n".join(buffer_lines) + "\n", encoding="utf-8")
                    buffer_lines.clear()

    if buffer_lines:
        file_idx += 1
        part_path = out_dir / f"telemetry_part_{file_idx:03d}.jsonl"
        part_path.write_text("\n".join(buffer_lines) + "\n", encoding="utf-8")

    return total_events


def main() -> None:
    random.seed(42)
    rng = np.random.default_rng(42)

    bronze_dir = ROOT_DIR / CFG["local_lakehouse_root"] / CFG["zones"]["bronze"]
    bronze_dir.mkdir(parents=True, exist_ok=True)

    num_users = CFG["dataset_sizes"]["num_users"]
    num_foods = CFG["dataset_sizes"]["num_food_listings"]
    days = CFG["dataset_sizes"]["telemetry_days"]

    users_df = generate_user_profiles(bronze_dir, rng, num_users)
    foods_df = generate_protein_foods(bronze_dir, rng, num_foods)

    valid_uids = [f"USR-{i:04d}" for i in range(1, num_users + 1)]
    telemetry_count = generate_wearable_telemetry(bronze_dir, rng, valid_uids, days)

    print(f"[FitGuard Generator] Bronze path: {bronze_dir}")
    print(f"  - User profiles generated : {len(users_df):,} rows")
    print(f"  - Protein food listings   : {len(foods_df):,} rows")
    print(f"  - Wearable telemetry logs : {telemetry_count:,} events")


if __name__ == "__main__":
    main()

