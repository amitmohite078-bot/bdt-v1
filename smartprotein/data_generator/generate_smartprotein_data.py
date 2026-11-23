"""SmartProtein Scalable Multi-Source Dirty Data Generator (Stage 3).

Generates million-row-ready synthetic datasets mirroring:
  1. Multi-source Food Composition (USDA_FDC, IFCT_2017, OPEN_FOOD_FACTS)
  2. Store Locations with Lat/Long & GeoHash-6
  3. Store Prices & Stock Offers
  4. User Profiles & Fitness Wearable Telemetry
  5. Crowdsourced Correction Flags

Usage:
  python smartprotein/data_generator/generate_smartprotein_data.py --scale dev
  python smartprotein/data_generator/generate_smartprotein_data.py --scale 1m
  python smartprotein/data_generator/generate_smartprotein_data.py --scale 5m
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
SEED_PATH = ROOT_DIR / "config" / "reference_foods_seed.json"
RAW_OUT_DIR = ROOT_DIR / "data" / "raw"

SCALE_PRESETS = {
    "dev": {"foods": 15_000, "stores": 400, "prices": 35_000, "wearables": 50_000, "flags": 1_500},
    "1m": {"foods": 150_000, "stores": 2_500, "prices": 400_000, "wearables": 500_000, "flags": 15_000},
    "5m": {"foods": 500_000, "stores": 5_000, "prices": 2_000_000, "wearables": 2_500_000, "flags": 50_000},
}

_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def encode_geohash(lat: float, lon: float, precision: int = 6) -> str:
    """Pure-Python GeoHash encoder (no external dependency required)."""
    lat_interval = [-90.0, 90.0]
    lon_interval = [-180.0, 180.0]
    geohash = []
    bits = [16, 8, 4, 2, 1]
    bit = 0
    ch = 0
    even = True
    while len(geohash) < precision:
        if even:
            mid = (lon_interval[0] + lon_interval[1]) / 2.0
            if lon > mid:
                ch |= bits[bit]
                lon_interval[0] = mid
            else:
                lon_interval[1] = mid
        else:
            mid = (lat_interval[0] + lat_interval[1]) / 2.0
            if lat > mid:
                ch |= bits[bit]
                lat_interval[0] = mid
            else:
                lat_interval[1] = mid
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash.append(_BASE32[ch])
            bit = 0
            ch = 0
    return "".join(geohash)


def load_seed() -> dict:
    with SEED_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _typo_name(name: str, rng: np.random.Generator) -> str:
    """Create realistic fuzzy duplicate string (extra spaces, dropped char, brand prefix)."""
    variant = int(rng.integers(0, 4))
    if variant == 0:
        return name.replace(" ", "  ").lower()
    if variant == 1 and len(name) > 6:
        idx = int(rng.integers(2, len(name) - 2))
        return name[:idx] + name[idx + 1 :]
    if variant == 2:
        return f"Fresh {name} (100g)"
    return f"{name} - BrandPack"


def generate_food_composition(
    out_dir: Path,
    seed: dict,
    total_rows: int,
    error_rate: float,
    ingestion_date: str,
    rng: np.random.Generator,
    chunk_size: int = 50_000,
) -> dict[str, int]:
    """Generate chunked multi-source food composition CSV with injected DQ anomalies."""
    part_dir = out_dir / "food_composition" / f"ingestion_date={ingestion_date}"
    part_dir.mkdir(parents=True, exist_ok=True)
    out_file = part_dir / "food_composition_raw.csv"
    if out_file.exists():
        out_file.unlink()

    foods = seed["foods"]
    n_foods = len(foods)
    sources = np.array(["USDA_FDC", "IFCT_2017", "OPEN_FOOD_FACTS"])
    source_probs = [0.30, 0.25, 0.45]
    states = np.array(["raw", "cooked_boiled", "cooked_grilled", "cooked_fried"])

    stats = {
        "total_rows": 0,
        "injected_nulls": 0,
        "injected_impossible_macros": 0,
        "injected_atwater_mismatch": 0,
        "injected_unit_errors": 0,
        "injected_diet_tag_lies": 0,
        "injected_fuzzy_duplicates": 0,
    }

    cols = [
        "record_id", "source_db", "food_name", "category", "diet_type",
        "state_raw_cooked", "serving_unit", "protein_g", "carbs_g", "fat_g",
        "fiber_g", "moisture_g", "reported_kcal", "energy_unit", "iron_mg",
        "calcium_mg", "vitamin_b12_mcg", "leucine_g_per_100g_protein",
        "phytate_mg", "allergens", "last_updated",
    ]

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    written = 0
    while written < total_rows:
        cur_n = min(chunk_size, total_rows - written)
        food_idx = rng.integers(0, n_foods, size=cur_n)
        src_arr = rng.choice(sources, size=cur_n, p=source_probs)

        # Open Food Facts gets higher synthetic noise than official USDA/IFCT
        noise = rng.normal(1.0, 0.04, size=cur_n)
        off_mask = src_arr == "OPEN_FOOD_FACTS"
        noise = np.where(off_mask, rng.normal(1.0, 0.09, size=cur_n), noise)

        records = []
        for i in range(cur_n):
            f = foods[int(food_idx[i])]
            rid = f"FC-{written + i + 1:08d}"
            src = str(src_arr[i])

            # Pick canonical name or synonym (tests synonym mapping & fuzzy dedup)
            fname = f["canonical_name"]
            if f["synonyms"] and rng.random() < 0.28:
                fname = f["synonyms"][int(rng.integers(0, len(f["synonyms"])))]

            diet = f["diet_type"]
            state = f["state"]
            if rng.random() < 0.15:
                state = str(states[int(rng.integers(0, len(states)))])

            unit = "per_100g"
            e_unit = "kcal"
            mult = 1.0
            n_factor = max(0.75, float(noise[i]))

            p = round(f["protein_g"] * n_factor, 2)
            c = round(f["carbs_g"] * n_factor, 2)
            fat = round(f["fat_g"] * n_factor, 2)
            fib = round(f["fiber_g"] * n_factor, 2)
            moist = round(f["moisture_g"], 2)
            kcal = round(4.0 * p + 4.0 * c + 9.0 * fat, 1)
            allergens = "|".join(f["allergens"]) if f["allergens"] else "none"
            updated = today_str

            # Controlled error injection
            if rng.random() < error_rate:
                err_type = int(rng.integers(0, 7))
                if err_type == 0:
                    # Null critical macro or name
                    if rng.random() < 0.5:
                        p = None
                    else:
                        fname = None
                    stats["injected_nulls"] += 1
                elif err_type == 1:
                    # Impossible macro (> 100g/100g or sum > 105)
                    p = round(float(rng.uniform(108.0, 165.0)), 1)
                    stats["injected_impossible_macros"] += 1
                elif err_type == 2:
                    # Atwater mismatch (kcal wildly inconsistent with 4P+4C+9F)
                    kcal = round(kcal * float(rng.choice([0.25, 2.8])), 1)
                    stats["injected_atwater_mismatch"] += 1
                elif err_type == 3:
                    # Unit inconsistency: kJ entered under kcal OR per_50g_serving
                    if rng.random() < 0.5:
                        kcal = round(kcal * 4.184, 1)
                        e_unit = "kJ"
                    else:
                        unit = "per_50g_serving"
                        p = round(p * 0.5, 2)
                        c = round(c * 0.5, 2)
                        fat = round(fat * 0.5, 2)
                        kcal = round(kcal * 0.5, 1)
                    stats["injected_unit_errors"] += 1
                elif err_type == 4:
                    # Semantic diet-tag lie (e.g. Poultry/Dairy tagged Vegan)
                    if diet in ("Non-Veg", "Eggetarian", "Veg"):
                        diet = "Vegan"
                        stats["injected_diet_tag_lies"] += 1
                elif err_type == 5:
                    # Fuzzy duplicate name with typo / whitespace
                    fname = _typo_name(f["canonical_name"], rng)
                    stats["injected_fuzzy_duplicates"] += 1
                else:
                    # Negative protein or carbs
                    p = -round(float(rng.uniform(1.0, 15.0)), 1)
                    stats["injected_impossible_macros"] += 1

            records.append(
                [
                    rid, src, fname, f["category"], diet, state, unit,
                    p, c, fat, fib, moist, kcal, e_unit,
                    f["iron_mg"], f["calcium_mg"], f["b12_mcg"],
                    f["leucine_g_per_100g_prot"], f["phytate_mg"], allergens, updated,
                ]
            )

        df_chunk = pd.DataFrame(records, columns=cols)
        df_chunk.to_csv(out_file, mode="a", header=(written == 0), index=False)
        written += cur_n
        stats["total_rows"] += cur_n

    return stats


def generate_store_locations(
    out_dir: Path,
    seed: dict,
    num_stores: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    loc_dir = out_dir / "store_locations"
    loc_dir.mkdir(parents=True, exist_ok=True)
    out_file = loc_dir / "store_locations.csv"

    city_pool = []
    for region, info in seed["regions"].items():
        for c in info["cities"]:
            city_pool.append((region, c["name"], c["lat"], c["lon"], c["cost_index"]))

    chains = ["NutriMart", "FreshBasket", "RelianceFresh", "LocalKirana", "ProteinHub", "OrganicStop"]
    store_types = ["Supermarket", "Kirana", "Supplement_Store", "Organic_Mart"]

    rows = []
    for i in range(1, num_stores + 1):
        region, cname, base_lat, base_lon, _cost_idx = city_pool[int(rng.integers(0, len(city_pool)))]
        lat = round(base_lat + float(rng.uniform(-0.08, 0.08)), 6)
        lon = round(base_lon + float(rng.uniform(-0.08, 0.08)), 6)
        gh = encode_geohash(lat, lon, precision=6)
        chain = chains[int(rng.integers(0, len(chains)))]
        stype = store_types[int(rng.integers(0, len(store_types)))]
        rows.append([f"STR-{i:05d}", f"{chain} {cname} #{i}", region, cname, lat, lon, gh, stype])

    df = pd.DataFrame(
        rows,
        columns=["store_id", "store_name", "region", "city", "latitude", "longitude", "geohash_6", "store_type"],
    )
    df.to_csv(out_file, index=False)
    return df


def generate_store_prices(
    out_dir: Path,
    seed: dict,
    stores_df: pd.DataFrame,
    total_rows: int,
    error_rate: float,
    ingestion_date: str,
    rng: np.random.Generator,
    chunk_size: int = 100_000,
) -> dict[str, int]:
    part_dir = out_dir / "store_prices" / f"ingestion_date={ingestion_date}"
    part_dir.mkdir(parents=True, exist_ok=True)
    out_file = part_dir / "store_prices_raw.csv"
    if out_file.exists():
        out_file.unlink()

    foods = seed["foods"]
    store_ids = stores_df["store_id"].to_numpy()
    store_cities = stores_df["city"].to_numpy()
    city_cost = {c["name"]: c["cost_index"] for reg in seed["regions"].values() for c in reg["cities"]}

    pack_weights = np.array([100.0, 250.0, 500.0, 1000.0])
    cols = [
        "offer_id", "store_id", "food_name_raw", "canonical_food_hint",
        "pack_weight_g", "price_value", "currency", "in_stock", "stock_qty", "updated_at",
    ]
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stats = {"total_rows": 0, "bad_prices": 0, "usd_currency": 0}

    written = 0
    while written < total_rows:
        cur_n = min(chunk_size, total_rows - written)
        s_idx = rng.integers(0, len(store_ids), size=cur_n)
        f_idx = rng.integers(0, len(foods), size=cur_n)
        pw_idx = rng.integers(0, len(pack_weights), size=cur_n)
        market_mult = rng.uniform(0.85, 1.22, size=cur_n)
        stock_roll = rng.random(size=cur_n)

        rows = []
        for i in range(cur_n):
            f = foods[int(f_idx[i])]
            sid = str(store_ids[int(s_idx[i])])
            cname = str(store_cities[int(s_idx[i])])
            pw = float(pack_weights[int(pw_idx[i])])
            price = round(f["base_inr_100g"] * (pw / 100.0) * city_cost.get(cname, 1.0) * float(market_mult[i]), 2)
            currency = "INR"
            in_stock = bool(stock_roll[i] > 0.08)
            qty = int(rng.integers(1, 80)) if in_stock else 0
            raw_name = f["canonical_name"]

            if rng.random() < error_rate:
                e = int(rng.integers(0, 4))
                if e == 0:
                    price = round(-float(rng.uniform(10.0, 200.0)), 2)
                    stats["bad_prices"] += 1
                elif e == 1:
                    price = round(price / 84.0, 2)
                    currency = "USD"
                    stats["usd_currency"] += 1
                elif e == 2:
                    price = 99999.0
                    stats["bad_prices"] += 1
                else:
                    raw_name = _typo_name(raw_name, rng)

            rows.append(
                [
                    f"OFF-{written + i + 1:08d}", sid, raw_name, f["canonical_name"],
                    pw, price, currency, in_stock, qty, now_iso,
                ]
            )

        pd.DataFrame(rows, columns=cols).to_csv(out_file, mode="a", header=(written == 0), index=False)
        written += cur_n
        stats["total_rows"] += cur_n

    return stats


def generate_user_wearables(
    out_dir: Path,
    seed: dict,
    total_rows: int,
    error_rate: float,
    ingestion_date: str,
    rng: np.random.Generator,
    chunk_size: int = 100_000,
) -> dict[str, int]:
    part_dir = out_dir / "user_wearables" / f"ingestion_date={ingestion_date}"
    part_dir.mkdir(parents=True, exist_ok=True)
    out_file = part_dir / "user_wearables_raw.csv"
    if out_file.exists():
        out_file.unlink()

    city_pool = [(reg, c["name"], c["lat"], c["lon"]) for reg, info in seed["regions"].items() for c in info["cities"]]
    goals = ["Muscle_Gain", "Fat_Loss", "Athletic_Endurance", "General_Wellness"]
    diets = ["Vegan", "Veg", "Eggetarian", "Non-Veg"]
    allergies_pool = ["none", "none", "none", "lactose", "nuts", "soy", "gluten", "egg"]
    med_pool = ["none", "none", "none", "none", "kidney_caution", "diabetes_type2"]
    workout_times = ["Morning", "Evening", "Afternoon"]

    num_unique_users = max(500, total_rows // 14)
    cols = [
        "event_id", "user_id", "region", "city", "latitude", "longitude",
        "age", "weight_kg", "goal", "diet_type", "daily_budget_inr",
        "allergies", "medical_conditions", "workout_time",
        "steps_daily", "workout_minutes", "sleep_hours", "event_ts",
    ]
    base_ts = datetime.now(timezone.utc)
    stats = {"total_rows": 0, "bad_wearables": 0}

    written = 0
    while written < total_rows:
        cur_n = min(chunk_size, total_rows - written)
        u_nums = rng.integers(1, num_unique_users + 1, size=cur_n)
        steps_arr = rng.integers(2500, 16500, size=cur_n)
        wmin_arr = rng.integers(0, 95, size=cur_n)
        sleep_arr = np.round(rng.uniform(5.0, 9.2, size=cur_n), 1)

        rows = []
        for i in range(cur_n):
            u_idx = int(u_nums[i])
            reg, cname, blat, blon = city_pool[u_idx % len(city_pool)]
            lat = round(blat + ((u_idx % 17) - 8) * 0.004, 5)
            lon = round(blon + ((u_idx % 13) - 6) * 0.004, 5)
            age = 19 + (u_idx % 38)
            weight = round(50.0 + (u_idx % 48), 1)
            goal = goals[u_idx % len(goals)]
            diet = diets[u_idx % len(diets)]
            budget = float(100 + (u_idx % 8) * 50)
            allergy = allergies_pool[u_idx % len(allergies_pool)]
            med = med_pool[u_idx % len(med_pool)]
            wtime = workout_times[u_idx % len(workout_times)]

            steps = int(steps_arr[i])
            wmin = int(wmin_arr[i])
            sleep = float(sleep_arr[i])
            ts = (base_ts - timedelta(days=int(i % 14))).strftime("%Y-%m-%dT%H:%M:%SZ")

            if rng.random() < error_rate * 0.5:
                err = int(rng.integers(0, 4))
                if err == 0:
                    weight = 4.0
                elif err == 1:
                    sleep = 29.5
                elif err == 2:
                    steps = -500
                else:
                    budget = -100.0
                stats["bad_wearables"] += 1

            rows.append(
                [
                    f"UW-{written + i + 1:08d}", f"USR-{u_idx:06d}", reg, cname, lat, lon,
                    age, weight, goal, diet, budget, allergy, med, wtime,
                    steps, wmin, sleep, ts,
                ]
            )

        pd.DataFrame(rows, columns=cols).to_csv(out_file, mode="a", header=(written == 0), index=False)
        written += cur_n
        stats["total_rows"] += cur_n

    return stats


def generate_crowd_flags(
    out_dir: Path,
    num_flags: int,
    num_food_records: int,
    rng: np.random.Generator,
) -> int:
    flag_dir = out_dir / "crowd_flags"
    flag_dir.mkdir(parents=True, exist_ok=True)
    out_file = flag_dir / "crowd_flags.csv"

    fields = ["protein_g", "diet_type", "price_value"]
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    with out_file.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["flag_id", "user_id", "user_trust_score", "record_id", "field_name", "suggested_value", "submitted_at"])
        for i in range(1, num_flags + 1):
            uid = f"USR-{int(rng.integers(1, 500)):06d}"
            trust = round(float(rng.uniform(0.30, 0.98)), 2)
            rid = f"FC-{int(rng.integers(1, max(2, num_food_records))):08d}"
            fld = fields[int(rng.integers(0, len(fields)))]
            val = "21.0" if fld == "protein_g" else ("Veg" if fld == "diet_type" else "45.0")
            writer.writerow([f"FLG-{i:06d}", uid, trust, rid, fld, val, now_iso])

    return num_flags


def main() -> None:
    parser = argparse.ArgumentParser(description="SmartProtein Million-Row Dirty Data Generator")
    parser.add_argument("--scale", choices=list(SCALE_PRESETS.keys()), default="dev", help="Dataset scale preset")
    parser.add_argument("--error-rate", type=float, default=0.18, help="Fraction of records with injected errors")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)
    random.seed(args.seed)
    seed_data = load_seed()
    counts = SCALE_PRESETS[args.scale]
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    RAW_OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[SmartProtein Generator] Scale='{args.scale}', ErrorRate={args.error_rate:.0%}, Date={ingestion_date}")
    fc_stats = generate_food_composition(RAW_OUT_DIR, seed_data, counts["foods"], args.error_rate, ingestion_date, rng)
    stores_df = generate_store_locations(RAW_OUT_DIR, seed_data, counts["stores"], rng)
    price_stats = generate_store_prices(RAW_OUT_DIR, seed_data, stores_df, counts["prices"], args.error_rate, ingestion_date, rng)
    wear_stats = generate_user_wearables(RAW_OUT_DIR, seed_data, counts["wearables"], args.error_rate, ingestion_date, rng)
    flag_count = generate_crowd_flags(RAW_OUT_DIR, counts["flags"], counts["foods"], rng)

    total_all = fc_stats["total_rows"] + len(stores_df) + price_stats["total_rows"] + wear_stats["total_rows"] + flag_count
    manifest = {
        "scale": args.scale,
        "error_rate": args.error_rate,
        "ingestion_date": ingestion_date,
        "total_records_all_tables": total_all,
        "food_composition": fc_stats,
        "store_locations": len(stores_df),
        "store_prices": price_stats,
        "user_wearables": wear_stats,
        "crowd_flags": flag_count,
    }
    manifest_path = RAW_OUT_DIR / "ground_truth_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()

