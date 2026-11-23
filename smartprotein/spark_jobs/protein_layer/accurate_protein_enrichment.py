"""SmartProtein Stage 8 (Modules #12-#19): Accurate Protein Informatics Layer.

Transforms crude protein into bioavailable, cooking-adjusted USABLE PROTEIN:
  12. DIAAS / PDCAAS quality adjustment (FAO 2013 Paper No. 92 framework)
  13. Cooked vs. raw moisture-retention & cooking-method retention factors
  14. Anti-nutrient (phytate) bioavailability adjustment (with cooking reduction)
  15. Amino acid complementarity table (limiting AA + cereal/legume pairings)
  16. Per-meal Leucine threshold (~2.5-3.0 g) & workout timing classification
  17. Protein density (usable g / 100 kcal) & Cost per gram of USABLE protein
      (benchmarking Whey, Soya Chunks, Eggs, Paneer, and Chicken)
  18. Source-agreement confidence score propagation
  19. Personalized user requirement (1.2-2.0 g/kg) from goal, age, steps, workouts & sleep
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT_DIR / "data"
CURATED_DIR = DATA_DIR / "curated"
ANALYTICS_DIR = DATA_DIR / "analytics"
PROTEIN_TABLES_PATH = ROOT_DIR / "config" / "protein_quality_tables.json"


def enrich_protein_foods(foods_df: pd.DataFrame, tables: dict) -> pd.DataFrame:
    diaas_map = tables["diaas_pdcaas_by_food"]
    cook_map = tables["cooking_adjustments"]

    df = foods_df.copy()

    df["diaas_score"] = df["canonical_name"].map(lambda x: diaas_map.get(x, {}).get("diaas", 0.75))
    df["pdcaas_score"] = df["canonical_name"].map(lambda x: diaas_map.get(x, {}).get("pdcaas", 0.75))
    df["limiting_amino_acid"] = df["canonical_name"].map(lambda x: diaas_map.get(x, {}).get("limiting_aa", "Unknown"))
    df["digestibility_speed"] = df["canonical_name"].map(lambda x: diaas_map.get(x, {}).get("digestibility_speed", "Medium"))
    df["recommended_timing"] = df["canonical_name"].map(lambda x: diaas_map.get(x, {}).get("best_timing", "Any Meal"))

    # Cooking method adjustments
    df["moisture_yield_factor"] = df["state_raw_cooked"].map(
        lambda s: cook_map.get(s, cook_map["raw"])["moisture_yield_factor"]
    )
    df["cooking_retention_factor"] = df["state_raw_cooked"].map(
        lambda s: cook_map.get(s, cook_map["raw"])["protein_retention"]
    )
    df["kcal_cooking_mult"] = df["state_raw_cooked"].map(
        lambda s: cook_map.get(s, cook_map["raw"])["kcal_multiplier"]
    )
    df["phytate_reduction"] = df["state_raw_cooked"].map(
        lambda s: cook_map.get(s, cook_map["raw"])["phytate_reduction"]
    )

    # Bioavailability factor accounting for phytates reduced by cooking
    df["effective_phytate_mg"] = (df["phytate_mg"] * (1.0 - df["phytate_reduction"])).round(1)
    df["bioavailability_factor"] = (
        (1.0 - (df["effective_phytate_mg"] / 5000.0)).clip(lower=0.78, upper=1.00)
    ).round(3)

    # Usable Protein per 100 g edible cooked/raw state
    # Cap DIAAS multiplier at 1.00 for usable gram calculation (values >1.0 indicate surplus EAA quality)
    df["effective_diaas_mult"] = df["diaas_score"].clip(upper=1.00)
    df["cooked_crude_protein_100g"] = (
        df["crude_protein_per_100g"] * df["moisture_yield_factor"] * df["cooking_retention_factor"]
    ).round(2)
    df["usable_protein_per_100g"] = (
        df["cooked_crude_protein_100g"] * df["effective_diaas_mult"] * df["bioavailability_factor"]
    ).round(2)

    df["adjusted_kcal_per_100g"] = (df["kcal_per_100g"] * df["kcal_cooking_mult"]).round(1)
    df["usable_protein_per_100_kcal"] = np.where(
        df["adjusted_kcal_per_100g"] > 0,
        ((df["usable_protein_per_100g"] / df["adjusted_kcal_per_100g"]) * 100.0).round(2),
        0.0,
    )

    # Leucine per 100 g food & grams of food needed to hit 2.5 g MPS leucine trigger
    df["leucine_g_per_100g_food"] = (
        (df["cooked_crude_protein_100g"] * (df["leucine_g_per_100g_protein"] / 100.0))
    ).round(2)
    df["grams_for_2_5g_leucine"] = np.where(
        df["leucine_g_per_100g_food"] > 0.1,
        ((2.5 / df["leucine_g_per_100g_food"]) * 100.0).clip(upper=1500.0).round(0),
        999.0,
    )
    df["hits_leucine_in_reasonable_portion"] = df["grams_for_2_5g_leucine"] <= 250.0
    return df


def build_usable_cost_comparison(enriched_foods: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    """Compare Whey, Soya Chunks, Eggs, Paneer, and Chicken on Cost per Gram of USABLE Protein."""
    # Prefer raw retail state (for boiled eggs, cooked_boiled is the primary state)
    tmp = enriched_foods.copy()
    tmp["_state_priority"] = np.where(tmp["state_raw_cooked"] == "raw", 0, 1)
    base_foods = (
        tmp.sort_values(by=["_state_priority", "state_raw_cooked"])
        .drop_duplicates(subset=["canonical_name"], keep="first")
        .drop(columns=["_state_priority"])
    )
    avg_prices = (
        inventory[inventory["in_stock"] == True]  # noqa: E712
        .groupby("canonical_name", as_index=False)["price_per_100g_inr"]
        .median()
        .rename(columns={"price_per_100g_inr": "median_price_100g_inr"})
    )
    merged = base_foods.merge(avg_prices, on="canonical_name", how="inner")
    merged["usable_protein_per_rupee"] = (
        merged["usable_protein_per_100g"] / merged["median_price_100g_inr"]
    ).round(3)
    merged["cost_per_g_usable_protein_inr"] = (
        merged["median_price_100g_inr"] / merged["usable_protein_per_100g"]
    ).round(2)
    merged["cost_per_g_crude_protein_inr"] = (
        merged["median_price_100g_inr"] / merged["crude_protein_per_100g"]
    ).round(2)
    merged["quality_penalty_pct"] = (
        ((merged["crude_protein_per_100g"] - merged["usable_protein_per_100g"]) / merged["crude_protein_per_100g"]) * 100.0
    ).round(1)

    cols = [
        "canonical_name", "diet_type", "diaas_score", "bioavailability_factor",
        "crude_protein_per_100g", "usable_protein_per_100g", "quality_penalty_pct",
        "usable_protein_per_100_kcal", "leucine_g_per_100g_food", "grams_for_2_5g_leucine",
        "median_price_100g_inr", "usable_protein_per_rupee", "cost_per_g_usable_protein_inr",
        "protein_confidence_score", "recommended_timing",
    ]
    return merged[cols].sort_values(by="cost_per_g_usable_protein_inr", ascending=True)


def compute_personalized_targets(users_df: pd.DataFrame) -> pd.DataFrame:
    """Compute 1.2-2.0 g/kg personalized protein requirement from goal, age, steps, workouts, sleep."""
    df = users_df.copy()

    goal_base = {
        "Muscle_Gain": 1.75,
        "Fat_Loss": 1.60,
        "Athletic_Endurance": 1.50,
        "General_Wellness": 1.20,
    }
    df["base_g_per_kg"] = df["goal"].map(goal_base).fillna(1.25)

    # Activity & recovery modifiers from wearable telemetry
    steps_adj = np.where(df["avg_steps_daily"] >= 10000, 0.15, np.where(df["avg_steps_daily"] >= 7500, 0.08, 0.0))
    workout_adj = np.where(df["avg_workout_minutes"] >= 45, 0.15, np.where(df["avg_workout_minutes"] >= 25, 0.05, 0.0))
    age_adj = np.where(df["age"] >= 45, 0.10, 0.0)  # Higher protein threshold for older adults
    sleep_adj = np.where(df["avg_sleep_hours"] < 6.0, 0.05, 0.0)  # Recovery stress support

    raw_factor = df["base_g_per_kg"] + steps_adj + workout_adj + age_adj + sleep_adj
    # Medical safety rule: cap at 1.0 g/kg if kidney_caution is flagged
    is_kidney = df["medical_conditions"].astype(str).str.contains("kidney", case=False, na=False)
    df["protein_g_per_kg"] = np.where(is_kidney, 1.00, np.clip(raw_factor, 1.20, 2.00)).round(2)
    df["daily_protein_target_g"] = (df["weight_kg"] * df["protein_g_per_kg"]).round(1)
    df["target_meals_per_day"] = np.where(df["daily_protein_target_g"] >= 120, 4, 3)
    df["per_meal_protein_target_g"] = (df["daily_protein_target_g"] / df["target_meals_per_day"]).round(1)
    df["workout_nutrition_window"] = np.where(
        df["workout_time"] == "Morning",
        "Fast whey/egg/sprouts post-workout by 09:00 AM",
        np.where(df["workout_time"] == "Evening", "Post-workout dinner + slow casein/paneer pre-sleep", "Balanced lunch + afternoon protein snack"),
    )
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="SmartProtein Stage 8 Accurate Protein Layer")
    parser.add_argument("--show-benchmark-foods", action="store_true", default=True)
    args = parser.parse_args()

    tables = json.loads(PROTEIN_TABLES_PATH.read_text(encoding="utf-8"))
    foods = pd.read_parquet(CURATED_DIR / "master_protein_foods.snappy.parquet")
    inventory = pd.read_parquet(CURATED_DIR / "local_store_inventory.snappy.parquet")
    users = pd.read_parquet(CURATED_DIR / "user_wellness_cleaned.snappy.parquet")

    enriched = enrich_protein_foods(foods, tables)
    enriched.to_parquet(CURATED_DIR / "protein_quality_enriched.snappy.parquet", compression="snappy", index=False)

    cost_cmp = build_usable_cost_comparison(enriched, inventory)
    cost_cmp.to_parquet(ANALYTICS_DIR / "usable_protein_cost_comparison.snappy.parquet", compression="snappy", index=False)

    pairings_df = pd.DataFrame(tables["complementary_pairings"])
    pairings_df.to_parquet(ANALYTICS_DIR / "complementary_pairings.snappy.parquet", compression="snappy", index=False)

    user_targets = compute_personalized_targets(users)
    user_targets.to_parquet(CURATED_DIR / "user_protein_targets.snappy.parquet", compression="snappy", index=False)

    print(f"[Stage 8] Enriched {len(enriched)} food-state rows -> protein_quality_enriched.snappy.parquet")
    print(f"[Stage 8] Computed personalized targets (1.2-2.0 g/kg) for {len(user_targets):,} anonymized users.")

    if args.show_benchmark_foods:
        key_items = [
            "Soya Chunks (Nutrela)",
            "Whole Boiled Eggs",
            "Chicken Breast (Skinless)",
            "Low-Fat Paneer",
            "Whey Protein Isolate",
        ]
        sub = cost_cmp[cost_cmp["canonical_name"].isin(key_items)]
        print("\n[Usable Protein & Cost Comparison: Whey vs Soy vs Eggs vs Paneer vs Chicken]")
        print(
            sub[
                [
                    "canonical_name", "diaas_score", "crude_protein_per_100g",
                    "usable_protein_per_100g", "median_price_100g_inr",
                    "cost_per_g_usable_protein_inr", "grams_for_2_5g_leucine",
                ]
            ].to_string(index=False)
        )


if __name__ == "__main__":
    main()
