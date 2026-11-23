"""SmartProtein Stage 9 (Modules #20-#29): Hybrid Cost-Aware Hyperlocal Recommender.

Combines:
  - Configurable Multi-Factor Scoring (w1..w5)
  - Hard Allergy & Medical Safety Filters
  - Haversine + GeoHash/Region Hyperlocal In-Stock Filtering
  - Regional Food Intelligence (Karnataka, North_India, Maharashtra, South_East)
  - Linear Programming Budget Optimizer (PuLP / SciPy HiGHS)
  - Hybrid Cold-Start + ALS Collaborative Filtering (Spark MLlib + Local SVD evaluator)
  - Explainable Recommendations & Precision@K / Recall@K / Cost Savings evaluation
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT_DIR.parent))

from smartprotein.spark_jobs.recommender.lp_budget_optimizer import (  # noqa: E402
    allocate_meal_plan,
    solve_daily_diet_lp,
)
from smartprotein.spark_jobs.recommender.safety_and_geo import (  # noqa: E402
    build_medical_warnings,
    compute_micronutrient_hints,
    haversine_km,
    is_food_safe_for_user,
)

DATA_DIR = ROOT_DIR / "data"
CURATED_DIR = DATA_DIR / "curated"
ANALYTICS_DIR = DATA_DIR / "analytics"
REC_CFG_PATH = ROOT_DIR / "config" / "recommender_config.json"
SEED_PATH = ROOT_DIR / "config" / "reference_foods_seed.json"


def run_spark_mllib_als(interactions_uri: str, output_uri: str) -> None:
    """Distributed PySpark MLlib ALS Collaborative Filtering for warm-start users."""
    from pyspark.ml.recommendation import ALS
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("SmartProtein-ALS-Recommender").getOrCreate()
    ratings = spark.read.parquet(interactions_uri)
    als = ALS(
        maxIter=10,
        regParam=0.1,
        userCol="user_idx",
        itemCol="food_idx",
        ratingCol="rating",
        coldStartStrategy="drop",
        nonnegative=True,
    )
    model = als.fit(ratings)
    recs = model.recommendForAllUsers(5)
    recs.write.mode("overwrite").parquet(output_uri)
    spark.stop()


def evaluate_collaborative_filtering_local(
    foods: pd.DataFrame,
    users: pd.DataFrame,
    seed: dict,
    k: int = 5,
) -> dict[str, float]:
    """Simulate user-food interaction history and evaluate SVD/ALS Precision@K & Recall@K."""
    rng = np.random.default_rng(42)
    sample_users = users.head(200).reset_index(drop=True)
    food_names = sorted(foods["canonical_name"].unique().tolist())
    n_u = len(sample_users)
    n_f = len(food_names)

    # Latent affinity matrix reflecting diet compatibility + regional staples
    R = np.zeros((n_u, n_f), dtype=float)
    food_diet = foods.drop_duplicates("canonical_name").set_index("canonical_name")["diet_type"].to_dict()
    diet_hierarchy = json.loads(REC_CFG_PATH.read_text(encoding="utf-8"))["diet_hierarchy"]

    for u_i, u in sample_users.iterrows():
        allowed = set(diet_hierarchy.get(u["diet_type"], ["Vegan"]))
        staples = set(seed["regions"].get(u["region"], {}).get("regional_staples", []))
        for f_j, fname in enumerate(food_names):
            if food_diet.get(fname) not in allowed:
                continue
            base = 4.3 if fname in staples else 3.2
            R[u_i, f_j] = np.clip(base + rng.normal(0, 0.5), 1.0, 5.0)

    # Hold out 30% of positive ratings (rating >= 3.8) as test ground truth
    train_R = R.copy()
    test_positives: dict[int, set[int]] = {}
    for u_i in range(n_u):
        pos_items = np.where(R[u_i] >= 3.8)[0]
        if len(pos_items) >= 3:
            holdout = rng.choice(pos_items, size=max(1, len(pos_items) // 3), replace=False)
            test_positives[u_i] = set(int(x) for x in holdout)
            train_R[u_i, holdout] = 0.0

    # Rank-8 truncated SVD (latent factor collaborative filtering proxy for Spark ALS)
    U, s, Vt = np.linalg.svd(train_R, full_matrices=False)
    rank = min(8, len(s))
    pred_R = (U[:, :rank] * s[:rank]) @ Vt[:rank, :]

    precisions, recalls = [], []
    for u_i, gt_set in test_positives.items():
        # Recommend top-K items not observed in train_R
        unseen = np.where(train_R[u_i] == 0)[0]
        if len(unseen) == 0:
            continue
        ranked_unseen = unseen[np.argsort(pred_R[u_i, unseen])[::-1]][:k]
        hits = len(set(int(x) for x in ranked_unseen) & gt_set)
        precisions.append(hits / k)
        recalls.append(hits / len(gt_set))

    return {
        "k": k,
        "evaluated_users": len(precisions),
        "precision_at_k": round(float(np.mean(precisions)), 4) if precisions else 0.0,
        "recall_at_k": round(float(np.mean(recalls)), 4) if recalls else 0.0,
    }


def score_candidates_for_user(
    user_row: pd.Series,
    foods_df: pd.DataFrame,
    inv_df: pd.DataFrame,
    cfg: dict,
    seed: dict,
) -> pd.DataFrame:
    """Filter safe, diet-compliant, hyperlocal in-stock offers and compute composite Score."""
    allowed_diets = cfg["diet_hierarchy"].get(user_row["diet_type"], ["Vegan"])
    allergy_aliases = cfg["allergy_aliases"]
    regional_staples = set(seed["regions"].get(user_row["region"], {}).get("regional_staples", []))

    # 1. Pre-filter foods by diet + allergy hard safety
    safe_foods = foods_df[foods_df["diet_type"].isin(allowed_diets)].copy()
    safe_mask = safe_foods["allergens"].apply(
        lambda a: is_food_safe_for_user(a, user_row["allergies"], allergy_aliases)
    )
    safe_foods = safe_foods[safe_mask].copy()
    # Keep primary retail state per food
    safe_foods["_p"] = np.where(safe_foods["state_raw_cooked"] == "raw", 0, 1)
    safe_foods = (
        safe_foods.sort_values(by=["_p", "usable_protein_per_100g"], ascending=[True, False])
        .drop_duplicates(subset=["canonical_name"], keep="first")
        .drop(columns=["_p"])
    )

    # 2. Hyperlocal filter: same city + in_stock + Haversine <= max_distance_km
    local_inv = inv_df[(inv_df["city"] == user_row["city"]) & (inv_df["in_stock"] == True)].copy()  # noqa: E712
    if local_inv.empty:
        local_inv = inv_df[(inv_df["region"] == user_row["region"]) & (inv_df["in_stock"] == True)].copy()  # noqa: E712

    local_inv["distance_km"] = local_inv.apply(
        lambda r: haversine_km(float(user_row["lat_grid"]), float(user_row["lon_grid"]), float(r["latitude"]), float(r["longitude"])),
        axis=1,
    )
    nearby = local_inv[local_inv["distance_km"] <= cfg["max_distance_km"]].copy()
    if nearby.empty:
        nearby = local_inv.sort_values("distance_km").head(60).copy()

    # Keep cheapest nearby store offer per canonical food
    nearby = nearby.sort_values(by=["price_per_100g_inr", "distance_km"]).drop_duplicates(
        subset=["canonical_name"], keep="first"
    )

    merged = safe_foods.merge(
        nearby[["canonical_name", "store_id", "store_name", "distance_km", "price_per_100g_inr"]],
        on="canonical_name",
        how="inner",
    )
    if merged.empty:
        return merged

    # 3. Configurable Multi-Factor Scoring (normalized 0..1 components)
    w = cfg["scoring_weights"]
    merged["usable_prot_per_rupee"] = merged["usable_protein_per_100g"] / merged["price_per_100g_inr"]
    merged["cost_per_10g_usable_inr"] = ((10.0 / merged["usable_protein_per_100g"]) * merged["price_per_100g_inr"]).round(2)

    prot_norm = merged["usable_protein_per_100g"] / max(float(merged["usable_protein_per_100g"].max()), 1.0)
    cost_eff_norm = merged["usable_prot_per_rupee"] / max(float(merged["usable_prot_per_rupee"].max()), 0.01)
    prox_norm = 1.0 - (merged["distance_km"] / max(cfg["max_distance_km"], 1.0)).clip(upper=1.0)
    pref_match = np.where(merged["canonical_name"].isin(regional_staples), 1.0, 0.55)
    kcal_penalty = (merged["adjusted_kcal_per_100g"] / 650.0).clip(upper=1.0)

    merged["composite_rec_score"] = (
        w["w1_adjusted_protein"] * prot_norm
        + w["w2_protein_per_cost"] * cost_eff_norm
        + w["w3_proximity"] * prox_norm
        + w["w4_preference_match"] * pref_match
        - w["w5_calorie_penalty"] * kcal_penalty
    ).round(4)

    merged["medical_warning"] = merged.apply(
        lambda r: build_medical_warnings(user_row["medical_conditions"], float(r["carbs_per_100g"]), float(r["usable_protein_per_100g"])),
        axis=1,
    )
    merged["explanation_text"] = merged.apply(
        lambda r: (
            f"{r['usable_protein_per_100g']}g usable protein/100g (DIAAS {r['diaas_score']}), "
            f"Rs {r['cost_per_10g_usable_inr']}/10g usable, {r['distance_km']} km away at {r['store_name']}"
        ),
        axis=1,
    )
    return merged.sort_values(by="composite_rec_score", ascending=False)


def main() -> None:
    cfg = json.loads(REC_CFG_PATH.read_text(encoding="utf-8"))
    seed = json.loads(SEED_PATH.read_text(encoding="utf-8"))

    foods = pd.read_parquet(CURATED_DIR / "protein_quality_enriched.snappy.parquet")
    inv = pd.read_parquet(CURATED_DIR / "local_store_inventory.snappy.parquet")
    users = pd.read_parquet(CURATED_DIR / "user_protein_targets.snappy.parquet")

    # Generate Top-5 Explainable Recommendations & LP Optimal Meal Plans for sample cohort (250 users)
    cohort = users.head(250).copy()
    rec_rows = []
    lp_rows = []
    cost_savings_pct = []

    for _, u in cohort.iterrows():
        scored = score_candidates_for_user(u, foods, inv, cfg, seed)
        if scored.empty:
            continue

        for rank, (_, r) in enumerate(scored.head(5).iterrows(), start=1):
            rec_rows.append(
                {
                    "user_hash_id": u["user_hash_id"],
                    "region": u["region"],
                    "city": u["city"],
                    "diet_type": u["diet_type"],
                    "allergies": u["allergies"],
                    "daily_budget_inr": u["daily_budget_inr"],
                    "daily_protein_target_g": u["daily_protein_target_g"],
                    "rank": rank,
                    "canonical_name": r["canonical_name"],
                    "store_name": r["store_name"],
                    "distance_km": r["distance_km"],
                    "usable_protein_per_100g": r["usable_protein_per_100g"],
                    "price_per_100g_inr": r["price_per_100g_inr"],
                    "cost_per_g_usable_protein": round(float(r["price_per_100g_inr"]) / max(float(r["usable_protein_per_100g"]), 0.1), 2),
                    "composite_rec_score": r["composite_rec_score"],
                    "medical_warning": r["medical_warning"],
                    "explanation_text": r["explanation_text"],
                }
            )

        # Solve Linear Programming Budget Diet Problem for this user
        lp_res = solve_daily_diet_lp(
            scored,
            protein_target_g=float(u["daily_protein_target_g"]),
            max_budget_inr=float(u["daily_budget_inr"]),
            max_single_food_g=cfg["lp_defaults"]["max_single_food_grams_per_day"],
            max_kcal=cfg["lp_defaults"]["daily_kcal_max"],
        )
        if lp_res["items"]:
            # Naive unoptimized cost baseline = buying median-priced protein foods to hit target
            naive_cost_per_g = float((scored["price_per_100g_inr"] / scored["usable_protein_per_100g"]).median())
            naive_daily_cost = naive_cost_per_g * float(u["daily_protein_target_g"])
            if naive_daily_cost > 0:
                cost_savings_pct.append(max(0.0, (1.0 - lp_res["total_cost_inr"] / naive_daily_cost) * 100.0))

            hints = compute_micronutrient_hints(
                str(u["diet_type"]),
                lp_res.get("total_iron_mg", 0.0),
                lp_res.get("total_calcium_mg", 0.0),
                lp_res.get("total_b12_mcg", 0.0),
                cfg["micronutrient_rda"],
            )
            meal_plan = allocate_meal_plan(lp_res["items"], str(u["workout_time"]), adherence_pct=82.0)
            lp_rows.append(
                {
                    "user_hash_id": u["user_hash_id"],
                    "region": u["region"],
                    "city": u["city"],
                    "diet_type": u["diet_type"],
                    "daily_protein_target_g": u["daily_protein_target_g"],
                    "daily_budget_inr": u["daily_budget_inr"],
                    "lp_status": lp_res["status"],
                    "lp_total_cost_inr": lp_res["total_cost_inr"],
                    "lp_usable_protein_g": lp_res["total_usable_protein_g"],
                    "within_budget": lp_res["within_budget"],
                    "meal_plan_json": json.dumps(meal_plan),
                    "micronutrient_hints": " | ".join(hints),
                }
            )

    recs_df = pd.DataFrame(rec_rows)
    lp_df = pd.DataFrame(lp_rows)
    recs_df.to_parquet(ANALYTICS_DIR / "gold_user_recommendations.snappy.parquet", compression="snappy", index=False)
    lp_df.to_parquet(ANALYTICS_DIR / "lp_optimal_meal_plans.snappy.parquet", compression="snappy", index=False)

    cf_metrics = evaluate_collaborative_filtering_local(foods, users, seed, k=5)
    avg_savings = round(float(np.mean(cost_savings_pct)), 1) if cost_savings_pct else 0.0
    eval_summary = pd.DataFrame(
        [
            {
                **cf_metrics,
                "lp_users_solved": len(lp_df),
                "lp_within_budget_pct": round(float(lp_df["within_budget"].mean() * 100.0), 1) if not lp_df.empty else 0.0,
                "avg_lp_cost_savings_pct": avg_savings,
            }
        ]
    )
    eval_summary.to_parquet(ANALYTICS_DIR / "recommender_eval_metrics.snappy.parquet", compression="snappy", index=False)

    print(f"[Stage 9 Recommender] Generated {len(recs_df):,} explainable recommendations & {len(lp_df):,} LP meal plans.")
    print(eval_summary.to_string(index=False))
    if not recs_df.empty:
        print("\n[Sample Explainable Output]:")
        print(recs_df[["user_hash_id", "city", "diet_type", "rank", "explanation_text"]].head(3).to_string(index=False))


if __name__ == "__main__":
    main()
