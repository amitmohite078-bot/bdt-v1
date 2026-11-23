"""FitGuard-HDFS Local Lakehouse Medallion Runner.

Mirrors the exact HDFS Bronze -> Silver (+ Quarantine) -> Gold architecture
using Apache Arrow / Snappy Parquet so the full pipeline runs natively on
Windows without requiring Docker or a live Hadoop daemon, while producing
identical Parquet tables and metrics as the PySpark HDFS job.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from spark_jobs.dq_rules import (  # noqa: E402
    UNIT_TO_100G_FACTOR,
    canonicalize_city,
    canonicalize_diet,
    compute_composite_dq_score,
    is_diet_tag_semantically_valid,
    load_aliases,
    load_config,
)


def _dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _file_count(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for f in path.rglob("*") if f.is_file())


def _parse_mixed_timestamp(raw_val: Any) -> pd.Timestamp | None:
    if raw_val is None or (isinstance(raw_val, float) and np.isnan(raw_val)):
        return None
    if isinstance(raw_val, (int, float)):
        return pd.to_datetime(int(raw_val), unit="ms", utc=True, errors="coerce")
    s = str(raw_val).strip()
    if s.isdigit():
        return pd.to_datetime(int(s), unit="ms", utc=True, errors="coerce")
    return pd.to_datetime(s, utc=True, errors="coerce")


def clean_user_profiles(
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    cfg: dict[str, Any],
    aliases: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    df = pd.read_csv(bronze_dir / "user_profiles" / "user_profiles.csv")
    total_raw = len(df)
    thresh = cfg["dq_thresholds"]
    city_map = aliases["aliases"]
    diet_map = aliases["diet_aliases"]

    df["canonical_city"] = df["location_city"].apply(lambda x: canonicalize_city(x if pd.notna(x) else None, city_map))
    df["canonical_diet"] = df["diet_choice"].apply(lambda x: canonicalize_diet(x if pd.notna(x) else None, diet_map))
    df["weight_kg_num"] = pd.to_numeric(df["weight_kg"], errors="coerce")
    df["budget_num"] = pd.to_numeric(df["daily_food_budget_inr"], errors="coerce")
    df["updated_ts"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")

    # Sort descending by updated_at so we keep newest profile on dedup
    df = df.sort_values(by=["user_id", "updated_ts"], ascending=[True, False], na_position="last")
    is_dup = df["user_id"].notna() & df.duplicated(subset=["user_id"], keep="first")

    reasons: list[str] = []
    rule_ids: list[str] = []
    dims: list[str] = []

    for idx, row in df.iterrows():
        if pd.isna(row["user_id"]) or str(row["user_id"]).strip() == "":
            reasons.append("Missing required user_id")
            rule_ids.append("DQ-CMP-001")
            dims.append("Completeness")
        elif bool(is_dup.loc[idx]):
            reasons.append("Duplicate user_id sync record")
            rule_ids.append("DQ-UNQ-001")
            dims.append("Uniqueness")
        elif row["canonical_diet"] is None:
            reasons.append(f"Unrecognized or missing diet_choice: {row['diet_choice']}")
            rule_ids.append("DQ-CMP-002")
            dims.append("Completeness")
        elif row["canonical_city"] is None:
            reasons.append(f"Unresolvable city location: {row['location_city']}")
            rule_ids.append("DQ-LOC-001")
            dims.append("Consistency")
        elif pd.isna(row["weight_kg_num"]) or not (thresh["min_weight_kg"] <= row["weight_kg_num"] <= thresh["max_weight_kg"]):
            reasons.append(f"Physiologically implausible weight_kg: {row['weight_kg']}")
            rule_ids.append("DQ-PHY-003")
            dims.append("Validity")
        elif pd.isna(row["budget_num"]) or not (thresh["min_budget_inr"] <= row["budget_num"] <= thresh["max_budget_inr"]):
            reasons.append(f"Invalid daily_food_budget_inr: {row['daily_food_budget_inr']}")
            rule_ids.append("DQ-VAL-002")
            dims.append("Validity")
        else:
            reasons.append("")
            rule_ids.append("")
            dims.append("")

    df["dq_error_reason"] = reasons
    df["dq_rule_id"] = rule_ids
    df["dq_dimension"] = dims

    bad_mask = df["dq_rule_id"] != ""
    quarantine_df = df[bad_mask].copy()
    quarantine_df["source_dataset"] = "user_profiles"

    silver_df = df[~bad_mask].copy()
    silver_df = silver_df.drop(columns=["dq_error_reason", "dq_rule_id", "dq_dimension"])
    silver_df["location_city"] = silver_df["canonical_city"]
    silver_df["diet_choice"] = silver_df["canonical_diet"]
    silver_df["weight_kg"] = silver_df["weight_kg_num"].round(1)
    silver_df["daily_food_budget_inr"] = silver_df["budget_num"].round(2)
    silver_df = silver_df.drop(columns=["canonical_city", "canonical_diet", "weight_kg_num", "budget_num", "updated_ts"])

    (silver_dir / "user_profiles").mkdir(parents=True, exist_ok=True)
    (quarantine_dir / "user_profiles").mkdir(parents=True, exist_ok=True)
    silver_df.to_parquet(silver_dir / "user_profiles" / "part-00000.snappy.parquet", compression="snappy", index=False)
    quarantine_df.to_parquet(quarantine_dir / "user_profiles" / "part-00000.snappy.parquet", compression="snappy", index=False)

    dim_scores = _compute_dimension_scores(total_raw, quarantine_df)
    return silver_df, quarantine_df, dim_scores


def clean_protein_foods(
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    cfg: dict[str, Any],
    aliases: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    df = pd.read_csv(bronze_dir / "protein_foods_catalog" / "protein_foods_catalog.csv")
    total_raw = len(df)
    thresh = cfg["dq_thresholds"]
    city_map = aliases["aliases"]
    diet_map = aliases["diet_aliases"]
    now_utc = pd.Timestamp.now(tz="UTC")

    df["canonical_city"] = df["city"].apply(lambda x: canonicalize_city(x if pd.notna(x) else None, city_map))
    df["canonical_diet"] = df["diet_tag"].apply(lambda x: canonicalize_diet(x if pd.notna(x) else None, diet_map))
    df["protein_num"] = pd.to_numeric(df["protein_g"], errors="coerce")
    df["calories_num"] = pd.to_numeric(df["calories_kcal"], errors="coerce")
    df["cost_num"] = pd.to_numeric(df["cost"], errors="coerce")
    df["scraped_ts"] = pd.to_datetime(df["scraped_at"], utc=True, errors="coerce")

    # Convert mixed units & currencies to per_100g_inr
    unit_factors = df["unit_basis"].astype(str).str.lower().map(UNIT_TO_100G_FACTOR)
    fx_mult = np.where(df["currency"].astype(str).str.upper() == "USD", thresh["usd_to_inr"], 1.0)

    df["protein_per_100g"] = (df["protein_num"] / unit_factors).round(2)
    df["calories_per_100g"] = (df["calories_num"] / unit_factors).round(1)
    df["cost_per_100g_inr"] = ((df["cost_num"] * fx_mult) / unit_factors).round(2)

    is_dup = (
        df["food_name"].notna()
        & df["canonical_city"].notna()
        & df.duplicated(subset=["food_name", "canonical_city", "vendor_name", "unit_basis"], keep="first")
    )

    reasons: list[str] = []
    rule_ids: list[str] = []
    dims: list[str] = []

    for idx, row in df.iterrows():
        fname = None if pd.isna(row["food_name"]) else str(row["food_name"]).strip()
        if not fname or pd.isna(row["protein_num"]) or pd.isna(row["calories_num"]):
            reasons.append("Missing critical food_name or macronutrient value")
            rule_ids.append("DQ-CMP-001")
            dims.append("Completeness")
        elif pd.isna(unit_factors.loc[idx]):
            reasons.append(f"Unsupported unit_basis: {row['unit_basis']}")
            rule_ids.append("DQ-SCH-002")
            dims.append("Consistency")
        elif row["canonical_city"] is None:
            reasons.append(f"Unresolvable food location city: {row['city']}")
            rule_ids.append("DQ-LOC-001")
            dims.append("Consistency")
        elif not is_diet_tag_semantically_valid(fname, row["canonical_diet"]):
            reasons.append(
                f"CRITICAL SEMANTIC LIE: '{fname}' dangerously tagged as '{row['diet_tag']}'"
            )
            rule_ids.append("DQ-SEM-001")
            dims.append("Semantic_Integrity")
        elif bool(is_dup.loc[idx]):
            reasons.append("Duplicate vendor food listing in same city")
            rule_ids.append("DQ-UNQ-001")
            dims.append("Uniqueness")
        elif not (0.5 <= row["protein_per_100g"] <= thresh["max_protein_per_100g"]):
            reasons.append(f"Impossible protein_per_100g: {row['protein_per_100g']}g")
            rule_ids.append("DQ-PHY-002")
            dims.append("Validity")
        elif row["calories_per_100g"] < row["protein_per_100g"] * 3.6:
            reasons.append(
                f"Thermodynamic macro mismatch: calories ({row['calories_per_100g']}) < 3.6 * protein ({row['protein_per_100g']})"
            )
            rule_ids.append("DQ-PHY-002")
            dims.append("Validity")
        elif pd.isna(row["cost_per_100g_inr"]) or not (1.0 <= row["cost_per_100g_inr"] <= thresh["max_cost_per_100g_inr"]):
            reasons.append(f"Invalid/outlier cost_per_100g_inr: {row['cost_per_100g_inr']}")
            rule_ids.append("DQ-VAL-002")
            dims.append("Validity")
        elif pd.isna(row["scraped_ts"]) or (now_utc - row["scraped_ts"]) > pd.Timedelta(days=60):
            reasons.append(f"Stale price listing (>60 days old): {row['scraped_at']}")
            rule_ids.append("DQ-TIM-001")
            dims.append("Timeliness")
        else:
            reasons.append("")
            rule_ids.append("")
            dims.append("")

    df["dq_error_reason"] = reasons
    df["dq_rule_id"] = rule_ids
    df["dq_dimension"] = dims

    bad_mask = df["dq_rule_id"] != ""
    quarantine_df = df[bad_mask].copy()
    quarantine_df["source_dataset"] = "protein_foods_catalog"

    silver_df = df[~bad_mask].copy()
    silver_df["city"] = silver_df["canonical_city"]
    silver_df["diet_tag"] = silver_df["canonical_diet"]
    silver_df["protein_per_100_inr"] = (
        (silver_df["protein_per_100g"] / silver_df["cost_per_100g_inr"]) * 100.0
    ).round(2)
    silver_df["cost_per_20g_protein_inr"] = (
        (20.0 / silver_df["protein_per_100g"]) * silver_df["cost_per_100g_inr"]
    ).round(2)

    keep_cols = [
        "listing_id",
        "food_name",
        "category",
        "diet_tag",
        "protein_per_100g",
        "calories_per_100g",
        "cost_per_100g_inr",
        "protein_per_100_inr",
        "cost_per_20g_protein_inr",
        "vendor_name",
        "city",
        "in_stock",
        "scraped_at",
    ]
    silver_df = silver_df[keep_cols]

    (silver_dir / "protein_foods_catalog").mkdir(parents=True, exist_ok=True)
    (quarantine_dir / "protein_foods_catalog").mkdir(parents=True, exist_ok=True)
    silver_df.to_parquet(
        silver_dir / "protein_foods_catalog" / "part-00000.snappy.parquet",
        compression="snappy",
        index=False,
    )
    quarantine_df.to_parquet(
        quarantine_dir / "protein_foods_catalog" / "part-00000.snappy.parquet",
        compression="snappy",
        index=False,
    )

    dim_scores = _compute_dimension_scores(total_raw, quarantine_df)
    return silver_df, quarantine_df, dim_scores


def clean_wearable_telemetry(
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    cfg: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    telemetry_dir = bronze_dir / "wearable_telemetry"
    part_files = sorted(telemetry_dir.glob("telemetry_part_*.jsonl"))
    thresh = cfg["dq_thresholds"]
    now_utc = pd.Timestamp.now(tz="UTC")

    parsed_records: list[dict[str, Any]] = []
    corrupt_rows: list[dict[str, Any]] = []

    for fpath in part_files:
        with fpath.open("r", encoding="utf-8") as fh:
            for line in fh:
                raw_line = line.strip()
                if not raw_line:
                    continue
                try:
                    obj = json.loads(raw_line)
                    parsed_records.append(obj)
                except json.JSONDecodeError:
                    corrupt_rows.append(
                        {
                            "raw_payload": raw_line[:240],
                            "dq_rule_id": "DQ-SCH-001",
                            "dq_dimension": "Validity",
                            "dq_error_reason": "Unparseable malformed JSON telemetry record",
                            "source_dataset": "wearable_telemetry",
                        }
                    )

    df = pd.DataFrame(parsed_records)
    total_raw = len(df) + len(corrupt_rows)

    ts_str = df["event_ts"].astype("string").str.strip()
    is_epoch = ts_str.str.fullmatch(r"\d+").fillna(False)
    parsed_iso = pd.to_datetime(ts_str.where(~is_epoch), utc=True, errors="coerce", format="ISO8601")
    parsed_epoch = pd.to_datetime(
        pd.to_numeric(ts_str.where(is_epoch), errors="coerce"),
        unit="ms",
        utc=True,
        errors="coerce",
    )
    df["parsed_ts"] = parsed_iso.fillna(parsed_epoch)
    df["hr_num"] = pd.to_numeric(df["heart_rate_bpm"], errors="coerce")
    df["spo2_num"] = pd.to_numeric(df["spo2_pct"], errors="coerce")
    df["steps_num"] = pd.to_numeric(df["steps"], errors="coerce")
    df["cal_num"] = pd.to_numeric(df["calories_burned"], errors="coerce")

    # Smart Imputation: Flag isolated missing HR dropouts and fill via user-level rolling median
    df = df.sort_values(by=["user_id", "parsed_ts"], na_position="last")
    df["is_imputed"] = df["hr_num"].isna() & df["user_id"].notna()
    user_medians = df.groupby("user_id")["hr_num"].transform("median")
    df["hr_imputed"] = df["hr_num"].fillna(user_medians).round(0)

    is_dup = (
        df["user_id"].notna()
        & df["parsed_ts"].notna()
        & df.duplicated(subset=["user_id", "parsed_ts"], keep="first")
    )

    # Vectorized rule assignment (fast over 50k+ rows)
    cond_missing_key = df["user_id"].isna() | df["parsed_ts"].isna()
    cond_timeliness = (df["parsed_ts"] > now_utc + pd.Timedelta(hours=1)) | (
        df["parsed_ts"] < now_utc - pd.Timedelta(days=120)
    )
    cond_dup = is_dup
    cond_hr_bad = df["hr_imputed"].isna() | (df["hr_imputed"] < thresh["heart_rate_min"]) | (df["hr_imputed"] > thresh["heart_rate_max"])
    cond_spo2_bad = df["spo2_num"].isna() | (df["spo2_num"] < thresh["spo2_min"]) | (df["spo2_num"] > thresh["spo2_max"])
    cond_steps_bad = df["steps_num"].isna() | (df["steps_num"] < 0) | (df["steps_num"] > thresh["max_steps_per_hour"])
    cond_xsig = (df["sleep_stage"] == "DEEP_SLEEP") & (df["steps_num"] > 300)

    conditions = [
        cond_missing_key,
        cond_timeliness,
        cond_dup,
        cond_xsig,
        cond_hr_bad,
        cond_spo2_bad,
        cond_steps_bad,
    ]
    rule_choices = [
        "DQ-CMP-001",
        "DQ-TIM-001",
        "DQ-UNQ-001",
        "DQ-XSIG-001",
        "DQ-PHY-001",
        "DQ-PHY-001",
        "DQ-PHY-001",
    ]
    dim_choices = [
        "Completeness",
        "Timeliness",
        "Uniqueness",
        "Semantic_Integrity",
        "Validity",
        "Validity",
        "Validity",
    ]
    reason_choices = [
        "Missing user_id or invalid timestamp",
        "Future or stale (>120d) event timestamp",
        "Multi-device duplicate sync event",
        "Cross-signal contradiction: High step count during DEEP_SLEEP",
        "Physiological heart rate out of bounds [32, 220] bpm",
        "Physiological SpO2 out of bounds [70, 100] %",
        "Implausible hourly step count",
    ]

    df["dq_rule_id"] = np.select(conditions, rule_choices, default="")
    df["dq_dimension"] = np.select(conditions, dim_choices, default="")
    df["dq_error_reason"] = np.select(conditions, reason_choices, default="")

    bad_mask = df["dq_rule_id"] != ""
    bad_df = df[bad_mask][["event_id", "user_id", "device_type", "event_ts", "dq_rule_id", "dq_dimension", "dq_error_reason"]].copy()
    bad_df["event_ts"] = bad_df["event_ts"].astype("string")
    bad_df["source_dataset"] = "wearable_telemetry"

    corrupt_df = pd.DataFrame(corrupt_rows)
    quarantine_df = pd.concat([bad_df, corrupt_df], ignore_index=True)
    for col in quarantine_df.columns:
        if quarantine_df[col].dtype == "object":
            quarantine_df[col] = quarantine_df[col].astype("string")

    silver_df = df[~bad_mask].copy()
    silver_df["heart_rate_bpm"] = silver_df["hr_imputed"].astype(int)
    silver_df["spo2_pct"] = silver_df["spo2_num"].round(1)
    silver_df["steps"] = silver_df["steps_num"].astype(int)
    silver_df["calories_burned"] = silver_df["cal_num"].round(1)
    silver_df["event_date"] = silver_df["parsed_ts"].dt.strftime("%Y-%m-%d")

    keep_cols = [
        "event_id",
        "user_id",
        "device_type",
        "parsed_ts",
        "event_date",
        "heart_rate_bpm",
        "spo2_pct",
        "steps",
        "sleep_stage",
        "calories_burned",
        "is_imputed",
    ]
    silver_df = silver_df[keep_cols].rename(columns={"parsed_ts": "event_ts_utc"})

    # Demonstrate HDFS Small-Files Compaction: many JSONL files -> 1 Snappy Parquet file!
    (silver_dir / "wearable_telemetry").mkdir(parents=True, exist_ok=True)
    (quarantine_dir / "wearable_telemetry").mkdir(parents=True, exist_ok=True)
    silver_df.to_parquet(
        silver_dir / "wearable_telemetry" / "part-00000.snappy.parquet",
        compression="snappy",
        index=False,
    )
    quarantine_df.to_parquet(
        quarantine_dir / "wearable_telemetry" / "part-00000.snappy.parquet",
        compression="snappy",
        index=False,
    )

    dim_scores = _compute_dimension_scores(total_raw, quarantine_df)
    return silver_df, quarantine_df, dim_scores


def _compute_dimension_scores(total_rows: int, quarantine_df: pd.DataFrame) -> dict[str, float]:
    if total_rows == 0:
        return {}
    dims = ["Completeness", "Validity", "Uniqueness", "Consistency", "Timeliness", "Semantic_Integrity"]
    counts = quarantine_df["dq_dimension"].value_counts().to_dict() if len(quarantine_df) else {}
    scores = {}
    for d in dims:
        bad = counts.get(d, 0)
        scores[d] = round(max(0.0, (1.0 - bad / total_rows) * 100.0), 2)
    scores["Composite_Score"] = compute_composite_dq_score(scores)
    scores["Pass_Rate_Pct"] = round(((total_rows - len(quarantine_df)) / total_rows) * 100.0, 2)
    scores["Total_Raw_Rows"] = float(total_rows)
    scores["Quarantined_Rows"] = float(len(quarantine_df))
    scores["Clean_Silver_Rows"] = float(total_rows - len(quarantine_df))
    return scores


def build_gold_layer(
    silver_users: pd.DataFrame,
    silver_foods: pd.DataFrame,
    silver_telemetry: pd.DataFrame,
    dq_summary: list[dict[str, Any]],
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    gold_dir: Path,
    cfg: dict[str, Any],
) -> None:
    gold_dir.mkdir(parents=True, exist_ok=True)

    # 1. User Daily Activity Summary from clean wearable telemetry
    daily_user_activity = (
        silver_telemetry.groupby(["user_id", "event_date"], as_index=False)
        .agg(
            daily_steps=("steps", "sum"),
            avg_heart_rate=("heart_rate_bpm", "mean"),
            avg_spo2=("spo2_pct", "mean"),
            daily_calories_burned=("calories_burned", "sum"),
            imputed_readings=("is_imputed", "sum"),
        )
    )
    user_wellness_agg = (
        daily_user_activity.groupby("user_id", as_index=False)
        .agg(
            avg_daily_steps=("daily_steps", "mean"),
            avg_heart_rate=("avg_heart_rate", "mean"),
            avg_spo2=("avg_spo2", "mean"),
            avg_daily_calories_burned=("daily_calories_burned", "mean"),
            total_imputed_readings=("imputed_readings", "sum"),
        )
    )

    # Join Clean Users + Clean Wellness Activity -> Activity-Adjusted Protein Target
    goal_factors = cfg["protein_target_g_per_kg"]
    user_targets = silver_users.merge(user_wellness_agg, on="user_id", how="left")
    user_targets["avg_daily_steps"] = user_targets["avg_daily_steps"].fillna(6500.0).round(0)
    user_targets["avg_heart_rate"] = user_targets["avg_heart_rate"].fillna(75.0).round(1)
    user_targets["avg_spo2"] = user_targets["avg_spo2"].fillna(97.5).round(1)
    user_targets["avg_daily_calories_burned"] = user_targets["avg_daily_calories_burned"].fillna(1800.0).round(1)

    # Activity bonus: +0.15 g/kg if avg_daily_steps >= 9500
    base_factor = user_targets["fitness_goal"].map(goal_factors).fillna(1.2)
    activity_bonus = np.where(user_targets["avg_daily_steps"] >= 9500, 0.15, 0.0)
    user_targets["daily_protein_target_g"] = (
        user_targets["weight_kg"] * (base_factor + activity_bonus)
    ).round(1)

    user_targets.to_parquet(gold_dir / "user_wellness_targets.snappy.parquet", compression="snappy", index=False)

    # 2. Precompute Personalized Protein Food Recommendations (Choice + Cost + Location)
    diet_hierarchy = cfg["diet_hierarchy"]
    in_stock_foods = silver_foods[silver_foods["in_stock"] == True].copy()  # noqa: E712

    rec_rows: list[dict[str, Any]] = []
    for _, u in user_targets.iterrows():
        allowed_diets = diet_hierarchy.get(u["diet_choice"], ["Vegan"])
        # Filter by Choice + Location + Affordable Cost (at least 100g serving fits within 60% of daily food budget)
        candidates = in_stock_foods[
            (in_stock_foods["city"] == u["location_city"])
            & (in_stock_foods["diet_tag"].isin(allowed_diets))
            & (in_stock_foods["cost_per_100g_inr"] <= u["daily_food_budget_inr"] * 0.70)
        ].sort_values(by=["protein_per_100_inr", "protein_per_100g"], ascending=[False, False])

        # Deduplicate food names per user recommendation so top-5 shows diverse foods
        candidates = candidates.drop_duplicates(subset=["food_name"], keep="first").head(5)

        for rank, (_, f) in enumerate(candidates.iterrows(), start=1):
            # Portion needed to supply 35% of daily protein target from this primary food
            grams_for_meal = min(300.0, round((u["daily_protein_target_g"] * 0.35 / f["protein_per_100g"]) * 100.0, 0))
            meal_cost = round((grams_for_meal / 100.0) * f["cost_per_100g_inr"], 2)
            meal_protein = round((grams_for_meal / 100.0) * f["protein_per_100g"], 1)
            rec_rows.append(
                {
                    "user_id": u["user_id"],
                    "location_city": u["location_city"],
                    "user_diet_choice": u["diet_choice"],
                    "fitness_goal": u["fitness_goal"],
                    "daily_food_budget_inr": u["daily_food_budget_inr"],
                    "daily_protein_target_g": u["daily_protein_target_g"],
                    "rank": rank,
                    "food_name": f["food_name"],
                    "food_diet_tag": f["diet_tag"],
                    "category": f["category"],
                    "vendor_name": f["vendor_name"],
                    "protein_per_100g": f["protein_per_100g"],
                    "cost_per_100g_inr": f["cost_per_100g_inr"],
                    "protein_per_100_inr": f["protein_per_100_inr"],
                    "recommended_portion_g": grams_for_meal,
                    "estimated_portion_protein_g": meal_protein,
                    "estimated_portion_cost_inr": meal_cost,
                }
            )

    recs_df = pd.DataFrame(rec_rows)
    recs_df.to_parquet(gold_dir / "protein_recommendations.snappy.parquet", compression="snappy", index=False)

    # 3. Gold DQ Scorecard & HDFS Storage Optimization Metrics
    dq_df = pd.DataFrame(dq_summary)
    dq_df.to_parquet(gold_dir / "dq_scorecard.snappy.parquet", compression="snappy", index=False)

    storage_stats = pd.DataFrame(
        [
            {
                "zone": "Bronze (Raw JSONL/CSV)",
                "file_count": _file_count(bronze_dir),
                "size_mb": round(_dir_size_bytes(bronze_dir) / (1024 * 1024), 3),
            },
            {
                "zone": "Silver (Compacted Snappy Parquet)",
                "file_count": _file_count(silver_dir),
                "size_mb": round(_dir_size_bytes(silver_dir) / (1024 * 1024), 3),
            },
            {
                "zone": "Quarantine (Dead-Letter Parquet)",
                "file_count": _file_count(quarantine_dir),
                "size_mb": round(_dir_size_bytes(quarantine_dir) / (1024 * 1024), 3),
            },
            {
                "zone": "Gold (Analytics & Recommendations)",
                "file_count": _file_count(gold_dir),
                "size_mb": round(_dir_size_bytes(gold_dir) / (1024 * 1024), 3),
            },
        ]
    )
    storage_stats.to_parquet(gold_dir / "hdfs_storage_stats.snappy.parquet", compression="snappy", index=False)


def main() -> None:
    t0 = time.perf_counter()
    cfg = load_config()
    aliases = load_aliases()

    lake_root = ROOT_DIR / cfg["local_lakehouse_root"]
    bronze_dir = lake_root / cfg["zones"]["bronze"]
    silver_dir = lake_root / cfg["zones"]["silver"]
    quarantine_dir = lake_root / cfg["zones"]["quarantine"]
    gold_dir = lake_root / cfg["zones"]["gold"]

    print("[FitGuard-HDFS] Running Bronze -> Silver + Quarantine -> Gold pipeline...")

    s_users, q_users, dq_users = clean_user_profiles(bronze_dir, silver_dir, quarantine_dir, cfg, aliases)
    s_foods, q_foods, dq_foods = clean_protein_foods(bronze_dir, silver_dir, quarantine_dir, cfg, aliases)
    s_tel, q_tel, dq_tel = clean_wearable_telemetry(bronze_dir, silver_dir, quarantine_dir, cfg)

    dq_summary = [
        {"dataset": "user_profiles", **dq_users},
        {"dataset": "protein_foods_catalog", **dq_foods},
        {"dataset": "wearable_telemetry", **dq_tel},
    ]

    build_gold_layer(
        s_users,
        s_foods,
        s_tel,
        dq_summary,
        bronze_dir,
        silver_dir,
        quarantine_dir,
        gold_dir,
        cfg,
    )

    elapsed = round(time.perf_counter() - t0, 2)
    print(f"[FitGuard-HDFS] Pipeline complete in {elapsed}s!")
    for row in dq_summary:
        print(
            f"  * {row['dataset']:22s} | Raw: {int(row['Total_Raw_Rows']):6,} | "
            f"Silver: {int(row['Clean_Silver_Rows']):6,} | "
            f"Quarantined: {int(row['Quarantined_Rows']):5,} | "
            f"DQ Score: {row['Composite_Score']:.2f}%"
        )


if __name__ == "__main__":
    main()
