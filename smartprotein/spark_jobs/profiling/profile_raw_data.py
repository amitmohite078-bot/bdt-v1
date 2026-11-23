"""SmartProtein Stage 6 (Module #2): Automated Raw Data Profiling.

Computes per-column null %, min, max, mean, standard deviation, distinct count,
and duplicate rate across raw_food_composition, raw_store_prices, and raw_user_wearables.

Runs in PySpark when pyspark is available (or on Spark cluster), with an automatic
local Pandas/PyArrow fallback (--engine auto) for rapid laptop testing.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT_DIR / "data"


def profile_dataframe_local(df: pd.DataFrame, dataset_name: str, key_cols: list[str]) -> dict[str, Any]:
    total = len(df)
    dup_count = int(df.duplicated(subset=key_cols).sum()) if key_cols else int(df.duplicated().sum())
    col_profiles = []
    for col in df.columns:
        null_cnt = int(df[col].isna().sum())
        null_pct = round((null_cnt / max(total, 1)) * 100.0, 2)
        distinct_cnt = int(df[col].nunique(dropna=True))
        entry: dict[str, Any] = {
            "column": col,
            "dtype": str(df[col].dtype),
            "null_count": null_cnt,
            "null_pct": null_pct,
            "distinct_count": distinct_cnt,
        }
        if pd.api.types.is_numeric_dtype(df[col]) and not pd.api.types.is_bool_dtype(df[col]):
            s = df[col].dropna()
            if len(s) > 0:
                entry.update(
                    {
                        "min": round(float(s.min()), 3),
                        "p25": round(float(s.quantile(0.25)), 3),
                        "median": round(float(s.median()), 3),
                        "p75": round(float(s.quantile(0.75)), 3),
                        "max": round(float(s.max()), 3),
                        "mean": round(float(s.mean()), 3),
                    }
                )
        col_profiles.append(entry)

    return {
        "dataset": dataset_name,
        "total_rows": total,
        "duplicate_rows_on_key": dup_count,
        "duplicate_pct": round((dup_count / max(total, 1)) * 100.0, 2),
        "columns": col_profiles,
    }


def profile_with_spark(base_path: str) -> list[dict[str, Any]]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("SmartProtein-Profiler").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    specs = [
        ("food_composition", f"{base_path}/raw/food_composition", ["source_db", "food_name", "state_raw_cooked"]),
        ("store_prices", f"{base_path}/raw/store_prices", ["store_id", "canonical_food_hint", "pack_weight_g"]),
        ("user_wearables", f"{base_path}/raw/user_wearables", ["user_id", "event_ts"]),
    ]

    results = []
    for ds_name, uri, key_cols in specs:
        df = spark.read.option("header", True).option("inferSchema", True).csv(uri)
        total = df.count()
        distinct_keys = df.select(*key_cols).dropDuplicates().count()
        dup_cnt = total - distinct_keys

        col_profiles = []
        for col_name, dtype in df.dtypes:
            agg_exprs = [
                F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias("null_cnt"),
                F.countDistinct(F.col(col_name)).alias("distinct_cnt"),
            ]
            if dtype in ("double", "int", "bigint", "float"):
                agg_exprs.extend(
                    [
                        F.min(col_name).alias("min_v"),
                        F.max(col_name).alias("max_v"),
                        F.avg(col_name).alias("mean_v"),
                    ]
                )
            row = df.agg(*agg_exprs).collect()[0].asDict()
            entry = {
                "column": col_name,
                "dtype": dtype,
                "null_count": int(row["null_cnt"] or 0),
                "null_pct": round(((row["null_cnt"] or 0) / max(total, 1)) * 100.0, 2),
                "distinct_count": int(row["distinct_cnt"] or 0),
            }
            if "min_v" in row and row["min_v"] is not None:
                entry.update(
                    {
                        "min": round(float(row["min_v"]), 3),
                        "max": round(float(row["max_v"]), 3),
                        "mean": round(float(row["mean_v"]), 3),
                    }
                )
            col_profiles.append(entry)

        results.append(
            {
                "dataset": ds_name,
                "total_rows": total,
                "duplicate_rows_on_key": dup_cnt,
                "duplicate_pct": round((dup_cnt / max(total, 1)) * 100.0, 2),
                "columns": col_profiles,
            }
        )

    spark.stop()
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="SmartProtein Raw Data Profiler")
    parser.add_argument("--engine", choices=["auto", "spark", "local"], default="auto")
    parser.add_argument("--base-path", default=str(DATA_DIR))
    args = parser.parse_args()

    use_spark = False
    if args.engine == "spark":
        use_spark = True
    elif args.engine == "auto":
        try:
            import pyspark  # noqa: F401
            use_spark = args.base_path.startswith("hdfs://")
        except ImportError:
            use_spark = False

    if use_spark:
        profiles = profile_with_spark(args.base_path.rstrip("/"))
    else:
        raw_root = Path(args.base_path) / "raw"
        fc_files = sorted((raw_root / "food_composition").rglob("*.csv"))
        sp_files = sorted((raw_root / "store_prices").rglob("*.csv"))
        uw_files = sorted((raw_root / "user_wearables").rglob("*.csv"))

        fc_df = pd.concat([pd.read_csv(f) for f in fc_files], ignore_index=True)
        sp_df = pd.concat([pd.read_csv(f) for f in sp_files], ignore_index=True)
        uw_df = pd.concat([pd.read_csv(f) for f in uw_files], ignore_index=True)

        profiles = [
            profile_dataframe_local(fc_df, "food_composition", ["source_db", "food_name", "state_raw_cooked"]),
            profile_dataframe_local(sp_df, "store_prices", ["store_id", "canonical_food_hint", "pack_weight_g"]),
            profile_dataframe_local(uw_df, "user_wearables", ["user_id", "event_ts"]),
        ]

    staging_dir = DATA_DIR / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    out_path = staging_dir / "raw_data_profile.json"
    out_path.write_text(json.dumps(profiles, indent=2), encoding="utf-8")

    for p in profiles:
        print(f"[Profile] {p['dataset']:18s} | Rows: {p['total_rows']:7,} | Key Dupes: {p['duplicate_rows_on_key']:6,} ({p['duplicate_pct']}%)")
    print(f"[Profile] Saved full column report -> {out_path}")


if __name__ == "__main__":
    main()

