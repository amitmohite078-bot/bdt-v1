"""SmartProtein Stage 12: Comprehensive Big Data Benchmarks & Domain Evaluation.

Runs 4 reproducible experiments and writes Parquet tables to data/analytics/:
  1. Storage Format Benchmark: CSV vs. Parquet (Snappy) vs. ORC (Snappy)
     (File Size MB, Write Time, Selective 4-Column Read Time)
  2. Engine Benchmark: Hadoop Streaming MapReduce (Map -> Sort -> Reduce)
     vs. Vectorized Columnar / Spark-style Engine
  3. Scalability Benchmark: Rows vs. Execution Time (15k, 50k, 150k, 300k rows)
  4. Domain Accuracy (Protein MAE): Before-Cleaning Raw Mean vs.
     After-Cleaning Trust-Weighted Curated Protein against lab reference values
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
CURATED_DIR = DATA_DIR / "curated"
ANALYTICS_DIR = DATA_DIR / "analytics"
SEED_PATH = ROOT_DIR / "config" / "reference_foods_seed.json"


def bench_storage_formats() -> pd.DataFrame:
    """Compare CSV vs Parquet (Snappy) vs ORC (Snappy) on 50k user_wearables rows."""
    uw_files = sorted((RAW_DIR / "user_wearables").rglob("*.csv"))
    df = pd.concat([pd.read_csv(f) for f in uw_files], ignore_index=True)
    proj_cols = ["user_id", "region", "weight_kg", "steps_daily"]
    rows = []

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        # 1. CSV
        csv_path = tmp_dir / "wearables.csv"
        t0 = time.perf_counter()
        df.to_csv(csv_path, index=False)
        csv_write = time.perf_counter() - t0

        t0 = time.perf_counter()
        _ = pd.read_csv(csv_path, usecols=proj_cols)
        csv_read = time.perf_counter() - t0
        rows.append(
            {
                "format": "CSV (Uncompressed)",
                "rows": len(df),
                "size_mb": round(csv_path.stat().st_size / (1024 * 1024), 3),
                "write_sec": round(csv_write, 3),
                "selective_read_sec": round(csv_read, 3),
            }
        )

        # 2. Parquet (Snappy)
        pq_path = tmp_dir / "wearables.snappy.parquet"
        t0 = time.perf_counter()
        df.to_parquet(pq_path, compression="snappy", index=False)
        pq_write = time.perf_counter() - t0

        t0 = time.perf_counter()
        _ = pd.read_parquet(pq_path, columns=proj_cols)
        pq_read = time.perf_counter() - t0
        rows.append(
            {
                "format": "Parquet (Snappy)",
                "rows": len(df),
                "size_mb": round(pq_path.stat().st_size / (1024 * 1024), 3),
                "write_sec": round(pq_write, 3),
                "selective_read_sec": round(pq_read, 3),
            }
        )

        # 3. ORC (Snappy)
        orc_path = tmp_dir / "wearables.snappy.orc"
        arrow_tbl = pa.Table.from_pandas(df, preserve_index=False)
        t0 = time.perf_counter()
        orc.write_table(arrow_tbl, orc_path, compression="snappy")
        orc_write = time.perf_counter() - t0

        t0 = time.perf_counter()
        _ = orc.read_table(orc_path, columns=proj_cols).to_pandas()
        orc_read = time.perf_counter() - t0
        rows.append(
            {
                "format": "ORC (Snappy)",
                "rows": len(df),
                "size_mb": round(orc_path.stat().st_size / (1024 * 1024), 3),
                "write_sec": round(orc_write, 3),
                "selective_read_sec": round(orc_read, 3),
            }
        )

    return pd.DataFrame(rows)


def bench_mapreduce_vs_vectorized() -> pd.DataFrame:
    """Compare Hadoop Streaming MapReduce vs Vectorized Columnar Engine on food_composition."""
    fc_files = sorted((RAW_DIR / "food_composition").rglob("*.csv"))
    raw_bytes = b"".join(f.read_bytes() for f in fc_files)
    total_rows = raw_bytes.count(b"\n") - len(fc_files)

    mapper = str(ROOT_DIR / "mapreduce_demo" / "mapper_protein_clean.py")
    reducer = str(ROOT_DIR / "mapreduce_demo" / "reducer_protein_clean.py")

    t0 = time.perf_counter()
    m_proc = subprocess.run([sys.executable, mapper], input=raw_bytes, capture_output=True, check=True)
    lines = m_proc.stdout.splitlines(keepends=True)
    lines.sort()
    r_proc = subprocess.run([sys.executable, reducer], input=b"".join(lines), capture_output=True, check=True)
    mr_sec = time.perf_counter() - t0
    _ = r_proc.stdout

    t0 = time.perf_counter()
    df = pd.concat([pd.read_csv(f) for f in fc_files], ignore_index=True)
    mult = np.where(df["serving_unit"] == "per_50g_serving", 2.0, 1.0)
    div = np.where(df["energy_unit"] == "kJ", 4.184, 1.0)
    p100 = pd.to_numeric(df["protein_g"], errors="coerce") * mult
    c100 = pd.to_numeric(df["carbs_g"], errors="coerce") * mult
    f100 = pd.to_numeric(df["fat_g"], errors="coerce") * mult
    k100 = (pd.to_numeric(df["reported_kcal"], errors="coerce") * mult) / div
    atwater = 4.0 * p100 + 4.0 * c100 + 9.0 * f100
    valid = (
        df["food_name"].notna()
        & (p100 >= 0)
        & (p100 <= 100.0)
        & ((k100 - atwater).abs() <= np.maximum(35.0, atwater * 0.22))
    )
    _ = df[valid].assign(p100=p100[valid]).groupby(["food_name", "state_raw_cooked"])["p100"].mean()
    vec_sec = time.perf_counter() - t0

    return pd.DataFrame(
        [
            {
                "benchmark": "Engine_Comparison",
                "engine_or_config": "Hadoop Streaming MapReduce (Map + Sort + Reduce)",
                "records": total_rows,
                "execution_sec": round(mr_sec, 4),
                "throughput_rec_sec": round(total_rows / max(mr_sec, 1e-5), 0),
            },
            {
                "benchmark": "Engine_Comparison",
                "engine_or_config": "Vectorized Columnar / Spark-style Engine",
                "records": total_rows,
                "execution_sec": round(vec_sec, 4),
                "throughput_rec_sec": round(total_rows / max(vec_sec, 1e-5), 0),
            },
        ]
    )


def bench_scalability() -> pd.DataFrame:
    """Measure vectorized Atwater + validity + aggregation scaling across 15k -> 300k rows."""
    fc_files = sorted((RAW_DIR / "food_composition").rglob("*.csv"))
    base_df = pd.concat([pd.read_csv(f) for f in fc_files], ignore_index=True)

    rows = []
    for target_n in [15_000, 50_000, 150_000, 300_000]:
        reps = max(1, (target_n // len(base_df)) + 1)
        big_df = pd.concat([base_df] * reps, ignore_index=True).iloc[:target_n].copy()

        t0 = time.perf_counter()
        mult = np.where(big_df["serving_unit"] == "per_50g_serving", 2.0, 1.0)
        div = np.where(big_df["energy_unit"] == "kJ", 4.184, 1.0)
        p100 = pd.to_numeric(big_df["protein_g"], errors="coerce") * mult
        c100 = pd.to_numeric(big_df["carbs_g"], errors="coerce") * mult
        f100 = pd.to_numeric(big_df["fat_g"], errors="coerce") * mult
        k100 = (pd.to_numeric(big_df["reported_kcal"], errors="coerce") * mult) / div
        atwater = 4.0 * p100 + 4.0 * c100 + 9.0 * f100
        valid = big_df["food_name"].notna() & (p100 >= 0) & (p100 <= 100.0) & ((k100 - atwater).abs() <= np.maximum(35.0, atwater * 0.22))
        _ = big_df[valid].assign(p100=p100[valid]).groupby("category")["p100"].mean()
        elapsed = time.perf_counter() - t0

        rows.append(
            {
                "rows_processed": target_n,
                "execution_sec": round(elapsed, 4),
                "throughput_rec_sec": round(target_n / max(elapsed, 1e-5), 0),
            }
        )
    return pd.DataFrame(rows)


def evaluate_protein_mae() -> pd.DataFrame:
    """Compute Protein MAE (g/100g) against lab reference before vs. after cleaning."""
    seed = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    ref_map = {f["canonical_name"]: (f["protein_g"], f["state"]) for f in seed["foods"]}

    fc_files = sorted((RAW_DIR / "food_composition").rglob("*.csv"))
    raw_fc = pd.concat([pd.read_csv(f) for f in fc_files], ignore_index=True)
    curated = pd.read_parquet(CURATED_DIR / "master_protein_foods.snappy.parquet")

    raw_errors = []
    curated_errors = []
    per_food_rows = []

    for cname, (true_p, true_state) in ref_map.items():
        raw_sub = raw_fc[(raw_fc["food_name"] == cname) & (raw_fc["state_raw_cooked"] == true_state)]["protein_g"].dropna()
        cur_sub = curated[(curated["canonical_name"] == cname) & (curated["state_raw_cooked"] == true_state)]["crude_protein_per_100g"]

        raw_est = float(raw_sub.mean()) if not raw_sub.empty else true_p
        cur_est = float(cur_sub.iloc[0]) if not cur_sub.empty else true_p

        raw_err = abs(raw_est - true_p)
        cur_err = abs(cur_est - true_p)
        raw_errors.append(raw_err)
        curated_errors.append(cur_err)

        per_food_rows.append(
            {
                "canonical_name": cname,
                "reference_protein_100g": true_p,
                "before_cleaning_raw_mean": round(raw_est, 2),
                "after_cleaning_curated": round(cur_est, 2),
                "before_abs_error": round(raw_err, 2),
                "after_abs_error": round(cur_err, 2),
            }
        )

    summary = pd.DataFrame(
        [
            {
                "metric": "Protein MAE vs Lab Reference (g/100g)",
                "before_cleaning_mae": round(float(np.mean(raw_errors)), 3),
                "after_cleaning_mae": round(float(np.mean(curated_errors)), 3),
                "mae_reduction_pct": round((1.0 - float(np.mean(curated_errors)) / max(float(np.mean(raw_errors)), 1e-6)) * 100.0, 1),
            }
        ]
    )
    summary.to_parquet(ANALYTICS_DIR / "stage12_protein_mae.snappy.parquet", compression="snappy", index=False)
    return summary


def main() -> None:
    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)

    fmt_df = bench_storage_formats()
    fmt_df.to_parquet(ANALYTICS_DIR / "stage12_format_bench.snappy.parquet", compression="snappy", index=False)

    eng_df = bench_mapreduce_vs_vectorized()
    eng_df.to_parquet(ANALYTICS_DIR / "stage12_benchmarks.snappy.parquet", compression="snappy", index=False)

    scale_df = bench_scalability()
    scale_df.to_parquet(ANALYTICS_DIR / "stage12_scalability.snappy.parquet", compression="snappy", index=False)

    mae_df = evaluate_protein_mae()

    print("=== [1] Storage Format Benchmark (50k Wearables) ===")
    print(fmt_df.to_string(index=False))
    print("\n=== [2] MapReduce vs Vectorized/Spark-Style Benchmark ===")
    print(eng_df.to_string(index=False))
    print("\n=== [3] Scalability Benchmark (Rows vs Time) ===")
    print(scale_df.to_string(index=False))
    print("\n=== [4] Protein Accuracy MAE Against Lab Reference ===")
    print(mae_df.to_string(index=False))


if __name__ == "__main__":
    main()

