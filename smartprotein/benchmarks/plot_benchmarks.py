"""SmartProtein Stage 12: Generate Report-Ready Benchmark Charts (Altair HTML)."""

from __future__ import annotations

from pathlib import Path

import altair as alt
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
ANALYTICS_DIR = ROOT_DIR / "data" / "analytics"
CHARTS_DIR = ROOT_DIR / "benchmarks" / "charts"


def main() -> None:
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    fmt_df = pd.read_parquet(ANALYTICS_DIR / "stage12_format_bench.snappy.parquet")
    eng_df = pd.read_parquet(ANALYTICS_DIR / "stage12_benchmarks.snappy.parquet")
    scale_df = pd.read_parquet(ANALYTICS_DIR / "stage12_scalability.snappy.parquet")
    mae_df = pd.read_parquet(ANALYTICS_DIR / "stage12_protein_mae.snappy.parquet")

    # 1. Storage Format Size & Read Time
    c_fmt = (
        alt.Chart(fmt_df)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("format:N", title="Storage Format"),
            y=alt.Y("size_mb:Q", title="Disk Footprint (MB)"),
            color="format:N",
            tooltip=["format", "size_mb", "write_sec", "selective_read_sec"],
        )
        .properties(width=420, height=300, title="Storage Footprint: CSV vs Parquet vs ORC")
    )
    c_fmt.save(str(CHARTS_DIR / "01_format_storage_size.html"))

    c_read = (
        alt.Chart(fmt_df)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("format:N", title="Storage Format"),
            y=alt.Y("selective_read_sec:Q", title="4-Column Projection Read Time (s)"),
            color="format:N",
            tooltip=["format", "selective_read_sec", "write_sec"],
        )
        .properties(width=420, height=300, title="Selective Columnar Read Speed: CSV vs Parquet vs ORC")
    )
    c_read.save(str(CHARTS_DIR / "02_format_read_speed.html"))

    # 2. MapReduce vs Vectorized/Spark Engine
    c_eng = (
        alt.Chart(eng_df)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("engine_or_config:N", title="Processing Engine"),
            y=alt.Y("throughput_rec_sec:Q", title="Throughput (records/sec)"),
            color="engine_or_config:N",
            tooltip=["engine_or_config", "execution_sec", "throughput_rec_sec"],
        )
        .properties(width=460, height=300, title="MapReduce Streaming vs Vectorized Columnar Throughput")
    )
    c_eng.save(str(CHARTS_DIR / "03_engine_throughput.html"))

    # 3. Rows vs Execution Time Scalability
    c_scale = (
        alt.Chart(scale_df)
        .mark_line(point=True, strokeWidth=3)
        .encode(
            x=alt.X("rows_processed:Q", title="Dataset Size (Rows)"),
            y=alt.Y("execution_sec:Q", title="Execution Time (seconds)"),
            tooltip=["rows_processed", "execution_sec", "throughput_rec_sec"],
        )
        .properties(width=460, height=300, title="Linear Scalability: Rows vs Execution Time")
    )
    c_scale.save(str(CHARTS_DIR / "04_scalability_rows_vs_time.html"))

    # 4. Protein MAE Before vs After Cleaning
    mae_melted = pd.DataFrame(
        [
            {"Stage": "Before Cleaning (Raw Mean)", "MAE_g_per_100g": float(mae_df["before_cleaning_mae"].iloc[0])},
            {"Stage": "After Cleaning (Trust-Reconciled)", "MAE_g_per_100g": float(mae_df["after_cleaning_mae"].iloc[0])},
        ]
    )
    c_mae = (
        alt.Chart(mae_melted)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("Stage:N", sort=None, title="Pipeline Stage"),
            y=alt.Y("MAE_g_per_100g:Q", title="Protein MAE vs Lab Reference (g/100g)"),
            color="Stage:N",
            tooltip=["Stage", "MAE_g_per_100g"],
        )
        .properties(width=400, height=300, title="Protein Accuracy Error (MAE): Before vs After Cleaning")
    )
    c_mae.save(str(CHARTS_DIR / "05_protein_mae_reduction.html"))

    print(f"[Charts] Saved 5 interactive benchmark charts -> {CHARTS_DIR}")


if __name__ == "__main__":
    main()

