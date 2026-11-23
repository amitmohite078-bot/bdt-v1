"""One-Command End-to-End Runner for SmartProtein (Local Windows / Laptop Mode).

Executes Stages 3, 5, 6, 7, 8, 9, 10, and 12 sequentially, then prints the
exact Streamlit command to open the interactive dashboard.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent


def run_step(label: str, cmd: list[str]) -> None:
    print(f"\n{'=' * 70}\n[{label}]\n{'=' * 70}")
    subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run entire SmartProtein pipeline end to end")
    parser.add_argument("--scale", choices=["dev", "1m", "5m"], default="dev")
    args = parser.parse_args()

    py = sys.executable
    steps = [
        ("Stage 3: Generate Dirty Multi-Source Data", [py, str(ROOT_DIR / "data_generator" / "generate_smartprotein_data.py"), "--scale", args.scale]),
        ("Stage 5a: Kafka Speed-Layer Dry-Run Producer", [py, str(ROOT_DIR / "ingestion" / "kafka_producers.py"), "--mode", "dry-run", "--count", "30"]),
        ("Stage 5b: Nutrition-Label OCR Pipeline", [py, str(ROOT_DIR / "ingestion" / "ocr_label_pipeline.py")]),
        ("Stage 6a: Profile Raw Data", [py, str(ROOT_DIR / "spark_jobs" / "profiling" / "profile_raw_data.py"), "--engine", "local"]),
        ("Stage 6b: Clean, DQ Score & Quarantine", [py, str(ROOT_DIR / "spark_jobs" / "cleaning" / "run_stage6_local.py")]),
        ("Stage 7a: Fuzzy Dedup (MinHash LSH + Levenshtein)", [py, str(ROOT_DIR / "spark_jobs" / "cleaning" / "fuzzy_dedup_minhash_lsh.py")]),
        ("Stage 7b: ML Anomaly Detection & Self-Healing Rules", [py, str(ROOT_DIR / "spark_jobs" / "cleaning" / "ml_anomaly_and_self_healing.py")]),
        ("Stage 7c: Data Lineage Tracking", [py, str(ROOT_DIR / "spark_jobs" / "cleaning" / "lineage_tracker.py")]),
        ("Stage 7d: Automated DQ Gate Tests", [py, str(ROOT_DIR / "tests" / "run_dq_validation_suite.py")]),
        ("Stage 8: Accurate Protein Layer (DIAAS/Cooking/Leucine)", [py, str(ROOT_DIR / "spark_jobs" / "protein_layer" / "accurate_protein_enrichment.py")]),
        ("Stage 9: Hybrid Recommender & LP Diet Optimizer", [py, str(ROOT_DIR / "spark_jobs" / "recommender" / "hybrid_recommender.py")]),
        ("Stage 10: Streaming Speed-Layer Replay & Drift Alerts", [py, str(ROOT_DIR / "spark_jobs" / "streaming" / "speed_layer_alerts.py"), "--mode", "local-replay"]),
        ("Stage 12a: Run Storage/Engine/Scalability/MAE Benchmarks", [py, str(ROOT_DIR / "benchmarks" / "run_stage12_benchmarks.py")]),
        ("Stage 12b: Generate Benchmark HTML Charts", [py, str(ROOT_DIR / "benchmarks" / "plot_benchmarks.py")]),
    ]

    for label, cmd in steps:
        run_step(label, cmd)

    print("\nALL STAGES COMPLETED SUCCESSFULLY!")
    print("Launch the dashboard with:")
    print("  streamlit run smartprotein/dashboard/app.py --server.port 8502")


if __name__ == "__main__":
    main()

