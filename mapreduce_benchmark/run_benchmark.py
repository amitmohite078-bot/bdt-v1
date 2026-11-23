"""Benchmark: Hadoop Streaming MapReduce (Map -> Shuffle/Sort -> Reduce) vs. Vectorized Columnar Engine.

Executes the actual mapper_dedup_clean.py -> external sort -> reducer_dedup_clean.py
pipeline on the Bronze wearable telemetry JSONL files and compares runtime & throughput
against the in-memory vectorized Pandas/Arrow cleaning stage.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from local_runner.run_local_pipeline import clean_wearable_telemetry  # noqa: E402
from spark_jobs.dq_rules import load_config  # noqa: E402


def run_hadoop_streaming_local(telemetry_dir: Path) -> tuple[float, int, str]:
    part_files = sorted(telemetry_dir.glob("telemetry_part_*.jsonl"))
    all_lines: list[bytes] = []
    for fpath in part_files:
        all_lines.append(fpath.read_bytes())
    raw_input = b"".join(all_lines)
    total_records = raw_input.count(b"\n")

    mapper_script = str(ROOT_DIR / "mapreduce_benchmark" / "mapper_dedup_clean.py")
    reducer_script = str(ROOT_DIR / "mapreduce_benchmark" / "reducer_dedup_clean.py")

    t0 = time.perf_counter()
    # Stage 1: Map
    map_proc = subprocess.run(
        [sys.executable, mapper_script],
        input=raw_input,
        capture_output=True,
        check=True,
    )
    # Stage 2: Hadoop Shuffle & Sort phase (lexicographic sort on key)
    mapped_lines = map_proc.stdout.splitlines(keepends=True)
    mapped_lines.sort()
    sorted_bytes = b"".join(mapped_lines)
    # Stage 3: Reduce
    red_proc = subprocess.run(
        [sys.executable, reducer_script],
        input=sorted_bytes,
        capture_output=True,
        check=True,
    )
    elapsed = time.perf_counter() - t0
    summary = red_proc.stdout.decode("utf-8", errors="replace").strip()
    return elapsed, total_records, summary


def main() -> None:
    cfg = load_config()
    lake_root = ROOT_DIR / cfg["local_lakehouse_root"]
    bronze_dir = lake_root / cfg["zones"]["bronze"]
    silver_dir = lake_root / cfg["zones"]["silver"]
    quarantine_dir = lake_root / cfg["zones"]["quarantine"]
    gold_dir = lake_root / cfg["zones"]["gold"]
    gold_dir.mkdir(parents=True, exist_ok=True)

    print("[Benchmark] Running Hadoop Streaming MapReduce (Map -> Shuffle/Sort -> Reduce)...")
    mr_time, total_records, mr_summary = run_hadoop_streaming_local(bronze_dir / "wearable_telemetry")

    print("[Benchmark] Running Columnar Vectorized / Spark-style DQ Engine...")
    t0 = time.perf_counter()
    clean_wearable_telemetry(bronze_dir, silver_dir, quarantine_dir, cfg)
    vec_time = time.perf_counter() - t0

    speedup = round(mr_time / max(vec_time, 1e-4), 2)
    bench_df = pd.DataFrame(
        [
            {
                "engine": "Hadoop MapReduce (Streaming Map + Sort + Reduce)",
                "records_processed": total_records,
                "execution_seconds": round(mr_time, 3),
                "throughput_rec_per_sec": round(total_records / max(mr_time, 1e-4), 0),
                "notes": mr_summary,
            },
            {
                "engine": "Spark / Columnar Vectorized Parquet Engine",
                "records_processed": total_records,
                "execution_seconds": round(vec_time, 3),
                "throughput_rec_per_sec": round(total_records / max(vec_time, 1e-4), 0),
                "notes": f"Speedup vs MapReduce: {speedup}x (Zero disk-spill shuffle + Snappy Parquet compaction)",
            },
        ]
    )
    bench_df.to_parquet(gold_dir / "benchmark_metrics.snappy.parquet", compression="snappy", index=False)

    print(bench_df.to_string(index=False))


if __name__ == "__main__":
    main()

