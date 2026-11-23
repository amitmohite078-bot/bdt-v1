"""SmartProtein Stage 7 (Module #3): Fuzzy Deduplication via MinHash LSH + Levenshtein.

Uses Category Blocking so fuzzy matching runs within each food category instead of O(N^2)
across the entire dataset:
  - PySpark mode: Uses pyspark.ml.feature.{NGram, HashingTF, MinHashLSH} + F.levenshtein()
  - Local mode: Uses pure-Python character 3-gram MinHash signatures + LSH buckets + Levenshtein ratio
Rescues typo/brand-wrapped names (e.g., 'Fresh Low-Fat Paneer (100g)') to canonical food names.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
SEED_PATH = ROOT_DIR / "config" / "reference_foods_seed.json"
STAGING_PATH = ROOT_DIR / "data" / "staging" / "food_composition_staged.snappy.parquet"
OUT_PATH = ROOT_DIR / "data" / "staging" / "fuzzy_rescued_foods.snappy.parquet"


def clean_food_tokens(name: str) -> str:
    """Strip common packaging noise before computing 3-gram shingles."""
    s = name.lower().strip()
    s = re.sub(r"\b(fresh|100g|brandpack|pack|organic|pure)\b", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return " ".join(s.split())


def levenshtein_distance(a: str, b: str) -> int:
    if len(a) < len(b):
        a, b = b, a
    if len(b) == 0:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (ca != cb)))
        prev = curr
    return prev[-1]


def levenshtein_similarity(a: str, b: str) -> float:
    max_len = max(len(a), len(b), 1)
    return 1.0 - (levenshtein_distance(a, b) / max_len)


def char_shingles(text: str, k: int = 3) -> set[str]:
    padded = f" {text} "
    if len(padded) < k:
        return {padded}
    return {padded[i : i + k] for i in range(len(padded) - k + 1)}


def minhash_signature(shingles: set[str], num_hashes: int = 24) -> list[int]:
    sig = []
    for seed_idx in range(num_hashes):
        min_h = None
        for sh in shingles:
            h = int(hashlib.md5(f"{seed_idx}:{sh}".encode("utf-8")).hexdigest()[:8], 16)
            if min_h is None or h < min_h:
                min_h = h
        sig.append(min_h or 0)
    return sig


def jaccard_sim(s1: set[str], s2: set[str]) -> float:
    if not s1 or not s2:
        return 0.0
    return len(s1 & s2) / len(s1 | s2)


def run_pyspark_minhash_lsh(spark_base: str) -> None:
    """Distributed PySpark MLlib MinHashLSH + Category Blocking + Levenshtein verification."""
    from pyspark.ml.feature import HashingTF, MinHashLSH, NGram, RegexTokenizer
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("SmartProtein-MinHashLSH-FuzzyDedup").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(f"{spark_base}/staging/food_composition_validated")
    canon_ref = df.select("canonical_name_exact", "category").dropna().dropDuplicates()

    # Tokenize into character 3-grams via NGram on split chars
    def prep_lsh(input_df, text_col: str):
        char_df = input_df.withColumn("_chars", F.split(F.lower(F.col(text_col)), ""))
        ngram = NGram(n=3, inputCol="_chars", outputCol="_ngrams")
        tf = HashingTF(inputCol="_ngrams", outputCol="features", numFeatures=256)
        return tf.transform(ngram.transform(char_df))

    ref_vec = prep_lsh(canon_ref, "canonical_name_exact")
    lsh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
    model = lsh.fit(ref_vec)

    unmapped = df.filter(F.col("canonical_name_exact").isNull() & F.col("food_name").isNotNull())
    unmapped_vec = prep_lsh(unmapped, "food_name")

    # Category Blocking + LSH Approximate Similarity Join + Levenshtein refinement
    joined = (
        model.approxSimilarityJoin(unmapped_vec, ref_vec, 0.70, distCol="jaccard_dist")
        .filter(F.col("datasetA.category") == F.col("datasetB.category"))
        .withColumn("lev_dist", F.levenshtein(F.lower(F.col("datasetA.food_name")), F.lower(F.col("datasetB.canonical_name_exact"))))
    )
    print(f"[PySpark MinHashLSH] Candidate fuzzy matches found: {joined.count()}")
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="SmartProtein Fuzzy Deduplication (MinHash LSH + Levenshtein)")
    parser.add_argument("--sim-threshold", type=float, default=0.52, help="Combined similarity cutoff")
    args = parser.parse_args()

    if not STAGING_PATH.exists():
        raise SystemExit("Run Stage 6 run_stage6_local.py first to create food_composition_staged.snappy.parquet")

    seed = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    staged = pd.read_parquet(STAGING_PATH)

    # Build LSH index per category (Category Blocking!)
    bands = 6
    rows_per_band = 4  # 6 * 4 = 24 hashes
    lsh_buckets: dict[tuple[str, int, tuple[int, ...]], list[tuple[str, str, set[str]]]] = defaultdict(list)
    canon_by_cat: dict[str, list[tuple[str, str, set[str]]]] = defaultdict(list)

    for food in seed["foods"]:
        cname = food["canonical_name"]
        cat = food["category"]
        cleaned = clean_food_tokens(cname)
        shingles = char_shingles(cleaned, k=3)
        sig = minhash_signature(shingles, num_hashes=bands * rows_per_band)
        canon_by_cat[cat].append((cname, cleaned, shingles))
        for b in range(bands):
            band_key = (cat, b, tuple(sig[b * rows_per_band : (b + 1) * rows_per_band]))
            lsh_buckets[band_key].append((cname, cleaned, shingles))

    unmapped = staged[staged["reject_reason_code"] == "ERR_UNMAPPED_FUZZY_OR_SYNONYM"].copy()
    rescued_rows = []

    for _, row in unmapped.iterrows():
        raw_name = row["food_name"]
        cat = row["category"]
        if pd.isna(raw_name) or cat not in canon_by_cat:
            continue
        cleaned = clean_food_tokens(str(raw_name))
        shingles = char_shingles(cleaned, k=3)
        sig = minhash_signature(shingles, num_hashes=bands * rows_per_band)

        # Retrieve candidate canonical foods from matching LSH bands within same category
        candidates: dict[str, tuple[str, set[str]]] = {}
        for b in range(bands):
            band_key = (cat, b, tuple(sig[b * rows_per_band : (b + 1) * rows_per_band]))
            for cname, c_clean, c_sh in lsh_buckets.get(band_key, []):
                candidates[cname] = (c_clean, c_sh)

        # Fallback within category block if LSH bands had zero exact band collisions
        if not candidates:
            for cname, c_clean, c_sh in canon_by_cat[cat]:
                candidates[cname] = (c_clean, c_sh)

        best_canon = None
        best_score = 0.0
        for cname, (c_clean, c_sh) in candidates.items():
            jac = jaccard_sim(shingles, c_sh)
            lev = levenshtein_similarity(cleaned, c_clean)
            blended = 0.55 * jac + 0.45 * lev
            if blended > best_score:
                best_score = blended
                best_canon = cname

        if best_canon and best_score >= args.sim_threshold:
            rescued_rows.append(
                {
                    "record_id": row["record_id"],
                    "raw_food_name": raw_name,
                    "category": cat,
                    "matched_canonical_name": best_canon,
                    "fuzzy_similarity_score": round(best_score, 3),
                }
            )

    rescued_df = pd.DataFrame(rescued_rows)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    rescued_df.to_parquet(OUT_PATH, compression="snappy", index=False)

    print(
        f"[Fuzzy Dedup MinHash-LSH] Unmapped candidates: {len(unmapped)} | "
        f"Rescued to canonical names: {len(rescued_df)} ({round(len(rescued_df)/max(len(unmapped),1)*100, 1)}%)"
    )
    if not rescued_df.empty:
        print(rescued_df.head(8).to_string(index=False))


if __name__ == "__main__":
    main()

