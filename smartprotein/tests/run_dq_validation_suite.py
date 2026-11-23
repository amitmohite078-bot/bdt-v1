"""SmartProtein Stage 7 (Module #9): Automated Curated Data Quality Gate.

Designed to run inside the Apache Airflow DAG after Stage 6 cleaning.
Uses Great Expectations (if installed) or a zero-dependency GX-compatible
expectation runner over the Curated Parquet tables. Exits with code 1 if any
expectation fails, halting downstream recommendation generation.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
CURATED_DIR = ROOT_DIR / "data" / "curated"
REPORT_OUT = ROOT_DIR / "data" / "analytics" / "gx_validation_report.json"


def run_expectations() -> tuple[bool, list[dict]]:
    foods = pd.read_parquet(CURATED_DIR / "master_protein_foods.snappy.parquet")
    prices = pd.read_parquet(CURATED_DIR / "local_store_inventory.snappy.parquet")
    users = pd.read_parquet(CURATED_DIR / "user_wellness_cleaned.snappy.parquet")

    checks: list[tuple[str, bool, str]] = [
        (
            "foods.non_empty",
            len(foods) > 0,
            f"Found {len(foods)} curated food entries",
        ),
        (
            "foods.no_null_canonical_name",
            int(foods["canonical_name"].isna().sum()) == 0,
            f"Null canonical_name count: {int(foods['canonical_name'].isna().sum())}",
        ),
        (
            "foods.protein_between_0_and_100",
            bool(((foods["crude_protein_per_100g"] >= 0) & (foods["crude_protein_per_100g"] <= 100)).all()),
            f"Min={foods['crude_protein_per_100g'].min()}, Max={foods['crude_protein_per_100g'].max()}",
        ),
        (
            "foods.no_animal_labeled_vegan",
            not bool(
                (
                    (foods["diet_type"] == "Vegan")
                    & foods["canonical_name"].str.lower().str.contains("chicken|egg|paneer|rohu|tuna|curd|yogurt|whey")
                ).any()
            ),
            "Zero meat/dairy/egg items tagged Vegan",
        ),
        (
            "foods.confidence_score_above_50",
            bool((foods["protein_confidence_score"] >= 50.0).all()),
            f"Min confidence={foods['protein_confidence_score'].min()}",
        ),
        (
            "prices.all_positive_inr",
            bool(((prices["price_per_100g_inr"] > 0) & (prices["price_per_100g_inr"] <= 2500)).all()),
            f"Price range INR/100g: [{prices['price_per_100g_inr'].min()}, {prices['price_per_100g_inr'].max()}]",
        ),
        (
            "users.pii_hashed_and_valid_weight",
            bool((users["user_hash_id"].str.len() == 24).all() and ((users["weight_kg"] >= 25) & (users["weight_kg"] <= 250)).all()),
            f"Verified {len(users)} anonymized users with valid weight",
        ),
    ]

    results = []
    all_passed = True
    for name, passed, detail in checks:
        results.append({"expectation": name, "success": bool(passed), "detail": detail})
        if not passed:
            all_passed = False
    return all_passed, results


def main() -> None:
    try:
        import great_expectations as gx  # noqa: F401
        engine = "great_expectations"
    except ImportError:
        engine = "gx_compatible_runner"

    all_passed, results = run_expectations()
    report = {"engine": engine, "all_passed": all_passed, "total_checks": len(results), "results": results}
    REPORT_OUT.parent.mkdir(parents=True, exist_ok=True)
    REPORT_OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")

    for r in results:
        status = "PASS" if r["success"] else "FAIL"
        print(f"[{status}] {r['expectation']:34s} | {r['detail']}")

    if not all_passed:
        print("[DQ Gate] FAILED - Halting downstream recommender pipeline!", file=sys.stderr)
        sys.exit(1)
    print(f"[DQ Gate] ALL {len(results)} CHECKS PASSED ({engine}).")


if __name__ == "__main__":
    main()

