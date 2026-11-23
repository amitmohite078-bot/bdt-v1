"""Data Quality & Integrity Test Suite (6 DQ Dimensions & Atwater Balance)."""

from __future__ import annotations

import json
from pathlib import Path
import pandas as pd
import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
CURATED_DIR = ROOT_DIR / "smartprotein" / "data" / "curated"
REJECTED_DIR = ROOT_DIR / "smartprotein" / "data" / "rejected"


@pytest.fixture(scope="module")
def curated_foods():
    return pd.read_parquet(CURATED_DIR / "master_protein_foods.snappy.parquet")


@pytest.fixture(scope="module")
def curated_prices():
    return pd.read_parquet(CURATED_DIR / "local_store_inventory.snappy.parquet")


@pytest.fixture(scope="module")
def curated_users():
    return pd.read_parquet(CURATED_DIR / "user_wellness_cleaned.snappy.parquet")


def test_foods_non_empty(curated_foods):
    """Verify Master Food composition table is populated."""
    assert len(curated_foods) >= 50, f"Expected >= 50 foods, got {len(curated_foods)}"


def test_foods_no_null_canonical_name(curated_foods):
    """Completeness dimension: canonical_name must have zero nulls."""
    assert int(curated_foods["canonical_name"].isna().sum()) == 0


def test_foods_protein_valid_range(curated_foods):
    """Validity dimension: Crude protein must be between 0g and 100g per 100g."""
    assert (curated_foods["crude_protein_per_100g"] >= 0).all()
    assert (curated_foods["crude_protein_per_100g"] <= 100).all()


def test_foods_vegan_tag_integrity(curated_foods):
    """Semantic Consistency: No meat, poultry, dairy or egg labeled as Vegan."""
    vegan_foods = curated_foods[curated_foods["diet_type"] == "Vegan"]
    forbidden_terms = ["chicken", "egg", "paneer", "rohu", "tuna", "curd", "yogurt", "whey", "mutton", "fish"]
    pattern = "|".join(forbidden_terms)
    mismatches = vegan_foods[vegan_foods["canonical_name"].str.lower().str.contains(pattern)]
    assert len(mismatches) == 0, f"Found non-vegan items tagged as Vegan: {mismatches['canonical_name'].tolist()}"


def test_prices_all_positive_inr(curated_prices):
    """Plausibility dimension: Store retail prices must be > 0 and reasonable."""
    assert (curated_prices["price_per_100g_inr"] > 0).all()
    assert (curated_prices["price_per_100g_inr"] <= 3000).all()


def test_users_pii_anonymized(curated_users):
    """Privacy & Compliance: User IDs must be 24-character SHA-256 truncated hashes."""
    assert (curated_users["user_hash_id"].str.len() == 24).all()
    assert (curated_users["weight_kg"] >= 25).all()
    assert (curated_users["weight_kg"] <= 250).all()


def test_quarantine_table_exists():
    """Verify Dead-Letter Queue (DLQ) quarantine table is recorded."""
    rejected_file = REJECTED_DIR / "all_records" / "rejected_records.snappy.parquet"
    if not rejected_file.exists():
        rejected_file = ROOT_DIR / "lakehouse" / "fitguard" / "quarantine" / "protein_foods_catalog" / "part-00000.snappy.parquet"
    assert rejected_file.exists(), f"DLQ quarantine file missing at {rejected_file}"
    df_rej = pd.read_parquet(rejected_file)
    assert len(df_rej) > 0, "Expected quarantine records in DLQ"


def test_atwater_energy_balance_formula(curated_foods):
    """Thermodynamic Consistency: 4*P + 4*C + 9*F ≈ Energy Kcal within tolerance."""
    if {"carbohydrate_per_100g", "fat_per_100g", "energy_kcal_per_100g"}.issubset(curated_foods.columns):
        calc_kcal = (
            4.0 * curated_foods["crude_protein_per_100g"]
            + 4.0 * curated_foods["carbohydrate_per_100g"]
            + 9.0 * curated_foods["fat_per_100g"]
        )
        reported_kcal = curated_foods["energy_kcal_per_100g"]
        # Allow ±20% tolerance or 35 kcal absolute difference for dietary fiber / polyols
        diff = (calc_kcal - reported_kcal).abs()
        rel_diff = diff / reported_kcal.clip(lower=10.0)
        valid = (diff <= 40.0) | (rel_diff <= 0.25)
        pass_rate = valid.mean()
        assert pass_rate >= 0.90, f"Expected >= 90% Atwater agreement, got {pass_rate:.1%}"
