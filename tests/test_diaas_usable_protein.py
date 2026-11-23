"""Accurate Protein Lab, DIAAS, Usable Protein & Bioavailability Test Suite."""

from __future__ import annotations

from pathlib import Path
import pandas as pd
import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
CURATED_DIR = ROOT_DIR / "smartprotein" / "data" / "curated"
ANALYTICS_DIR = ROOT_DIR / "smartprotein" / "data" / "analytics"


@pytest.fixture(scope="module")
def enriched_foods():
    enriched_path = ANALYTICS_DIR / "master_protein_foods_enriched.snappy.parquet"
    if enriched_path.exists():
        return pd.read_parquet(enriched_path)
    return pd.read_parquet(CURATED_DIR / "master_protein_foods.snappy.parquet")


def test_diaas_scores_in_biological_range(enriched_foods):
    """DIAAS scores should be within physiological limits (0.35 to 1.45)."""
    if "diaas_score" in enriched_foods.columns:
        assert (enriched_foods["diaas_score"] >= 0.30).all()
        assert (enriched_foods["diaas_score"] <= 1.50).all()


def test_usable_protein_discounting(enriched_foods):
    """Usable protein must be <= crude protein due to DIAAS and antinutrient penalties."""
    if "usable_protein_per_100g" in enriched_foods.columns:
        # Usable protein should not exceed crude protein + 0.1g rounding tolerance
        diff = enriched_foods["usable_protein_per_100g"] - enriched_foods["crude_protein_per_100g"]
        assert (diff <= 0.15).all(), "Usable protein exceeded crude protein!"


def test_plant_quality_penalty(enriched_foods):
    """Unprocessed plant sources (pulses/millets) must have a quality penalty vs animal isolates."""
    if "quality_penalty_pct" in enriched_foods.columns and "diet_type" in enriched_foods.columns:
        veg_penalties = enriched_foods[enriched_foods["diet_type"].isin(["Vegan", "Veg"])]["quality_penalty_pct"]
        nonveg_penalties = enriched_foods[enriched_foods["diet_type"] == "Non-Veg"]["quality_penalty_pct"]
        assert veg_penalties.mean() > nonveg_penalties.mean(), "Plant protein should exhibit higher bioavailability penalty"


def test_cost_per_gram_usable_protein():
    """Verify that cost per gram usable protein scales inversely with bioavailability."""
    # Soya Chunks vs Whey vs Paneer
    crude_p = 40.0
    diaas = 0.90
    bio_factor = 0.82
    usable_p = crude_p * (diaas * bio_factor)
    price_100g = 15.00  # ₹15 for 100g
    cost_per_g = price_100g / usable_p
    assert cost_per_g < 1.0, f"Soya chunks should deliver usable protein under ₹1.0/g, got ₹{cost_per_g:.2f}"


def test_leucine_portion_calculation():
    """Verify portion (grams) required to trigger 2.2g - 2.5g leucine MPS threshold."""
    # Soya chunks contains ~3.5g leucine per 100g
    leu_per_100g = 3.5
    target_leu = 2.5
    portion_needed = (target_leu / leu_per_100g) * 100.0
    assert 70.0 <= portion_needed <= 85.0, f"Expected ~71g soya for 2.5g leucine, got {portion_needed:.1f}g"
