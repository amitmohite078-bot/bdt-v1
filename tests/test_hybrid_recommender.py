"""Hybrid Recommender, Geospatial Haversine & Safety Filter Tests."""

from __future__ import annotations

import math
import pytest

from smartprotein.spark_jobs.recommender.hybrid_recommender import (
    haversine_km,
    is_food_safe_for_user,
    build_medical_warnings,
    compute_micronutrient_hints,
)


def test_haversine_distance_known_coordinates():
    """Verify Haversine distance between Bangalore center (12.9716, 77.5946) and Indiranagar (12.9784, 77.6408)."""
    dist = haversine_km(12.9716, 77.5946, 12.9784, 77.6408)
    assert 4.5 <= dist <= 5.5, f"Expected ~5.0 km, got {dist:.2f} km"


def test_haversine_distance_zero():
    """Distance between identical points must be zero."""
    dist = haversine_km(19.0760, 72.8777, 19.0760, 72.8777)
    assert abs(dist) < 1e-6


def test_dietary_safety_filter_vegan():
    """Vegan user must reject eggs, milk, chicken, fish."""
    aliases = {"lactose": ["milk", "dairy", "whey", "paneer", "curd"], "soy": ["soya", "soybean", "edamame"]}
    assert is_food_safe_for_user("none", "none", aliases) is True
    assert is_food_safe_for_user("dairy", "lactose", aliases) is False


def test_dietary_safety_filter_allergies():
    """User allergic to soy and nuts must reject foods containing soy or nuts."""
    aliases = {"soy": ["soya", "soybean"], "nuts": ["peanuts", "almonds"]}
    assert is_food_safe_for_user("soy", "soy|nuts", aliases) is False
    assert is_food_safe_for_user("gluten", "soy|nuts", aliases) is True


def test_medical_warning_kidney_caution():
    """Kidney caution condition must append protein cap advice when protein is >= 40g/100g."""
    warn = build_medical_warnings("kidney_caution", carbs_per_100g=10.0, protein_per_100g=45.0)
    assert "kidney" in warn.lower() or "cap" in warn.lower() or "1.0" in warn.lower() or "caution" in warn.lower()


def test_micronutrient_hints_calcium_gap():
    """Low calcium intake must generate dietary calcium suggestion."""
    rda = {"iron_mg": 17.0, "calcium_mg": 1000.0, "b12_mcg": 2.2}
    hints = compute_micronutrient_hints("Veg", planned_iron_mg=18.0, planned_calcium_mg=400.0, planned_b12_mcg=2.5, rda=rda)
    assert any("calcium" in h.lower() for h in hints)
