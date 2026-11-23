"""Stigler Linear Programming (LP) Budget & Protein Optimizer Tests."""

from __future__ import annotations

import pandas as pd
import pytest

from smartprotein.spark_jobs.recommender.lp_budget_optimizer import (
    solve_daily_diet_lp,
    allocate_meal_plan,
)


@pytest.fixture
def sample_candidates():
    return pd.DataFrame([
        {
            "canonical_name": "Soya Chunks (Nutrela)",
            "usable_protein_per_100g": 29.1,
            "price_per_100g_inr": 14.50,
            "energy_kcal_per_100g": 345.0,
            "iron_mg_per_100g": 9.5,
            "calcium_mg_per_100g": 210.0,
            "vitamin_b12_mcg_per_100g": 0.0,
            "leucine_g_per_100g": 3.52,
            "store_name": "OrganicStop Bengaluru #241",
            "distance_km": 3.2,
            "food_group": "soy_products",
            "recommended_timing": "Post-Workout Fuel",
        },
        {
            "canonical_name": "Moong Dal Split",
            "usable_protein_per_100g": 13.2,
            "price_per_100g_inr": 13.93,
            "energy_kcal_per_100g": 334.0,
            "iron_mg_per_100g": 3.8,
            "calcium_mg_per_100g": 75.0,
            "vitamin_b12_mcg_per_100g": 0.0,
            "leucine_g_per_100g": 1.62,
            "store_name": "ProteinHub Bengaluru #170",
            "distance_km": 4.1,
            "food_group": "legumes_pulses",
            "recommended_timing": "Lunch",
        },
        {
            "canonical_name": "Roasted Chana",
            "usable_protein_per_100g": 12.7,
            "price_per_100g_inr": 12.30,
            "energy_kcal_per_100g": 369.0,
            "iron_mg_per_100g": 5.2,
            "calcium_mg_per_100g": 58.0,
            "vitamin_b12_mcg_per_100g": 0.0,
            "leucine_g_per_100g": 1.58,
            "store_name": "FreshBasket Bengaluru #52",
            "distance_km": 2.8,
            "food_group": "legumes_pulses",
            "recommended_timing": "Breakfast",
        },
        {
            "canonical_name": "Low-Fat Paneer",
            "usable_protein_per_100g": 22.0,
            "price_per_100g_inr": 38.00,
            "energy_kcal_per_100g": 195.0,
            "iron_mg_per_100g": 0.5,
            "calcium_mg_per_100g": 380.0,
            "vitamin_b12_mcg_per_100g": 1.1,
            "leucine_g_per_100g": 2.15,
            "store_name": "NatureMart Bengaluru #14",
            "distance_km": 5.0,
            "food_group": "dairy",
            "recommended_timing": "Dinner",
        },
    ])


def test_lp_solves_successfully(sample_candidates):
    """Verify LP solver finds a feasible plan for standard 120g target and ₹160 budget."""
    res = solve_daily_diet_lp(sample_candidates, protein_target_g=120.0, max_budget_inr=160.0)
    assert res["status"] in ["Optimal", "Suboptimal", "Feasible", "BUDGET_OPTIMAL_PORTION"], f"LP failed with status: {res.get('status')}"
    assert res["total_usable_protein_g"] > 0
    assert res["total_cost_inr"] > 0
    assert len(res["items"]) >= 1


def test_lp_respects_budget_constraint(sample_candidates):
    """Verify LP delivers cost <= budget when feasible."""
    budget = 150.0
    res = solve_daily_diet_lp(sample_candidates, protein_target_g=100.0, max_budget_inr=budget)
    if res["within_budget"]:
        assert res["total_cost_inr"] <= budget + 0.5


def test_meal_allocation_slots(sample_candidates):
    """Verify meal allocator populates 4 meal slots with portion and leucine details."""
    lp_res = solve_daily_diet_lp(sample_candidates, protein_target_g=110.0, max_budget_inr=160.0)
    assert len(lp_res["items"]) > 0
    meals = allocate_meal_plan(lp_res["items"], workout_time="Evening", adherence_pct=85.0)
    assert len(meals) == 4, f"Expected 4 meal slots, got {len(meals)}"
    slots = [m["meal_slot"] for m in meals]
    assert "Breakfast" in slots
    assert "Lunch" in slots
    assert "Dinner" in slots
    assert any("Workout" in s for s in slots)


def test_leucine_trigger_evaluation(sample_candidates):
    """Verify leucine threshold (>= 2.2g) is recorded per meal slot."""
    lp_res = solve_daily_diet_lp(sample_candidates, protein_target_g=120.0, max_budget_inr=160.0)
    meals = allocate_meal_plan(lp_res["items"], workout_time="Morning", adherence_pct=90.0)
    for m in meals:
        assert "leucine_g" in m
        assert "leucine_trigger_met" in m
