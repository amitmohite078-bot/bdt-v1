"""Shared Data Quality (DQ) rules, canonicalizers, and semantic truth tables.

Used identically by:
  - PySpark HDFS Medallion pipeline (spark_jobs/spark_medallion_pipeline.py)
  - Local Lakehouse pipeline (local_runner/run_local_pipeline.py)
  - Hadoop Streaming MapReduce benchmark (mapreduce_benchmark/)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config" / "pipeline_config.json"
ALIASES_PATH = ROOT_DIR / "config" / "city_aliases.json"


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def load_aliases() -> dict[str, Any]:
    with ALIASES_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


# Authoritative food-keyword -> true minimum dietary category.
# Used by DQ-SEM-001 to catch mislabeled vendor entries (e.g., Chicken tagged "Vegan").
FOOD_TRUE_DIET_KEYWORDS: dict[str, str] = {
    "chicken": "Non-Veg",
    "salmon": "Non-Veg",
    "rohu": "Non-Veg",
    "tuna": "Non-Veg",
    "fish": "Non-Veg",
    "prawns": "Non-Veg",
    "mutton": "Non-Veg",
    "turkey": "Non-Veg",
    "egg": "Eggetarian",
    "omelette": "Eggetarian",
    "paneer": "Veg",
    "curd": "Veg",
    "greek yogurt": "Veg",
    "yogurt": "Veg",
    "milk": "Veg",
    "whey": "Veg",
    "cottage cheese": "Veg",
    "cheese": "Veg",
    "tofu": "Vegan",
    "tempeh": "Vegan",
    "soya": "Vegan",
    "soy": "Vegan",
    "moong": "Vegan",
    "chickpea": "Vegan",
    "chana": "Vegan",
    "rajma": "Vegan",
    "lentil": "Vegan",
    "dal": "Vegan",
    "peanut": "Vegan",
    "almond": "Vegan",
    "quinoa": "Vegan",
    "oats": "Vegan",
    "sprouts": "Vegan",
    "edamame": "Vegan",
    "pea protein": "Vegan",
    "spirulina": "Vegan",
    "chia": "Vegan",
    "pumpkin seed": "Vegan",
}

DIET_RANK = {
    "Vegan": 0,
    "Veg": 1,
    "Eggetarian": 2,
    "Non-Veg": 3,
}

# Unit divisors to convert price and macros to standard per-100g basis
UNIT_TO_100G_FACTOR: dict[str, float] = {
    "per_100g": 1.0,
    "100g": 1.0,
    "per_kg": 10.0,
    "1kg": 10.0,
    "kg": 10.0,
    "per_500g": 5.0,
    "500g": 5.0,
    "per_250g": 2.5,
    "250g": 2.5,
    "per_serving_30g": 0.30,
    "per_serving_50g": 0.50,
    "per_serving": 0.50,
}

DQ_DIMENSIONS = [
    "Completeness",
    "Validity",
    "Uniqueness",
    "Consistency",
    "Timeliness",
    "Semantic_Integrity",
]


def infer_true_diet_from_name(food_name: str | None) -> str | None:
    """Return the strictest true diet category implied by known food keywords."""
    if not food_name:
        return None
    lower = food_name.lower()
    for keyword, true_diet in FOOD_TRUE_DIET_KEYWORDS.items():
        if keyword in lower:
            return true_diet
    return None


def is_diet_tag_semantically_valid(food_name: str | None, claimed_diet: str | None) -> bool:
    """Detect semantic lies, e.g., Chicken tagged as Veg, or Paneer tagged as Vegan."""
    if not claimed_diet or claimed_diet not in DIET_RANK:
        return False
    inferred = infer_true_diet_from_name(food_name)
    if inferred is None:
        return True
    # Claimed diet cannot be stricter (lower rank) than the food's true animal-product rank.
    return DIET_RANK[claimed_diet] >= DIET_RANK[inferred]


def canonicalize_city(raw_city: str | None, aliases: dict[str, str]) -> str | None:
    if raw_city is None:
        return None
    cleaned = " ".join(str(raw_city).strip().lower().split())
    if not cleaned:
        return None
    return aliases.get(cleaned)


def canonicalize_diet(raw_diet: str | None, diet_aliases: dict[str, str]) -> str | None:
    if raw_diet is None:
        return None
    cleaned = " ".join(str(raw_diet).strip().lower().split())
    if not cleaned:
        return None
    return diet_aliases.get(cleaned)


def compute_composite_dq_score(dim_scores: dict[str, float]) -> float:
    """Weighted 0-100 composite Data Quality Score across 6 dimensions."""
    weights = {
        "Completeness": 0.18,
        "Validity": 0.22,
        "Uniqueness": 0.15,
        "Consistency": 0.15,
        "Timeliness": 0.10,
        "Semantic_Integrity": 0.20,
    }
    total = sum(dim_scores.get(dim, 100.0) * w for dim, w in weights.items())
    return round(total, 2)

