"""SmartProtein Stage 9 (Modules #23, #24, #27, #29): Geo-Location, Regional & Medical Safety Filters.

Provides:
  - Haversine distance (km) & GeoHash-4/6 spatial blocking
  - Hard allergy exclusion (dairy/lactose, soy, egg, fish, peanuts/nuts, gluten)
  - Medical safety warnings (kidney_caution protein ceiling, diabetes glycemic check)
  - Regional staple matching (Karnataka, North_India, Maharashtra, South_East)
  - Micronutrient gap hints (Iron, Vitamin B12, Calcium) especially for Vegan/Veg diets
"""

from __future__ import annotations

import math


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance in kilometers between two lat/lon points."""
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
    return round(2.0 * r * math.asin(math.sqrt(min(1.0, a))), 2)


def is_food_safe_for_user(
    food_allergens: str | None,
    user_allergies: str | None,
    allergy_aliases: dict[str, list[str]],
) -> bool:
    """Hard filter: return False if any user allergen matches the food's allergens."""
    if not user_allergies or str(user_allergies).strip().lower() in ("none", "nan", ""):
        return True
    if not food_allergens or str(food_allergens).strip().lower() in ("none", "nan", ""):
        return True

    food_tokens = {t.strip().lower() for t in str(food_allergens).split("|") if t.strip()}
    user_tokens = [t.strip().lower() for t in str(user_allergies).split("|") if t.strip()]

    expanded_user = set()
    for ut in user_tokens:
        expanded_user.add(ut)
        expanded_user.update(allergy_aliases.get(ut, []))

    return len(food_tokens & expanded_user) == 0


def build_medical_warnings(
    medical_conditions: str | None,
    carbs_per_100g: float,
    protein_per_100g: float,
) -> str:
    """Return non-blocking clinical caution tags or 'OK'."""
    if not medical_conditions or str(medical_conditions).strip().lower() in ("none", "nan", ""):
        return "OK"
    cond = str(medical_conditions).lower()
    warnings = []
    if "kidney" in cond and protein_per_100g >= 40.0:
        warnings.append("KIDNEY_CAUTION: Moderate portion (daily target capped at 1.0 g/kg)")
    if "diabetes" in cond and carbs_per_100g >= 45.0:
        warnings.append("GLYCEMIC_NOTE: Pair high-carb legume/grain with fiber or curd")
    return " | ".join(warnings) if warnings else "OK"


def compute_micronutrient_hints(
    diet_type: str,
    planned_iron_mg: float,
    planned_calcium_mg: float,
    planned_b12_mcg: float,
    rda: dict[str, float],
) -> list[str]:
    """Surface Iron, Calcium, and Vitamin B12 gap hints for Veg/Vegan meal plans."""
    hints = []
    if planned_iron_mg < rda["iron_mg"] * 0.75:
        hints.append(
            f"Iron gap ({planned_iron_mg:.1f}/{rda['iron_mg']} mg): Add Moong Sprouts, Pumpkin Seeds, or Vitamin-C with Ragi/Dal to boost non-heme absorption."
        )
    if planned_calcium_mg < rda["calcium_mg"] * 0.70:
        hints.append(
            f"Calcium gap ({planned_calcium_mg:.0f}/{rda['calcium_mg']:.0f} mg): Add Ragi Malt (344 mg/100g), Tofu, or Low-Fat Paneer."
        )
    if planned_b12_mcg < rda["b12_mcg"] * 0.80:
        if diet_type == "Vegan":
            hints.append(
                f"Vitamin B12 gap ({planned_b12_mcg:.2f}/{rda['b12_mcg']} mcg): Plant foods lack B12 — use B12-fortified soy milk/nutritional yeast or supplement."
            )
        else:
            hints.append(
                f"Vitamin B12 gap ({planned_b12_mcg:.2f}/{rda['b12_mcg']} mcg): Include Dahi/Curd, Paneer, or Eggs to meet B12 RDA."
            )
    if not hints:
        hints.append("Micronutrients (Iron, Calcium, B12) are well-covered by your protein plan.")
    return hints

