"""SmartProtein Stage 9 (Modules #22 & #28): Budget-Aware Linear Programming Diet Optimizer & Meal Planner.

Solves Stigler's Diet Problem for accurate, budget-aware protein nutrition:
  1. Enforces hard user daily budget constraints (cost <= max_budget_inr).
  2. Dynamically scales protein quality with budget:
     - Tight budget (<= Rs 100): Maximizes protein delivered from low-cost bioavailable staples (Soya, Eggs, Dals, Chana).
     - Higher budget (>= Rs 150): Upgrades basket to premium high-DIAAS sources (Chicken, Fish, Whey, Paneer).
  3. Uses realistic human portion boundaries per category (no single-food unpalatability).
  4. Automatically constructs a complete 4-slot daily meal plan:
     - Post-Workout Fuel (MPS Leucine >= 2.2g trigger check)
     - Breakfast
     - Lunch
     - Dinner
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _get_food_max_units(canonical_name: str) -> float:
    """Realistic single-day upper bounds in 100g units to prevent unpalatable single-food dominance."""
    n = canonical_name.lower()
    if "whey" in n or "pea protein" in n:
        return 0.45  # 45g max powder (~1.5 scoops)
    if "soya chunk" in n:
        return 0.65  # 65g dry soya chunks (~200g cooked)
    if "peanut butter" in n:
        return 0.40  # 40g (~2 tbsp)
    if "egg" in n:
        return 2.50  # 250g (~5 whole eggs)
    if "chicken" in n or "fish" in n or "tuna" in n:
        return 2.50  # 250g
    if "paneer" in n or "tofu" in n:
        return 1.80  # 180g
    return 2.00      # 200g for legumes, dals, sprouts, seeds


def _get_slot_preference(name: str, timing: str) -> str:
    """Assigns food to its most natural human meal slot."""
    n = name.lower()
    t = str(timing).lower()
    if "whey" in n or "pea protein" in n:
        return "Post-Workout Fuel"
    if "egg" in n or "sprout" in n or "peanut" in n or "oat" in n or "ragi" in n or "breakfast" in t:
        return "Breakfast"
    if "paneer" in n or "tofu" in n or "dinner" in t or "sleep" in t:
        return "Dinner"
    return "Lunch"


def solve_daily_diet_lp(
    candidates: pd.DataFrame,
    protein_target_g: float,
    max_budget_inr: float,
    max_single_food_g: float = 250.0,
    max_kcal: float = 2800.0,
) -> dict:
    """Solve minimum-cost or maximum-protein diet allocation strictly respecting max_budget_inr."""
    if candidates.empty:
        return {
            "status": "NO_CANDIDATES",
            "items": [],
            "total_cost_inr": 0.0,
            "total_usable_protein_g": 0.0,
            "within_budget": True,
        }

    # Deduplicate to best offer per canonical food
    sort_cols = ["composite_rec_score"] if "composite_rec_score" in candidates.columns else ["usable_protein_per_100g"]
    df = candidates.sort_values(by=sort_cols, ascending=False).drop_duplicates(subset=["canonical_name"]).reset_index(drop=True).copy()
    n = len(df)

    costs = df["price_per_100g_inr"].to_numpy(dtype=float)
    prot = df["usable_protein_per_100g"].to_numpy(dtype=float)
    diaas = df["diaas_score"].to_numpy(dtype=float) if "diaas_score" in df.columns else np.full(n, 1.0)
    kcal = df["adjusted_kcal_per_100g"].to_numpy(dtype=float) if "adjusted_kcal_per_100g" in df.columns else np.full(n, 200.0)

    # Realistic upper bounds
    bounds = [(0.0, _get_food_max_units(str(name))) for name in df["canonical_name"]]

    # Budget-proportional objective:
    # Low budget: minimize cost directly.
    # Higher budget: discount higher DIAAS foods so Chicken, Whey, Eggs, Paneer get selected!
    budget_level = float(max_budget_inr)
    q_weight = min(max((budget_level - 90.0) / 160.0, 0.0), 1.0)
    obj = costs / (1.0 + q_weight * 2.8 * np.maximum(diaas - 0.70, 0.0))

    solution_x: np.ndarray | None = None
    solver_used = "scipy_highs"

    try:
        from scipy.optimize import linprog

        # Attempt 1: Hit protein target strictly within user's budget and kcal
        # -prot^T x <= -target, costs^T x <= budget, kcal^T x <= max_kcal
        A_ub = np.vstack([-prot, costs, kcal])
        b_ub = np.array([-protein_target_g, budget_level, max_kcal])
        res = linprog(c=obj, A_ub=A_ub, b_ub=b_ub, bounds=bounds, method="highs")

        if res.success:
            solution_x = res.x
            status_label = "OPTIMAL"
        else:
            # Attempt 2: Relax kcal slightly for heavy protein goals
            A_ub2 = np.vstack([-prot, costs])
            b_ub2 = np.array([-protein_target_g, budget_level])
            res2 = linprog(c=obj, A_ub=A_ub2, b_ub=b_ub2, bounds=bounds, method="highs")
            if res2.success:
                solution_x = res2.x
                status_label = "OPTIMAL"
            else:
                # Attempt 3: Budget is tight; MAXIMIZE protein achievable strictly within budget!
                # Maximize prot^T x subject to costs^T x <= budget (Minimize -prot^T x)
                res3 = linprog(c=-prot, A_ub=np.array([costs]), b_ub=np.array([budget_level]), bounds=bounds, method="highs")
                if res3.success:
                    solution_x = res3.x
                    status_label = "BUDGET_OPTIMAL_PORTION"
                else:
                    solution_x = None
                    status_label = "LP_INFEASIBLE"
    except Exception:
        solution_x = None
        status_label = "LP_ERROR"

    # Fallback if solver produced no solution
    if solution_x is None:
        solution_x = np.zeros(n)
        accum_cost = 0.0
        for i in np.argsort(-prot / np.maximum(costs, 0.1)):
            c_per_unit = costs[i]
            max_u = bounds[i][1]
            avail_budget = max(budget_level - accum_cost, 0.0)
            u_buy = min(max_u, avail_budget / max(c_per_unit, 0.1))
            solution_x[i] = u_buy
            accum_cost += u_buy * c_per_unit
            if accum_cost >= budget_level:
                break
        status_label = "FALLBACK_GREEDY"

    items = []
    tot_cost = 0.0
    tot_prot = 0.0
    tot_kcal = 0.0
    tot_iron = 0.0
    tot_calc = 0.0
    tot_b12 = 0.0

    for i, units in enumerate(solution_x):
        grams = round(float(units) * 100.0, 0)
        if grams < 15.0:
            continue
        u = grams / 100.0
        row = df.iloc[i]
        item_cost = round(u * float(row["price_per_100g_inr"]), 2)
        item_prot = round(u * float(row["usable_protein_per_100g"]), 1)
        item_leu = round(u * float(row.get("leucine_g_per_100g_food", 1.5)), 2)

        tot_cost += item_cost
        tot_prot += item_prot
        tot_kcal += u * float(row.get("adjusted_kcal_per_100g", 200.0))
        tot_iron += u * float(row.get("iron_mg_per_100g", 2.0))
        tot_calc += u * float(row.get("calcium_mg_per_100g", 80.0))
        tot_b12 += u * float(row.get("b12_mcg_per_100g", 0.0))

        items.append(
            {
                "canonical_name": row["canonical_name"],
                "store_name": row.get("store_name", "Local Partner Store"),
                "distance_km": float(row.get("distance_km", 3.5)),
                "grams": grams,
                "cost_inr": item_cost,
                "usable_protein_g": item_prot,
                "leucine_g": item_leu,
                "recommended_timing": row.get("recommended_timing", "Anytime"),
            }
        )

    items.sort(key=lambda x: x["usable_protein_g"], reverse=True)
    within_budget = tot_cost <= (budget_level + 2.0)  # slight rounding margin
    return {
        "status": status_label,
        "solver": solver_used,
        "within_budget": within_budget,
        "total_cost_inr": round(tot_cost, 2),
        "total_usable_protein_g": round(tot_prot, 1),
        "total_kcal": round(tot_kcal, 0),
        "total_iron_mg": round(tot_iron, 1),
        "total_calcium_mg": round(tot_calc, 0),
        "total_b12_mcg": round(tot_b12, 2),
        "items": items,
    }


def allocate_meal_plan(lp_items: list[dict], workout_time: str, adherence_pct: float = 100.0) -> list[dict]:
    """Distribute LP-selected foods logically across 4 meal slots (Breakfast, Lunch, Post-Workout Fuel, Dinner)."""
    if not lp_items:
        return []

    slot_pools: dict[str, list[dict]] = {
        "Breakfast": [],
        "Lunch": [],
        "Post-Workout Fuel": [],
        "Dinner": [],
    }

    for item in lp_items:
        pref = _get_slot_preference(item["canonical_name"], item.get("recommended_timing", ""))
        slot_pools[pref].append(item)

    slots_order = ["Breakfast", "Lunch", "Post-Workout Fuel", "Dinner"]
    wt = str(workout_time).lower()
    if "morning" in wt:
        slots_order = ["Post-Workout Fuel", "Breakfast", "Lunch", "Dinner"]
    elif "evening" in wt:
        slots_order = ["Breakfast", "Lunch", "Dinner", "Post-Workout Fuel"]

    assigned_rows: list[dict] = []
    unassigned = [it for it in lp_items]

    # First pass: assign preferred foods to their natural slots
    for slot in slots_order:
        if slot_pools[slot]:
            best_food = slot_pools[slot].pop(0)
            if best_food in unassigned:
                unassigned.remove(best_food)
            assigned_rows.append({"meal_slot": slot, **best_food})

    # Second pass: fill empty slots with remaining unassigned items
    empty_slots = [s for s in slots_order if not any(r["meal_slot"] == s for r in assigned_rows)]
    for slot in empty_slots:
        if unassigned:
            pick = unassigned.pop(0)
            assigned_rows.append({"meal_slot": slot, **pick})

    # Third pass: assign remaining items as complementary meal pairings
    for extra in unassigned:
        target_slot = "Lunch" if len([r for r in assigned_rows if r["meal_slot"] == "Lunch"]) <= 1 else "Dinner"
        assigned_rows.append({"meal_slot": target_slot, **extra})

    # Standardize column outputs
    standardized_rows: list[dict] = []
    for r in assigned_rows:
        food_name = str(r.get("canonical_name", r.get("food_name", "")))
        portion = float(r.get("grams", r.get("portion_g", 0.0)))
        leu = float(r.get("leucine_g", 0.0))
        standardized_rows.append(
            {
                "meal_slot": r["meal_slot"],
                "food_name": food_name,
                "canonical_name": food_name,
                "portion_g": portion,
                "grams": portion,
                "usable_protein_g": float(r.get("usable_protein_g", 0.0)),
                "leucine_g": leu,
                "leucine_trigger_met": leu >= 2.2,
                "cost_inr": float(r.get("cost_inr", 0.0)),
                "store_name": r.get("store_name", "Local Partner Store"),
                "distance_km": float(r.get("distance_km", 0.0)),
                "adaptive_note": r.get("adaptive_note", ""),
            }
        )

    # Sort in chronological order
    order_map = {slot: i for i, slot in enumerate(slots_order)}
    standardized_rows.sort(key=lambda x: (order_map.get(x["meal_slot"], 99), -x["usable_protein_g"]))

    if adherence_pct < 75.0 and standardized_rows:
        standardized_rows[0]["adaptive_note"] = (
            f"Adherence={adherence_pct:.0f}% (<75%): Increasing taste preference & convenience weight w4 by +15% next week."
        )
    return standardized_rows
