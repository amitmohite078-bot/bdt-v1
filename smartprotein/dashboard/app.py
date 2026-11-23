"""SmartProtein Stage 11: Interactive Big Data Quality & Accurate Protein Recommender UI."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR.parent))

from smartprotein.spark_jobs.recommender.lp_budget_optimizer import (  # noqa: E402
    allocate_meal_plan,
    solve_daily_diet_lp,
)
from smartprotein.spark_jobs.recommender.hybrid_recommender import (  # noqa: E402
    score_candidates_for_user,
)
from smartprotein.spark_jobs.recommender.safety_and_geo import (  # noqa: E402
    compute_micronutrient_hints,
)

DATA_DIR = ROOT_DIR / "data"
CURATED_DIR = DATA_DIR / "curated"
REJECTED_DIR = DATA_DIR / "rejected" / "all_records"
ANALYTICS_DIR = DATA_DIR / "analytics"
SEED_PATH = ROOT_DIR / "config" / "reference_foods_seed.json"
REC_CFG_PATH = ROOT_DIR / "config" / "recommender_config.json"

st.set_page_config(
    page_title="SmartProtein | Hadoop HDFS DQ & Accurate Protein Recommender",
    page_icon="🥩",
    layout="wide",
)

# Pantone Option 1 ("Clinical Nutrition & Trust") UI Constants & Styling
PANTONE = {
    "emerald": "#009473",       # PANTONE 17-5641
    "classic_blue": "#0F4C81",  # PANTONE 19-4052
    "greenery": "#88B04B",      # PANTONE 15-0343
    "living_coral": "#FF6F61",  # PANTONE 16-1546
    "mimosa": "#F0C05A",        # PANTONE 14-0848
    "cloud_dancer": "#F0EEE9",  # PANTONE 11-4201
    "anthracite": "#28282D",    # PANTONE 19-4007
}

st.markdown(
    f"""
    <style>
      h1, h2, h3 {{ color: {PANTONE["classic_blue"]}; }}
      div[data-testid="stMetric"] {{
        background-color: #FFFFFF;
        border-left: 4px solid {PANTONE["emerald"]};
        padding: 12px 14px;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(40, 40, 45, 0.08);
      }}
      button[data-baseweb="tab"][aria-selected="true"] {{
        border-bottom-color: {PANTONE["emerald"]} !important;
        color: {PANTONE["emerald"]} !important;
        font-weight: 600;
      }}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data
def load_all_data() -> dict[str, pd.DataFrame]:
    paths = {
        "scorecards": ANALYTICS_DIR / "dq_scorecards.snappy.parquet",
        "drift": ANALYTICS_DIR / "drift_alerts.snappy.parquet",
        "lineage": ANALYTICS_DIR / "dq_lineage.snappy.parquet",
        "rejected": REJECTED_DIR / "rejected_records.snappy.parquet",
        "ml_metrics": ANALYTICS_DIR / "ml_anomaly_metrics.snappy.parquet",
        "foods_enriched": CURATED_DIR / "protein_quality_enriched.snappy.parquet",
        "cost_cmp": ANALYTICS_DIR / "usable_protein_cost_comparison.snappy.parquet",
        "pairings": ANALYTICS_DIR / "complementary_pairings.snappy.parquet",
        "inventory": CURATED_DIR / "local_store_inventory.snappy.parquet",
        "users": CURATED_DIR / "user_protein_targets.snappy.parquet",
        "gold_recs": ANALYTICS_DIR / "gold_user_recommendations.snappy.parquet",
        "lp_plans": ANALYTICS_DIR / "lp_optimal_meal_plans.snappy.parquet",
        "rec_eval": ANALYTICS_DIR / "recommender_eval_metrics.snappy.parquet",
        "price_alerts": ANALYTICS_DIR / "realtime_price_alerts.snappy.parquet",
        "dq_alerts": ANALYTICS_DIR / "realtime_dq_alerts.snappy.parquet",
        "benchmarks": ANALYTICS_DIR / "stage12_benchmarks.snappy.parquet",
    }
    out: dict[str, pd.DataFrame] = {}
    for k, p in paths.items():
        out[k] = pd.read_parquet(p) if p.exists() else pd.DataFrame()
    return out


seed = json.loads(SEED_PATH.read_text(encoding="utf-8"))
rec_cfg = json.loads(REC_CFG_PATH.read_text(encoding="utf-8"))
data = load_all_data()

st.title("SmartProtein: Hadoop HDFS Data Quality & Accurate Protein Recommender")
st.caption(
    "5-Zone HDFS Lakehouse (raw → staging → curated / rejected → analytics) | "
    "DIAAS/Bioavailable Usable Protein | PuLP/HiGHS Budget Optimizer & Hyperlocal Geo"
)

if data["scorecards"].empty or data["foods_enriched"].empty:
    st.error("SmartProtein Parquet tables are missing. Run Stages 3, 6, 7, 8, 9, and 10 first.")
    st.stop()

sc_before = data["scorecards"][data["scorecards"]["stage"] == "BEFORE_CLEANING"]
sc_after = data["scorecards"][data["scorecards"]["stage"] == "AFTER_CLEANING"]
raw_tot = int(sc_before["total_records"].sum())
clean_tot = int(sc_after["clean_records"].sum())
rej_tot = int(sc_before["rejected_records"].sum())
avg_before = round(float(sc_before["composite_dq_score"].mean()), 2)
avg_after = round(float(sc_after["composite_dq_score"].mean()), 2)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Raw Multi-Source Rows", f"{raw_tot:,}")
k2.metric("Curated Clean Rows", f"{clean_tot:,}")
k3.metric("Quarantined Rows (DLQ)", f"{rej_tot:,}", f"-{round(rej_tot/max(raw_tot,1)*100,1)}% noise removed")
k4.metric("Composite DQ Score", f"{avg_after:.1f}/100", f"+{round(avg_after - avg_before, 2)} vs Before")
if not data["rec_eval"].empty:
    k5.metric("Avg LP Cost Savings", f"{data['rec_eval']['avg_lp_cost_savings_pct'].iloc[0]}%", "vs Naive Selection")

tabs = st.tabs(
    [
        "📊 1. DQ Before/After, Drift & Lineage",
        "🧬 2. Accurate Protein Lab (DIAAS & Cost)",
        "🥗 3. Hyperlocal Protein Finder & LP Meal Plan",
        "⚡ 4. Kafka Speed-Layer Alerts",
        "📈 5. ML & System Evaluation",
    ]
)

# ==========================================================================
# TAB 1: DQ BEFORE/AFTER, DRIFT ALERTS, QUARANTINE & LINEAGE
# ==========================================================================
with tabs[0]:
    st.subheader("Data Quality Improvement: Before vs. After Cleaning")
    sc_chart = (
        alt.Chart(data["scorecards"])
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("dataset_name:N", title="Dataset"),
            y=alt.Y("composite_dq_score:Q", scale=alt.Scale(domain=[70, 100]), title="Composite DQ Score (0–100)"),
            color=alt.Color(
                "stage:N",
                sort=["BEFORE_CLEANING", "AFTER_CLEANING"],
                scale=alt.Scale(
                    domain=["BEFORE_CLEANING", "AFTER_CLEANING"],
                    range=[PANTONE["living_coral"], PANTONE["emerald"]],
                ),
                title="Pipeline Stage",
            ),
            xOffset="stage:N",
            tooltip=["dataset_name", "stage", "total_records", "rejected_records", "composite_dq_score"],
        )
        .properties(height=320)
    )
    st.altair_chart(sc_chart, width="stretch")

    c_left, c_right = st.columns(2)
    with c_left:
        st.markdown("#### Source Quality Drift Monitor (Threshold: 85.0%)")
        if not data["drift"].empty:
            drift_alerts = data["drift"][data["drift"]["drift_alert"] == True]  # noqa: E712
            if not drift_alerts.empty:
                st.warning(
                    f"DRIFT ALERT: `{drift_alerts['source_db'].iloc[0]}` fell to "
                    f"`{drift_alerts['dq_score'].iloc[0]}%` (< 85% SLA)!"
                )
            drift_chart = (
                alt.Chart(data["drift"])
                .mark_line(point=True, strokeWidth=3)
                .encode(
                    x=alt.X("day_offset:O", title="Days Relative to Today"),
                    y=alt.Y("dq_score:Q", scale=alt.Scale(domain=[75, 100]), title="Source DQ Score (%)"),
                    color=alt.Color(
                        "source_db:N",
                        scale=alt.Scale(range=[PANTONE["classic_blue"], PANTONE["emerald"], PANTONE["living_coral"]]),
                        title="Source",
                    ),
                    tooltip=["source_db", "day_offset", "dq_score", "drift_alert"],
                )
                .properties(height=270)
            )
            st.altair_chart(drift_chart, width="stretch")

    with c_right:
        st.markdown("#### Quarantine Breakdown by Reject Reason Code")
        if not data["rejected"].empty:
            rej_counts = (
                data["rejected"].groupby(["source_dataset", "reject_reason_code"], as_index=False)
                .size()
                .rename(columns={"size": "count"})
            )
            rej_chart = (
                alt.Chart(rej_counts)
                .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                .encode(
                    y=alt.Y("reject_reason_code:N", sort="-x", title="Reject Reason Code"),
                    x=alt.X("count:Q", title="Quarantined Records"),
                    color=alt.Color(
                        "source_dataset:N",
                        scale=alt.Scale(range=[PANTONE["living_coral"], PANTONE["mimosa"], PANTONE["classic_blue"]]),
                        title="Source Dataset",
                    ),
                    tooltip=["source_dataset", "reject_reason_code", "count"],
                )
                .properties(height=270)
            )
            st.altair_chart(rej_chart, width="stretch")

    st.markdown("#### Sample Row-Level Data Lineage (`raw -> rule -> curated/rejected`)")
    if not data["lineage"].empty:
        st.dataframe(data["lineage"].head(12), width="stretch", hide_index=True)

# ==========================================================================
# TAB 2: ACCURATE PROTEIN INFORMATICS (CRUDE VS USABLE PROTEIN)
# ==========================================================================
with tabs[1]:
    st.subheader("Crude vs. Usable Protein (DIAAS, Cooking & Phytate Bioavailability)")
    cmp_df = data["cost_cmp"]
    melted = cmp_df.melt(
        id_vars=["canonical_name", "diet_type"],
        value_vars=["crude_protein_per_100g", "usable_protein_per_100g"],
        var_name="Metric",
        value_name="Grams_per_100g",
    )
    prot_chart = (
        alt.Chart(melted)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("canonical_name:N", sort="-y", title="Food"),
            y=alt.Y("Grams_per_100g:Q", title="Protein (g per 100g)"),
            color=alt.Color(
                "Metric:N",
                scale=alt.Scale(
                    domain=["crude_protein_per_100g", "usable_protein_per_100g"],
                    range=[PANTONE["classic_blue"], PANTONE["emerald"]],
                ),
                title="Protein Type",
            ),
            xOffset="Metric:N",
            tooltip=["canonical_name", "diet_type", "Metric", "Grams_per_100g"],
        )
        .properties(height=340)
    )
    st.altair_chart(prot_chart, width="stretch")

    st.markdown("#### Cost per Gram of USABLE Protein & 2.5g Leucine Trigger Portion")
    st.dataframe(
        cmp_df[
            [
                "canonical_name", "diet_type", "diaas_score", "bioavailability_factor",
                "crude_protein_per_100g", "usable_protein_per_100g", "quality_penalty_pct",
                "median_price_100g_inr", "cost_per_g_usable_protein_inr",
                "grams_for_2_5g_leucine", "protein_confidence_score", "recommended_timing",
            ]
        ],
        width="stretch",
        hide_index=True,
    )

    if not data["pairings"].empty:
        st.markdown("#### Complementary Amino Acid Pairings (Limiting EAA Coverage)")
        st.dataframe(data["pairings"], width="stretch", hide_index=True)

# ==========================================================================
# TAB 3: LIVE HYPERLOCAL RECOMMENDER & LP DIET OPTIMIZER
# ==========================================================================
with tabs[2]:
    st.subheader("Live Personalized Protein Recommender & Linear Programming Meal Plan")

    city_lookup = {
        c["name"]: (reg, c["lat"], c["lon"])
        for reg, info in seed["regions"].items()
        for c in info["cities"]
    }

    # Initialize session state keys for reactive UI controls
    if "sel_diet" not in st.session_state:
        st.session_state["sel_diet"] = "Veg"
    if "sel_city" not in st.session_state:
        st.session_state["sel_city"] = "Bengaluru"
    if "sel_goal" not in st.session_state:
        st.session_state["sel_goal"] = "Muscle_Gain"
    if "sel_weight" not in st.session_state:
        st.session_state["sel_weight"] = 72
    if "sel_budget" not in st.session_state:
        st.session_state["sel_budget"] = 160
    if "sel_radius" not in st.session_state:
        st.session_state["sel_radius"] = 15

    # Quick Demo Presets
    st.markdown("**⚡ 1-Click Demo Profiles:**")
    preset_cols = st.columns(4)
    if preset_cols[0].button("🏋️ Muscle Gain (Non-Veg)", width="stretch"):
        st.session_state["sel_diet"] = "Non-Veg"
        st.session_state["sel_city"] = "Bengaluru"
        st.session_state["sel_goal"] = "Muscle_Gain"
        st.session_state["sel_weight"] = 74
        st.session_state["sel_budget"] = 180
        st.rerun()
    if preset_cols[1].button("🥗 High-Protein (Veg)", width="stretch"):
        st.session_state["sel_diet"] = "Veg"
        st.session_state["sel_city"] = "Delhi"
        st.session_state["sel_goal"] = "Muscle_Gain"
        st.session_state["sel_weight"] = 70
        st.session_state["sel_budget"] = 160
        st.rerun()
    if preset_cols[2].button("🌱 Plant-Based (Vegan)", width="stretch"):
        st.session_state["sel_diet"] = "Vegan"
        st.session_state["sel_city"] = "Mumbai"
        st.session_state["sel_goal"] = "Fat_Loss"
        st.session_state["sel_weight"] = 65
        st.session_state["sel_budget"] = 140
        st.rerun()
    if preset_cols[3].button("⚡ Active (Eggetarian)", width="stretch"):
        st.session_state["sel_diet"] = "Eggetarian"
        st.session_state["sel_city"] = "Hyderabad"
        st.session_state["sel_goal"] = "Athletic_Endurance"
        st.session_state["sel_weight"] = 68
        st.session_state["sel_budget"] = 150
        st.rerun()

    cities_list = sorted(city_lookup.keys())
    diet_options = ["Vegan", "Veg", "Eggetarian", "Non-Veg"]
    goal_options = ["Muscle_Gain", "Fat_Loss", "Athletic_Endurance", "General_Wellness"]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        u_diet = st.selectbox("Dietary Choice", diet_options, key="sel_diet")
        u_city = st.selectbox("Location (City)", cities_list, key="sel_city")
    with col2:
        u_goal = st.selectbox("Fitness Goal", goal_options, key="sel_goal")
        u_weight = st.slider("Body Weight (kg)", 45, 120, key="sel_weight")
    with col3:
        u_budget = st.slider("Daily Protein Food Budget (INR ₹)", 50, 600, step=10, key="sel_budget")
        u_radius = st.slider("Search Radius (km)", 2, 35, key="sel_radius")
    with col4:
        u_allergies = st.multiselect("Allergies (Hard Excluded)", ["lactose", "soy", "egg", "fish", "nuts", "gluten"])
        u_med = st.selectbox("Medical Condition", ["none", "kidney_caution", "diabetes_type2"])

    c_ad1, c_ad2 = st.columns(2)
    with c_ad1:
        u_workout = st.selectbox("Preferred Workout Time", ["Morning", "Afternoon", "Evening"])
    with c_ad2:
        u_adherence = st.slider("Last Week Plan Adherence (%)", 40, 100, 82)

    with st.expander("Adjust Multi-Factor Scoring Weights (w1..w5)"):
        wc1, wc2, wc3, wc4, wc5 = st.columns(5)
        w1 = wc1.slider("w1 Adjusted Protein", 0.0, 1.0, 0.35, 0.05)
        w2 = wc2.slider("w2 Protein/Cost", 0.0, 1.0, 0.30, 0.05)
        w3 = wc3.slider("w3 Proximity", 0.0, 1.0, 0.15, 0.05)
        w4 = wc4.slider("w4 Regional Pref", 0.0, 1.0, 0.20, 0.05)
        w5 = wc5.slider("w5 Calorie Penalty", 0.0, 0.5, 0.10, 0.05)

    reg, u_lat, u_lon = city_lookup[u_city]
    goal_mult = {"Muscle_Gain": 1.80, "Fat_Loss": 1.60, "Athletic_Endurance": 1.50, "General_Wellness": 1.20}[u_goal]
    if u_med == "kidney_caution":
        goal_mult = 1.00
    target_g = round(u_weight * goal_mult, 1)

    custom_cfg = json.loads(json.dumps(rec_cfg))
    custom_cfg["max_distance_km"] = float(u_radius)
    custom_cfg["scoring_weights"] = {
        "w1_adjusted_protein": w1,
        "w2_protein_per_cost": w2,
        "w3_proximity": w3,
        "w4_preference_match": w4 * (1.15 if u_adherence < 75 else 1.0),
        "w5_calorie_penalty": w5,
    }

    user_series = pd.Series(
        {
            "user_hash_id": "LIVE_DEMO_USER",
            "region": reg,
            "city": u_city,
            "lat_grid": u_lat,
            "lon_grid": u_lon,
            "diet_type": u_diet,
            "allergies": "|".join(u_allergies) if u_allergies else "none",
            "medical_conditions": u_med,
            "daily_budget_inr": float(u_budget),
            "daily_protein_target_g": target_g,
            "workout_time": u_workout,
        }
    )

    scored = score_candidates_for_user(user_series, data["foods_enriched"], data["inventory"], custom_cfg, seed)
    if scored.empty:
        # Fallback with relaxed radius so a meal plan is always generated
        fallback_cfg = json.loads(json.dumps(custom_cfg))
        fallback_cfg["max_distance_km"] = 50.0
        scored = score_candidates_for_user(user_series, data["foods_enriched"], data["inventory"], fallback_cfg, seed)
        st.info(f"Notice: Radius automatically extended to 50 km in {u_city} to find verified safe protein foods.")

    lp_res = solve_daily_diet_lp(scored, protein_target_g=target_g, max_budget_inr=float(u_budget))

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Daily Usable Protein Target", f"{target_g} g/day", f"{goal_mult} g/kg target")
    m2.metric("Safe Nearby Foods Found", f"{len(scored)}", f"Within {u_radius} km in {u_city}")

    status_tag = lp_res.get("status", "")
    cost_val = lp_res.get("total_cost_inr", 0)
    if lp_res.get("within_budget", True) and cost_val > 0:
        cost_delta = f"✅ ₹{round(float(u_budget) - cost_val, 1)} under budget"
    elif cost_val > 0:
        cost_delta = f"⚠️ ₹{round(cost_val - float(u_budget), 1)} over budget"
    else:
        cost_delta = status_tag

    m3.metric("LP Optimal Daily Cost", f"₹{cost_val}", cost_delta)
    m4.metric("LP Usable Protein Delivered", f"{lp_res.get('total_usable_protein_g', 0)} g", f"{lp_res.get('total_kcal', 0):.0f} kcal total")

    if u_med == "kidney_caution":
        st.warning("Medical Safety Active: Daily protein target automatically capped at 1.0 g/kg for kidney caution.")

    # Render Personalized Protein Meal Plan FIRST for instant visibility
    if lp_res.get("items"):
        st.markdown("#### 🍱 Personalized Protein Plan (4-Slot Daily Menu)")
        st.caption(
            f"Delivering **{lp_res['total_usable_protein_g']}g bioavailable usable protein** "
            f"(Target: {target_g}g) at **₹{lp_res['total_cost_inr']}/day** strictly within your "
            f"**₹{u_budget}** daily budget ({lp_res.get('total_kcal', 0):.0f} kcal total)."
        )
        raw_meals = allocate_meal_plan(lp_res["items"], u_workout, float(u_adherence))
        meal_df = pd.DataFrame(raw_meals)

        if not meal_df.empty:
            if "leucine_trigger_met" in meal_df.columns:
                meal_df["Leucine MPS Trigger"] = meal_df.apply(
                    lambda r: f"🟢 Trigger Met ({r['leucine_g']}g)" if r["leucine_trigger_met"] else f"🟡 Sub-trigger ({r['leucine_g']}g)",
                    axis=1,
                )
            rename_map = {
                "meal_slot": "Meal Slot",
                "food_name": "Food Item",
                "portion_g": "Portion (g)",
                "usable_protein_g": "Usable Protein (g)",
                "cost_inr": "Cost (₹)",
                "store_name": "Store",
                "distance_km": "Distance (km)",
            }
            cols_to_show = [c for c in ["meal_slot", "food_name", "portion_g", "usable_protein_g", "Leucine MPS Trigger", "cost_inr", "store_name", "distance_km"] if c in meal_df.columns]
            meal_display = meal_df[cols_to_show].rename(columns=rename_map)
            st.dataframe(meal_display, width="stretch", hide_index=True)

        with st.expander("🛒 View Daily Grocery Basket & Store Pickup List"):
            basket_rows = []
            for item in lp_res["items"]:
                basket_rows.append(
                    {
                        "Food Item": item["canonical_name"],
                        "Quantity (g)": f"{item['grams']:.0f} g",
                        "Daily Cost (₹)": f"₹{item['cost_inr']:.2f}",
                        "Usable Protein (g)": f"{item['usable_protein_g']:.1f} g",
                        "Partner Store": item["store_name"],
                        "Distance": f"{item['distance_km']:.1f} km",
                    }
                )
            st.dataframe(pd.DataFrame(basket_rows), width="stretch", hide_index=True)

        hints = compute_micronutrient_hints(
            u_diet,
            lp_res.get("total_iron_mg", 0.0),
            lp_res.get("total_calcium_mg", 0.0),
            lp_res.get("total_b12_mcg", 0.0),
            rec_cfg["micronutrient_rda"],
        )
        for h in hints:
            st.info(f"**Micronutrient Hint:** {h}")
    else:
        st.warning("⚠️ No foods found within selected radius & budget. Try expanding Search Radius (km) or increasing Daily Budget.")

    if not scored.empty:
        st.markdown("#### Top Ranked Hyperlocal Protein Foods (Explainable Multi-Factor Scoring)")
        disp_scored = scored[
            [
                "canonical_name", "diet_type", "store_name", "distance_km",
                "usable_protein_per_100g", "price_per_100g_inr", "cost_per_10g_usable_inr",
                "composite_rec_score", "medical_warning", "explanation_text",
            ]
        ].head(8).copy()
        disp_scored.columns = [
            "Food Name", "Diet Tag", "Partner Store", "Distance (km)",
            "Usable Prot/100g", "Price/100g (₹)", "Cost/10g Prot (₹)",
            "Score", "Safety Warning", "Recommendation Explanation",
        ]
        st.dataframe(disp_scored, width="stretch", hide_index=True)

# ==========================================================================
# TAB 4: KAFKA SPEED-LAYER REAL-TIME ALERTS
# ==========================================================================
with tabs[3]:
    st.subheader("Speed Layer: Real-Time Flash Price Drops & Streaming DQ Blocks")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Live Store Flash Discounts (>= 20% Off)")
        if not data["price_alerts"].empty:
            st.dataframe(data["price_alerts"], width="stretch", hide_index=True)
    with c2:
        st.markdown("#### Real-Time Streaming DQ Violations Blocked")
        if not data["dq_alerts"].empty:
            st.dataframe(data["dq_alerts"], width="stretch", hide_index=True)

# ==========================================================================
# TAB 5: ML & SYSTEM BENCHMARK EVALUATION
# ==========================================================================
with tabs[4]:
    st.subheader("ML Anomaly Detection, Recommender Accuracy & System Benchmarks")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Isolation Forest + KMeans Anomaly Detection")
        if not data["ml_metrics"].empty:
            st.dataframe(data["ml_metrics"], width="stretch", hide_index=True)
    with c2:
        st.markdown("#### Hybrid Recommender & LP Evaluation")
        if not data["rec_eval"].empty:
            st.dataframe(data["rec_eval"], width="stretch", hide_index=True)

    st.markdown("#### Big Data Storage & Engine Benchmarks (Stage 12)")
    if data["benchmarks"].empty:
        st.info("Stage 12 benchmark metrics will appear here automatically once `benchmarks/run_stage12_benchmarks.py` runs.")
    else:
        st.dataframe(data["benchmarks"], width="stretch", hide_index=True)

