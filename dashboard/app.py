"""FitGuard-HDFS Interactive Data Quality & Personalized Protein Dashboard."""

from __future__ import annotations

import json
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from scipy.optimize import linprog

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config" / "pipeline_config.json"
ALIASES_PATH = ROOT_DIR / "config" / "city_aliases.json"

with CONFIG_PATH.open("r", encoding="utf-8") as fh:
    CFG = json.load(fh)
with ALIASES_PATH.open("r", encoding="utf-8") as fh:
    ALIASES = json.load(fh)

LAKE_ROOT = ROOT_DIR / CFG["local_lakehouse_root"]
SILVER_DIR = LAKE_ROOT / CFG["zones"]["silver"]
QUARANTINE_DIR = LAKE_ROOT / CFG["zones"]["quarantine"]
GOLD_DIR = LAKE_ROOT / CFG["zones"]["gold"]

st.set_page_config(
    page_title="FitGuard-HDFS | Big Data Quality & Protein Recommender",
    page_icon="🏋️",
    layout="wide",
)


@st.cache_data
def load_lakehouse_tables() -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    files = {
        "dq_scorecard": GOLD_DIR / "dq_scorecard.snappy.parquet",
        "storage_stats": GOLD_DIR / "hdfs_storage_stats.snappy.parquet",
        "user_targets": GOLD_DIR / "user_wellness_targets.snappy.parquet",
        "gold_recs": GOLD_DIR / "protein_recommendations.snappy.parquet",
        "silver_foods": SILVER_DIR / "protein_foods_catalog" / "part-00000.snappy.parquet",
        "silver_telemetry": SILVER_DIR / "wearable_telemetry" / "part-00000.snappy.parquet",
        "q_users": QUARANTINE_DIR / "user_profiles" / "part-00000.snappy.parquet",
        "q_foods": QUARANTINE_DIR / "protein_foods_catalog" / "part-00000.snappy.parquet",
        "q_telemetry": QUARANTINE_DIR / "wearable_telemetry" / "part-00000.snappy.parquet",
        "benchmark": GOLD_DIR / "benchmark_metrics.snappy.parquet",
    }
    for key, fpath in files.items():
        if fpath.exists():
            tables[key] = pd.read_parquet(fpath)
        else:
            tables[key] = pd.DataFrame()
    return tables


data = load_lakehouse_tables()

st.title("FitGuard-HDFS: Big Data Quality & Personalized Protein Pipeline")
st.caption(
    "Medallion Lakehouse on Hadoop HDFS (Bronze → Silver + Quarantine → Gold) | "
    "Personalized Protein Recommendations by Dietary Choice, Cost & Location"
)

if data["dq_scorecard"].empty:
    st.error(
        "Lakehouse Parquet files not found. Run `python data_generator/generate_dirty_datasets.py` "
        "and `python local_runner/run_local_pipeline.py` first."
    )
    st.stop()

dq_df = data["dq_scorecard"]
total_raw = int(dq_df["Total_Raw_Rows"].sum())
total_silver = int(dq_df["Clean_Silver_Rows"].sum())
total_quarantined = int(dq_df["Quarantined_Rows"].sum())
overall_pass = round((total_silver / max(total_raw, 1)) * 100.0, 2)
imputed_count = int(data["silver_telemetry"]["is_imputed"].sum()) if not data["silver_telemetry"].empty else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Bronze Raw Records", f"{total_raw:,}")
k2.metric("Silver Clean Records", f"{total_silver:,}", f"{overall_pass}% Pass")
k3.metric("Quarantined (DLQ)", f"{total_quarantined:,}", f"{round(100 - overall_pass, 2)}% Blocked", delta_color="inverse")
k4.metric("Smart-Imputed HR Dropouts", f"{imputed_count:,}", "Preserved w/ Lineage Flag")
k5.metric("Verified Food Listings", f"{len(data['silver_foods']):,}", "Unit & City Normalized")

tab1, tab2, tab3, tab4 = st.tabs(
    [
        "📊 1. HDFS Medallion & DQ Scorecard",
        "🛡️ 2. Quarantine Inspector (DLQ)",
        "🥗 3. Protein Finder (Choice • Cost • Location)",
        "⚡ 4. MapReduce vs Spark & HDFS Storage",
    ]
)

# ------------------------------------------------------------------
# TAB 1: MEDALLION & 6-DIMENSION DQ SCORECARD
# ------------------------------------------------------------------
with tab1:
    st.subheader("6-Dimension Data Quality Scorecard Across Multi-Modal Sources")
    dims = ["Completeness", "Validity", "Uniqueness", "Consistency", "Timeliness", "Semantic_Integrity"]
    melted_dq = dq_df.melt(
        id_vars=["dataset"],
        value_vars=dims,
        var_name="Dimension",
        value_name="Score_Pct",
    )

    chart_dq = (
        alt.Chart(melted_dq)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("Dimension:N", sort=dims, title="DQ Dimension"),
            y=alt.Y("Score_Pct:Q", scale=alt.Scale(domain=[70, 100]), title="Quality Score (%)"),
            color=alt.Color("dataset:N", title="Dataset"),
            xOffset="dataset:N",
            tooltip=["dataset", "Dimension", alt.Tooltip("Score_Pct:Q", format=".2f")],
        )
        .properties(height=360)
    )
    st.altair_chart(chart_dq, width='stretch')

    st.dataframe(
        dq_df[
            [
                "dataset",
                "Total_Raw_Rows",
                "Clean_Silver_Rows",
                "Quarantined_Rows",
                "Pass_Rate_Pct",
                "Composite_Score",
                *dims,
            ]
        ],
        width='stretch',
        hide_index=True,
    )

    if not data["silver_telemetry"].empty:
        st.subheader("Wearable Wellness Signal Insights (Post-Cleaning Silver Telemetry)")
        c1, c2 = st.columns(2)
        with c1:
            hr_sample = data["silver_telemetry"].sample(min(3000, len(data["silver_telemetry"])), random_state=42)
            hr_hist = (
                alt.Chart(hr_sample)
                .mark_bar()
                .encode(
                    x=alt.X("heart_rate_bpm:Q", bin=alt.Bin(maxbins=25), title="Cleaned Heart Rate (bpm)"),
                    y=alt.Y("count()", title="Event Count"),
                    color=alt.Color("sleep_stage:N", title="Activity / Sleep State"),
                )
                .properties(height=300, title="Physiologically Validated HR Distribution (32–220 bpm)")
            )
            st.altair_chart(hr_hist, width='stretch')
        with c2:
            goals_df = data["user_targets"].groupby("fitness_goal", as_index=False)["daily_protein_target_g"].mean()
            goal_bar = (
                alt.Chart(goals_df)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("fitness_goal:N", title="Fitness Goal"),
                    y=alt.Y("daily_protein_target_g:Q", title="Avg Daily Protein Target (g)"),
                    color="fitness_goal:N",
                    tooltip=["fitness_goal", alt.Tooltip("daily_protein_target_g:Q", format=".1f")],
                )
                .properties(height=300, title="Activity-Adjusted Daily Protein Targets by Goal")
            )
            st.altair_chart(goal_bar, width='stretch')

# ------------------------------------------------------------------
# TAB 2: QUARANTINE INSPECTOR (DEAD-LETTER QUEUE)
# ------------------------------------------------------------------
with tab2:
    st.subheader("Dead-Letter Quarantine Zone: Blocked Anomalies & Dangerous Lies")

    q_frames = []
    for name, qdf in [("protein_foods_catalog", data["q_foods"]), ("user_profiles", data["q_users"]), ("wearable_telemetry", data["q_telemetry"])]:
        if not qdf.empty:
            sub = qdf.copy()
            if "source_dataset" not in sub.columns:
                sub["source_dataset"] = name
            q_frames.append(sub)

    all_q = pd.concat(q_frames, ignore_index=True) if q_frames else pd.DataFrame()
    if not all_q.empty:
        rule_counts = (
            all_q.groupby(["source_dataset", "dq_rule_id"], as_index=False)
            .size()
            .rename(columns={"size": "Quarantined_Records"})
        )
        col_left, col_right = st.columns([1.2, 1])
        with col_left:
            rule_chart = (
                alt.Chart(rule_counts)
                .mark_bar()
                .encode(
                    y=alt.Y("dq_rule_id:N", sort="-x", title="DQ Rule ID"),
                    x=alt.X("Quarantined_Records:Q", title="Blocked Records"),
                    color=alt.Color("source_dataset:N", title="Source Dataset"),
                    tooltip=["source_dataset", "dq_rule_id", "Quarantined_Records"],
                )
                .properties(height=320, title="Quarantine Volume by Data Quality Rule")
            )
            st.altair_chart(rule_chart, width='stretch')
        with col_right:
            sem_lies = data["q_foods"][data["q_foods"]["dq_rule_id"] == "DQ-SEM-001"] if not data["q_foods"].empty else pd.DataFrame()
            st.info(
                f"**Semantic Integrity Guard (`DQ-SEM-001`):** Blocked **{len(sem_lies):,}** mislabeled food listings "
                "(e.g., Chicken/Fish/Eggs tagged as `Vegan` or `Veg`) before they could reach the recommender!"
            )
            if not sem_lies.empty:
                st.dataframe(
                    sem_lies[["listing_id", "food_name", "diet_tag", "city", "dq_error_reason"]].head(8),
                    width='stretch',
                    hide_index=True,
                )

        st.markdown("### Inspect Quarantined Records")
        selected_ds = st.selectbox("Filter by Source Dataset", ["All", "protein_foods_catalog", "user_profiles", "wearable_telemetry"])
        filtered_q = all_q if selected_ds == "All" else all_q[all_q["source_dataset"] == selected_ds]
        rules_avail = ["All"] + sorted(filtered_q["dq_rule_id"].dropna().unique().tolist())
        selected_rule = st.selectbox("Filter by DQ Rule ID", rules_avail)
        if selected_rule != "All":
            filtered_q = filtered_q[filtered_q["dq_rule_id"] == selected_rule]
        display_cols = [c for c in ["source_dataset", "dq_rule_id", "dq_dimension", "dq_error_reason", "food_name", "diet_tag", "city", "user_id", "event_id", "raw_payload"] if c in filtered_q.columns]
        st.dataframe(filtered_q[display_cols].head(100), width='stretch', hide_index=True)

# ------------------------------------------------------------------
# TAB 3: PERSONALIZED PROTEIN FINDER (CHOICE + COST + LOCATION)
# ------------------------------------------------------------------
with tab3:
    st.subheader("Personalized Protein Food Recommendations (Silver-Verified Data Only)")
    mode = st.radio(
        "Recommendation Mode",
        ["Live Custom Finder (Choice + Cost + Location)", "Inspect Existing Gold User Profile"],
        horizontal=True,
    )

    silver_foods = data["silver_foods"]
    diet_hierarchy = CFG["diet_hierarchy"]

    if mode.startswith("Live"):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            choice = st.selectbox("Dietary Choice", ["Vegan", "Veg", "Eggetarian", "Non-Veg"], index=1)
        with c2:
            city = st.selectbox("Location (City)", ALIASES["canonical_cities"], index=0)
        with c3:
            budget = st.slider("Daily Food Budget (INR ₹)", min_value=50, max_value=600, value=200, step=10)
        with c4:
            goal = st.selectbox("Fitness Goal", list(CFG["protein_target_g_per_kg"].keys()), index=0)

        weight = st.slider("Body Weight (kg)", min_value=40, max_value=130, value=70, step=1)
        daily_target = round(weight * CFG["protein_target_g_per_kg"][goal], 1)

        allowed = diet_hierarchy[choice]
        matches = silver_foods[
            (silver_foods["city"] == city)
            & (silver_foods["diet_tag"].isin(allowed))
            & (silver_foods["in_stock"] == True)  # noqa: E712
            & (silver_foods["cost_per_100g_inr"] <= budget * 0.70)
        ].sort_values(by=["protein_per_100_inr", "protein_per_100g"], ascending=[False, False])
        if matches.empty:
            matches = silver_foods[
                (silver_foods["city"] == city)
                & (silver_foods["diet_tag"].isin(allowed))
                & (silver_foods["in_stock"] == True)
            ].sort_values(by=["protein_per_100_inr", "protein_per_100g"], ascending=[False, False])

        best_unique = matches.drop_duplicates(subset=["food_name"], keep="first").copy()
        best_unique["portion_for_35pct_target_g"] = (
            (daily_target * 0.35 / best_unique["protein_per_100g"]) * 100.0
        ).clip(upper=300.0).round(0)
        best_unique["portion_cost_inr"] = (
            (best_unique["portion_for_35pct_target_g"] / 100.0) * best_unique["cost_per_100g_inr"]
        ).round(2)
        best_unique["portion_protein_g"] = (
            (best_unique["portion_for_35pct_target_g"] / 100.0) * best_unique["protein_per_100g"]
        ).round(1)

        m1, m2, m3 = st.columns(3)
        m1.metric("Daily Protein Target", f"{daily_target} g/day")
        m2.metric("Verified Local Options", f"{len(best_unique)} foods in {city}")
        if not best_unique.empty:
            top_row = best_unique.iloc[0]
            m3.metric(
                "Best Value Pick",
                top_row["food_name"],
                f"{top_row['protein_per_100_inr']}g protein / ₹100",
            )

        if best_unique.empty:
            st.warning("No verified foods fit this strict budget in this city. Try increasing the daily budget slightly.")
        else:
            # -------------------------------------------------------------
            # Personalized 4-Slot Daily Protein Plan Generator
            # -------------------------------------------------------------
            def _build_daily_meal_plan(foods_df: pd.DataFrame, target_p: float, max_b: float) -> list[dict]:
                def slot_pref(name: str) -> str:
                    n = str(name).lower()
                    if "egg" in n or "sprout" in n or "peanut" in n or "curd" in n or "yogurt" in n:
                        return "Breakfast"
                    if "whey" in n or "soya" in n or "chana" in n:
                        return "Post-Workout Fuel"
                    if "paneer" in n or "tofu" in n or "fish" in n:
                        return "Dinner"
                    return "Lunch"

                df_c = foods_df.copy()
                df_c["slot"] = df_c["food_name"].apply(slot_pref)
                slots = ["Breakfast", "Lunch", "Post-Workout Fuel", "Dinner"]
                selected = []
                used = set()
                for s in slots:
                    cands = df_c[(df_c["slot"] == s) & (~df_c["food_name"].isin(used))]
                    if cands.empty:
                        cands = df_c[~df_c["food_name"].isin(used)]
                    if not cands.empty:
                        p = cands.iloc[0]
                        used.add(p["food_name"])
                        selected.append({"slot": s, **p.to_dict()})

                df_plan = pd.DataFrame(selected)
                costs = df_plan["cost_per_100g_inr"].to_numpy(dtype=float)
                prot = df_plan["protein_per_100g"].to_numpy(dtype=float)

                def bnd(nm: str) -> tuple[float, float]:
                    n = str(nm).lower()
                    if "soya" in n: return (0.45, 0.75)
                    if "egg" in n: return (1.00, 2.50)
                    if "chicken" in n or "fish" in n: return (1.20, 2.20)
                    if "paneer" in n or "tofu" in n: return (1.00, 1.80)
                    return (1.20, 2.00)

                bounds = [bnd(r["food_name"]) for _, r in df_plan.iterrows()]
                try:
                    res = linprog(c=costs, A_ub=np.vstack([-prot, costs]), b_ub=np.array([-target_p, max_b]), bounds=bounds, method="highs")
                    if not res.success:
                        res = linprog(c=-prot, A_ub=np.array([costs]), b_ub=np.array([max_b]), bounds=bounds, method="highs")
                    sol = res.x if res.success else np.array([b[0] for b in bounds])
                except Exception:
                    sol = np.array([b[0] for b in bounds])

                items = []
                for i, x in enumerate(sol):
                    r = df_plan.iloc[i]
                    g = round(float(x) * 100.0, 0)
                    p = round(float(x) * float(r["protein_per_100g"]), 1)
                    c = round(float(x) * float(r["cost_per_100g_inr"]), 2)
                    items.append({
                        "Meal Slot": r["slot"],
                        "Food Item": r["food_name"],
                        "Portion (g)": f"{g:.0f} g",
                        "Protein Delivered": f"{p:.1f} g",
                        "Daily Cost (₹)": f"₹{c:.2f}",
                        "Local Vendor": r["vendor_name"],
                        "Verified Tag": r["diet_tag"],
                        "_num_prot": p,
                        "_num_cost": c,
                    })
                return items

            meal_plan = _build_daily_meal_plan(best_unique, daily_target, float(budget))
            if meal_plan:
                tot_p = sum(it["_num_prot"] for it in meal_plan)
                tot_c = sum(it["_num_cost"] for it in meal_plan)

                st.markdown("### 🍱 Personalized Daily Protein Meal Plan")
                st.caption(
                    f"Optimized for **{city}** ({choice}) | Delivers **{tot_p:.1f}g Protein** (Target: {daily_target}g) "
                    f"at **₹{tot_c:.2f}/day** strictly within your **₹{budget}** budget."
                )

                plan_cols = ["Meal Slot", "Food Item", "Portion (g)", "Protein Delivered", "Daily Cost (₹)", "Local Vendor", "Verified Tag"]
                st.dataframe(pd.DataFrame(meal_plan)[plan_cols], width="stretch", hide_index=True)

            scatter = (
                alt.Chart(best_unique.head(15))
                .mark_circle(size=140)
                .encode(
                    x=alt.X("cost_per_100g_inr:Q", title="Standardized Cost (₹ per 100g)"),
                    y=alt.Y("protein_per_100g:Q", title="Protein Density (g per 100g)"),
                    color=alt.Color("diet_tag:N", title="Verified Diet Tag"),
                    tooltip=[
                        "food_name",
                        "vendor_name",
                        "diet_tag",
                        "protein_per_100g",
                        "cost_per_100g_inr",
                        "protein_per_100_inr",
                    ],
                )
                .properties(height=320, title=f"Protein Density vs. Regional Cost in {city} ({choice} Compatible)")
            )
            st.altair_chart(scatter, width='stretch')

            st.dataframe(
                best_unique[
                    [
                        "food_name",
                        "category",
                        "diet_tag",
                        "vendor_name",
                        "protein_per_100g",
                        "cost_per_100g_inr",
                        "protein_per_100_inr",
                        "cost_per_20g_protein_inr",
                        "portion_for_35pct_target_g",
                        "portion_protein_g",
                        "portion_cost_inr",
                    ]
                ].head(12),
                width='stretch',
                hide_index=True,
            )
    else:
        users_df = data["user_targets"]
        selected_uid = st.selectbox("Select Clean User ID", users_df["user_id"].tolist()[:100])
        u_row = users_df[users_df["user_id"] == selected_uid].iloc[0]
        u_recs = data["gold_recs"][data["gold_recs"]["user_id"] == selected_uid]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Choice & Location", f"{u_row['diet_choice']} | {u_row['location_city']}")
        c2.metric("Budget", f"₹{u_row['daily_food_budget_inr']}/day")
        c3.metric("Avg Daily Steps (Silver)", f"{int(u_row['avg_daily_steps']):,}")
        c4.metric("Activity-Adjusted Protein Target", f"{u_row['daily_protein_target_g']} g/day")

        st.dataframe(u_recs, width='stretch', hide_index=True)

# ------------------------------------------------------------------
# TAB 4: MAPREDUCE VS SPARK BENCHMARK & HDFS STORAGE OPTIMIZATION
# ------------------------------------------------------------------
with tab4:
    st.subheader("HDFS Small-Files Compaction & Snappy Parquet Optimization")
    storage_df = data["storage_stats"]
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(storage_df, width='stretch', hide_index=True)
    with col2:
        st_chart = (
            alt.Chart(storage_df)
            .mark_bar()
            .encode(
                x=alt.X("zone:N", sort=None, title="HDFS Lakehouse Zone"),
                y=alt.Y("size_mb:Q", title="Storage Footprint (MB)"),
                color="zone:N",
                tooltip=["zone", "file_count", "size_mb"],
            )
            .properties(height=280, title="HDFS Storage Size Reduction (Raw JSONL/CSV → Snappy Parquet)")
        )
        st.altair_chart(st_chart, width='stretch')

    st.subheader("Hadoop Streaming MapReduce vs. Columnar Vectorized / Spark Benchmark")
    bench_df = data["benchmark"]
    if bench_df.empty:
        st.info("Run `python mapreduce_benchmark/run_benchmark.py` to record live MapReduce vs. Vectorized timings.")
    else:
        st.dataframe(bench_df, width='stretch', hide_index=True)
        b_chart = (
            alt.Chart(bench_df)
            .mark_bar()
            .encode(
                x=alt.X("engine:N", title="Processing Engine"),
                y=alt.Y("throughput_rec_per_sec:Q", title="Throughput (records / sec)"),
                color="engine:N",
                tooltip=["engine", "execution_seconds", "throughput_rec_per_sec"],
            )
            .properties(height=280)
        )
        st.altair_chart(b_chart, width='stretch')

