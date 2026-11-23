"""SmartProtein Stage 7 (Modules #7, #8, #11): Self-Healing Rules, ML Anomaly Detection & Crowdsourcing.

1. Self-Healing Rules (Module #7): Automatically learns p01-p99 nutrition bounds per category
   from validated history and writes config/self_healing_thresholds.json.
2. ML Anomaly Detection (Module #8): Uses scikit-learn IsolationForest + KMeans distance on
   (p_100g, c_100g, f_100g, kcal_100g, atwater_ratio) and evaluates Precision / Recall.
3. Crowdsourced Corrections (Module #11): Aggregates user flags via trust-weighted voting.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
STAGING_PATH = ROOT_DIR / "data" / "staging" / "food_composition_staged.snappy.parquet"
CROWD_FLAGS_PATH = ROOT_DIR / "data" / "raw" / "crowd_flags" / "crowd_flags.csv"
SELF_HEAL_OUT = ROOT_DIR / "config" / "self_healing_thresholds.json"
ANOMALY_OUT = ROOT_DIR / "data" / "analytics" / "ml_anomaly_metrics.snappy.parquet"
CROWD_OUT = ROOT_DIR / "data" / "analytics" / "crowd_approved_corrections.snappy.parquet"


def learn_self_healing_thresholds(staged: pd.DataFrame) -> dict:
    """Learn empirical 1st-99th percentile bounds per category from non-rejected rows."""
    clean = staged[staged["reject_reason_code"] == ""].copy()
    learned: dict[str, dict] = {
        "learned_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "categories": {},
    }
    for cat, grp in clean.groupby("category"):
        learned["categories"][str(cat)] = {
            "sample_size": int(len(grp)),
            "protein_p01": round(float(grp["p_100g"].quantile(0.01)), 2),
            "protein_p99": round(float(grp["p_100g"].quantile(0.99)), 2),
            "kcal_p01": round(float(grp["kcal_100g"].quantile(0.01)), 1),
            "kcal_p99": round(float(grp["kcal_100g"].quantile(0.99)), 1),
            "protein_median": round(float(grp["p_100g"].median()), 2),
        }
    SELF_HEAL_OUT.write_text(json.dumps(learned, indent=2), encoding="utf-8")
    return learned


def run_ml_anomaly_detection(staged: pd.DataFrame) -> pd.DataFrame:
    """Detect suspicious multivariate macro combinations via Isolation Forest & K-Means."""
    df = staged.dropna(subset=["p_100g", "c_100g", "f_100g", "kcal_100g"]).copy()
    df["atwater_ratio"] = np.where(df["atwater_kcal"] > 1.0, df["kcal_100g"] / df["atwater_kcal"], 1.0)
    features = ["p_100g", "c_100g", "f_100g", "kcal_100g", "atwater_ratio"]

    X = StandardScaler().fit_transform(df[features].to_numpy())

    # 1. Isolation Forest (trained in unsupervised mode matching approximate fault rate)
    iso = IsolationForest(n_estimators=120, contamination=0.12, random_state=42)
    iso_pred = iso.fit_predict(X)  # -1 = anomaly, 1 = normal
    df["iso_anomaly"] = iso_pred == -1

    # 2. K-Means distance to nearest category/macro centroid (> 92nd percentile = suspicious)
    km = KMeans(n_clusters=6, n_init=10, random_state=42)
    dists = km.fit_transform(X).min(axis=1)
    dist_thresh = float(np.quantile(dists, 0.90))
    df["kmeans_anomaly"] = dists > dist_thresh

    df["ml_flagged"] = df["iso_anomaly"] | df["kmeans_anomaly"]

    # Ground-truth comparison against numerical rule faults (impossible macro, Atwater mismatch, IQR outlier)
    true_numeric_fault = df["reject_reason_code"].isin(
        ["ERR_IMPOSSIBLE_MACRO", "ERR_ATWATER_MISMATCH", "ERR_IQR_OUTLIER"]
    )
    tp = int((df["ml_flagged"] & true_numeric_fault).sum())
    fp = int((df["ml_flagged"] & ~true_numeric_fault).sum())
    fn = int((~df["ml_flagged"] & true_numeric_fault).sum())

    precision = round(tp / max(tp + fp, 1), 4)
    recall = round(tp / max(tp + fn, 1), 4)
    f1 = round((2 * precision * recall) / max(precision + recall, 1e-6), 4)

    metrics_df = pd.DataFrame(
        [
            {
                "model": "IsolationForest + KMeans Ensemble",
                "evaluated_rows": len(df),
                "true_numeric_anomalies": int(true_numeric_fault.sum()),
                "ml_flagged_count": int(df["ml_flagged"].sum()),
                "true_positives": tp,
                "false_positives": fp,
                "false_negatives": fn,
                "precision": precision,
                "recall": recall,
                "f1_score": f1,
            }
        ]
    )
    ANOMALY_OUT.parent.mkdir(parents=True, exist_ok=True)
    metrics_df.to_parquet(ANOMALY_OUT, compression="snappy", index=False)
    return metrics_df


def run_crowdsourced_voting() -> pd.DataFrame:
    """Aggregate user correction flags via trust-weighted voting."""
    if not CROWD_FLAGS_PATH.exists():
        return pd.DataFrame()
    flags = pd.read_csv(CROWD_FLAGS_PATH)
    voted = (
        flags.groupby(["record_id", "field_name", "suggested_value"], as_index=False)
        .agg(
            vote_count=("flag_id", "count"),
            weighted_trust_sum=("user_trust_score", "sum"),
        )
    )
    # Require >= 1.2 cumulative trust weight to trigger a curated correction review
    approved = voted[voted["weighted_trust_sum"] >= 1.2].sort_values(
        by="weighted_trust_sum", ascending=False
    )
    CROWD_OUT.parent.mkdir(parents=True, exist_ok=True)
    approved.to_parquet(CROWD_OUT, compression="snappy", index=False)
    return approved


def main() -> None:
    if not STAGING_PATH.exists():
        raise SystemExit("Run Stage 6 run_stage6_local.py first.")

    staged = pd.read_parquet(STAGING_PATH)
    learned = learn_self_healing_thresholds(staged)
    print(f"[Self-Healing Rules] Learned empirical bounds for {len(learned['categories'])} categories -> {SELF_HEAL_OUT}")

    metrics = run_ml_anomaly_detection(staged)
    print("\n[ML Anomaly Detection (Isolation Forest + KMeans)]:")
    print(metrics.to_string(index=False))

    approved_flags = run_crowdsourced_voting()
    print(f"\n[Crowdsourced Trust Voting] Approved correction candidates (trust >= 1.2): {len(approved_flags)}")


if __name__ == "__main__":
    main()

