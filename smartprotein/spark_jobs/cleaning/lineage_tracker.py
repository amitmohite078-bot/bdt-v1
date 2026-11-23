"""SmartProtein Stage 7 (Module #6): Data Lineage Tracking.

Records end-to-end provenance (raw_source_file -> rule_applied -> target_zone -> target_record_id)
matching the smartprotein_analytics.dq_lineage Hive table schema.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT_DIR / "data"


def main() -> None:
    staging_fc = pd.read_parquet(DATA_DIR / "staging" / "food_composition_staged.snappy.parquet")
    scorecard = pd.read_parquet(DATA_DIR / "analytics" / "dq_scorecards.snappy.parquet")
    batch_id = str(scorecard["batch_id"].iloc[0]) if not scorecard.empty else f"BATCH-{uuid.uuid4().hex[:8].upper()}"
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build row-level lineage across passed, imputed, and quarantined records
    rows = []
    for idx, r in staging_fc.iterrows():
        raw_id = str(r["record_id"])
        rej = str(r["reject_reason_code"])
        if rej != "":
            action = "QUARANTINED"
            rule = rej
            zone = "/smartprotein/rejected/all_records"
            target_id = raw_id
        elif bool(r.get("was_imputed", False)):
            action = "IMPUTED_AND_RECONCILED"
            rule = "CAT_MEDIAN_IMPUTATION + TRUST_RECONCILIATION"
            zone = "/smartprotein/curated/master_protein_foods"
            target_id = "CF-" + hashlib.sha256(str(r["canonical_name_exact"]).encode("utf-8")).hexdigest()[:8]
        else:
            action = "PASSED_AND_RECONCILED"
            rule = "ATWATER_VALID + TRUST_RECONCILIATION"
            zone = "/smartprotein/curated/master_protein_foods"
            target_id = "CF-" + hashlib.sha256(str(r["canonical_name_exact"]).encode("utf-8")).hexdigest()[:8]

        rows.append(
            {
                "lineage_id": f"LIN-{idx + 1:08d}",
                "batch_id": batch_id,
                "raw_source_file": f"raw/food_composition/{r['source_db']}",
                "raw_record_id": raw_id,
                "rule_applied": rule,
                "action_taken": action,
                "target_zone": zone,
                "target_record_id": target_id,
                "processed_at": now_iso,
            }
        )

    lineage_df = pd.DataFrame(rows)
    out_path = DATA_DIR / "analytics" / "dq_lineage.snappy.parquet"
    lineage_df.to_parquet(out_path, compression="snappy", index=False)

    print(f"[Lineage Tracker] Wrote {len(lineage_df):,} provenance records -> {out_path}")
    print(lineage_df["action_taken"].value_counts().to_string())


if __name__ == "__main__":
    main()

