# FitGuard-HDFS: Big Data Quality, Cleaning & Personalized Protein Pipeline on Hadoop HDFS

**FitGuard-HDFS** is an advanced Big Data Technologies (BDT) project implementing a **4-Zone Medallion Lakehouse** (`Bronze → Silver + Quarantine → Gold`) on **Hadoop HDFS**. It ingests messy multi-modal wearable telemetry, user fitness profiles, and regional vendor food catalogs, enforces a **6-Dimension Automated Data Quality (DQ) Engine**, quarantines dangerous/corrupt anomalies with lineage error codes, compacts small files into **Snappy Parquet**, and powers a **Personalized Protein Food Recommendation Engine** filtered by **Dietary Choice, Cost Budget, and City Location**.

---

## Architecture Overview

```text
  [Raw Sources]
   ├─ wearable_telemetry/*.jsonl  (57k+ IoT small JSONL files)
   ├─ user_profiles.csv           (diet choice, budget, city, weight, goal)
   └─ protein_foods_catalog.csv   (regional prices, units, macros, vendor tags)
          │
          ▼
  ┌──────────────────────────────────────────────────────────┐
  │  BRONZE ZONE  (/fitguard/bronze/)                        │  Raw Immutable Landing
  └──────────────────────────────────────────────────────────┘
          │  PySpark / Local DQ Engine (6 DQ Dimensions)
          ├──────────────────────────────────────────────────┐
          ▼                                                  ▼
  ┌────────────────────────────────────────┐   ┌───────────────────────────────┐
  │  SILVER ZONE (/fitguard/silver/)       │   │  QUARANTINE (/fitguard/...)   │
  │  • Deduplicated (Window / Composite)   │   │  • Dead-Letter Parquet        │
  │  • Unit & Currency Norm (per_100g_inr) │   │  • Rule ID + Error Reason     │
  │  • City & Diet Canonicalized           │   │  • e.g. DQ-SEM-001:           │
  │  • Smart HR Imputation (is_imputed)    │   │    "Chicken tagged Vegan!"    │
  │  • Compacted Snappy Parquet            │   └───────────────────────────────┘
  └────────────────────────────────────────┘
          │
          ▼
  ┌──────────────────────────────────────────────────────────┐
  │  GOLD ZONE (/fitguard/gold/)                             │
  │  • Activity-Adjusted Daily Protein Targets               │
  │  • Top-5 Protein Foods per User (Choice + Cost + City)   │
  │  • 6-Dimension DQ Scorecard & Storage Optimization Stats │
  └──────────────────────────────────────────────────────────┘
          │
          ├─► Apache Hive External Tables (hive/create_external_tables.hql)
          └─► Interactive Streamlit Dashboard (dashboard/app.py)
```

---

## Key Data Quality Rules Implemented

| Rule ID | Dimension | Target | Description |
| :--- | :--- | :--- | :--- |
| `DQ-SEM-001` | **Semantic Integrity** | Protein Foods | **Semantic Diet-Tag Guard:** Blocks mislabeled listings (e.g., Chicken/Fish tagged `Vegan` or `Veg`, Paneer tagged `Vegan`) before they poison recommendations. |
| `DQ-XSIG-001` | **Semantic Integrity** | Wearables | **Cross-Signal Contradiction:** Flags `steps > 300` logged simultaneously with `sleep_stage = DEEP_SLEEP`. |
| `DQ-PHY-001` | **Validity** | Wearables | Physiological bounds (`32 <= heart_rate <= 220`, `70 <= SpO2 <= 100`, `0 <= steps <= 18000`). |
| `DQ-PHY-002` | **Validity** | Protein Foods | Nutritional thermodynamics (`protein_per_100g <= 95g`, `calories >= 3.6 * protein`). |
| `DQ-VAL-002` | **Validity** | Foods & Users | Rejects negative/outlier costs (`cost <= 0` or `> ₹2500/100g`) and invalid daily budgets. |
| `DQ-LOC-001` | **Consistency** | Foods & Users | Canonicalizes city variants (`blr`, `Bombay`, `ncr` → `Bengaluru`, `Mumbai`, `Delhi`) and quarantines unresolvable cities (`Atlantis`). |
| `DQ-UNQ-001` | **Uniqueness** | All 3 Sources | Removes multi-device wearable sync duplicates and duplicate vendor food listings. |
| `DQ-TIM-001` | **Timeliness** | Wearables & Foods | Blocks future-dated timestamps (`2032`) and stale price listings (`> 60 days`). |
| `DQ-SCH-001` | **Validity** | Wearables | Captures corrupt/unparseable JSON lines in Quarantine instead of crashing the batch job. |
| *Smart Imputation* | **Completeness** | Wearables | Isolated missing `heart_rate_bpm` dropouts are imputed via user median and tracked with `is_imputed = true`. |

---

## Quick Start (Local Windows Runner — Works Immediately)

1. **Generate Dirty Multi-Modal Datasets (Bronze):**
   ```powershell
   python data_generator/generate_dirty_datasets.py
   ```
2. **Run Medallion Data Quality & Protein Recommendation Pipeline:**
   ```powershell
   python local_runner/run_local_pipeline.py
   ```
3. **Run Hadoop Streaming MapReduce vs. Vectorized Benchmark:**
   ```powershell
   python mapreduce_benchmark/run_benchmark.py
   ```
4. **Launch Interactive Dashboard:**
   ```powershell
   streamlit run dashboard/app.py
   ```

---

## Running on a Real Hadoop HDFS + Spark Cluster (Docker)

If Docker Desktop is installed:
```bash
docker compose up -d
bash spark_jobs/hdfs_upload.sh
```
This launches real **Hadoop NameNode (`:9870`)**, **DataNode**, **Spark Master (`:8080`)**, and **Spark Worker**, uploads raw Bronze files into `hdfs://namenode:9000/fitguard/bronze/`, and executes `spark_jobs/spark_medallion_pipeline.py`. Expose tables in Hive using `hive/create_external_tables.hql`.

